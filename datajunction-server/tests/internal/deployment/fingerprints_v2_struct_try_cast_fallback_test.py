"""Struct fields and safe TRY_CAST joins retain conservative fallback coverage."""

import asyncio

from datajunction_server.models.deployment import (
    ColumnSpec,
    DimensionSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.semantic_fingerprints.v2 import (
    SemanticGraphSnapshot,
    compare_components,
    compute_v2_fingerprints,
)


def _source(name, table, columns):
    return SourceSpec(
        name=name,
        catalog="warehouse",
        schema_="sales",
        table=table,
        columns=[ColumnSpec(name=column, type=kind) for column, kind in columns],
    )


def _evaluate(*specs):
    snapshot = SemanticGraphSnapshot(
        {spec.rendered_name: spec for spec in specs},
    )
    return asyncio.run(compute_v2_fingerprints(snapshot))


def test_alias_qualified_struct_fields_are_bound_to_the_input_schema():
    profile = _source(
        "warehouse.raw.profile_sessions",
        "profile_sessions",
        [
            ("profile_session_id", "string"),
            (
                "device_data",
                "struct<device_type_id:int,device_model:string,app_version:string>",
            ),
        ],
    )
    device_source = _source(
        "ns.device_model_rows",
        "device_model_rows",
        [
            ("device_type_id", "int"),
            ("device_model", "string"),
            ("hw_major_category", "string"),
        ],
    )
    device = DimensionSpec(
        name="ns.device_model_rollup_d",
        query=(
            "SELECT device_type_id, device_model, hw_major_category "
            "FROM ns.device_model_rows"
        ),
        columns=[
            ColumnSpec(name="device_type_id", type="int"),
            ColumnSpec(name="device_model", type="string"),
            ColumnSpec(name="hw_major_category", type="string"),
        ],
        primary_key=["device_type_id", "device_model"],
    )
    mobile = TransformSpec(
        name="ns.mobile_app_profile_session",
        query=(
            "SELECT s.device_data.device_type_id AS device_type_id, "
            "s.device_data.device_model AS device_model, "
            "s.device_data.app_version AS app_version "
            "FROM warehouse.raw.profile_sessions AS s "
            "JOIN ns.device_model_rollup_d AS d "
            "ON s.device_data.device_type_id = d.device_type_id "
            "AND s.device_data.device_model = d.device_model "
            "WHERE d.hw_major_category IN ('Mobile', 'Tablet')"
        ),
    )

    result = _evaluate(profile, device_source, device, mobile)

    assert result.nodes["ns.mobile_app_profile_session"].precision == "conservative"
    assert result.nodes["ns.mobile_app_profile_session"].full.digest is not None


def test_unqualified_struct_paths_are_bound_when_one_input_schema_matches():
    playback = _source(
        "warehouse.raw.playback_allocations",
        "playback_allocations",
        [
            (
                "viewing_secs",
                "struct<wall_clock:struct<content:int,ads:int>,media_clock:struct<content:int>>",
            ),
            ("is_tester_playback", "boolean"),
        ],
    )
    transform = TransformSpec(
        name="ns.playback_non_supplemental",
        query=(
            "SELECT viewing_secs.wall_clock.content AS view_secs, "
            "IF(viewing_secs.wall_clock.content >= 360, 1, 0) AS qualified "
            "FROM warehouse.raw.playback_allocations "
            "WHERE is_tester_playback = false"
        ),
    )

    result = _evaluate(playback, transform)

    assert result.nodes["ns.playback_non_supplemental"].precision == "conservative"
    assert result.nodes["ns.playback_non_supplemental"].full.digest is not None


def test_try_cast_filter_join_fallback_is_conservative_and_tracks_parents():
    fact = _source(
        "ns.fact",
        "fact",
        [("id_text", "string"), ("amount", "double")],
    )
    lookup = _source("ns.lookup", "lookup", [("id", "bigint")])
    transform = TransformSpec(
        name="ns.try_cast_filter",
        query=(
            "SELECT COUNT(*) FILTER (WHERE f.amount > 0) AS row_count "
            "FROM ns.fact f JOIN ns.lookup l "
            "ON l.id = TRY_CAST(f.id_text AS BIGINT)"
        ),
    )

    result = _evaluate(fact, lookup, transform)

    changed = _evaluate(
        fact.model_copy(update={"table": "fact_after"}),
        lookup,
        transform,
    )

    assert result.nodes["ns.try_cast_filter"].precision == "conservative"
    assert result.nodes["ns.try_cast_filter"].full.digest is not None
    assert (
        compare_components(
            result.nodes["ns.try_cast_filter"].full,
            changed.nodes["ns.try_cast_filter"].full,
        )
        == "changed"
    )


def test_try_cast_fallback_stays_unknown_when_a_relation_is_unresolved():
    fact = _source(
        "ns.fact",
        "fact",
        [("id_text", "string"), ("amount", "double")],
    )
    transform = TransformSpec(
        name="ns.try_cast_missing_relation",
        query=(
            "SELECT SUM(f.amount) FROM ns.fact f JOIN ns.missing m "
            "ON m.id = TRY_CAST(f.id_text AS BIGINT)"
        ),
    )

    result = _evaluate(fact, transform)

    assert result.nodes["ns.try_cast_missing_relation"].precision == "unknown"
