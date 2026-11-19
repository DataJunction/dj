import asyncio

from datajunction_server.models.deployment import (
    ColumnSpec,
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
