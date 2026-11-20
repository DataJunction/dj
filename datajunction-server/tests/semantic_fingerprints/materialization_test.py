"""Materialization reuse requires matching semantic and planner storage evidence."""

import hashlib

from datajunction_server.models.cube_materialization import (
    CombineMaterialization,
    DruidCubeConfig,
    DruidCubeV3Config,
    MeasuresMaterialization,
    PreAggTableInfo,
)
from datajunction_server.models.decompose import (
    Aggregability,
    AggregationRule,
    MetricComponent,
)
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.partition import Granularity
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.models.semantic_fingerprint import SemanticFingerprint
from datajunction_server.semantic_fingerprints.materialization import (
    MATERIALIZATION_CONTRACT_VERSION,
    MATERIALIZATION_PLANNER_EPOCH,
    MaterializationContract,
    MaterializationFingerprint,
    materialization_artifact_matches,
    materialization_contract,
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _semantic(digest: str) -> SemanticFingerprint:
    return SemanticFingerprint(version=2, digest=_digest(digest))


def _storage(digest: str = "layout") -> MaterializationContract:
    return MaterializationContract(
        version=MATERIALIZATION_CONTRACT_VERSION,
        planner_epoch=MATERIALIZATION_PLANNER_EPOCH,
        digest=_digest(digest),
    )


def _contract_config(grain: list[str], version: str = "1") -> DruidCubeConfig:
    components = [
        MetricComponent(
            name="every_flag",
            expression="flag",
            aggregation=None,
            rule=AggregationRule(type=Aggregability.NONE),
        ),
    ]
    columns = [
        ColumnMetadata(
            name=name,
            type="boolean" if name == "flag" else "string",
            column=name,
            node="ns.fact",
            semantic_entity=f"ns.fact.{name}",
            semantic_type="dimension",
        )
        for name in grain
    ]
    stage = MeasuresMaterialization(
        node=NodeNameVersion(name="ns.fact", version=version),
        grain=grain,
        dimensions=grain,
        measures=components,
        query="SELECT planner generated SQL",
        columns=columns,
        timestamp_column=None,
        timestamp_format=None,
        granularity=None,
        spark_conf={"executor.memory": "4g"},
        upstream_tables=[f"input_{version}"],
    )
    combiner = CombineMaterialization(
        node=NodeNameVersion(name="ns.cube", version=version),
        query="SELECT combiner SQL",
        columns=columns,
        grain=grain,
        dimensions=grain,
        measures=components,
    )
    return DruidCubeConfig(
        cube=NodeNameVersion(name="ns.cube", version=version),
        dimensions=grain,
        metrics=[],
        measures_materializations=[stage],
        combiners=[combiner],
        lookback_window="1 DAY",
    )


def _v3_contract_config() -> DruidCubeV3Config:
    component = MetricComponent(
        name="sum_value",
        expression="value",
        aggregation="SUM",
        merge="SUM",
        rule=AggregationRule(type=Aggregability.FULL),
    )
    return DruidCubeV3Config(
        druid_datasource="ns_cube",
        preagg_tables=[
            PreAggTableInfo(
                table_ref="catalog.schema.preagg_1",
                parent_node="ns.fact",
                grain=["ns.fact.id"],
            ),
        ],
        combined_sql="SELECT value FROM preagg",
        combined_columns=[
            ColumnMetadata(
                name="value",
                type="double",
                semantic_entity="ns.metric",
                semantic_type="metric_component",
            ),
        ],
        combined_grain=["ns.date.day"],
        measure_components=[component],
        component_aliases={"sum_value": "value"},
        cube_metrics=["ns.metric"],
        metrics=[],
        timestamp_column="day",
        timestamp_format="yyyyMMdd",
        druid_spec={
            "dataSchema": {
                "dataSource": "ns_cube",
                "parser": {"parseSpec": {"timestampSpec": {"column": "day"}}},
                "metricsSpec": [{"type": "doubleSum", "name": "value"}],
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "DAY",
                    "queryGranularity": "NONE",
                    "intervals": [],
                },
            },
        },
    )


def test_storage_contract_uses_emitted_layout_and_ignores_generation_metadata():
    before = materialization_contract(_contract_config(["flag"], "1"))
    after = materialization_contract(_contract_config(["flag"], "2"))

    assert before is not None
    assert after == before
    assert before.version == MATERIALIZATION_CONTRACT_VERSION
    assert type(before).from_value(before.to_dict()) == before
    assert type(before).from_value({"version": 0}) is None
    assert type(before).from_value({**before.to_dict(), "future": 1}) is None

    first = _contract_config(["flag"])
    second = _contract_config(["other"])
    first.measures_materializations.extend(second.measures_materializations)
    first.combiners.extend(second.combiners)
    reversed_stages = first.model_copy(deep=True)
    reversed_stages.measures_materializations.reverse()
    reversed_stages.combiners.reverse()
    for stage in reversed_stages.measures_materializations + reversed_stages.combiners:
        stage.columns.reverse()
        stage.measures.reverse()
    assert materialization_contract(first) == materialization_contract(reversed_stages)

    duplicate_column = _contract_config(["flag"])
    duplicate_column.measures_materializations[0].columns.append(
        duplicate_column.measures_materializations[0]
        .columns[0]
        .model_copy(
            update={"semantic_entity": "ns.other.flag"},
        ),
    )
    assert materialization_contract(duplicate_column) is None


def test_storage_contract_changes_with_grain_or_aggregate_state():
    before = materialization_contract(_contract_config(["flag"]))
    changed_grain = materialization_contract(
        _contract_config(["flag", "unused"]),
    )

    assert before is not None
    assert changed_grain is not None
    assert before.digest != changed_grain.digest

    changed_component = _contract_config(["flag"])
    changed_component.measures_materializations[0].measures[
        0
    ].serialize = "CAST({} AS BINARY)"
    changed_component.measures_materializations[0].measures[0].serialize_type = "binary"
    changed_state = materialization_contract(changed_component)
    assert changed_state is not None
    assert before.digest != changed_state.digest

    changed_component.combiners[0].druid_overrides = {"rollup": False}
    assert materialization_contract(changed_component) is None

    changed_component = _contract_config(["flag"])
    changed_component.druid = {"transformSpec": {"transforms": []}}
    assert materialization_contract(changed_component) is None


def test_v3_contract_covers_final_cube_aliases_but_not_preagg_table_names():
    before = materialization_contract(_v3_contract_config())
    changed_preagg = _v3_contract_config().model_copy(
        update={
            "preagg_tables": [
                PreAggTableInfo(
                    table_ref="catalog.schema.preagg_2",
                    parent_node="ns.fact",
                    grain=["ns.fact.other_id"],
                ),
            ],
        },
        deep=True,
    )
    changed_alias = _v3_contract_config().model_copy(
        update={"component_aliases": {"sum_value": "renamed_value"}},
        deep=True,
    )
    changed_sink = _v3_contract_config().model_copy(
        update={"druid_datasource": "another_cube"},
        deep=True,
    )
    current_config = _v3_contract_config()
    changed_segment = current_config.model_copy(
        update={
            "druid_spec": {
                "dataSchema": {
                    **current_config.druid_spec["dataSchema"],
                    "granularitySpec": {
                        **current_config.druid_spec["dataSchema"]["granularitySpec"],
                        "segmentGranularity": "MONTH",
                    },
                },
            },
        },
        deep=True,
    )

    preagg_contract = materialization_contract(changed_preagg)
    alias_contract = materialization_contract(changed_alias)
    sink_contract = materialization_contract(changed_sink)
    segment_contract = materialization_contract(changed_segment)
    assert before is not None
    assert preagg_contract == before
    assert sink_contract == before
    assert alias_contract is not None
    assert alias_contract.digest != before.digest
    assert segment_contract is not None
    assert segment_contract.digest != before.digest


def test_v3_contract_fails_closed_without_generated_druid_schema():
    config = _v3_contract_config().model_copy(update={"druid_spec": None})
    assert materialization_contract(config) is None


def test_callback_artifact_must_match_a_persisted_druid_target():
    config = _contract_config(["flag"])
    config.combiners[0].timestamp_column = "flag"
    config.combiners[0].granularity = Granularity.DAY
    fused = config.model_dump()
    fused_table = fused["combiners"][0]["druid_spec"]["dataSchema"]["dataSource"]
    planner = {"version": "v3", "druid_datasource": "cube_output"}

    assert materialization_artifact_matches(fused, fused_table)
    assert materialization_artifact_matches(planner, "cube_output")
    assert not materialization_artifact_matches(fused, "other_output")
    assert not materialization_artifact_matches(planner, "other_output")
    assert not materialization_artifact_matches({}, "cube_output")
    assert not materialization_artifact_matches(
        {"combiners": [{"druid_spec": {}}]},
        "cube_output",
    )


def test_storage_contract_preserves_aggregation_and_merge_text():
    before_config = _contract_config(["flag"])
    after_config = before_config.model_copy(deep=True)
    before_component = before_config.measures_materializations[0].measures[0]
    after_component = after_config.measures_materializations[0].measures[0]
    before_component.aggregation = "SUM(CASE WHEN status = 'A' THEN {} END)"
    after_component.aggregation = "SUM(CASE WHEN status = 'a' THEN {} END)"
    before_component.merge = "merge_fn('A')"
    after_component.merge = "merge_fn('a')"

    before = materialization_contract(before_config)
    after = materialization_contract(after_config)
    assert before is not None
    assert after is not None
    assert before.digest != after.digest


def test_unknown_types_and_old_contract_versions_fail_closed():
    config = _contract_config(["flag"])
    config.measures_materializations[0].columns[0].type = "made_up_type"
    assert materialization_contract(config) is None

    old_contract = MaterializationFingerprint.from_value(
        {
            "semantic": _semantic("same").model_dump(mode="json"),
            "storage": {
                "version": MATERIALIZATION_CONTRACT_VERSION - 1,
                "planner_epoch": MATERIALIZATION_PLANNER_EPOCH,
                "digest": _digest("layout"),
            },
        },
    )
    assert old_contract is None


def test_manifest_roundtrip_rejects_unknown_or_malformed_values():
    storage = _storage()
    fingerprint = MaterializationFingerprint(
        semantic=_semantic("same"), storage=storage
    )
    assert MaterializationFingerprint.from_value(fingerprint.to_dict()) == fingerprint
    assert (
        MaterializationFingerprint.from_value(
            {
                "semantic": {"version": 1, "digest": _digest("same")},
                "storage": storage.to_dict(),
            },
        )
        is None
    )
    assert (
        MaterializationFingerprint.from_value(
            {**fingerprint.to_dict(), "future": 1},
        )
        is None
    )
    assert (
        MaterializationFingerprint.from_value(
            {"semantic": fingerprint.to_dict()["semantic"], "storage": None},
        )
        is None
    )

    old_epoch = MaterializationFingerprint.from_value(
        {
            "semantic": _semantic("same").model_dump(mode="json"),
            "storage": {
                "version": MATERIALIZATION_CONTRACT_VERSION,
                "planner_epoch": MATERIALIZATION_PLANNER_EPOCH - 1,
                "digest": _digest("layout"),
            },
        },
    )
    assert old_epoch is None
