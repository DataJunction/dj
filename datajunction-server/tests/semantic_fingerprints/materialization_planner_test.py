"""Planner integration for semantic and materialization fingerprints."""

import pytest

from datajunction_server.construction.build_v3.builder import build_measures_sql
from datajunction_server.construction.build_v3.types import snapshot_node_versions
from datajunction_server.internal.deployment.fingerprints import (
    build_deployment_fingerprints,
)
from datajunction_server.models.cube_materialization import (
    CombineMaterialization,
    DruidCubeConfig,
    MeasuresMaterialization,
)
from datajunction_server.models.deployment import CubeSpec
from datajunction_server.models.materialization import MaterializationTarget
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.models.semantic_fingerprint import SemanticFingerprint
from datajunction_server.semantic_fingerprints.materialization import (
    materialization_contract,
)


async def _plan(session):
    return await build_measures_sql(
        session=session,
        metrics=["v3.fingerprint_every_flag"],
        dimensions=[],
        use_materialized=False,
        materialization_target=MaterializationTarget.DRUID,
    )


def _config_from_plan(result) -> DruidCubeConfig:
    group = result.grain_groups[0]
    columns = [
        ColumnMetadata(
            name=column.name,
            type=column.type,
            column=column.name,
            node=group.parent_name,
            semantic_entity=column.semantic_name,
            semantic_type=column.semantic_type,
        )
        for column in group.columns
    ]
    dimensions = [
        column.name for column in group.columns if column.semantic_type == "dimension"
    ]
    stage = MeasuresMaterialization(
        node=NodeNameVersion(name=group.parent_name, version="1"),
        grain=group.grain,
        dimensions=dimensions,
        measures=group.components,
        query=group.sql,
        columns=[column.model_dump() for column in columns],
        timestamp_column=None,
        timestamp_format=None,
        granularity=None,
        spark_conf=None,
        upstream_tables=[],
    )
    combiner = CombineMaterialization(
        node=NodeNameVersion(name="v3.fingerprint_cube", version="1"),
        columns=[column.model_dump() for column in columns],
        grain=group.grain,
        dimensions=dimensions,
        measures=group.components,
    )
    return DruidCubeConfig(
        cube=NodeNameVersion(name="v3.fingerprint_cube", version="1"),
        planned_node_versions=snapshot_node_versions(result.ctx.nodes),
        dimensions=[],
        metrics=[],
        measures_materializations=[stage],
        combiners=[combiner],
    )


async def _cube_semantic_digest(session):
    cube = CubeSpec(
        name="v3.fingerprint_cube",
        metrics=["v3.fingerprint_every_flag"],
        dimensions=[],
    )
    _, proposed = await build_deployment_fingerprints(
        session,
        {},
        [cube],
        [],
        version=2,
    )
    fingerprint = proposed[cube.name]
    assert isinstance(fingerprint, SemanticFingerprint)
    return fingerprint.digest


@pytest.mark.asyncio
async def test_every_without_primary_key_tracks_unused_parent_columns_in_storage_only(
    client_with_build_v3,
    session,
):
    response = await client_with_build_v3.post(
        "/nodes/transform/",
        json={
            "name": "v3.fingerprint_fact",
            "mode": "published",
            "query": ("SELECT status = 'completed' AS flag FROM v3.src_orders"),
        },
    )
    assert response.status_code in (200, 201), response.text
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.fingerprint_every_flag",
            "mode": "published",
            "query": "SELECT EVERY(flag) FROM v3.fingerprint_fact",
        },
    )
    assert response.status_code in (200, 201), response.text

    before_plan = await _plan(session)
    before_config = _config_from_plan(before_plan)
    assert before_config.planned_node_versions["v3.fingerprint_every_flag"]
    assert before_config.planned_node_versions["v3.fingerprint_fact"]
    before_storage = materialization_contract(before_config)
    before_semantic = await _cube_semantic_digest(session)
    assert before_storage is not None
    before_grain = before_config.measures_materializations[0].grain

    response = await client_with_build_v3.patch(
        "/nodes/v3.fingerprint_fact/",
        json={
            "query": (
                "SELECT status = 'completed' AS flag, customer_id AS unused "
                "FROM v3.src_orders"
            ),
        },
    )
    assert response.status_code == 200, response.text
    session.expire_all()

    after_plan = await _plan(session)
    after_config = _config_from_plan(after_plan)
    assert after_config.planned_node_versions["v3.fingerprint_fact"]
    after_storage = materialization_contract(after_config)
    after_semantic = await _cube_semantic_digest(session)
    assert after_storage is not None

    assert "unused" not in before_grain
    assert "unused" in after_config.measures_materializations[0].grain
    assert before_semantic == after_semantic
    assert before_storage.digest != after_storage.digest
