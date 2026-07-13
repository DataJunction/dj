"""Tests for upsert_schema_specs — namespace-scoped schema registration via deploy."""

import pytest
from sqlalchemy import select, text
from unittest.mock import MagicMock

from datajunction_server.database.custom_metadata_schema import CustomMetadataSchema
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.internal.custom_metadata import upsert_schema_specs
from datajunction_server.internal.deployment.orchestrator import DeploymentOrchestrator
from datajunction_server.internal.deployment.utils import DeploymentContext
from datajunction_server.models.deployment import (
    CustomMetadataSchemaSpec,
    DeploymentSpec,
)
from datajunction_server.models.node_type import NodeType


@pytest.mark.asyncio
async def test_deploy_registers_namespace_scoped_schema(session):
    """Insert path: a new schema spec creates a namespace-scoped row."""
    await upsert_schema_specs(
        session,
        namespace="finance",
        specs=[
            CustomMetadataSchemaSpec(key="grain", json_schema={"type": "string"}),
        ],
    )
    row = (
        await session.execute(
            select(CustomMetadataSchema).where(CustomMetadataSchema.key == "grain"),
        )
    ).scalar_one()
    assert row.namespace == "finance"
    assert row.value_kind == "string"


@pytest.mark.asyncio
async def test_deploy_updates_existing_schema(session):
    """Update path: a pre-existing row is updated in-place, not duplicated."""
    # Pre-seed a row for the same key/namespace/node_type=None
    session.add(
        CustomMetadataSchema(
            key="grain",
            namespace="finance",
            json_schema={"type": "string"},
            value_kind="string",
            filterable=True,
        ),
    )
    await session.commit()

    # Call upsert again with a changed schema (now integer)
    await upsert_schema_specs(
        session,
        namespace="finance",
        specs=[
            CustomMetadataSchemaSpec(
                key="grain",
                json_schema={"type": "integer"},
                filterable=False,
                description="updated",
            ),
        ],
    )

    rows = (
        (
            await session.execute(
                select(CustomMetadataSchema).where(
                    CustomMetadataSchema.key == "grain",
                    CustomMetadataSchema.namespace == "finance",
                ),
            )
        )
        .scalars()
        .all()
    )
    # Should still be exactly one row (updated, not duplicated)
    assert len(rows) == 1
    assert rows[0].value_kind == "integer"
    assert rows[0].filterable is False
    assert rows[0].description == "updated"


@pytest.mark.asyncio
async def test_deploy_node_type_scoped_schema(session):
    """Node-type-scoped spec creates a row with node_type set."""
    await upsert_schema_specs(
        session,
        namespace="eng",
        specs=[
            CustomMetadataSchemaSpec(
                key="owner_team",
                node_type=NodeType.METRIC,
                json_schema={"type": "string"},
            ),
        ],
    )
    row = (
        await session.execute(
            select(CustomMetadataSchema).where(
                CustomMetadataSchema.key == "owner_team",
                CustomMetadataSchema.namespace == "eng",
            ),
        )
    ).scalar_one()
    assert row.node_type == NodeType.METRIC.value
    assert row.value_kind == "string"


@pytest.mark.asyncio
async def test_deploy_empty_specs_is_noop(session):
    """Calling upsert_schema_specs with no specs is a harmless no-op."""
    await upsert_schema_specs(session, namespace="empty_ns", specs=[])
    rows = (
        (
            await session.execute(
                select(CustomMetadataSchema).where(
                    CustomMetadataSchema.namespace == "empty_ns",
                ),
            )
        )
        .scalars()
        .all()
    )
    assert rows == []


@pytest.mark.asyncio
async def test_upsert_invalid_json_schema_raises(session):
    """upsert_schema_specs raises DJInvalidInputException for an invalid JSON Schema."""
    with pytest.raises(DJInvalidInputException) as exc_info:
        await upsert_schema_specs(
            session,
            namespace="validation_test",
            specs=[
                CustomMetadataSchemaSpec(
                    key="bad_schema_key",
                    json_schema={"type": "not-a-type"},
                ),
            ],
        )
    assert "bad_schema_key" in exc_info.value.message
    assert "Invalid JSON Schema" in exc_info.value.message


@pytest.mark.asyncio
async def test_upsert_filterable_numeric_builds_index(session):
    """upsert_schema_specs with a filterable numeric spec creates the expression index."""
    unique_key = "deploy_numeric_score_idx_test"
    await upsert_schema_specs(
        session,
        namespace="index_test_ns",
        specs=[
            CustomMetadataSchemaSpec(
                key=unique_key,
                json_schema={"type": "number"},
                filterable=True,
            ),
        ],
    )
    # Verify the index was created in pg_indexes
    result = await session.execute(
        text(
            "SELECT indexname FROM pg_indexes WHERE indexname = :idx",
        ),
        {"idx": f"ix_cm_{unique_key}"},
    )
    row = result.fetchone()
    assert row is not None, f"Expected index ix_cm_{unique_key} to exist in pg_indexes"


@pytest.mark.asyncio
async def test_orchestrator_setup_calls_upsert_schema_specs(session, current_user):
    """The deploy wiring: _setup_deployment_resources calls upsert_schema_specs
    when custom_metadata_schemas is non-empty, registering the row in the DB."""
    spec = DeploymentSpec(
        namespace="wiring_test",
        nodes=[],
        custom_metadata_schemas=[
            CustomMetadataSchemaSpec(key="region", json_schema={"type": "string"}),
        ],
    )
    mock_context = MagicMock(spec=DeploymentContext)
    mock_context.current_user = current_user
    orchestrator = DeploymentOrchestrator(
        deployment_spec=spec,
        deployment_id="wiring-test-id",
        session=session,
        context=mock_context,
    )
    await orchestrator._setup_deployment_resources()

    row = (
        await session.execute(
            select(CustomMetadataSchema).where(
                CustomMetadataSchema.key == "region",
                CustomMetadataSchema.namespace == "wiring_test",
            ),
        )
    ).scalar_one()
    assert row.value_kind == "string"
