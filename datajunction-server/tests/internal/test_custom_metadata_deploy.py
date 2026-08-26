"""Tests for upsert_schema_specs — namespace-scoped schema registration via deploy."""

import datetime

import pytest
from sqlalchemy import select, text
from unittest.mock import MagicMock

from datajunction_server.database.custom_metadata_schema import CustomMetadataSchema
from datajunction_server.errors import (
    DJAlreadyExistsException,
    DJInvalidInputException,
)
from datajunction_server.internal.custom_metadata import upsert_schema_specs
from datajunction_server.internal.deployment.orchestrator import DeploymentOrchestrator
from datajunction_server.internal.deployment.utils import DeploymentContext
from datajunction_server.models.deployment import (
    CustomMetadataSchemaSpec,
    DeploymentSpec,
)
from datajunction_server.models.node_type import NodeType


@pytest.mark.asyncio
async def test_deploy_registers_namespace_scoped_schema(session, current_user):
    """Insert path: a new schema spec creates a namespace-scoped row."""
    await upsert_schema_specs(
        session,
        namespace="finance",
        specs=[
            CustomMetadataSchemaSpec(key="grain", json_schema={"type": "string"}),
        ],
        current_user_id=current_user.id,
    )
    row = (
        await session.execute(
            select(CustomMetadataSchema).where(CustomMetadataSchema.key == "grain"),
        )
    ).scalar_one()
    assert row.namespace == "finance"
    assert row.value_kind == "string"


@pytest.mark.asyncio
async def test_deploy_updates_existing_schema(session, current_user):
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
        current_user_id=current_user.id,
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
async def test_deploy_node_type_scoped_schema(session, current_user):
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
        current_user_id=current_user.id,
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
async def test_deploy_empty_specs_on_an_empty_namespace(session, current_user):
    """An empty spec list against a namespace with no rows registers nothing.

    `[]` means "this manifest manages schemas and declares none", so it retires
    whatever the namespace had -- which here is nothing.
    """
    await upsert_schema_specs(
        session,
        namespace="empty_ns",
        specs=[],
        current_user_id=current_user.id,
    )
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
async def test_upsert_invalid_json_schema_raises(session, current_user):
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
            current_user_id=current_user.id,
        )
    assert "bad_schema_key" in exc_info.value.message
    assert "Invalid JSON Schema" in exc_info.value.message


@pytest.mark.asyncio
async def test_upsert_filterable_numeric_builds_index(session, current_user):
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
        current_user_id=current_user.id,
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


@pytest.mark.asyncio
async def test_a_retired_key_can_be_registered_again(session, current_user):
    """
    Soft-deleting a key must not make its scope permanently unusable.

    The unique index spans deactivated rows while every read hides them, so an
    insert beside a tombstone violates the constraint. The upsert revives the
    row instead, which also keeps its id and created_at: a key that comes back
    is the same registration, not a new one.
    """
    spec = CustomMetadataSchemaSpec(key="lifecycle", json_schema={"type": "string"})
    await upsert_schema_specs(
        session,
        namespace="revive_ns",
        specs=[spec],
        current_user_id=current_user.id,
    )
    original = (
        await session.execute(
            select(CustomMetadataSchema).where(
                CustomMetadataSchema.key == "lifecycle",
                CustomMetadataSchema.namespace == "revive_ns",
            ),
        )
    ).scalar_one()
    original_id, original_created = original.id, original.created_at

    original.deactivated_at = datetime.datetime.now(datetime.UTC)
    await session.commit()

    await upsert_schema_specs(
        session,
        namespace="revive_ns",
        specs=[spec],
        current_user_id=current_user.id,
    )
    revived = (
        (
            await session.execute(
                select(CustomMetadataSchema).where(
                    CustomMetadataSchema.key == "lifecycle",
                    CustomMetadataSchema.namespace == "revive_ns",
                ),
            )
        )
        .scalars()
        .all()
    )
    assert len(revived) == 1
    assert revived[0].id == original_id
    assert revived[0].created_at == original_created
    assert revived[0].deactivated_at is None


@pytest.mark.asyncio
async def test_a_key_the_manifest_drops_is_retired(session, current_user):
    """Reconciliation: the manifest is the whole truth for its own namespace."""
    await upsert_schema_specs(
        session,
        namespace="reconcile_ns",
        specs=[
            CustomMetadataSchemaSpec(key="keep", json_schema={"type": "string"}),
            CustomMetadataSchemaSpec(key="drop", json_schema={"type": "string"}),
        ],
        current_user_id=current_user.id,
    )
    await upsert_schema_specs(
        session,
        namespace="reconcile_ns",
        specs=[
            CustomMetadataSchemaSpec(key="keep", json_schema={"type": "string"}),
        ],
        current_user_id=current_user.id,
    )
    live = sorted(
        row.key
        for row in (
            await session.execute(
                select(CustomMetadataSchema).where(
                    CustomMetadataSchema.namespace == "reconcile_ns",
                    CustomMetadataSchema.deactivated_at.is_(None),
                ),
            )
        )
        .scalars()
        .all()
    )
    assert live == ["keep"]


@pytest.mark.asyncio
async def test_reconciliation_leaves_global_rows_alone(session, current_user):
    """No namespace owns a global key, so no deployment may retire one."""
    session.add(
        CustomMetadataSchema(
            key="global_key",
            namespace=None,
            json_schema={"type": "string"},
            value_kind="string",
        ),
    )
    await session.commit()

    await upsert_schema_specs(
        session,
        namespace="some_ns",
        specs=[],
        current_user_id=current_user.id,
    )
    row = (
        await session.execute(
            select(CustomMetadataSchema).where(
                CustomMetadataSchema.key == "global_key",
            ),
        )
    ).scalar_one()
    assert row.deactivated_at is None


@pytest.mark.asyncio
async def test_a_deploy_cannot_shadow_a_reserved_global_key(session, current_user):
    """
    The API refuses this; the deploy path used to be the way around it.

    Both writers hit the same table, so a check only one of them makes is not a
    check at all.
    """
    session.add(
        CustomMetadataSchema(
            key="lifecycle",
            namespace=None,
            reserved=True,
            json_schema={"type": "string", "enum": ["beta"]},
            value_kind="string",
        ),
    )
    await session.commit()

    with pytest.raises(DJAlreadyExistsException) as exc_info:
        await upsert_schema_specs(
            session,
            namespace="sneaky_ns",
            specs=[
                CustomMetadataSchemaSpec(
                    key="lifecycle",
                    json_schema={"type": "string"},
                ),
            ],
            current_user_id=current_user.id,
        )
    assert "reserved globally" in exc_info.value.message


@pytest.mark.asyncio
async def test_a_deploy_records_who_registered_the_schema(session, current_user):
    """Ownership and provenance, which the deploy path was not setting at all."""
    await upsert_schema_specs(
        session,
        namespace="prov_ns",
        specs=[
            CustomMetadataSchemaSpec(
                key="grain",
                json_schema={"type": "string"},
                owner="@corp/some-team",
            ),
        ],
        current_user_id=current_user.id,
    )
    row = (
        await session.execute(
            select(CustomMetadataSchema).where(
                CustomMetadataSchema.namespace == "prov_ns",
            ),
        )
    ).scalar_one()
    assert row.created_by_id == current_user.id
    assert row.updated_by_id == current_user.id
    assert row.owner == "@corp/some-team"
    assert row.reserved is False


@pytest.mark.asyncio
async def test_upsert_leaves_the_transaction_to_its_caller(session, current_user):
    """
    Registration must be rollback-able, which is what makes a dry run safe.

    `POST /deployments/impact` runs the whole orchestrator inside a SAVEPOINT
    purely to report what a deployment would do, then unwinds it. A commit in
    here releases that SAVEPOINT, so analysing a spec would permanently
    register its schemas.
    """
    await upsert_schema_specs(
        session,
        namespace="rollback_ns",
        specs=[
            CustomMetadataSchemaSpec(key="grain", json_schema={"type": "string"}),
        ],
        current_user_id=current_user.id,
    )
    await session.rollback()

    rows = (
        (
            await session.execute(
                select(CustomMetadataSchema).where(
                    CustomMetadataSchema.namespace == "rollback_ns",
                ),
            )
        )
        .scalars()
        .all()
    )
    assert rows == []


@pytest.mark.asyncio
async def test_a_dry_run_builds_no_index(session, current_user):
    """
    Index DDL is the one thing a rolled-back SAVEPOINT would do for nothing,
    and impact analysis needs no index to report impact.
    """
    unique_key = "dry_run_no_index_probe"
    await upsert_schema_specs(
        session,
        namespace="dry_run_ns",
        specs=[
            CustomMetadataSchemaSpec(
                key=unique_key,
                json_schema={"type": "number"},
                filterable=True,
            ),
        ],
        current_user_id=current_user.id,
        build_indexes=False,
    )
    found = (
        await session.execute(
            text("SELECT indexname FROM pg_indexes WHERE indexname = :idx"),
            {"idx": f"ix_cm_{unique_key}"},
        )
    ).fetchone()
    assert found is None


@pytest.mark.asyncio
async def test_an_omitted_section_leaves_schemas_alone(session, current_user):
    """
    None and [] are different manifests.

    Every deployment that predates this field sends no section at all. If that
    read as "declares none", the next deploy of any existing repo would retire
    every schema it had.
    """
    session.add(
        CustomMetadataSchema(
            key="pre_existing",
            namespace="untouched_ns",
            json_schema={"type": "string"},
            value_kind="string",
        ),
    )
    await session.commit()

    spec = DeploymentSpec(namespace="untouched_ns", nodes=[])
    assert spec.custom_metadata_schemas is None

    mock_context = MagicMock(spec=DeploymentContext)
    mock_context.current_user = current_user
    orchestrator = DeploymentOrchestrator(
        deployment_spec=spec,
        deployment_id="omitted-section-id",
        session=session,
        context=mock_context,
    )
    await orchestrator._setup_deployment_resources()

    row = (
        await session.execute(
            select(CustomMetadataSchema).where(
                CustomMetadataSchema.key == "pre_existing",
            ),
        )
    ).scalar_one()
    assert row.deactivated_at is None


@pytest.mark.asyncio
async def test_a_schema_can_be_scoped_to_a_sub_namespace(session, current_user):
    """
    Rolling a vocabulary out to part of a repo's graph before all of it.

    The USG rollout gates conformed dimensions first, which needs a schema scoped
    narrower than the namespace being deployed.
    """
    await upsert_schema_specs(
        session,
        namespace="shared",
        specs=[
            CustomMetadataSchemaSpec(
                key="system",
                namespace="shared.conformed",
                json_schema={"type": "object"},
            ),
        ],
        current_user_id=current_user.id,
    )
    row = (
        await session.execute(
            select(CustomMetadataSchema).where(CustomMetadataSchema.key == "system"),
        )
    ).scalar_one()
    assert row.namespace == "shared.conformed"


@pytest.mark.asyncio
async def test_a_sub_namespace_declaration_spares_its_siblings(session, current_user):
    """
    Reconciliation covers what the specs name, not the whole subtree.

    `shared.finance` is deployed by whoever owns it. A deployment of `shared` that
    declares a schema for `shared.conformed` says nothing about it, so retiring it
    would be one repo reaching into another's rows.
    """
    session.add(
        CustomMetadataSchema(
            key="system",
            namespace="shared.finance",
            json_schema={"type": "object"},
            value_kind="object",
        ),
    )
    await session.commit()

    await upsert_schema_specs(
        session,
        namespace="shared",
        specs=[
            CustomMetadataSchemaSpec(
                key="system",
                namespace="shared.conformed",
                json_schema={"type": "object"},
            ),
        ],
        current_user_id=current_user.id,
    )
    live = sorted(
        row.namespace
        for row in (
            await session.execute(
                select(CustomMetadataSchema).where(
                    CustomMetadataSchema.key == "system",
                    CustomMetadataSchema.deactivated_at.is_(None),
                ),
            )
        )
        .scalars()
        .all()
    )
    assert live == ["shared.conformed", "shared.finance"]


@pytest.mark.asyncio
async def test_an_empty_list_still_retires_the_deploying_namespace(
    session,
    current_user,
):
    """
    The deploying namespace stays in scope even when no spec names it, so `[]`
    keeps meaning "manages schemas, declares none".
    """
    await upsert_schema_specs(
        session,
        namespace="retire_ns",
        specs=[
            CustomMetadataSchemaSpec(key="system", json_schema={"type": "object"}),
        ],
        current_user_id=current_user.id,
    )
    await upsert_schema_specs(
        session,
        namespace="retire_ns",
        specs=[],
        current_user_id=current_user.id,
    )
    live = (
        (
            await session.execute(
                select(CustomMetadataSchema).where(
                    CustomMetadataSchema.namespace == "retire_ns",
                    CustomMetadataSchema.deactivated_at.is_(None),
                ),
            )
        )
        .scalars()
        .all()
    )
    assert live == []
