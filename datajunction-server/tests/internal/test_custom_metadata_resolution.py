"""Tests for custom_metadata schema resolution helper."""

import pytest

from datajunction_server.database.custom_metadata_schema import CustomMetadataSchema
from datajunction_server.internal.custom_metadata import resolve_schemas
from datajunction_server.models.node_type import NodeType


@pytest.mark.asyncio
async def test_namespace_scoped_overrides_global(session):
    session.add_all(
        [
            CustomMetadataSchema(
                key="grain",
                node_type=None,
                namespace=None,
                json_schema={"type": "string"},
                value_kind="string",
            ),
            CustomMetadataSchema(
                key="grain",
                node_type=None,
                namespace="finance",
                json_schema={"type": "object"},
                value_kind="object",
            ),
        ],
    )
    await session.commit()

    resolved = await resolve_schemas(
        session,
        namespace="finance.orders",
        node_type=NodeType.METRIC,
    )
    assert resolved["grain"] == {"type": "object"}  # namespace-scoped wins

    resolved_other = await resolve_schemas(
        session,
        namespace="growth",
        node_type=NodeType.METRIC,
    )
    assert resolved_other["grain"] == {"type": "string"}  # falls back to global


@pytest.mark.asyncio
async def test_node_type_scoped_overrides_global(session):
    """node_type-scoped row wins over global; non-matching node_type is excluded."""
    session.add_all(
        [
            CustomMetadataSchema(
                key="owner",
                node_type=None,
                namespace=None,
                json_schema={"type": "string"},
                value_kind="string",
            ),
            CustomMetadataSchema(
                key="owner",
                node_type=NodeType.METRIC.value,
                namespace=None,
                json_schema={"type": "array"},
                value_kind="array",
            ),
        ],
    )
    await session.commit()

    # node_type match wins
    resolved_metric = await resolve_schemas(
        session,
        namespace=None,
        node_type=NodeType.METRIC,
    )
    assert resolved_metric["owner"] == {"type": "array"}

    # different node_type falls back to global
    resolved_transform = await resolve_schemas(
        session,
        namespace=None,
        node_type=NodeType.TRANSFORM,
    )
    assert resolved_transform["owner"] == {"type": "string"}


@pytest.mark.asyncio
async def test_most_specific_wins_all_four_combinations(session):
    """Both namespace+node_type wins over all less-specific rows."""
    session.add_all(
        [
            # global (score=0)
            CustomMetadataSchema(
                key="label",
                node_type=None,
                namespace=None,
                json_schema={"type": "string"},
                value_kind="string",
            ),
            # node_type only (score=1)
            CustomMetadataSchema(
                key="label",
                node_type=NodeType.METRIC.value,
                namespace=None,
                json_schema={"type": "boolean"},
                value_kind="boolean",
            ),
            # namespace only (score=2)
            CustomMetadataSchema(
                key="label",
                node_type=None,
                namespace="sales",
                json_schema={"type": "number"},
                value_kind="number",
            ),
            # both namespace + node_type (score=3)
            CustomMetadataSchema(
                key="label",
                node_type=NodeType.METRIC.value,
                namespace="sales",
                json_schema={"type": "integer"},
                value_kind="integer",
            ),
        ],
    )
    await session.commit()

    resolved = await resolve_schemas(
        session,
        namespace="sales.q1",
        node_type=NodeType.METRIC,
    )
    assert resolved["label"] == {"type": "integer"}  # most specific wins


@pytest.mark.asyncio
async def test_deactivated_rows_are_excluded(session):
    """Rows with deactivated_at set are not returned."""
    import datetime

    session.add_all(
        [
            CustomMetadataSchema(
                key="tag",
                node_type=None,
                namespace=None,
                json_schema={"type": "string"},
                value_kind="string",
                deactivated_at=datetime.datetime(
                    2024,
                    1,
                    1,
                    tzinfo=datetime.timezone.utc,
                ),
            ),
        ],
    )
    await session.commit()

    resolved = await resolve_schemas(
        session,
        namespace=None,
        node_type=NodeType.METRIC,
    )
    assert "tag" not in resolved


@pytest.mark.asyncio
async def test_empty_registry_returns_empty_dict(session):
    """No schemas registered → empty result."""
    resolved = await resolve_schemas(
        session,
        namespace="any.namespace",
        node_type=NodeType.METRIC,
    )
    assert resolved == {}


@pytest.mark.asyncio
async def test_namespace_scoped_row_does_not_apply_to_none_namespace(session):
    """A namespace-scoped row does not apply when the query namespace is None (line 22)."""
    session.add(
        CustomMetadataSchema(
            key="k",
            node_type=None,
            namespace="finance",
            json_schema={"type": "string"},
            value_kind="string",
        ),
    )
    await session.commit()

    resolved = await resolve_schemas(
        session,
        namespace=None,
        node_type=NodeType.METRIC,
    )
    assert resolved == {}


@pytest.mark.asyncio
async def test_equal_specificity_second_row_does_not_displace_first(session):
    """Two global rows for the same key: the second-seen does not beat the first (branch 68->64)."""
    schema_a = {"type": "string"}
    schema_b = {"type": "boolean"}
    session.add_all(
        [
            CustomMetadataSchema(
                key="k",
                node_type=None,
                namespace=None,
                json_schema=schema_a,
                value_kind="string",
            ),
            CustomMetadataSchema(
                key="k",
                node_type=None,
                namespace=None,
                json_schema=schema_b,
                value_kind="boolean",
            ),
        ],
    )
    await session.commit()

    resolved = await resolve_schemas(
        session,
        namespace=None,
        node_type=NodeType.METRIC,
    )
    assert len(resolved) == 1
    assert resolved["k"] in (schema_a, schema_b)
