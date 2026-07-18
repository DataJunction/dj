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
    """Two namespace-scoped rows for the same key at equal specificity: first-seen is kept."""
    schema_a = {"type": "string"}
    schema_b = {"type": "boolean"}
    session.add_all(
        [
            CustomMetadataSchema(
                key="k",
                node_type=None,
                namespace="alpha",
                json_schema=schema_a,
                value_kind="string",
            ),
            CustomMetadataSchema(
                key="k",
                node_type=None,
                namespace="beta",
                json_schema=schema_b,
                value_kind="boolean",
            ),
        ],
    )
    await session.commit()

    # Query with a namespace that matches only "alpha" — only schema_a qualifies
    resolved = await resolve_schemas(
        session,
        namespace="alpha.sub",
        node_type=NodeType.METRIC,
    )
    assert resolved["k"] == schema_a


@pytest.mark.asyncio
async def test_reserved_global_wins_over_scoped(session):
    """A reserved global row supersedes a more-specific namespace-scoped row for the same key."""
    session.add_all(
        [
            CustomMetadataSchema(
                key="tier",
                node_type=None,
                namespace=None,
                json_schema={"type": "string", "description": "global-reserved"},
                value_kind="string",
                reserved=True,
            ),
            CustomMetadataSchema(
                key="tier",
                node_type=None,
                namespace="eng",
                json_schema={"type": "number", "description": "namespace-scoped"},
                value_kind="number",
            ),
        ],
    )
    await session.commit()

    resolved = await resolve_schemas(
        session,
        namespace="eng.backend",
        node_type=NodeType.METRIC,
    )
    # Reserved global must win even though namespace-scoped is more specific
    assert resolved["tier"] == {"type": "string", "description": "global-reserved"}


@pytest.mark.asyncio
async def test_non_reserved_global_loses_to_scoped(session):
    """A non-reserved global row still loses to a more-specific namespace-scoped row."""
    session.add_all(
        [
            CustomMetadataSchema(
                key="tier",
                node_type=None,
                namespace=None,
                json_schema={"type": "string", "description": "global-non-reserved"},
                value_kind="string",
                reserved=False,
            ),
            CustomMetadataSchema(
                key="tier",
                node_type=None,
                namespace="eng",
                json_schema={"type": "number", "description": "namespace-scoped"},
                value_kind="number",
            ),
        ],
    )
    await session.commit()

    resolved = await resolve_schemas(
        session,
        namespace="eng.backend",
        node_type=NodeType.METRIC,
    )
    # Non-reserved global: namespace-scoped still wins
    assert resolved["tier"] == {"type": "number", "description": "namespace-scoped"}


@pytest.mark.asyncio
async def test_duplicate_global_violates_unique_index(session):
    """Inserting two global rows with the same key should raise an IntegrityError."""
    import pytest
    from sqlalchemy.exc import IntegrityError

    session.add(
        CustomMetadataSchema(
            key="dup",
            node_type=None,
            namespace=None,
            json_schema={"type": "string"},
            value_kind="string",
        ),
    )
    await session.commit()

    session.add(
        CustomMetadataSchema(
            key="dup",
            node_type=None,
            namespace=None,
            json_schema={"type": "number"},
            value_kind="number",
        ),
    )
    with pytest.raises(IntegrityError):
        await session.commit()
