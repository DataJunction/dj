"""
Tests for custom_metadata validation wired into node create and update endpoints.

DB seeding strategy: the ``session`` and ``client`` fixtures share the same
AsyncSession object (``client`` calls ``get_session_override`` → ``session``).
We insert a ``CustomMetadataSchema`` row directly through ``session`` and
commit it so the API request (running in the same session / same DB) sees it.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.custom_metadata_schema import CustomMetadataSchema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _register_schema(session: AsyncSession, key: str, json_schema: dict) -> None:
    """Insert a CustomMetadataSchema row and flush so the client sees it."""
    session.add(
        CustomMetadataSchema(
            key=key,
            json_schema=json_schema,
            value_kind="string",
        ),
    )
    await session.commit()


# ---------------------------------------------------------------------------
# create_a_node hook
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_node_rejects_bad_custom_metadata(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    """
    Creating a node with a registered key that fails the JSON Schema → 422.
    """
    # RED: this will pass once the hook is wired in nodes.py
    await _register_schema(session, "grain", {"type": "string"})

    resp = await client.post(
        "/nodes/metric/",
        json={
            "name": "default.cm_bad_metric",
            "description": "test metric",
            "query": "SELECT COUNT(repair_order_id) FROM default.repair_orders",
            "mode": "draft",
            "custom_metadata": {"grain": 123},  # int, but schema requires string
        },
    )
    assert resp.status_code == 422
    assert "custom_metadata.grain" in resp.text


@pytest.mark.asyncio
async def test_create_node_allows_unregistered_key(
    client: AsyncClient,
    session: AsyncSession,  # noqa: ARG001 — needed to share the same DB
) -> None:
    """
    Creating a node with an unregistered custom_metadata key → success (lax validation).
    """
    resp = await client.post(
        "/nodes/metric/",
        json={
            "name": "default.cm_ok_metric",
            "description": "test metric",
            "query": "SELECT COUNT(repair_order_id) FROM default.repair_orders",
            "mode": "draft",
            "custom_metadata": {"anything_goes": {"nested": 1}},
        },
    )
    assert resp.status_code in (200, 201)


# ---------------------------------------------------------------------------
# update_any_node hook
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_node_rejects_bad_custom_metadata(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    """
    Patching an existing node with a registered key that fails the JSON Schema → 422.
    """
    await _register_schema(session, "grain", {"type": "string"})

    # default.repair_orders is a source node in the template DB
    resp = await client.patch(
        "/nodes/default.repair_orders/",
        json={
            "custom_metadata": {"grain": 456},  # int, but schema requires string
        },
    )
    assert resp.status_code == 422
    assert "custom_metadata.grain" in resp.text


# ---------------------------------------------------------------------------
# create_a_cube hook
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_cube_rejects_bad_custom_metadata(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    """
    Creating a cube with a registered key that fails the JSON Schema → 422.
    Payload modeled on test_read_cube in cubes_test.py (account_revenue examples,
    which are pre-loaded in the template DB).
    """
    await _register_schema(session, "owner", {"type": "string"})

    resp = await client.post(
        "/nodes/cube/",
        json={
            "name": "default.cm_bad_cube",
            "description": "Cube with bad custom_metadata",
            "metrics": ["default.number_of_account_types"],
            "dimensions": ["default.account_type.account_type_name"],
            "filters": [],
            "mode": "published",
            "custom_metadata": {"owner": 999},  # int, but schema requires string
        },
    )
    assert resp.status_code == 422
    assert "custom_metadata.owner" in resp.text
