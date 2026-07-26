"""
Tests that availability posting is scoped to the node revision it was produced
for (via the optional ``node_version`` on POST /data/{node}/availability).

Kept in its own module: it needs a function-scoped, mutable client + session to
build a two-revision node, and interleaving those with the module-scoped
availability suite in ``data_test.py`` perturbs that suite's shared DB state.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from datajunction_server.database.node import NodeRevision
from datajunction_server.models.cube_materialization import materialized_table_name
from datajunction_server.utils import get_settings


@pytest.mark.asyncio
async def test_availability_scoped_to_named_revision(
    session: AsyncSession,
    client_with_roads: AsyncClient,
) -> None:
    """
    Availability tagged with an older node_version is attached to THAT revision,
    not the current one — so a workflow still running against a superseded
    revision cannot pollute the current revision's availability.
    """
    node_name = "default.avail_scope_test"
    create = await client_with_roads.post(
        "/nodes/transform/",
        json={
            "name": node_name,
            "description": "availability scoping repro",
            "query": "SELECT repair_order_id FROM default.repair_orders",
            "mode": "published",
        },
    )
    assert create.status_code in (200, 201), create.json()
    v1 = create.json()["version"]

    # Change the query to cut a new revision.
    patch = await client_with_roads.patch(
        f"/nodes/{node_name}/",
        json={
            "query": (
                "SELECT repair_order_id, municipality_id FROM default.repair_orders"
            ),
        },
    )
    assert patch.status_code == 200, patch.json()
    v2 = patch.json()["version"]
    assert v2 != v1

    # Post availability tagged for the OLD version.
    response = await client_with_roads.post(
        f"/data/{node_name}/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": "old_repair_orders",
            "valid_through_ts": 20230101,
            "node_version": v1,
        },
    )
    assert response.status_code == 200, response.json()

    # It landed on the old revision; the current revision is untouched.
    session.expire_all()
    revisions = (
        (
            await session.execute(
                select(NodeRevision)
                .where(NodeRevision.name == node_name)
                .options(selectinload(NodeRevision.availability)),
            )
        )
        .scalars()
        .all()
    )
    by_version = {r.version: r for r in revisions}
    assert by_version[v1].availability is not None
    assert by_version[v1].availability.table == "old_repair_orders"
    assert by_version[v2].availability is None


@pytest.mark.asyncio
async def test_availability_unknown_version_rejected(
    client_with_roads: AsyncClient,
) -> None:
    """A node_version the node has never had is rejected."""
    node_name = "default.avail_scope_unknown"
    create = await client_with_roads.post(
        "/nodes/transform/",
        json={
            "name": node_name,
            "description": "availability scoping repro",
            "query": "SELECT repair_order_id FROM default.repair_orders",
            "mode": "published",
        },
    )
    assert create.status_code in (200, 201), create.json()

    response = await client_with_roads.post(
        f"/data/{node_name}/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": "t",
            "valid_through_ts": 20230101,
            "node_version": "v9.9",
        },
    )
    assert response.status_code == 422
    assert "has no version v9.9" in response.json()["message"]


@pytest.mark.asyncio
async def test_availability_version_derived_from_table_name(
    session: AsyncSession,
    client_with_roads: AsyncClient,
) -> None:
    """
    With no explicit node_version, the target revision is derived from the
    materialized table name (which encodes <node>_<version>_<hash>), so a
    workflow that only reports its Druid datasource is still scoped correctly.
    """
    node_name = "default.avail_derive_test"
    create = await client_with_roads.post(
        "/nodes/transform/",
        json={
            "name": node_name,
            "description": "availability derivation repro",
            "query": "SELECT repair_order_id FROM default.repair_orders",
            "mode": "published",
        },
    )
    assert create.status_code in (200, 201), create.json()
    v1 = create.json()["version"]

    patch = await client_with_roads.patch(
        f"/nodes/{node_name}/",
        json={
            "query": (
                "SELECT repair_order_id, municipality_id FROM default.repair_orders"
            ),
        },
    )
    assert patch.status_code == 200, patch.json()
    v2 = patch.json()["version"]
    assert v2 != v1

    # No node_version; the v1 version is embedded in the reported table name.
    table = get_settings().druid_datasource_prefix + materialized_table_name(
        node_name,
        v1,
        "deadbeefdeadbeef",
    )
    response = await client_with_roads.post(
        f"/data/{node_name}/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": table,
            "valid_through_ts": 20230101,
        },
    )
    assert response.status_code == 200, response.json()

    session.expire_all()
    revisions = (
        (
            await session.execute(
                select(NodeRevision)
                .where(NodeRevision.name == node_name)
                .options(selectinload(NodeRevision.availability)),
            )
        )
        .scalars()
        .all()
    )
    by_version = {r.version: r for r in revisions}
    assert by_version[v1].availability is not None
    assert by_version[v2].availability is None


@pytest.mark.asyncio
async def test_availability_derived_unknown_version_falls_back_to_current(
    session: AsyncSession,
    client_with_roads: AsyncClient,
) -> None:
    """
    A version derived from the table name that matches no revision is a
    best-effort miss (unlike an explicit node_version): it falls back to the
    current revision rather than being rejected.
    """
    node_name = "default.avail_derive_fallback"
    create = await client_with_roads.post(
        "/nodes/transform/",
        json={
            "name": node_name,
            "description": "availability derivation fallback repro",
            "query": "SELECT repair_order_id FROM default.repair_orders",
            "mode": "published",
        },
    )
    assert create.status_code in (200, 201), create.json()
    current_version = create.json()["version"]

    # Table encodes a version the node never had; no explicit node_version.
    table = get_settings().druid_datasource_prefix + materialized_table_name(
        node_name,
        "v9.0",
        "deadbeefdeadbeef",
    )
    response = await client_with_roads.post(
        f"/data/{node_name}/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": table,
            "valid_through_ts": 20230101,
        },
    )
    assert response.status_code == 200, response.json()

    # Fell back to the current revision.
    session.expire_all()
    revision = (
        await session.execute(
            select(NodeRevision)
            .where(
                NodeRevision.name == node_name,
                NodeRevision.version == current_version,
            )
            .options(selectinload(NodeRevision.availability)),
        )
    ).scalar_one()
    assert revision.availability is not None
    assert revision.availability.table == table


async def _make_two_revision_node(client: AsyncClient, node_name: str) -> tuple:
    """Create a transform node, then change its query to cut a second revision.

    Returns (old_version, current_version).
    """
    create = await client.post(
        "/nodes/transform/",
        json={
            "name": node_name,
            "description": "availability scoping repro",
            "query": "SELECT repair_order_id FROM default.repair_orders",
            "mode": "published",
        },
    )
    assert create.status_code in (200, 201), create.json()
    v1 = create.json()["version"]
    patch = await client.patch(
        f"/nodes/{node_name}/",
        json={
            "query": (
                "SELECT repair_order_id, municipality_id FROM default.repair_orders"
            ),
        },
    )
    assert patch.status_code == 200, patch.json()
    v2 = patch.json()["version"]
    assert v2 != v1
    return v1, v2


async def _availability_by_version(session: AsyncSession, node_name: str) -> dict:
    session.expire_all()
    revisions = (
        (
            await session.execute(
                select(NodeRevision)
                .where(NodeRevision.name == node_name)
                .options(selectinload(NodeRevision.availability)),
            )
        )
        .scalars()
        .all()
    )
    return {r.version: r for r in revisions}


@pytest.mark.asyncio
async def test_two_revisions_hold_availability_independently(
    session: AsyncSession,
    client_with_roads: AsyncClient,
) -> None:
    """Posting for the current and an older revision leaves each with its own
    availability — the current revision is not clobbered by the stale post, and
    the old revision is not clobbered by the current one."""
    node_name = "default.avail_two_rev"
    v1, v2 = await _make_two_revision_node(client_with_roads, node_name)
    prefix = get_settings().druid_datasource_prefix
    table_v2 = prefix + materialized_table_name(node_name, v2, "cafecafecafecafe")
    table_v1 = prefix + materialized_table_name(node_name, v1, "deadbeefdeadbeef")

    for table in (table_v2, table_v1):
        resp = await client_with_roads.post(
            f"/data/{node_name}/availability/",
            json={
                "catalog": "default",
                "schema_": "roads",
                "table": table,
                "valid_through_ts": 20230101,
            },
        )
        assert resp.status_code == 200, resp.json()

    by_version = await _availability_by_version(session, node_name)
    assert by_version[v1].availability is not None
    assert by_version[v1].availability.table == table_v1
    assert by_version[v2].availability is not None
    assert by_version[v2].availability.table == table_v2


@pytest.mark.asyncio
async def test_merge_applied_to_scoped_revision(
    session: AsyncSession,
    client_with_roads: AsyncClient,
) -> None:
    """A second post for the same older revision merges into THAT revision's
    availability (widest temporal range wins), never the current revision."""
    node_name = "default.avail_merge_scope"
    v1, v2 = await _make_two_revision_node(client_with_roads, node_name)
    table_v1 = get_settings().druid_datasource_prefix + materialized_table_name(
        node_name,
        v1,
        "deadbeefdeadbeef",
    )

    first = await client_with_roads.post(
        f"/data/{node_name}/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": table_v1,
            "valid_through_ts": 20230201,
            "min_temporal_partition": ["20230101"],
            "max_temporal_partition": ["20230201"],
        },
    )
    assert first.status_code == 200, first.json()
    # Second post carries a narrower range; merge must keep the wider one.
    second = await client_with_roads.post(
        f"/data/{node_name}/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": table_v1,
            "valid_through_ts": 20230101,
            "min_temporal_partition": ["20230101"],
            "max_temporal_partition": ["20230115"],
        },
    )
    assert second.status_code == 200, second.json()

    by_version = await _availability_by_version(session, node_name)
    assert by_version[v1].availability is not None
    assert by_version[v1].availability.valid_through_ts == 20230201
    assert by_version[v1].availability.max_temporal_partition == ["20230201"]
    assert by_version[v2].availability is None


@pytest.mark.asyncio
async def test_derived_version_equal_to_current_attaches_to_current(
    session: AsyncSession,
    client_with_roads: AsyncClient,
) -> None:
    """When the derived version equals the current one, availability attaches to
    the current revision (no scoping detour)."""
    node_name = "default.avail_current_noop"
    create = await client_with_roads.post(
        "/nodes/transform/",
        json={
            "name": node_name,
            "description": "availability current no-op repro",
            "query": "SELECT repair_order_id FROM default.repair_orders",
            "mode": "published",
        },
    )
    assert create.status_code in (200, 201), create.json()
    current_version = create.json()["version"]
    table = get_settings().druid_datasource_prefix + materialized_table_name(
        node_name,
        current_version,
        "deadbeefdeadbeef",
    )
    resp = await client_with_roads.post(
        f"/data/{node_name}/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": table,
            "valid_through_ts": 20230101,
        },
    )
    assert resp.status_code == 200, resp.json()

    by_version = await _availability_by_version(session, node_name)
    assert by_version[current_version].availability is not None
    assert by_version[current_version].availability.table == table


@pytest.mark.asyncio
async def test_source_node_availability_unaffected(
    client_with_roads: AsyncClient,
) -> None:
    """Source-node availability is untouched by revision scoping: derivation does
    not apply, and the existing source table-match guard still governs."""
    node_name = "default.repair_orders"  # roads source: default.roads.repair_orders
    # A mismatched table is still rejected by the source guard.
    mismatch = await client_with_roads.post(
        f"/data/{node_name}/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": "not_repair_orders",
            "valid_through_ts": 20230101,
        },
    )
    assert mismatch.status_code == 422
    assert "source nodes require" in mismatch.json()["message"]

    # The matching source table is accepted.
    match = await client_with_roads.post(
        f"/data/{node_name}/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": "repair_orders",
            "valid_through_ts": 20230101,
        },
    )
    assert match.status_code == 200, match.json()


@pytest.mark.asyncio
async def test_unprefixed_table_derivation(
    session: AsyncSession,
    client_with_roads: AsyncClient,
) -> None:
    """Version derivation also works for an unprefixed <node>_<version>_<hash>
    table (engines that report the bare table rather than the Druid datasource)."""
    node_name = "default.avail_unprefixed"
    v1, v2 = await _make_two_revision_node(client_with_roads, node_name)
    table = materialized_table_name(node_name, v1, "deadbeefdeadbeef")  # no prefix
    resp = await client_with_roads.post(
        f"/data/{node_name}/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": table,
            "valid_through_ts": 20230101,
        },
    )
    assert resp.status_code == 200, resp.json()

    by_version = await _availability_by_version(session, node_name)
    assert by_version[v1].availability is not None
    assert by_version[v2].availability is None
