"""
Tests for the custom_metadata schema registry REST API.

Covers:
- POST /custom-metadata/schemas/ (create + upsert)
- GET /custom-metadata/schemas/ (list with optional filters)
- DELETE /custom-metadata/schemas/{id} (soft-delete)
- GET /custom-metadata/facets/ (filterable-only catalog)
- GET /custom-metadata/violations/ (advisory report)
"""

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession


# ---------------------------------------------------------------------------
# POST /custom-metadata/schemas/ — create
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_and_list_schema(client: AsyncClient) -> None:
    """Creating a schema and listing it should return the correct data."""
    resp = await client.post(
        "/custom-metadata/schemas/",
        json={
            "key": "table_group",
            "json_schema": {"type": "string"},
            "description": "arc",
        },
    )
    assert resp.status_code in (200, 201)
    body = resp.json()
    assert body["key"] == "table_group"
    assert body["value_kind"] == "string"
    assert body["filterable"] is True
    assert body["description"] == "arc"

    listed = await client.get("/custom-metadata/schemas/")
    assert listed.status_code == 200
    assert any(s["key"] == "table_group" for s in listed.json())


@pytest.mark.asyncio
async def test_register_schema_with_node_type_and_namespace(
    client: AsyncClient,
) -> None:
    """Creating a schema with node_type and namespace stores them."""
    resp = await client.post(
        "/custom-metadata/schemas/",
        json={
            "key": "owner",
            "node_type": "metric",
            "namespace": "default",
            "json_schema": {"type": "string"},
        },
    )
    assert resp.status_code in (200, 201)
    body = resp.json()
    assert body["node_type"] == "metric"
    assert body["namespace"] == "default"


@pytest.mark.asyncio
async def test_register_schema_reject_invalid_json_schema(client: AsyncClient) -> None:
    """Posting an invalid JSON Schema must return 422."""
    resp = await client.post(
        "/custom-metadata/schemas/",
        json={
            "key": "bad",
            "json_schema": {"type": "not-a-real-type"},
        },
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_register_schema_upsert_updates_existing(client: AsyncClient) -> None:
    """POSTing the same (key, node_type, namespace) twice should upsert, not duplicate."""
    payload = {
        "key": "grain",
        "json_schema": {"type": "string"},
        "description": "first",
    }
    resp1 = await client.post("/custom-metadata/schemas/", json=payload)
    assert resp1.status_code in (200, 201)
    id1 = resp1.json()["id"]

    # Update description via another POST (upsert)
    resp2 = await client.post(
        "/custom-metadata/schemas/",
        json={
            "key": "grain",
            "json_schema": {"type": "integer"},
            "description": "updated",
        },
    )
    assert resp2.status_code in (200, 201)
    body2 = resp2.json()
    assert body2["id"] == id1  # same row
    assert body2["description"] == "updated"
    assert body2["value_kind"] == "integer"

    # Only one active row in list
    listed = await client.get("/custom-metadata/schemas/")
    grains = [s for s in listed.json() if s["key"] == "grain"]
    assert len(grains) == 1


@pytest.mark.asyncio
async def test_list_schemas_filter_by_namespace(client: AsyncClient) -> None:
    """GET /custom-metadata/schemas/?namespace= filters to matching rows."""
    await client.post(
        "/custom-metadata/schemas/",
        json={"key": "k1", "namespace": "ns_a", "json_schema": {"type": "string"}},
    )
    await client.post(
        "/custom-metadata/schemas/",
        json={"key": "k2", "namespace": "ns_b", "json_schema": {"type": "string"}},
    )
    resp = await client.get("/custom-metadata/schemas/?namespace=ns_a")
    assert resp.status_code == 200
    data = resp.json()
    assert all(s["namespace"] == "ns_a" for s in data)
    assert any(s["key"] == "k1" for s in data)
    assert not any(s["key"] == "k2" for s in data)


# ---------------------------------------------------------------------------
# DELETE /custom-metadata/schemas/{id} — soft delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_schema_soft_deletes(client: AsyncClient) -> None:
    """DELETE sets deactivated_at and removes the schema from active listing."""
    create_resp = await client.post(
        "/custom-metadata/schemas/",
        json={"key": "to_delete", "json_schema": {"type": "string"}},
    )
    assert create_resp.status_code in (200, 201)
    schema_id = create_resp.json()["id"]

    del_resp = await client.delete(f"/custom-metadata/schemas/{schema_id}")
    assert del_resp.status_code in (200, 204)

    # Should no longer appear in list
    listed = await client.get("/custom-metadata/schemas/")
    assert not any(s["id"] == schema_id for s in listed.json())


@pytest.mark.asyncio
async def test_delete_schema_not_found(client: AsyncClient) -> None:
    """DELETE for a nonexistent schema_id returns 404."""
    resp = await client.delete("/custom-metadata/schemas/999999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /custom-metadata/facets/ — filterable-only catalog
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_facets_only_lists_filterable(client: AsyncClient) -> None:
    """GET /custom-metadata/facets/ excludes schemas with filterable=False."""
    await client.post(
        "/custom-metadata/schemas/",
        json={"key": "notes", "json_schema": {"type": "string"}, "filterable": False},
    )
    await client.post(
        "/custom-metadata/schemas/",
        json={"key": "score", "json_schema": {"type": "number"}, "filterable": True},
    )
    facets = await client.get("/custom-metadata/facets/")
    assert facets.status_code == 200
    data = facets.json()
    assert not any(f["key"] == "notes" for f in data)
    assert any(f["key"] == "score" for f in data)


# ---------------------------------------------------------------------------
# GET /custom-metadata/violations/ — advisory report
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_violations_missing_schema_id_returns_422(client: AsyncClient) -> None:
    """GET /custom-metadata/violations/ without schema_id returns 422."""
    resp = await client.get("/custom-metadata/violations/")
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_violations_nonexistent_schema_id_returns_404(
    client: AsyncClient,
) -> None:
    """GET /custom-metadata/violations/?schema_id=9999 with nonexistent id returns 404."""
    resp = await client.get("/custom-metadata/violations/?schema_id=9999")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_violations_returns_report_for_clean_nodes(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    """Violation report returns count=0 when no nodes violate the schema."""
    # Register schema
    create_resp = await client.post(
        "/custom-metadata/schemas/",
        json={"key": "grain", "json_schema": {"type": "string"}},
    )
    assert create_resp.status_code in (200, 201)
    schema_id = create_resp.json()["id"]

    resp = await client.get(f"/custom-metadata/violations/?schema_id={schema_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert "violation_count" in body
    assert body["violation_count"] == 0


@pytest.mark.asyncio
async def test_list_schemas_filter_by_node_type(client: AsyncClient) -> None:
    """GET /custom-metadata/schemas/?node_type= filters to matching rows."""
    await client.post(
        "/custom-metadata/schemas/",
        json={
            "key": "nt_metric",
            "node_type": "metric",
            "json_schema": {"type": "string"},
        },
    )
    await client.post(
        "/custom-metadata/schemas/",
        json={
            "key": "nt_source",
            "node_type": "source",
            "json_schema": {"type": "string"},
        },
    )
    resp = await client.get("/custom-metadata/schemas/?node_type=metric")
    assert resp.status_code == 200
    data = resp.json()
    assert all(s["node_type"] == "metric" for s in data if s["node_type"] is not None)
    assert any(s["key"] == "nt_metric" for s in data)
    assert not any(s["key"] == "nt_source" for s in data)


@pytest.mark.asyncio
async def test_violations_node_has_key_but_passes(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    """Violation report: node with the key but a valid value should not be counted."""
    from datajunction_server.database.node import Node, NodeRevision
    from sqlalchemy import select

    create_resp = await client.post(
        "/custom-metadata/schemas/",
        json={"key": "grain", "json_schema": {"type": "string"}},
    )
    assert create_resp.status_code in (200, 201)
    schema_id = create_resp.json()["id"]

    # Set a valid string value
    nr = (
        await session.execute(
            select(NodeRevision)
            .join(Node, Node.id == NodeRevision.node_id)
            .where(
                Node.current_version == NodeRevision.version,
                Node.deactivated_at.is_(None),
            )
            .limit(1),
        )
    ).scalar_one_or_none()
    if nr is not None:
        nr.custom_metadata = {"grain": "daily"}  # valid string
        await session.commit()

    resp = await client.get(f"/custom-metadata/violations/?schema_id={schema_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["violation_count"] == 0


@pytest.mark.asyncio
async def test_violations_detects_violating_nodes(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    """Violation report detects nodes whose custom_metadata violates the schema."""
    from datajunction_server.database.node import Node, NodeRevision

    # Create a schema requiring "grain" to be a string
    create_resp = await client.post(
        "/custom-metadata/schemas/",
        json={"key": "grain", "json_schema": {"type": "string"}},
    )
    assert create_resp.status_code in (200, 201)
    schema_id = create_resp.json()["id"]

    # Directly inject a violating node revision (custom_metadata.grain = 123, not string)
    # Use existing default.repair_orders source node
    from sqlalchemy import select

    nr = (
        await session.execute(
            select(NodeRevision)
            .join(Node, Node.id == NodeRevision.node_id)
            .where(
                Node.current_version == NodeRevision.version,
                Node.deactivated_at.is_(None),
            )
            .limit(1),
        )
    ).scalar_one_or_none()
    if nr is not None:
        nr.custom_metadata = {"grain": 123}  # violates {type: string}
        await session.commit()

        resp = await client.get(f"/custom-metadata/violations/?schema_id={schema_id}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["violation_count"] >= 1
        assert len(body["samples"]) >= 1
        sample = body["samples"][0]
        assert "node_name" in sample
        assert "errors" in sample
