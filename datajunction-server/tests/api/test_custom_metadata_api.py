"""
Tests for the custom_metadata schema registry REST API.

Covers:
- POST /metadata-schemas/ (create + upsert)
- GET /metadata-schemas/ (list with optional filters)
- DELETE /metadata-schemas/{id} (soft-delete)
- GET /metadata-schemas/{id}/violations (advisory report)
"""

from datetime import timedelta

import httpx
import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.main import app
from datajunction_server.database.user import User
from datajunction_server.internal.access.authentication.tokens import create_token
from datajunction_server.models.user import OAuthProvider


# ---------------------------------------------------------------------------
# POST /metadata-schemas/ — create
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_and_list_schema(client: AsyncClient) -> None:
    """Creating a schema and listing it should return the correct data."""
    resp = await client.post(
        "/metadata-schemas/",
        json={
            "key": "table_group",
            "namespace": "test.ns",
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

    listed = await client.get("/metadata-schemas/")
    assert listed.status_code == 200
    assert any(s["key"] == "table_group" for s in listed.json())


@pytest.mark.asyncio
async def test_register_schema_with_node_type_and_namespace(
    client: AsyncClient,
) -> None:
    """Creating a schema with node_type and namespace stores them."""
    resp = await client.post(
        "/metadata-schemas/",
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
        "/metadata-schemas/",
        json={
            "key": "bad",
            "namespace": "test.ns",
            "json_schema": {"type": "not-a-real-type"},
        },
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_register_schema_upsert_updates_existing(client: AsyncClient) -> None:
    """POSTing the same (key, node_type, namespace) twice should upsert, not duplicate."""
    payload = {
        "key": "grain",
        "namespace": "test.ns",
        "json_schema": {"type": "string"},
        "description": "first",
    }
    resp1 = await client.post("/metadata-schemas/", json=payload)
    assert resp1.status_code in (200, 201)
    id1 = resp1.json()["id"]

    # Update description via another POST (upsert)
    resp2 = await client.post(
        "/metadata-schemas/",
        json={
            "key": "grain",
            "namespace": "test.ns",
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
    listed = await client.get("/metadata-schemas/")
    grains = [s for s in listed.json() if s["key"] == "grain"]
    assert len(grains) == 1


@pytest.mark.asyncio
async def test_list_schemas_filter_by_namespace(client: AsyncClient) -> None:
    """GET /metadata-schemas/?namespace= filters to matching rows."""
    await client.post(
        "/metadata-schemas/",
        json={"key": "k1", "namespace": "ns_a", "json_schema": {"type": "string"}},
    )
    await client.post(
        "/metadata-schemas/",
        json={"key": "k2", "namespace": "ns_b", "json_schema": {"type": "string"}},
    )
    resp = await client.get("/metadata-schemas/?namespace=ns_a")
    assert resp.status_code == 200
    data = resp.json()
    assert all(s["namespace"] == "ns_a" for s in data)
    assert any(s["key"] == "k1" for s in data)
    assert not any(s["key"] == "k2" for s in data)


# ---------------------------------------------------------------------------
# DELETE /metadata-schemas/{id} — soft delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_schema_soft_deletes(client: AsyncClient) -> None:
    """DELETE sets deactivated_at and removes the schema from active listing."""
    create_resp = await client.post(
        "/metadata-schemas/",
        json={
            "key": "to_delete",
            "namespace": "test.ns",
            "json_schema": {"type": "string"},
        },
    )
    assert create_resp.status_code in (200, 201)
    schema_id = create_resp.json()["id"]

    del_resp = await client.delete(f"/metadata-schemas/{schema_id}")
    assert del_resp.status_code in (200, 204)

    # Should no longer appear in list
    listed = await client.get("/metadata-schemas/")
    assert not any(s["id"] == schema_id for s in listed.json())


@pytest.mark.asyncio
async def test_delete_schema_not_found(client: AsyncClient) -> None:
    """DELETE for a nonexistent schema_id returns 404."""
    resp = await client.delete("/metadata-schemas/999999")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_global_key_non_admin_forbidden(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    """A non-admin deleting a global (namespace=None) schema key → 403."""
    from datajunction_server.database.custom_metadata_schema import (
        CustomMetadataSchema,
    )

    row = CustomMetadataSchema(
        key="global_del_key",
        node_type=None,
        namespace=None,
        json_schema={"type": "string"},
        value_kind="string",
    )
    session.add(row)
    await session.commit()

    resp = await client.delete(f"/metadata-schemas/{row.id}")
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# `filterable` on the list response
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_reports_filterable_per_schema(client: AsyncClient) -> None:
    """The filterable subset is a read off the list, not an endpoint of its own.

    `filterable` curates which keys a UI offers as filter controls; it does not
    gate filtering, which goes through `customMetadataFilters` and never consults
    the registry. A caller wanting the subset filters the list itself.
    """
    await client.post(
        "/metadata-schemas/",
        json={
            "key": "notes",
            "namespace": "test.ns",
            "json_schema": {"type": "string"},
            "filterable": False,
        },
    )
    await client.post(
        "/metadata-schemas/",
        json={
            "key": "score",
            "namespace": "test.ns",
            "json_schema": {"type": "number"},
            "filterable": True,
        },
    )
    resp = await client.get("/metadata-schemas/?namespace=test.ns")
    assert resp.status_code == 200
    by_key = {row["key"]: row["filterable"] for row in resp.json()}
    assert by_key["notes"] is False
    assert by_key["score"] is True


# ---------------------------------------------------------------------------
# GET /metadata-schemas/{id}/violations — advisory report
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_violations_nonexistent_schema_id_returns_404(
    client: AsyncClient,
) -> None:
    """An id that matches no live registration is a 404."""
    resp = await client.get("/metadata-schemas/9999/violations")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_violations_returns_report_for_clean_nodes(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    """Violation report returns count=0 when no nodes violate the schema."""
    # Register schema
    create_resp = await client.post(
        "/metadata-schemas/",
        json={
            "key": "grain",
            "namespace": "test.ns",
            "json_schema": {"type": "string"},
        },
    )
    assert create_resp.status_code in (200, 201)
    schema_id = create_resp.json()["id"]

    resp = await client.get(f"/metadata-schemas/{schema_id}/violations")
    assert resp.status_code == 200
    body = resp.json()
    assert "violation_count" in body
    assert body["violation_count"] == 0


@pytest.mark.asyncio
async def test_list_schemas_filter_by_node_type(client: AsyncClient) -> None:
    """GET /metadata-schemas/?node_type= filters to matching rows."""
    await client.post(
        "/metadata-schemas/",
        json={
            "key": "nt_metric",
            "node_type": "metric",
            "namespace": "test.ns",
            "json_schema": {"type": "string"},
        },
    )
    await client.post(
        "/metadata-schemas/",
        json={
            "key": "nt_source",
            "node_type": "source",
            "namespace": "test.ns",
            "json_schema": {"type": "string"},
        },
    )
    resp = await client.get("/metadata-schemas/?node_type=metric")
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
        "/metadata-schemas/",
        json={
            "key": "grain",
            "namespace": "test.ns",
            "json_schema": {"type": "string"},
        },
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

    resp = await client.get(f"/metadata-schemas/{schema_id}/violations")
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
        "/metadata-schemas/",
        json={
            "key": "grain",
            "namespace": "test.ns",
            "json_schema": {"type": "string"},
        },
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

        resp = await client.get(f"/metadata-schemas/{schema_id}/violations")
        assert resp.status_code == 200
        body = resp.json()
        assert body["violation_count"] >= 1
        assert len(body["samples"]) >= 1
        sample = body["samples"][0]
        assert "node_name" in sample
        assert "errors" in sample


# ---------------------------------------------------------------------------
# Authorization tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_global_key_non_admin_forbidden(client: AsyncClient) -> None:
    """Non-admin registering a global key (namespace=None) → 403."""
    resp = await client.post(
        "/metadata-schemas/",
        json={"key": "global_key", "json_schema": {"type": "string"}},
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_register_global_key_admin_succeeds(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    """Admin registering a global key → success."""
    from datajunction_server.database.user import User
    from datajunction_server.models.user import OAuthProvider
    from datajunction_server.internal.access.authentication.tokens import create_token
    from datetime import timedelta
    import httpx
    from datajunction_server.api.main import app

    admin_user = User(
        username="admin_test_global",
        email=None,
        name=None,
        oauth_provider=OAuthProvider.BASIC,
        is_admin=True,
    )
    session.add(admin_user)
    await session.commit()
    admin_token = create_token(
        {"username": "admin_test_global"},
        secret="a-fake-secretkey",
        iss="http://localhost:8000/",
        expires_delta=timedelta(hours=24),
    )
    async with AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {admin_token}"},
    ) as admin_client:
        resp = await admin_client.post(
            "/metadata-schemas/",
            json={"key": "admin_global_key", "json_schema": {"type": "string"}},
        )
        assert resp.status_code in (200, 201)
        body = resp.json()
        assert body["key"] == "admin_global_key"
        assert body["namespace"] is None

        # Admin can also delete a global key (covers the admin-delete-global path).
        del_resp = await admin_client.delete(
            f"/metadata-schemas/{body['id']}",
        )
        assert del_resp.status_code == 200


@pytest.mark.asyncio
async def test_register_namespace_key_with_access_succeeds(
    client: AsyncClient,
) -> None:
    """Namespace-scoped key with access (PassthroughAuth approves all) → success."""
    resp = await client.post(
        "/metadata-schemas/",
        json={
            "key": "ns_key",
            "namespace": "my.namespace",
            "json_schema": {"type": "string"},
        },
    )
    assert resp.status_code in (200, 201)
    body = resp.json()
    assert body["namespace"] == "my.namespace"


@pytest.mark.asyncio
async def test_register_reserved_true_non_admin_forbidden(
    client: AsyncClient,
) -> None:
    """Non-admin setting reserved=True → 403."""
    resp = await client.post(
        "/metadata-schemas/",
        json={
            "key": "some_ns_key",
            "namespace": "my.namespace",
            "reserved": True,
            "json_schema": {"type": "string"},
        },
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_register_namespace_key_blocked_by_reserved_global(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    """Namespace registration of a key with a reserved global row → 409."""
    from datajunction_server.database.user import User
    from datajunction_server.models.user import OAuthProvider
    from datajunction_server.internal.access.authentication.tokens import create_token
    from datetime import timedelta
    import httpx
    from datajunction_server.api.main import app

    # First, create the reserved global schema as admin
    admin_user = User(
        username="admin_reserved",
        email=None,
        name=None,
        oauth_provider=OAuthProvider.BASIC,
        is_admin=True,
    )
    session.add(admin_user)
    await session.commit()
    admin_token = create_token(
        {"username": "admin_reserved"},
        secret="a-fake-secretkey",
        iss="http://localhost:8000/",
        expires_delta=timedelta(hours=24),
    )
    async with AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {admin_token}"},
    ) as admin_client:
        resp = await admin_client.post(
            "/metadata-schemas/",
            json={
                "key": "reserved_key",
                "reserved": True,
                "json_schema": {"type": "string"},
            },
        )
    assert resp.status_code in (200, 201)

    # Now try to register namespace-scoped (non-admin, namespace write OK via Passthrough)
    resp2 = await client.post(
        "/metadata-schemas/",
        json={
            "key": "reserved_key",
            "namespace": "some.namespace",
            "json_schema": {"type": "string"},
        },
    )
    assert resp2.status_code == 409


@pytest.mark.asyncio
async def test_created_by_id_and_updated_by_id_populated(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    """created_by_id and updated_by_id are set from the current user."""
    from datajunction_server.database.user import User
    from datajunction_server.models.user import OAuthProvider
    from datajunction_server.internal.access.authentication.tokens import create_token
    from datetime import timedelta
    import httpx
    from datajunction_server.api.main import app

    admin_user = User(
        username="admin_audit",
        email=None,
        name=None,
        oauth_provider=OAuthProvider.BASIC,
        is_admin=True,
    )
    session.add(admin_user)
    await session.commit()
    await session.refresh(admin_user)
    admin_id = admin_user.id
    admin_token = create_token(
        {"username": "admin_audit"},
        secret="a-fake-secretkey",
        iss="http://localhost:8000/",
        expires_delta=timedelta(hours=24),
    )
    async with AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {admin_token}"},
    ) as admin_client:
        resp = await admin_client.post(
            "/metadata-schemas/",
            json={"key": "audit_key", "json_schema": {"type": "string"}},
        )
    assert resp.status_code in (200, 201)
    body = resp.json()
    assert body["created_by_id"] == admin_id
    assert body["updated_by_id"] == admin_id


@pytest.mark.asyncio
async def test_owner_round_trips(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    """owner field is persisted and returned in output."""
    from datajunction_server.database.user import User
    from datajunction_server.models.user import OAuthProvider
    from datajunction_server.internal.access.authentication.tokens import create_token
    from datetime import timedelta
    import httpx
    from datajunction_server.api.main import app

    admin_user = User(
        username="admin_owner",
        email=None,
        name=None,
        oauth_provider=OAuthProvider.BASIC,
        is_admin=True,
    )
    session.add(admin_user)
    await session.commit()
    admin_token = create_token(
        {"username": "admin_owner"},
        secret="a-fake-secretkey",
        iss="http://localhost:8000/",
        expires_delta=timedelta(hours=24),
    )
    async with AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {admin_token}"},
    ) as admin_client:
        resp = await admin_client.post(
            "/metadata-schemas/",
            json={
                "key": "owner_key",
                "json_schema": {"type": "string"},
                "owner": "team-data-eng",
            },
        )
    assert resp.status_code in (200, 201)
    body = resp.json()
    assert body["owner"] == "team-data-eng"


# ---------------------------------------------------------------------------
# Repo-managed namespaces — the API is not a second writer
# ---------------------------------------------------------------------------


async def _make_git_managed(client: AsyncClient, namespace: str) -> None:
    """Create a namespace and mark it as owning a repo."""
    await client.post(f"/namespaces/{namespace}")
    resp = await client.patch(
        f"/namespaces/{namespace}/git",
        json={"github_repo_path": "myorg/myrepo"},
    )
    assert resp.status_code in (200, 201), resp.text


@pytest.mark.asyncio
async def test_registering_into_a_repo_managed_namespace_is_refused(
    client: AsyncClient,
) -> None:
    """
    A repo-managed namespace has one writer, and it is not this endpoint.

    Beyond being unreviewed, a registration here does not survive: a deployment
    reconciles the namespace to exactly what its manifest declares, so the row
    would disappear at the next push with nothing to explain it. Better to
    refuse and say where the change belongs.
    """
    await _make_git_managed(client, "gitmanaged")

    resp = await client.post(
        "/metadata-schemas/",
        json={
            "key": "system",
            "namespace": "gitmanaged",
            "json_schema": {"type": "object"},
        },
    )
    assert resp.status_code == 422, resp.text
    assert "is git-managed" in resp.text
    assert "custom_metadata_schemas:" in resp.text


@pytest.mark.asyncio
async def test_deleting_a_repo_managed_schema_is_refused(client: AsyncClient) -> None:
    """Deleting has the same problem in reverse: the next deploy re-creates it."""
    resp = await client.post(
        "/metadata-schemas/",
        json={
            "key": "system",
            "namespace": "gitlater",
            "json_schema": {"type": "object"},
        },
    )
    assert resp.status_code in (200, 201), resp.text
    schema_id = resp.json()["id"]

    # The namespace becomes repo-managed after the fact, as it would in practice.
    await _make_git_managed(client, "gitlater")

    resp = await client.delete(f"/metadata-schemas/{schema_id}")
    assert resp.status_code == 422, resp.text
    assert "is git-managed" in resp.text


@pytest_asyncio.fixture
async def admin_client(client: AsyncClient, session: AsyncSession):
    """An AsyncClient authenticated as an admin, which registering a global key needs.

    Depends on ``client`` so its app-level get_session override is installed first.
    """
    admin_user = User(
        username="admin_git_gate",
        email=None,
        name=None,
        oauth_provider=OAuthProvider.BASIC,
        is_admin=True,
    )
    session.add(admin_user)
    await session.commit()
    token = create_token(
        {"username": "admin_git_gate"},
        secret="a-fake-secretkey",
        iss="http://localhost:8000/",
        expires_delta=timedelta(hours=24),
    )
    async with AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {token}"},
    ) as authed:
        yield authed


@pytest.mark.asyncio
async def test_a_global_key_is_unaffected(
    client: AsyncClient,
    admin_client: AsyncClient,
) -> None:
    """No repository owns a global key, so no repository can be told to own it."""
    await _make_git_managed(client, "gitglobal")

    resp = await admin_client.post(
        "/metadata-schemas/",
        json={"key": "global_only_key", "json_schema": {"type": "string"}},
    )
    assert resp.status_code in (200, 201), resp.text
    assert resp.json()["namespace"] is None


@pytest.mark.asyncio
async def test_an_ordinary_namespace_is_unaffected(client: AsyncClient) -> None:
    """The gate is about repo-managed namespaces, not about the API."""
    await client.post("/namespaces/plain_ns")

    resp = await client.post(
        "/metadata-schemas/",
        json={
            "key": "system",
            "namespace": "plain_ns",
            "json_schema": {"type": "object"},
        },
    )
    assert resp.status_code in (200, 201), resp.text


@pytest.mark.asyncio
async def test_get_one_schema_by_id(client: AsyncClient) -> None:
    """An id you cannot fetch with is half an identifier.

    Delete has always taken one; without a matching read there was no way to
    confirm what you were about to remove.
    """
    created = await client.post(
        "/metadata-schemas/",
        json={
            "key": "grain",
            "namespace": "byid.ns",
            "json_schema": {"type": "string"},
        },
    )
    schema_id = created.json()["id"]

    resp = await client.get(f"/metadata-schemas/{schema_id}")
    assert resp.status_code == 200, resp.text
    assert resp.json()["key"] == "grain"
    assert resp.json()["namespace"] == "byid.ns"


@pytest.mark.asyncio
async def test_get_a_deleted_schema_is_a_404(client: AsyncClient) -> None:
    """Soft-deleted rows are invisible to reads, as they are to resolution."""
    created = await client.post(
        "/metadata-schemas/",
        json={
            "key": "gone",
            "namespace": "byid.ns",
            "json_schema": {"type": "string"},
        },
    )
    schema_id = created.json()["id"]
    assert (await client.delete(f"/metadata-schemas/{schema_id}")).status_code == 200

    assert (await client.get(f"/metadata-schemas/{schema_id}")).status_code == 404


@pytest.mark.asyncio
async def test_list_filters_by_key(client: AsyncClient) -> None:
    """Resolving an id for a key you already know should be one precise call.

    Without this the only route to a schema's id was listing everything and
    scanning, which is the friction that made ids feel like the wrong handle.
    """
    for key in ("wanted", "unwanted"):
        await client.post(
            "/metadata-schemas/",
            json={
                "key": key,
                "namespace": "bykey.ns",
                "json_schema": {"type": "string"},
            },
        )

    resp = await client.get("/metadata-schemas/?key=wanted&namespace=bykey.ns")
    assert resp.status_code == 200, resp.text
    assert [row["key"] for row in resp.json()] == ["wanted"]
