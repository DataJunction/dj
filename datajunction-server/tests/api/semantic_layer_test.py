"""Unit + integration tests for the semantic-layer REST API.

The unit tests cover the pure logic — filter translation and value quoting —
without a database. The DB-backed endpoint tests (list / view / sql against a
seeded cube) use the standard ``client`` fixture over the testcontainers
Postgres harness.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient

from datajunction_server.errors import DJException

from datajunction_server.api.semantic_layer import (
    FilterPayload,
    MAX_ROW_LIMIT,
    _filter_to_sql,
    _quote_value,
)


class TestFilterToSql:
    def test_equality_quotes_string_value(self):
        flt = FilterPayload(column="ns.dim", operator="=", value="North")
        assert _filter_to_sql(flt) == "ns.dim = 'North'"

    def test_is_null_omits_value(self):
        flt = FilterPayload(column="ns.dim", operator="IS NULL")
        assert _filter_to_sql(flt) == "ns.dim IS NULL"

    def test_comparison_operator_passes_through(self):
        flt = FilterPayload(column="ns.metric", operator=">=", value=10)
        assert _filter_to_sql(flt) == "ns.metric >= 10"

    def test_unsupported_operator_rejected_400(self):
        flt = FilterPayload(column="ns.dim", operator="LIKE", value="x%")
        with pytest.raises(DJException) as exc:
            _filter_to_sql(flt)
        assert exc.value.http_status_code == 400

    def test_missing_column_rejected_400(self):
        flt = FilterPayload(column=None, operator="=", value="x")
        with pytest.raises(DJException) as exc:
            _filter_to_sql(flt)
        assert exc.value.http_status_code == 400


class TestQuoteValue:
    def test_none_is_null(self):
        assert _quote_value(None) == "NULL"

    def test_bool_renders_sql_literal(self):
        assert _quote_value(True) == "TRUE"
        assert _quote_value(False) == "FALSE"

    def test_number_is_bare(self):
        assert _quote_value(42) == "42"

    def test_string_escapes_single_quotes(self):
        # DJ's parser uses Spark's grammar (BACKSLASH escaping); a doubled ''
        # would mis-lex into two adjacent strings. So the correct literal is
        # backslash-escaped.
        assert _quote_value("O'Brien") == "'O\\'Brien'"


# ---------------------------------------------------------------------------
# DB-backed integration tests (require the testcontainers Postgres harness)
# ---------------------------------------------------------------------------


async def _expect(resp, *codes):
    assert resp.status_code in codes, f"{resp.status_code}: {resp.text}"
    return resp


async def _setup_cube(client: AsyncClient) -> str:
    """Build a minimal materialization-free cube and return its node name.

    sem.sales (fact) --link region_id--> sem.region (dim over sem.regions);
    metric sem.total_amount = SUM(amount); cube sem.sales_cube over
    [sem.total_amount] x [sem.region.region_name]. Catalog ``semcat`` has only a
    trino engine, so the cube resolves to the trino dialect deterministically.
    """
    await _expect(
        await client.post("/catalogs/", json={"name": "semcat"}),
        200,
        201,
        409,
    )
    await _expect(
        await client.post(
            "/engines/",
            json={"name": "trino", "version": "451", "dialect": "trino"},
        ),
        200,
        201,
        409,
    )
    await _expect(
        await client.post(
            "/catalogs/semcat/engines/",
            json=[{"name": "trino", "version": "451"}],
        ),
        200,
        201,
    )
    await _expect(await client.post("/namespaces/sem"), 200, 201, 409)
    await _expect(
        await client.post(
            "/nodes/source/",
            json={
                "name": "sem.sales",
                "display_name": "Sales",
                "description": "",
                "mode": "published",
                "catalog": "semcat",
                "schema_": "sem",
                "table": "sales",
                "columns": [
                    {"name": "id", "type": "bigint"},
                    {"name": "region_id", "type": "bigint"},
                    {"name": "amount", "type": "double"},
                ],
            },
        ),
        200,
        201,
    )
    await _expect(
        await client.post(
            "/nodes/source/",
            json={
                "name": "sem.regions",
                "display_name": "Regions",
                "description": "",
                "mode": "published",
                "catalog": "semcat",
                "schema_": "sem",
                "table": "regions",
                "columns": [
                    {"name": "region_id", "type": "bigint"},
                    {"name": "region_name", "type": "string"},
                ],
            },
        ),
        200,
        201,
    )
    await _expect(
        await client.post(
            "/nodes/dimension/",
            json={
                "name": "sem.region",
                "display_name": "Region",
                "description": "",
                "mode": "published",
                "primary_key": ["region_id"],
                "query": "SELECT region_id, region_name FROM sem.regions",
            },
        ),
        200,
        201,
    )
    await _expect(
        await client.post(
            "/nodes/sem.sales/link/",
            json={
                "dimension_node": "sem.region",
                "join_type": "inner",
                "join_on": "sem.sales.region_id = sem.region.region_id",
            },
        ),
        200,
        201,
    )
    await _expect(
        await client.post(
            "/nodes/metric/",
            json={
                "name": "sem.total_amount",
                "display_name": "Total Amount",
                "description": "",
                "mode": "published",
                "query": "SELECT SUM(amount) FROM sem.sales",
            },
        ),
        200,
        201,
    )
    await _expect(
        await client.post(
            "/nodes/cube/",
            json={
                "name": "sem.sales_cube",
                "display_name": "Sales Cube",
                "description": "Sales by region",
                "mode": "published",
                "metrics": ["sem.total_amount"],
                "dimensions": ["sem.region.region_name"],
            },
        ),
        200,
        201,
    )
    return "sem.sales_cube"


@pytest.mark.asyncio
async def test_semantic_endpoints_end_to_end(client: AsyncClient):
    view = await _setup_cube(client)
    query = {
        "additional_configuration": {},
        "query": {
            "metrics": ["sem.total_amount"],
            "dimensions": ["sem.region.region_name"],
        },
    }

    # /views/list includes the cube.
    resp = await _expect(
        await client.post("/semantic/views/list", json={"runtime_configuration": {}}),
        200,
    )
    assert any(v["name"] == view for v in resp.json())

    # /views/{view} returns the cube's metrics and dimensions in spec shape.
    resp = await _expect(
        await client.post(
            f"/semantic/views/{view}",
            json={"additional_configuration": {}},
        ),
        200,
    )
    detail = resp.json()
    assert {m["id"] for m in detail["metrics"]} == {"sem.total_amount"}
    assert any(d["id"] == "sem.region.region_name" for d in detail["dimensions"])

    # /sql generates physical SQL, pinned to this cube, in the trino dialect.
    resp = await _expect(
        await client.post(f"/semantic/views/{view}/sql", json=query),
        200,
    )
    sql_body = resp.json()
    assert sql_body["dialect"] == "trino"
    assert "SELECT" in sql_body["sql"].upper()
    assert sql_body["columns"]

    # Unknown metric/dimension ids are rejected (400).
    resp = await client.post(
        f"/semantic/views/{view}/sql",
        json={"query": {"metrics": ["sem.not_a_metric"], "dimensions": []}},
    )
    assert resp.status_code == 400, resp.text

    # Dimension-only queries are rejected at the boundary (400).
    resp = await client.post(
        f"/semantic/views/{view}/sql",
        json={"query": {"metrics": [], "dimensions": ["sem.region.region_name"]}},
    )
    assert resp.status_code == 400, resp.text

    # Unknown view -> 404.
    resp = await client.post(
        "/semantic/views/sem.does_not_exist/sql",
        json={"query": {"metrics": ["sem.total_amount"], "dimensions": []}},
    )
    assert resp.status_code == 404, resp.text


# ---------------------------------------------------------------------------
# generate_query_sql request-validation rejections
#
# These fire before any cube lookup, so no seeded cube is required — the
# view-name path param can be arbitrary.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_sql_rejects_offset(client: AsyncClient):
    """`offset` present -> 400 (covers the ``if payload.offset`` branch)."""
    resp = await client.post(
        "/semantic/views/any_view/sql",
        json={"query": {"metrics": ["m"], "offset": 5}},
    )
    assert resp.status_code == 400, resp.text
    assert "offset" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_generate_sql_rejects_negative_limit(client: AsyncClient):
    """Negative `limit` -> 400 (covers the ``limit < 0`` branch)."""
    resp = await client.post(
        "/semantic/views/any_view/sql",
        json={"query": {"metrics": ["m"], "limit": -1}},
    )
    assert resp.status_code == 400, resp.text
    assert "non-negative" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_generate_sql_rejects_limit_over_max(client: AsyncClient):
    """`limit` > MAX_ROW_LIMIT -> 400 (covers the ``limit > MAX_ROW_LIMIT`` branch)."""
    resp = await client.post(
        "/semantic/views/any_view/sql",
        json={"query": {"metrics": ["m"], "limit": MAX_ROW_LIMIT + 1}},
    )
    assert resp.status_code == 400, resp.text
    assert str(MAX_ROW_LIMIT) in resp.json()["detail"]


# ---------------------------------------------------------------------------
# get_view unknown-view 404 (the ``cube_node is None`` branch)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_view_unknown_returns_404(client: AsyncClient):
    """An unknown view name on ``/views/{view}`` -> 404."""
    resp = await client.post(
        "/semantic/views/sem.no_such_view",
        json={"additional_configuration": {}},
    )
    assert resp.status_code == 404, resp.text
    assert "does not exist" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# DJException handlers (the ``except DJException`` branches) — forced via
# monkeypatch so the underlying call raises a DJException with a specific
# status code, and we assert the ``{status_code, detail}`` problem shape.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_views_djexception_returns_problem(
    client: AsyncClient,
    monkeypatch,
):
    """``Node.find`` raising DJException -> problem response in list_views."""
    monkeypatch.setattr(
        "datajunction_server.api.semantic_layer.Node.find",
        AsyncMock(
            side_effect=DJException(message="find blew up", http_status_code=418),
        ),
    )
    resp = await client.post("/semantic/views/list", json={"runtime_configuration": {}})
    assert resp.status_code == 418, resp.text
    assert resp.json() == {"status_code": 418, "detail": "find blew up"}


@pytest.mark.asyncio
async def test_get_view_djexception_returns_problem(
    client: AsyncClient,
    monkeypatch,
):
    """``Node.get_cube_by_name`` raising DJException -> problem in get_view."""
    monkeypatch.setattr(
        "datajunction_server.api.semantic_layer.Node.get_cube_by_name",
        AsyncMock(
            side_effect=DJException(message="cube blew up", http_status_code=422),
        ),
    )
    resp = await client.post(
        "/semantic/views/some_view",
        json={"additional_configuration": {}},
    )
    assert resp.status_code == 422, resp.text
    assert resp.json() == {"status_code": 422, "detail": "cube blew up"}


@pytest.mark.asyncio
async def test_generate_sql_djexception_returns_problem(
    client: AsyncClient,
    monkeypatch,
):
    """``generate_metrics_sql`` raising DJException -> problem in generate_query_sql.

    The request passes validation and cube-membership (a fake cube exposing the
    requested metric/dimension is returned) so execution reaches ``_generate_sql``,
    where the patched ``generate_metrics_sql`` raises.
    """
    fake_cube = SimpleNamespace(
        current=SimpleNamespace(
            cube_node_metrics=["sem.total_amount"],
            cube_node_dimensions=["sem.region.region_name"],
        ),
    )
    monkeypatch.setattr(
        "datajunction_server.api.semantic_layer.Node.get_cube_by_name",
        AsyncMock(return_value=fake_cube),
    )
    monkeypatch.setattr(
        "datajunction_server.api.semantic_layer.generate_metrics_sql",
        AsyncMock(
            side_effect=DJException(message="sql gen blew up", http_status_code=400),
        ),
    )
    resp = await client.post(
        "/semantic/views/sem.sales_cube/sql",
        json={
            "query": {
                "metrics": ["sem.total_amount"],
                "dimensions": ["sem.region.region_name"],
            },
        },
    )
    assert resp.status_code == 400, resp.text
    assert resp.json() == {"status_code": 400, "detail": "sql gen blew up"}
