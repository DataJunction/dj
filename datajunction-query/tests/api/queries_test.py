"""
Tests for the queries API.
"""

import datetime
import json
from dataclasses import asdict
from http import HTTPStatus
from unittest import mock

import msgpack
from fastapi.testclient import TestClient
from freezegun import freeze_time
from pytest_mock import MockerFixture

from djqs.config import Settings
from djqs.engine import process_query
from djqs.models.query import (
    Query,
    QueryCreate,
    QueryState,
    StatementResults,
    decode_results,
    encode_results,
)
from djqs.utils import get_settings


def test_submit_query_default_engine(client: TestClient) -> None:
    """
    Test ``POST /queries/``.
    """
    query_create = QueryCreate(
        catalog_name="warehouse_inmemory",
        submitted_query="SELECT 1 AS col",
    )
    payload = json.dumps(asdict(query_create))
    assert payload == json.dumps(
        {
            "catalog_name": "warehouse_inmemory",
            "engine_name": None,
            "engine_version": None,
            "submitted_query": "SELECT 1 AS col",
            "async_": False,
        },
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "warehouse_inmemory"
    assert data["engine_name"] == "duckdb_inmemory"
    assert data["engine_version"] == "0.7.1"
    assert data["submitted_query"] == "SELECT 1 AS col"
    assert data["executed_query"] == "SELECT 1 AS col"
    assert data["scheduled"] == "2021-01-01 00:00:00+00:00"
    assert data["started"] == "2021-01-01 00:00:00+00:00"
    assert data["finished"] == "2021-01-01 00:00:00+00:00"
    assert data["state"] == QueryState.FINISHED.value
    assert data["progress"] == 1.0
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1 AS col"
    assert data["results"][0]["rows"] == [[1]]
    assert data["errors"] == []


def test_submit_query(client: TestClient) -> None:
    """
    Test ``POST /queries/``.
    """
    query_create = QueryCreate(
        catalog_name="warehouse_inmemory",
        engine_name="duckdb_inmemory",
        engine_version="0.7.1",
        submitted_query="SELECT 1 AS col",
    )
    payload = json.dumps(asdict(query_create))
    assert payload == json.dumps(
        {
            "catalog_name": "warehouse_inmemory",
            "engine_name": "duckdb_inmemory",
            "engine_version": "0.7.1",
            "submitted_query": "SELECT 1 AS col",
            "async_": False,
        },
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "warehouse_inmemory"
    assert data["engine_name"] == "duckdb_inmemory"
    assert data["engine_version"] == "0.7.1"
    assert data["submitted_query"] == "SELECT 1 AS col"
    assert data["executed_query"] == "SELECT 1 AS col"
    assert data["scheduled"] == "2021-01-01 00:00:00+00:00"
    assert data["started"] == "2021-01-01 00:00:00+00:00"
    assert data["finished"] == "2021-01-01 00:00:00+00:00"
    assert data["state"] == QueryState.FINISHED.value
    assert data["progress"] == 1.0
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1 AS col"
    assert data["results"][0]["rows"] == [[1]]
    assert data["errors"] == []


def test_submit_query_generic_sqlalchemy(client: TestClient) -> None:
    """
    Test ``POST /queries/``.
    """
    query_create = QueryCreate(
        catalog_name="sqlite_warehouse",
        engine_name="sqlite_inmemory",
        engine_version="1.0",
        submitted_query="SELECT 1 AS col",
    )
    payload = json.dumps(asdict(query_create))
    assert payload == json.dumps(
        {
            "catalog_name": "sqlite_warehouse",
            "engine_name": "sqlite_inmemory",
            "engine_version": "1.0",
            "submitted_query": "SELECT 1 AS col",
            "async_": False,
        },
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "sqlite_warehouse"
    assert data["engine_name"] == "sqlite_inmemory"
    assert data["engine_version"] == "1.0"
    assert data["submitted_query"] == "SELECT 1 AS col"
    assert data["executed_query"] == "SELECT 1 AS col"
    assert data["scheduled"] == "2021-01-01 00:00:00+00:00"
    assert data["started"] == "2021-01-01 00:00:00+00:00"
    assert data["finished"] == "2021-01-01 00:00:00+00:00"
    assert data["state"] == QueryState.FINISHED.value
    assert data["progress"] == 1.0
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1 AS col"
    assert data["results"][0]["rows"] == [[1]]
    assert data["errors"] == []


def test_submit_query_with_sqlalchemy_uri_header(
    client: TestClient,
) -> None:
    """
    Test ``POST /queries/`` with the SQLALCHEMY_URI defined in the header.
    """
    query_create = QueryCreate(
        catalog_name="warehouse_inmemory",
        engine_name="duckdb_inmemory",
        engine_version="0.7.1",
        submitted_query="SELECT 1 AS col",
    )
    payload = json.dumps(asdict(query_create))
    assert payload == json.dumps(
        {
            "catalog_name": "warehouse_inmemory",
            "engine_name": "duckdb_inmemory",
            "engine_version": "0.7.1",
            "submitted_query": "SELECT 1 AS col",
            "async_": False,
        },
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "SQLALCHEMY_URI": "trino://example@foo.bar/catalog/schema",
            },
        )
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "warehouse_inmemory"
    assert data["engine_name"] == "duckdb_inmemory"
    assert data["engine_version"] == "0.7.1"
    assert data["submitted_query"] == "SELECT 1 AS col"
    assert data["executed_query"] == "SELECT 1 AS col"
    assert data["scheduled"] == "2021-01-01 00:00:00+00:00"
    assert data["started"] == "2021-01-01 00:00:00+00:00"
    assert data["finished"] == "2021-01-01 00:00:00+00:00"
    assert data["state"] == QueryState.FINISHED.value
    assert data["progress"] == 1.0
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1 AS col"
    assert data["results"][0]["rows"] == [[1]]
    assert data["errors"] == []


def test_submit_query_msgpack(client: TestClient) -> None:
    """
    Test ``POST /queries/`` using msgpack.
    """
    query_create = QueryCreate(
        catalog_name="warehouse_inmemory",
        engine_name="duckdb_inmemory",
        engine_version="0.7.1",
        submitted_query="SELECT 1 AS col",
    )
    payload = json.dumps(asdict(query_create))
    data = msgpack.packb(payload, default=encode_results)

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=data,
            headers={
                "Content-Type": "application/msgpack",
                "Accept": "application/msgpack; q=1.0, application/json; q=0.5",
            },
        )
    data = msgpack.unpackb(response.content, ext_hook=decode_results)

    assert response.headers.get("content-type") == "application/msgpack"
    assert response.status_code == 200
    assert data["catalog_name"] == "warehouse_inmemory"
    assert data["engine_name"] == "duckdb_inmemory"
    assert data["engine_version"] == "0.7.1"
    assert data["submitted_query"] == "SELECT 1 AS col"
    assert data["executed_query"] == "SELECT 1 AS col"
    assert data["scheduled"] == datetime.datetime(
        2021,
        1,
        1,
        tzinfo=datetime.timezone.utc,
    )
    assert data["started"] == datetime.datetime(
        2021,
        1,
        1,
        tzinfo=datetime.timezone.utc,
    )
    assert data["finished"] == datetime.datetime(
        2021,
        1,
        1,
        tzinfo=datetime.timezone.utc,
    )
    assert data["state"] == QueryState.FINISHED.value
    assert data["progress"] == 1.0
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1 AS col"
    assert data["results"][0]["rows"] == [[1]]
    assert data["errors"] == []


def test_submit_query_errors(
    client: TestClient,
) -> None:
    """
    Test ``POST /queries/`` with missing/invalid content type.
    """
    query_create = QueryCreate(
        catalog_name="warehouse_inmemory",
        engine_name="duckdb_inmemory",
        engine_version="0.7.1",
        submitted_query="SELECT 1 AS col",
    )
    payload = json.dumps(asdict(query_create))

    response = client.post(
        "/queries/",
        data=payload,
        headers={"Accept": "application/json"},
    )
    assert response.status_code == 400
    assert response.json() == {"detail": "Content type must be specified"}

    response = client.post(
        "/queries/",
        data=payload,
        headers={
            "Content-Type": "application/protobuf",
            "Accept": "application/json",
        },
    )
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "detail": "Content type not accepted: application/protobuf",
    }

    response = client.post(
        "/queries/",
        data=payload,
        headers={"Content-Type": "application/json", "Accept": "application/protobuf"},
    )
    assert response.status_code == 406
    assert response.json() == {
        "detail": "Client MUST accept: application/json, application/msgpack",
    }


def test_submit_query_multiple_statements(
    client: TestClient,
) -> None:
    """
    Test ``POST /queries/``.
    """
    query_create = QueryCreate(
        catalog_name="warehouse_inmemory",
        engine_name="duckdb_inmemory",
        engine_version="0.7.1",
        submitted_query="SELECT 1 AS col; SELECT 2 AS another_col",
    )

    payload = json.dumps(asdict(query_create))
    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "warehouse_inmemory"
    assert data["engine_name"] == "duckdb_inmemory"
    assert data["engine_version"] == "0.7.1"
    assert data["submitted_query"] == "SELECT 1 AS col; SELECT 2 AS another_col"
    assert data["executed_query"] == "SELECT 1 AS col; SELECT 2 AS another_col"
    assert data["scheduled"] == "2021-01-01 00:00:00+00:00"
    assert data["started"] == "2021-01-01 00:00:00+00:00"
    assert data["finished"] == "2021-01-01 00:00:00+00:00"
    assert data["state"] == QueryState.FINISHED.value
    assert data["progress"] == 1.0
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1 AS col; SELECT 2 AS another_col"
    assert data["results"][0]["rows"] == [[2]]
    assert data["errors"] == []


def test_submit_query_results_backend(
    client: TestClient,
) -> None:
    """
    Test that ``POST /queries/`` stores results.
    """
    query_create = QueryCreate(
        catalog_name="warehouse_inmemory",
        engine_name="duckdb_inmemory",
        engine_version="0.7.1",
        submitted_query="SELECT 1 AS col",
    )
    payload = json.dumps(asdict(query_create))
    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()
    assert data == {
        "catalog_name": "warehouse_inmemory",
        "engine_name": "duckdb_inmemory",
        "engine_version": "0.7.1",
        "id": mock.ANY,
        "submitted_query": "SELECT 1 AS col",
        "executed_query": "SELECT 1 AS col",
        "scheduled": "2021-01-01 00:00:00+00:00",
        "started": "2021-01-01 00:00:00+00:00",
        "finished": "2021-01-01 00:00:00+00:00",
        "state": QueryState.FINISHED.value,
        "progress": 1.0,
        "results": [
            {
                "sql": "SELECT 1 AS col",
                "columns": mock.ANY,
                "rows": [[1]],
                "row_count": 1,
            },
        ],
        "next": None,
        "previous": None,
        "errors": [],
        "async_": False,
    }
    settings = get_settings()
    cached = settings.results_backend.get(data["id"])
    assert json.loads(cached) == [
        {
            "sql": "SELECT 1 AS col",
            "columns": mock.ANY,
            "rows": [[1]],
            "row_count": 1,
        },
    ]


def test_submit_query_async(
    mocker: MockerFixture,
    client: TestClient,
) -> None:
    """
    Test ``POST /queries/`` on an async database.
    """
    add_task = mocker.patch("fastapi.BackgroundTasks.add_task")
    query_create = QueryCreate(
        catalog_name="warehouse_inmemory",
        engine_name="duckdb_inmemory",
        engine_version="0.7.1",
        submitted_query="SELECT 1 AS col",
        async_=True,
    )
    payload = json.dumps(asdict(query_create))
    response = client.post(
        "/queries/",
        data=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    data = response.json()

    assert response.status_code == 201
    assert data["catalog_name"] == "warehouse_inmemory"
    assert data["engine_name"] == "duckdb_inmemory"
    assert data["engine_version"] == "0.7.1"
    assert data["submitted_query"] == "SELECT 1 AS col"
    assert data["executed_query"] is None
    assert data["scheduled"] is None
    assert data["started"] is None
    assert data["finished"] is None
    assert data["state"] == QueryState.SCHEDULED.value
    assert data["progress"] == 0.0
    assert data["results"] == []
    assert data["errors"] == []

    # check that ``BackgroundTasks.add_task`` was called
    add_task.assert_called()
    arguments = add_task.mock_calls[0].args
    assert arguments[0] == process_query  # pylint: disable=comparison-with-callable
    assert isinstance(arguments[1], Settings)
    assert isinstance(arguments[3], Query)


def test_submit_query_error(client: TestClient) -> None:
    """
    Test submitting invalid query to ``POST /queries/``.
    """
    query_create = QueryCreate(
        catalog_name="warehouse_inmemory",
        engine_name="duckdb_inmemory",
        engine_version="0.7.1",
        submitted_query="SELECT FROM",
        async_=False,
    )

    payload = json.dumps(asdict(query_create))
    response = client.post(
        "/queries/",
        data=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "warehouse_inmemory"
    assert data["engine_name"] == "duckdb_inmemory"
    assert data["engine_version"] == "0.7.1"
    assert data["submitted_query"] == "SELECT FROM"
    assert data["executed_query"] == "SELECT FROM"
    assert data["state"] == QueryState.FAILED.value
    assert data["progress"] == 0.0
    assert data["results"] == []
    assert "Parser Error: syntax error at end of input" in data["errors"][0]


def test_read_query(client: TestClient) -> None:
    """
    Test ``GET /queries/{query_id}``.
    """
    query_create = QueryCreate(
        catalog_name="warehouse_inmemory",
        engine_name="duckdb_inmemory",
        engine_version="0.7.1",
        submitted_query="SELECT 1 AS col",
    )
    payload = json.dumps(asdict(query_create))
    assert payload == json.dumps(
        {
            "catalog_name": "warehouse_inmemory",
            "engine_name": "duckdb_inmemory",
            "engine_version": "0.7.1",
            "submitted_query": "SELECT 1 AS col",
            "async_": False,
        },
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()

    results = [
        StatementResults(
            sql="SELECT 2 as foo",
            columns=[{"name": "foo", "type": "STR"}],  # type: ignore
            rows=[[2]],  # type: ignore
        ),
    ]
    settings = get_settings()
    settings.results_backend.set(
        str(data["id"]),
        json.dumps([asdict(result) for result in results]),
    )
    response = client.get(f"/queries/{data['id']}")
    data = response.json()

    assert response.status_code == 200

    # Make sure the results are pulling from the cache, evidenced by the fact that it pulled
    # the data that was manually set in the cache for this query ID
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 2 as foo"
    assert data["results"][0]["columns"] == [{"name": "foo", "type": "STR"}]
    assert data["results"][0]["rows"] == [[2]]
    assert data["errors"] == []

    response = client.get("/queries/27289db6-a75c-47fc-b451-da59a743a168")
    assert response.status_code == 404

    response = client.get("/queries/123")
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


def test_submit_duckdb_query(client: TestClient) -> None:
    """
    Test submitting a duckdb query
    """
    query_create = QueryCreate(
        catalog_name="warehouse_inmemory",
        engine_name="duckdb_inmemory",
        engine_version="0.7.1",
        submitted_query="SELECT 1 AS col",
    )
    payload = json.dumps(asdict(query_create))
    assert payload == json.dumps(
        {
            "catalog_name": "warehouse_inmemory",
            "engine_name": "duckdb_inmemory",
            "engine_version": "0.7.1",
            "submitted_query": "SELECT 1 AS col",
            "async_": False,
        },
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "warehouse_inmemory"
    assert data["engine_name"] == "duckdb_inmemory"
    assert data["engine_version"] == "0.7.1"
    assert data["submitted_query"] == "SELECT 1 AS col"
    assert data["executed_query"] == "SELECT 1 AS col"
    assert data["scheduled"] == "2021-01-01 00:00:00+00:00"
    assert data["started"] == "2021-01-01 00:00:00+00:00"
    assert data["finished"] == "2021-01-01 00:00:00+00:00"
    assert data["state"] == QueryState.FINISHED.value
    assert data["progress"] == 1.0
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1 AS col"
    assert data["results"][0]["rows"] == [[1]]
    assert data["errors"] == []


@mock.patch("djqs.engine.snowflake.connector")
def test_submit_snowflake_query(
    mock_snowflake_connect,
    client: TestClient,
) -> None:
    """
    Test submitting a Snowflake query
    """
    mock_exec = mock.MagicMock()
    mock_exec.fetchall.return_value = [[1, "a"]]
    mock_cur = mock.MagicMock()
    mock_cur.execute.return_value = mock_exec
    mock_conn = mock.MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_snowflake_connect.connect.return_value = mock_conn

    query_create = QueryCreate(
        catalog_name="snowflake_warehouse",
        engine_name="snowflake_test",
        engine_version="7.37",
        submitted_query="SELECT 1 AS int_col, 'a' as str_col",
    )
    payload = json.dumps(asdict(query_create))
    assert payload == json.dumps(
        {
            "catalog_name": "snowflake_warehouse",
            "engine_name": "snowflake_test",
            "engine_version": "7.37",
            "submitted_query": "SELECT 1 AS int_col, 'a' as str_col",
            "async_": False,
        },
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "snowflake_warehouse"
    assert data["engine_name"] == "snowflake_test"
    assert data["engine_version"] == "7.37"
    assert data["submitted_query"] == "SELECT 1 AS int_col, 'a' as str_col"
    assert data["executed_query"] == "SELECT 1 AS int_col, 'a' as str_col"
    assert data["scheduled"] == "2021-01-01 00:00:00+00:00"
    assert data["started"] == "2021-01-01 00:00:00+00:00"
    assert data["finished"] == "2021-01-01 00:00:00+00:00"
    assert data["state"] == QueryState.FINISHED.value
    assert data["progress"] == 1.0
    assert data["errors"] == []
