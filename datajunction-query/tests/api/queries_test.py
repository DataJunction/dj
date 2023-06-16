"""
Tests for the queries API.
"""

import datetime
import json
from http import HTTPStatus
from unittest import mock

import msgpack
from fastapi.testclient import TestClient
from freezegun import freeze_time
from pytest_mock import MockerFixture
from sqlmodel import Session

from djqs.config import Settings
from djqs.engine import describe_table_via_spark, process_query
from djqs.models.catalog import Catalog
from djqs.models.engine import Engine
from djqs.models.query import (
    Query,
    QueryCreate,
    QueryState,
    Results,
    StatementResults,
    decode_results,
    encode_results,
)


def test_submit_query(session: Session, client: TestClient) -> None:
    """
    Test ``POST /queries/``.
    """
    engine = Engine(name="test_engine", version="1.0", uri="sqlite://")
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1 AS col",
    )
    payload = query_create.json(by_alias=True)
    assert payload == json.dumps(
        {
            "catalog_name": "test_catalog",
            "engine_name": "test_engine",
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
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_engine"
    assert data["engine_version"] == "1.0"
    assert data["submitted_query"] == "SELECT 1 AS col"
    assert data["executed_query"] == "SELECT 1 AS col"
    assert data["scheduled"] == "2021-01-01T00:00:00"
    assert data["started"] == "2021-01-01T00:00:00"
    assert data["finished"] == "2021-01-01T00:00:00"
    assert data["state"] == "FINISHED"
    assert data["progress"] == 1.0
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1 AS col"
    assert data["results"][0]["columns"] == [{"name": "col", "type": "STR"}]
    assert data["results"][0]["rows"] == [[1]]
    assert data["errors"] == []


def test_submit_query_msgpack(session: Session, client: TestClient) -> None:
    """
    Test ``POST /queries/`` using msgpack.
    """
    engine = Engine(name="test_engine", version="1.0", uri="sqlite://")
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1 AS col",
    )
    payload = query_create.dict(by_alias=True)
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
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_engine"
    assert data["engine_version"] == "1.0"
    assert data["submitted_query"] == "SELECT 1 AS col"
    assert data["executed_query"] == "SELECT 1 AS col"
    assert data["scheduled"] == datetime.datetime(2021, 1, 1)
    assert data["started"] == datetime.datetime(2021, 1, 1)
    assert data["finished"] == datetime.datetime(2021, 1, 1)
    assert data["state"] == "FINISHED"
    assert data["progress"] == 1.0
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1 AS col"
    assert data["results"][0]["columns"] == [{"name": "col", "type": "STR"}]
    assert data["results"][0]["rows"] == [[1]]
    assert data["errors"] == []


def test_submit_query_errors(
    session: Session,
    client: TestClient,
) -> None:
    """
    Test ``POST /queries/`` with missing/invalid content type.
    """
    engine = Engine(name="test_engine", version="1.0", uri="sqlite://")
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1 AS col",
    )
    payload = query_create.json(by_alias=True)

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


def test_submit_query_multiple_statements(session: Session, client: TestClient) -> None:
    """
    Test ``POST /queries/``.
    """
    engine = Engine(name="test_engine", version="1.0", uri="sqlite://")
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1 AS col; SELECT 2 AS another_col",
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=query_create.json(),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_engine"
    assert data["engine_version"] == "1.0"
    assert data["submitted_query"] == "SELECT 1 AS col; SELECT 2 AS another_col"
    assert data["executed_query"] == "SELECT 1 AS col; SELECT 2 AS another_col"
    assert data["scheduled"] == "2021-01-01T00:00:00"
    assert data["started"] == "2021-01-01T00:00:00"
    assert data["finished"] == "2021-01-01T00:00:00"
    assert data["state"] == "FINISHED"
    assert data["progress"] == 1.0
    assert len(data["results"]) == 2
    assert data["results"][0]["sql"] == "SELECT 1 AS col"
    assert data["results"][0]["columns"] == [{"name": "col", "type": "STR"}]
    assert data["results"][0]["rows"] == [[1]]
    assert data["results"][1]["sql"] == "SELECT 2 AS another_col"
    assert data["results"][1]["columns"] == [{"name": "another_col", "type": "STR"}]
    assert data["results"][1]["rows"] == [[2]]
    assert data["errors"] == []


def test_submit_query_results_backend(
    session: Session,
    settings: Settings,
    client: TestClient,
) -> None:
    """
    Test that ``POST /queries/`` stores results.
    """
    engine = Engine(name="test_engine", version="1.0", uri="sqlite://")
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1 AS col",
    )

    with freeze_time("2021-01-01T00:00:00Z"):
        response = client.post(
            "/queries/",
            data=query_create.json(),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()
    assert data == {
        "catalog_name": "test_catalog",
        "engine_name": "test_engine",
        "engine_version": "1.0",
        "id": mock.ANY,
        "submitted_query": "SELECT 1 AS col",
        "executed_query": "SELECT 1 AS col",
        "scheduled": "2021-01-01T00:00:00",
        "started": "2021-01-01T00:00:00",
        "finished": "2021-01-01T00:00:00",
        "state": "FINISHED",
        "progress": 1.0,
        "results": [
            {
                "sql": "SELECT 1 AS col",
                "columns": [{"name": "col", "type": "STR"}],
                "rows": [[1]],
                "row_count": 1,
            },
        ],
        "next": None,
        "previous": None,
        "errors": [],
    }
    cached = settings.results_backend.get(data["id"])
    assert json.loads(cached) == [
        {
            "sql": "SELECT 1 AS col",
            "columns": [{"name": "col", "type": "STR"}],
            "rows": [[1]],
            "row_count": 1,
        },
    ]


def test_submit_query_async(
    mocker: MockerFixture,
    session: Session,
    client: TestClient,
) -> None:
    """
    Test ``POST /queries/`` on an async database.
    """
    add_task = mocker.patch("fastapi.BackgroundTasks.add_task")

    engine = Engine(name="test_engine", version="1.0", uri="sqlite://")
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1 AS col",
        async_=True,
    )

    with freeze_time("2021-01-01T00:00:00Z", auto_tick_seconds=300):
        response = client.post(
            "/queries/",
            data=query_create.json(),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
    data = response.json()

    assert response.status_code == 201
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_engine"
    assert data["engine_version"] == "1.0"
    assert data["submitted_query"] == "SELECT 1 AS col"
    assert data["executed_query"] is None
    assert data["scheduled"] is None
    assert data["started"] is None
    assert data["finished"] is None
    assert data["state"] == "ACCEPTED"
    assert data["progress"] == 0.0
    assert data["results"] == []
    assert data["errors"] == []

    # check that ``BackgroundTasks.add_task`` was called
    add_task.assert_called()
    arguments = add_task.mock_calls[0].args
    assert arguments[0] == process_query  # pylint: disable=comparison-with-callable
    assert arguments[1] == session
    assert isinstance(arguments[2], Settings)
    assert isinstance(arguments[3], Query)


def test_submit_query_error(session: Session, client: TestClient) -> None:
    """
    Test submitting invalid query to ``POST /queries/``.
    """
    engine = Engine(name="test_engine", version="1.0", uri="sqlite://")
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT FROM",
        async_=False,
    )

    response = client.post(
        "/queries/",
        data=query_create.json(),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_engine"
    assert data["engine_version"] == "1.0"
    assert data["submitted_query"] == "SELECT FROM"
    assert data["executed_query"] == "SELECT FROM"
    assert data["state"] == "FAILED"
    assert data["progress"] == 0.0
    assert data["results"] == []
    assert data["errors"] == [
        '(sqlite3.OperationalError) near "FROM": syntax error\n'
        "[SQL: SELECT FROM]\n"
        "(Background on this error at: https://sqlalche.me/e/14/e3q8)",
    ]


def test_read_query(session: Session, settings: Settings, client: TestClient) -> None:
    """
    Test ``GET /queries/{query_id}``.
    """
    engine = Engine(name="test_engine", version="1.0", uri="sqlite://")
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query = Query(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1",
        executed_query="SELECT 1",
        state=QueryState.RUNNING,
        progress=0.5,
        async_=False,
    )
    session.add(query)
    session.commit()
    session.refresh(query)

    results = Results(
        __root__=[
            StatementResults(
                sql="SELECT 1",
                columns=[{"name": "col", "type": "STR"}],
                rows=[[1]],
            ),
        ],
    )
    settings.results_backend.add(str(query.id), results.json())

    response = client.get(f"/queries/{query.id}")
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_engine"
    assert data["engine_version"] == "1.0"
    assert data["submitted_query"] == "SELECT 1"
    assert data["executed_query"] == "SELECT 1"
    assert data["state"] == "RUNNING"
    assert data["progress"] == 0.5
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1"
    assert data["results"][0]["columns"] == [{"name": "col", "type": "STR"}]
    assert data["results"][0]["rows"] == [[1]]
    assert data["errors"] == []

    response = client.get("/queries/27289db6-a75c-47fc-b451-da59a743a168")
    assert response.status_code == 404

    response = client.get("/queries/123")
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


def test_read_query_no_results_backend(session: Session, client: TestClient) -> None:
    """
    Test ``GET /queries/{query_id}``.
    """
    engine = Engine(name="test_engine", version="1.0", uri="sqlite://")
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query = Query(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1",
        executed_query="SELECT 1",
        state=QueryState.RUNNING,
        progress=0.5,
        async_=False,
    )
    session.add(query)
    session.commit()
    session.refresh(query)

    response = client.get(f"/queries/{query.id}")
    data = response.json()

    assert response.status_code == 200
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_engine"
    assert data["engine_version"] == "1.0"
    assert data["submitted_query"] == "SELECT 1"
    assert data["executed_query"] == "SELECT 1"
    assert data["state"] == "RUNNING"
    assert data["progress"] == 0.5
    assert data["results"] == []
    assert data["errors"] == []

    response = client.get("/queries/27289db6-a75c-47fc-b451-da59a743a168")
    assert response.status_code == 404

    response = client.get("/queries/123")


def test_submit_spark_query(session: Session, client: TestClient) -> None:
    """
    Test submitting a Spark query
    """
    engine = Engine(name="test_spark_engine", version="3.3.2", uri="spark://local[*]")
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1 AS int_col, 'a' as str_col",
    )
    payload = query_create.json(by_alias=True)
    assert payload == json.dumps(
        {
            "catalog_name": "test_catalog",
            "engine_name": "test_spark_engine",
            "engine_version": "3.3.2",
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
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_spark_engine"
    assert data["engine_version"] == "3.3.2"
    assert data["submitted_query"] == "SELECT 1 AS int_col, 'a' as str_col"
    assert data["executed_query"] == "SELECT 1 AS int_col, 'a' as str_col"
    assert data["scheduled"] == "2021-01-01T00:00:00"
    assert data["started"] == "2021-01-01T00:00:00"
    assert data["finished"] == "2021-01-01T00:00:00"
    assert data["state"] == "FINISHED"
    assert data["progress"] == 1.0
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1 AS int_col, 'a' as str_col"
    assert data["results"][0]["columns"] == []
    assert data["results"][0]["rows"] == [[1, "a"]]
    assert data["errors"] == []


def test_spark_fixture_show_tables(spark) -> None:
    """
    Test that show tables of the spark fixture lists the roads tables
    """
    spark_df = spark.sql("show tables")
    records = spark_df.rdd.map(tuple).collect()
    assert records == [
        ("", "contractors", True),
        ("", "dispatchers", True),
        ("", "hard_hat_state", True),
        ("", "hard_hats", True),
        ("", "municipality", True),
        ("", "municipality_municipality_type", True),
        ("", "municipality_type", True),
        ("", "repair_order_details", True),
        ("", "repair_orders", True),
        ("", "repair_type", True),
        ("", "us_region", True),
        ("", "us_states", True),
    ]


def test_spark_describe_tables(spark) -> None:
    """
    Test that using spark to describe tables works
    """
    column_metadata = describe_table_via_spark(spark, None, "contractors")
    assert column_metadata == [
        {"name": "contractor_id", "type": "int"},
        {"name": "company_name", "type": "string"},
        {"name": "contact_name", "type": "string"},
        {"name": "contact_title", "type": "string"},
        {"name": "address", "type": "string"},
        {"name": "city", "type": "string"},
        {"name": "state", "type": "string"},
        {"name": "postal_code", "type": "string"},
        {"name": "country", "type": "string"},
        {"name": "phone", "type": "string"},
    ]


def test_spark_fixture_query(spark) -> None:
    """
    Test a spark query against the roads database in the spark fixture
    """
    spark_df = spark.sql(
        """
        SELECT ro.repair_order_id, ro.order_date
        FROM repair_orders ro
        LEFT JOIN repair_order_details roi
        ON ro.repair_order_id = roi.repair_order_id
        ORDER BY ro.repair_order_id
        LIMIT 5
    """,
    )
    records = spark_df.rdd.map(tuple).collect()
    assert records == [
        (10001, datetime.date(2007, 7, 4)),
        (10002, datetime.date(2007, 7, 5)),
        (10003, datetime.date(2007, 7, 8)),
        (10004, datetime.date(2007, 7, 8)),
        (10005, datetime.date(2007, 7, 9)),
    ]


@mock.patch("djqs.engine.duckdb.connect")
def test_submit_duckdb_query(
    mock_duckdb_connect,
    session: Session,
    client: TestClient,
    duckdb_conn,
) -> None:
    """
    Test submitting a Spark query
    """
    mock_duckdb_connect.return_value = duckdb_conn
    engine = Engine(name="test_duckdb_engine", version="0.7.1", uri="duckdb://local[*]")
    catalog = Catalog(name="test_catalog", engines=[engine])
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    query_create = QueryCreate(
        catalog_name=catalog.name,
        engine_name=engine.name,
        engine_version=engine.version,
        submitted_query="SELECT 1 AS int_col, 'a' as str_col",
    )
    payload = query_create.json(by_alias=True)
    assert payload == json.dumps(
        {
            "catalog_name": "test_catalog",
            "engine_name": "test_duckdb_engine",
            "engine_version": "0.7.1",
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
    assert data["catalog_name"] == "test_catalog"
    assert data["engine_name"] == "test_duckdb_engine"
    assert data["engine_version"] == "0.7.1"
    assert data["submitted_query"] == "SELECT 1 AS int_col, 'a' as str_col"
    assert data["executed_query"] == "SELECT 1 AS int_col, 'a' as str_col"
    assert data["scheduled"] == "2021-01-01T00:00:00"
    assert data["started"] == "2021-01-01T00:00:00"
    assert data["finished"] == "2021-01-01T00:00:00"
    assert data["state"] == "FINISHED"
    assert data["progress"] == 1.0
    assert len(data["results"]) == 1
    assert data["results"][0]["sql"] == "SELECT 1 AS int_col, 'a' as str_col"
    assert data["results"][0]["columns"] == []
    assert data["results"][0]["rows"] == [[1, "a"]]
    assert data["errors"] == []
