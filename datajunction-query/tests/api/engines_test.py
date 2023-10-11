"""
Tests for the engine API.
"""

from fastapi.testclient import TestClient


def test_engine_adding_a_new_engine(
    client: TestClient,
) -> None:
    """
    Test adding an engine
    """
    response = client.post(
        "/engines/",
        json={
            "name": "foo",
            "type": "duckdb",
            "version": "1.0",
            "uri": "bar",
        },
    )
    data = response.json()
    assert response.status_code == 201
    assert data == {
        "name": "foo",
        "version": "1.0",
        "type": "duckdb",
        "extra_params": {},
    }


def test_engine_list(
    client: TestClient,
) -> None:
    """
    Test listing engines
    """
    response = client.post(
        "/engines/",
        json={"name": "foo", "type": "duckdb", "version": "1.0", "uri": "bar"},
    )
    assert response.status_code == 201

    response = client.post(
        "/engines/",
        json={
            "name": "foo",
            "type": "duckdb",
            "version": "1.1",
            "uri": "baz",
        },
    )
    assert response.status_code == 201

    response = client.post(
        "/engines/",
        json={
            "name": "foo",
            "type": "duckdb",
            "version": "1.2",
            "uri": "qux",
        },
    )
    assert response.status_code == 201

    response = client.get("/engines/")
    assert response.status_code == 200
    data = response.json()
    assert data == [
        {
            "name": "foo",
            "version": "1.0",
            "type": "duckdb",
            "extra_params": {},
            "uri": "bar",
        },
        {
            "name": "foo",
            "version": "1.1",
            "type": "duckdb",
            "extra_params": {},
            "uri": "baz",
        },
        {
            "name": "foo",
            "version": "1.2",
            "type": "duckdb",
            "extra_params": {},
            "uri": "qux",
        },
    ]


def test_engine_get_engine(
    client: TestClient,
) -> None:
    """
    Test getting an engine
    """
    response = client.post(
        "/engines/",
        json={
            "name": "foo",
            "type": "duckdb",
            "version": "1.0",
            "uri": "bar",
        },
    )
    assert response.status_code == 201

    response = client.get(
        "/engines/foo/1.0",
    )
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "name": "foo",
        "version": "1.0",
        "type": "duckdb",
        "extra_params": {},
    }


def test_engine_raise_on_engine_already_exists(
    client: TestClient,
) -> None:
    """
    Test raise on engine already exists
    """
    response = client.post(
        "/engines/",
        json={
            "name": "foo",
            "type": "duckdb",
            "version": "1.0",
            "uri": "bar",
        },
    )
    assert response.status_code == 201

    response = client.post(
        "/engines/",
        json={
            "name": "foo",
            "type": "duckdb",
            "version": "1.0",
            "uri": "bar",
        },
    )
    assert response.status_code == 409
    data = response.json()
    assert data == {"detail": "Engine already exists: `foo` version `1.0`"}
