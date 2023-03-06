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
            "name": "spark",
            "version": "3.3.1",
        },
    )
    data = response.json()
    assert response.status_code == 201
    assert data == {"name": "spark", "uri": None, "version": "3.3.1"}


def test_engine_list(
    client: TestClient,
) -> None:
    """
    Test listing engines
    """
    response = client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "2.4.4",
        },
    )
    assert response.status_code == 201

    response = client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "3.3.0",
        },
    )
    assert response.status_code == 201

    response = client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "3.3.1",
        },
    )
    assert response.status_code == 201

    response = client.get("/engines/")
    assert response.status_code == 200
    data = response.json()
    assert data == [
        {
            "name": "spark",
            "uri": None,
            "version": "3.1.1",
        },
        {
            "name": "spark",
            "uri": None,
            "version": "2.4.4",
        },
        {
            "name": "spark",
            "uri": None,
            "version": "3.3.0",
        },
        {
            "name": "spark",
            "uri": None,
            "version": "3.3.1",
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
            "name": "spark",
            "version": "3.3.1",
        },
    )
    assert response.status_code == 201

    response = client.get(
        "/engines/spark/3.3.1",
    )
    assert response.status_code == 200
    data = response.json()
    assert data == {"name": "spark", "uri": None, "version": "3.3.1"}


def test_engine_raise_on_engine_already_exists(
    client: TestClient,
) -> None:
    """
    Test raise on engine already exists
    """
    response = client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "3.3.1",
        },
    )
    assert response.status_code == 201

    response = client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "3.3.1",
        },
    )
    assert response.status_code == 409
    data = response.json()
    assert data == {"detail": "Engine already exists: `spark` version `3.3.1`"}
