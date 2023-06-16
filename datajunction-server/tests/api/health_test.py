"""
Tests for the healthcheck API.
"""

from fastapi.testclient import TestClient
from sqlmodel import Session


def test_successful_health(client: TestClient) -> None:
    """
    Test ``GET /health/``.
    """
    response = client.get("/health/")
    data = response.json()
    assert data == [{"name": "database", "status": "ok"}]


def test_failed_health(session: Session, client: TestClient) -> None:
    """
    Test failed healthcheck.
    """

    session.execute = lambda: False
    response = client.get("/health/")
    data = response.json()
    assert data == [{"name": "database", "status": "failed"}]
