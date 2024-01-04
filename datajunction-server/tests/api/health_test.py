"""
Tests for the healthcheck API.
"""

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session


def test_successful_health(client: TestClient) -> None:
    """
    Test ``GET /health/``.
    """
    response = client.get("/health/")
    data = response.json()
    assert data == [{"name": "database", "status": "ok"}]


def test_failed_health(session: Session, client: TestClient, mocker) -> None:
    """
    Test failed healthcheck.
    """

    session.execute = mocker.MagicMock()
    response = client.get("/health/")
    data = response.json()
    assert data == [{"name": "database", "status": "failed"}]
