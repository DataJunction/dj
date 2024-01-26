"""
Tests for whoami router
"""

from fastapi.testclient import TestClient

from datajunction_server.internal.access.authentication.tokens import decode_token


def test_whoami(client: TestClient):
    """
    Test /whoami endpoint
    """
    response = client.get("/whoami/")
    assert response.ok
    assert response.json()["username"] == "dj"


def test_short_lived_token(client: TestClient):
    """
    Test getting a short-lived token from the /token endpoint
    """
    response = client.get("/token/")
    assert response.ok
    data = response.json()
    user = decode_token(data["token"])
    assert user["username"] == "dj"
