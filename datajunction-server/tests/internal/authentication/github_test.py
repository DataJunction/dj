"""
Tests for GitHub OAuth helper functions
"""
from datajunction_server.internal.access.authentication import github


def test_get_authorize_url():
    """
    Test generating a GitHub OAuth authorize url for a GitHub app client ID
    """
    assert github.get_authorize_url("foo") == (
        "https://github.com/login/oauth/authorize?"
        "client_id=foo&scope=read:user&redirect_uri="
        "http://localhost:8000/github/token/"
    )
