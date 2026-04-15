"""Tests for user load options."""

from datajunction_server.api.graphql.resolvers.users import user_load_options


def test_user_load_options_with_fields():
    """Requesting specific fields returns load_only + noloads."""
    options = user_load_options({"username": None})
    assert len(options) == 4  # load_only + 3 noloads


def test_user_load_options_empty_fields():
    """Empty/None requested_fields returns no options."""
    assert user_load_options(None) == []
    assert user_load_options({}) == []
