"""Test query_clients/__init__.py lazy import mechanism."""

import pytest


def test_lazy_import_snowflake_client():
    """Test lazy import of SnowflakeClient."""
    from datajunction_server.query_clients import SnowflakeClient

    # Should be able to import the class
    assert SnowflakeClient is not None


def test_lazy_import_nonexistent_client():
    """Test lazy import of non-existent client raises AttributeError."""
    import datajunction_server.query_clients as qc

    with pytest.raises(AttributeError) as exc_info:
        getattr(qc, "NonExistentClient")

    assert "has no attribute 'NonExistentClient'" in str(exc_info.value)


def test_getattr_implementation():
    """Test __getattr__ implementation directly."""
    import datajunction_server.query_clients as qc

    # Test successful lazy import
    SnowflakeClient = qc.__getattr__("SnowflakeClient")
    assert SnowflakeClient is not None

    # Test failed lazy import
    with pytest.raises(AttributeError):
        qc.__getattr__("NonExistentClient")


def test_all_exports():
    """Test that __all__ includes the expected exports."""
    from datajunction_server.query_clients import __all__

    expected = [
        "BaseQueryServiceClient",
        "HttpQueryServiceClient",
        "SnowflakeClient",
    ]

    assert all(item in __all__ for item in expected)
