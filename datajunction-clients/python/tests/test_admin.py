"""Tests DJ client"""

import pytest

from datajunction import DJAdmin
from datajunction.exceptions import DJClientException


class TestDJAdmin:  # pylint: disable=too-many-public-methods
    """
    Tests for DJ client/builder functionality.
    """

    @pytest.fixture
    def client(self, module__session_with_examples):
        """
        Returns a DJ client instance
        """
        return DJAdmin(requests_session=module__session_with_examples)  # type: ignore

    #
    # Data Catalogs
    #
    def test_get_catalog(self, client):
        """
        Check that `client.get_catalog()` works as expected.
        """
        result = client.get_catalog(name="default")
        assert result == {
            "name": "default",
            "engines": [
                {"name": "spark", "version": "3.1.1", "uri": None, "dialect": "spark"},
            ],
        }

    def test_add_catalog(self, client):
        """
        Check that `client.add_catalog()` works as expected.
        """
        # check not exists
        result = client.get_catalog(name="foo-bar-baz")
        assert "Catalog with name `foo-bar-baz` does not exist." in result["message"]
        # add catalog
        result = client.add_catalog(name="foo-bar-baz")
        assert result is None
        # check does exist
        result = client.get_catalog(name="foo-bar-baz")
        assert result == {
            "name": "foo-bar-baz",
            "engines": [],
        }

    #
    # Database Engines
    #
    def test_get_engine(self, client):
        """
        Check that `client.get_engine()` works as expected.
        """
        result = client.get_engine(name="spark", version="3.1.1")
        assert result == {
            "dialect": "spark",
            "name": "spark",
            "uri": None,
            "version": "3.1.1",
        }

    def test_add_engine(self, client):
        """
        Check that `client.get_engine()` works as expected.
        """
        # check not exist
        result = client.get_engine(name="8-cylinder", version="7.11")
        assert "Engine not found: `8-cylinder` version `7.11`" in result["detail"]
        # add engine
        client.add_engine(
            name="8-cylinder",
            version="7.11",
            uri="go/to-the-corner",
            dialect="trino",
        )
        # check not exist
        result = client.get_engine(name="8-cylinder", version="7.11")
        assert result == {
            "dialect": "trino",
            "name": "8-cylinder",
            "uri": "go/to-the-corner",
            "version": "7.11",
        }

    def test_link_engine_to_catalog(self, client):
        """
        Check that `client.get_engine()` works as expected.
        """
        # try to link engine to catalog
        with pytest.raises(DJClientException) as exc:
            result = client.link_engine_to_catalog(
                engine="12-cylinder",
                version="7.11",
                catalog="public",
            )
        assert "Engine not found: `12-cylinder` version `7.11`" in str(exc)
        # add engine
        client.add_engine(
            name="12-cylinder",
            version="7.11",
            uri="go/to-the-corner",
            dialect="trino",
        )
        # try to link engine to catalog (again)
        result = client.link_engine_to_catalog(
            engine="12-cylinder",
            version="7.11",
            catalog="public",
        )
        assert result is None
        # check the catalog
        result = client.get_catalog(name="public")
        assert result == {
            "engines": [
                {"dialect": None, "name": "postgres", "uri": None, "version": "15.2"},
                {
                    "dialect": "trino",
                    "name": "12-cylinder",
                    "uri": "go/to-the-corner",
                    "version": "7.11",
                },
            ],
            "name": "public",
        }
