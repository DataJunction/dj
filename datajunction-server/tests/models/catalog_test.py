"""
Tests for ``datajunction_server.models.catalog``.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datajunction_server.database.catalog import Catalog


def test_catalog_str_and_hash():
    """
    Test that a catalog instance works properly with str and hash
    """
    spark = Catalog(id=1, name="spark")
    assert str(spark) == "spark"
    assert hash(spark) == 1
    trino = Catalog(id=2, name="trino")
    assert str(trino) == "trino"
    assert hash(trino) == 2
    druid = Catalog(id=3, name="druid")
    assert str(druid) == "druid"
    assert hash(druid) == 3


@pytest.mark.asyncio
async def test_get_default_catalog_returns_none_when_not_configured():
    """get_default_catalog returns None when default_catalog_name is not set."""
    session = AsyncMock()
    with patch(
        "datajunction_server.database.catalog.get_settings",
    ) as mock_settings:
        mock_settings.return_value.seed_setup.default_catalog_name = None
        result = await Catalog.get_default_catalog(session)
    assert result is None


@pytest.mark.asyncio
async def test_get_default_catalog_returns_catalog_when_configured():
    """get_default_catalog returns the named catalog when configured."""
    mock_catalog = MagicMock(spec=Catalog)
    session = AsyncMock()
    with (
        patch(
            "datajunction_server.database.catalog.get_settings",
        ) as mock_settings,
        patch.object(Catalog, "get_by_name", return_value=mock_catalog) as mock_get,
    ):
        mock_settings.return_value.seed_setup.default_catalog_name = "my_catalog"
        result = await Catalog.get_default_catalog(session)
    mock_get.assert_awaited_once_with(session, "my_catalog")
    assert result == mock_catalog
