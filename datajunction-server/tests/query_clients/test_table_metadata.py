"""Tests for TableMetadata and the query-client default."""

from dataclasses import dataclass, field
from typing import Any

import pytest
from pytest_mock import MockerFixture

from datajunction_server.database.column import Column
from datajunction_server.database.engine import Engine
from datajunction_server.errors import (
    DJDoesNotExistException,
    DJQueryServiceClientException,
)
from datajunction_server.models.table_metadata import TableMetadata, TableOwner
from datajunction_server.query_clients.http import HttpQueryServiceClient
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.parsing.types import IntegerType, StringType


@dataclass
class FakeResponse:
    """
    A stand-in for an ``httpx.Response``, typed so that a renamed attribute
    fails loudly instead of being silently absorbed by a bare mock.
    """

    status_code: int
    text: str = ""
    payload: dict[str, Any] = field(default_factory=dict)

    def json(self) -> dict[str, Any]:
        """The decoded response body."""
        return self.payload


class FakeClient:
    """Exercises the base-class default without a real backend."""

    def __init__(self, columns):
        self._columns = columns

    async def get_columns_for_table(
        self,
        catalog,
        schema,
        table,
        request_headers=None,
        engine=None,
    ):
        return self._columns


@pytest.mark.asyncio
async def test_base_default_returns_columns_and_no_owner():
    """A client that cannot supply an owner returns columns with owner=None."""
    from datajunction_server.query_clients.base import BaseQueryServiceClient

    class Client(FakeClient, BaseQueryServiceClient):
        """A minimal client relying on the inherited default."""

    cols = [Column(name="id", type=IntegerType(), order=0)]
    client = Client(cols)
    result = await client.get_table_metadata("c", "s", "t")
    assert result == TableMetadata(
        columns=cols,
        owner=None,
        partitions=[],
    )


def test_table_owner_defaults():
    """Only ``username`` is required; not every catalog reports the rest."""
    assert TableOwner(username="someone@example.com") == TableOwner(
        username="someone@example.com",
        email=None,
        display_name="",
    )


@pytest.mark.asyncio
async def test_query_service_client_returns_owner(mocker: MockerFixture):
    """A metadata response with an owner is surfaced in full."""
    client = QueryServiceClient(uri="http://queryservice:8001")
    request = mocker.patch.object(
        client,
        "_arequest",
        return_value=FakeResponse(
            status_code=200,
            payload={
                "name": "hive.test.pies",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "flavor", "type": "string"},
                ],
                "partitions": ["dateint"],
                "owner": {
                    "username": "owner@example.com",
                    "display_name": "Example Owner",
                },
            },
        ),
    )
    result = await client.get_table_metadata(
        "hive",
        "test",
        "pies",
        engine=Engine(name="spark", version="2.4.4"),
    )
    # ``Column`` is a SQLAlchemy model without ``__eq__``, so compare the
    # columns by value and everything else by whole-model equality.
    assert [(col.name, col.type, col.order) for col in result.columns] == [
        ("id", IntegerType(), 0),
        ("flavor", StringType(), 1),
    ]
    assert result.model_copy(update={"columns": []}) == TableMetadata(
        columns=[],
        owner=TableOwner(username="owner@example.com", display_name="Example Owner"),
        partitions=["dateint"],
    )
    request.assert_called_once_with(
        "GET",
        "/table/hive.test.pies/metadata/",
        params={"engine": "spark", "engine_version": "2.4.4"},
    )


@pytest.mark.asyncio
async def test_query_service_client_metadata_without_owner(mocker: MockerFixture):
    """A table with no resolvable owner reports owner=None, not an error."""
    client = QueryServiceClient(uri="http://queryservice:8001")
    request = mocker.patch.object(
        client,
        "_arequest",
        return_value=FakeResponse(
            status_code=200,
            payload={
                "name": "hive.test.pies",
                "columns": [{"name": "id", "type": "int"}],
            },
        ),
    )
    result = await client.get_table_metadata("hive", "test", "pies")
    assert [(col.name, col.type, col.order) for col in result.columns] == [
        ("id", IntegerType(), 0),
    ]
    assert result.model_copy(update={"columns": []}) == TableMetadata(
        columns=[],
        owner=None,
        partitions=[],
    )
    request.assert_called_once_with(
        "GET",
        "/table/hive.test.pies/metadata/",
        params=None,
    )


@pytest.mark.asyncio
async def test_query_service_client_falls_back_on_404(mocker: MockerFixture):
    """An older query service without the endpoint yields columns and owner=None."""
    client = QueryServiceClient(uri="http://queryservice:8001")
    mocker.patch.object(
        client,
        "_arequest",
        return_value=FakeResponse(status_code=404, text="Not Found"),
    )
    columns = [Column(name="id", type=IntegerType(), order=0)]
    fallback = mocker.patch.object(
        client,
        "get_columns_for_table",
        return_value=columns,
    )
    result = await client.get_table_metadata(
        "hive",
        "test",
        "pies",
        {"accept": "application/json"},
        None,
    )
    assert result == TableMetadata(
        columns=columns,
        owner=None,
        partitions=[],
    )
    fallback.assert_called_once_with(
        "hive",
        "test",
        "pies",
        {"accept": "application/json"},
        None,
    )


@pytest.mark.asyncio
async def test_query_service_client_missing_table_still_raises(mocker: MockerFixture):
    """
    A 404 for a genuinely missing table surfaces as DJDoesNotExistException,
    because the fallback columns call raises it.
    """
    client = QueryServiceClient(uri="http://queryservice:8001")
    mocker.patch.object(
        client,
        "_arequest",
        return_value=FakeResponse(status_code=404, text="Table not found"),
    )
    with pytest.raises(DJDoesNotExistException) as exc_info:
        await client.get_table_metadata("hive", "test", "nope")
    assert "Table not found" in str(exc_info.value)


@pytest.mark.asyncio
async def test_query_service_client_metadata_error(mocker: MockerFixture):
    """Any other non-success status is an error, not a silent fallback."""
    client = QueryServiceClient(uri="http://queryservice:8001")
    mocker.patch.object(
        client,
        "_arequest",
        return_value=FakeResponse(status_code=500, text="boom"),
    )
    with pytest.raises(DJQueryServiceClientException) as exc_info:
        await client.get_table_metadata("hive", "test", "pies")
    assert str(exc_info.value) == "Error response from query service: boom"


@pytest.mark.asyncio
async def test_http_query_client_delegates(mocker: MockerFixture):
    """The HTTP query client passes the call through to the service client."""
    client = HttpQueryServiceClient(uri="http://queryservice:8001")
    expected = TableMetadata(
        columns=[Column(name="id", type=IntegerType(), order=0)],
        owner=TableOwner(username="owner@example.com", display_name="Example Owner"),
    )
    delegate = mocker.patch.object(
        client._client,
        "get_table_metadata",
        return_value=expected,
    )
    engine = Engine(name="spark", version="2.4.4")
    result = await client.get_table_metadata(
        "hive",
        "test",
        "pies",
        {"accept": "application/json"},
        engine,
    )
    assert result == expected
    delegate.assert_called_once_with(
        catalog="hive",
        schema="test",
        table="pies",
        request_headers={"accept": "application/json"},
        engine=engine,
    )
