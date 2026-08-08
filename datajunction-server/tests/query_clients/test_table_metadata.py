"""Tests for TableMetadata and the query-client default."""

import pytest

from datajunction_server.database.column import Column
from datajunction_server.models.table_metadata import TableMetadata, TableOwner
from datajunction_server.sql.parsing.types import IntegerType


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
        broken_ref=None,
        valid_through=None,
    )


def test_table_owner_defaults_full_name():
    """``full_name`` is optional, since not every source reports one."""
    assert TableOwner(email="someone@example.com") == TableOwner(
        email="someone@example.com",
        full_name="",
    )
