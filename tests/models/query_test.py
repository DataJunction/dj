"""
Tests for the query model.
"""

from datetime import datetime
from uuid import UUID

import msgpack

from dj.models.query import (
    ColumnMetadata,
    QueryExecutionResult,
    QueryWithResults,
    decode_results,
    encode_results,
)
from dj.typing import ColumnType, QueryState


def test_msgpack() -> None:
    """
    Test the msgpack encoding/decoding
    """
    query_with_results = QueryWithResults(
        catalog=None,
        schema=None,
        id=UUID("5599b970-23f0-449b-baea-c87a2735423b"),
        query="SELECT 42 AS answer",
        executed_query="SELECT 42 AS answer",
        created=datetime(2021, 1, 1),
        started=datetime(2021, 1, 2),
        finished=datetime(2021, 1, 3),
        state=QueryState.FINISHED,
        progress=1,
        results=QueryExecutionResult(
            columns=[ColumnMetadata(name="answer", type=ColumnType.INT)],
            rows=[(42,)],
        ),
        next=None,
        previous=None,
        errors=[],
    )
    encoded = msgpack.packb(
        query_with_results.dict(by_alias=True),
        default=encode_results,
    )
    decoded = msgpack.unpackb(encoded, ext_hook=decode_results)
    assert decoded == {
        "id": UUID("5599b970-23f0-449b-baea-c87a2735423b"),
        "query": "SELECT 42 AS answer",
        "engine_name": None,
        "engine_version": None,
        "output_table": None,
        "created": datetime(2021, 1, 1, 0, 0),
        "started": datetime(2021, 1, 2, 0, 0),
        "finished": datetime(2021, 1, 3, 0, 0),
        "state": "FINISHED",
        "results": {
            "columns": [{"name": "answer", "type": "INT"}],
            "rows": [[42]],
        },
        "next": None,
        "previous": None,
        "errors": [],
    }


def test_encode_results_unknown() -> None:
    """
    Test that ``encode_results`` passes through unknown objects.
    """
    assert encode_results(1) == 1


def test_decode_results_unknown() -> None:
    """
    Test that ``decode_results`` passes through unknown objects.
    """
    assert decode_results(42, b"packed") == msgpack.ExtType(42, b"packed")
