"""
Tests for the query model.
"""

from datetime import datetime

import msgpack

from datajunction_server.models.query import (
    ColumnMetadata,
    QueryResults,
    QueryWithResults,
    StatementResults,
    decode_results,
    encode_results,
)
from datajunction_server.typing import QueryState


def test_msgpack() -> None:
    """
    Test the msgpack encoding/decoding
    """
    query_with_results = QueryWithResults(
        catalog=None,
        schema=None,
        id="5599b970-23f0-449b-baea-c87a2735423b",
        submitted_query="SELECT 42 AS answer",
        executed_query="SELECT 42 AS answer",
        scheduled=datetime(2021, 1, 1),
        started=datetime(2021, 1, 2),
        finished=datetime(2021, 1, 3),
        state=QueryState.FINISHED,
        progress=1,
        output_table=None,
        results=QueryResults(
            __root__=[
                StatementResults(
                    sql="SELECT 42 AS answer",
                    columns=[ColumnMetadata(name="answer", type="int")],
                    rows=[(42,)],
                    row_count=1,
                ),
            ],
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
        "id": "5599b970-23f0-449b-baea-c87a2735423b",
        "submitted_query": "SELECT 42 AS answer",
        "executed_query": "SELECT 42 AS answer",
        "engine_name": None,
        "engine_version": None,
        "output_table": None,
        "scheduled": datetime(2021, 1, 1, 0, 0),
        "started": datetime(2021, 1, 2, 0, 0),
        "finished": datetime(2021, 1, 3, 0, 0),
        "progress": 1.0,
        "state": "FINISHED",
        "results": [
            {
                "sql": "SELECT 42 AS answer",
                "columns": [
                    {
                        "name": "answer",
                        "type": "int",
                        "node": None,
                        "column": None,
                        "semantic_type": None,
                        "semantic_entity": None,
                    },
                ],
                "rows": [[42]],
                "row_count": 1,
            },
        ],
        "next": None,
        "previous": None,
        "errors": [],
        "links": None,
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
