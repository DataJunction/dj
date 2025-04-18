"""
Tests for the catalog API.
"""

from fastapi.testclient import TestClient


def test_table_columns(client: TestClient, mocker):
    """
    Test getting table columns
    """
    columns = [
        {"name": "col_a", "type": "STR"},
        {"name": "col_b", "type": "INT"},
        {"name": "col_c", "type": "MAP"},
        {"name": "col_d", "type": "STR"},
        {"name": "col_e", "type": "DECIMAL"},
    ]
    mocker.patch("djqs.api.tables.get_columns", return_value=columns)
    response = client.get(
        "/table/foo.bar.baz/columns/?engine=duckdb_inmemory&engine_version=0.7.1",
    )
    assert response.json() == {
        "name": "foo.bar.baz",
        "columns": [
            {"name": "col_a", "type": "STR"},
            {"name": "col_b", "type": "INT"},
            {"name": "col_c", "type": "MAP"},
            {"name": "col_d", "type": "STR"},
            {"name": "col_e", "type": "DECIMAL"},
        ],
    }


def test_table_columns_w_default_engine(client: TestClient, mocker):
    """
    Test getting table columns using the default engine
    """
    columns = [
        {"name": "col_a", "type": "STR"},
        {"name": "col_b", "type": "INT"},
        {"name": "col_c", "type": "MAP"},
        {"name": "col_d", "type": "STR"},
        {"name": "col_e", "type": "DECIMAL"},
    ]
    mocker.patch("djqs.api.tables.get_columns", return_value=columns)
    response = client.get(
        "/table/foo.bar.baz/columns/",
    )
    assert response.json() == {
        "name": "foo.bar.baz",
        "columns": [
            {"name": "col_a", "type": "STR"},
            {"name": "col_b", "type": "INT"},
            {"name": "col_c", "type": "MAP"},
            {"name": "col_d", "type": "STR"},
            {"name": "col_e", "type": "DECIMAL"},
        ],
    }


def test_raise_on_invalid_table_name(client: TestClient):
    """
    Test raising on invalid table names
    """
    response = client.get("/table/foo.bar.baz.qux/columns/")
    assert response.json() == {
        "message": (
            "The provided table value `foo.bar.baz.qux` is invalid. "
            "A valid value for `table` must be in the format "
            "`<catalog>.<schema>.<table>`"
        ),
        "errors": [],
        "warnings": [],
    }

    response = client.get("/table/foo/columns/")
    assert response.json() == {
        "message": (
            "The provided table value `foo` is invalid. "
            "A valid value for `table` must be in the format "
            "`<catalog>.<schema>.<table>`"
        ),
        "errors": [],
        "warnings": [],
    }
