"""
Tests for the Superset DB engine spec.
"""

from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker
from yarl import URL

from dj.superset import DJEngineSpec


def test_select_star() -> None:
    """
    Test ``select_star``.
    """
    assert DJEngineSpec.select_star() == (
        "SELECT 'DJ does not support data preview, since the `metrics` table is a "
        "virtual table representing the whole repository of metrics. An "
        "administrator should configure the DJ database with the "
        "`disable_data_preview` attribute set to `true` in the `extra` field.' AS "
        "warning"
    )


def test_get_metrics(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test ``get_metrics``.
    """
    database = mocker.MagicMock()
    database.get_sqla_engine().connect().connection.base_url = URL(
        "https://localhost:8000/0",
    )
    inspector = mocker.MagicMock()

    requests_mock.get(
        "https://localhost:8000/0/metrics/",
        json=[
            {
                "name": "core.num_comments",
                "description": "All the cmments",
            },
        ],
    )

    assert DJEngineSpec.get_metrics(database, inspector, "some-table", "main") == [
        {
            "metric_name": "core.num_comments",
            "expression": '"core.num_comments"',
            "description": "All the cmments",
        },
    ]


def test_execute(mocker: MockerFixture) -> None:
    """
    Test ``execute``.

    The method is almost identical to the superclass, with the only difference that it
    quotes identifiers starting with an underscore.
    """
    cursor = mocker.MagicMock()
    super_ = mocker.patch("dj.superset.super")
    DJEngineSpec.execute(cursor, "SELECT time AS __timestamp FROM table")
    super_().execute.assert_called_with(
        cursor,
        'SELECT time AS "__timestamp" FROM table',
    )
    print(super_.mock_calls)
