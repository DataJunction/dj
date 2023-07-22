"""Tests the celery app."""
from unittest.mock import call

from datajunction_reflection.worker.tasks import reflect_source, refresh


def test_refresh(celery_app, mocker):
    """
    Tests that the reflection service refreshes DJ source nodes
    """
    mock_dj_get_sources = mocker.patch("requests.get")
    mock_dj_get_sources.return_value.json = lambda: ["postgres.test.revenue"]
    mock_dj_get_sources.return_value.status_code = 200

    refresh()

    assert {
        "datajunction_reflection.worker.app.refresh",
        "datajunction_reflection.worker.tasks.reflect_source",
    }.intersection(
        celery_app.tasks.keys(),
    )
    assert mock_dj_get_sources.call_args_list == [
        call("http://dj:8000/nodes/?node_type=source", timeout=30),
    ]


def test_reflect_source(celery_app, mocker, freezer):  # pylint: disable=unused-argument
    """
    Tests the reflection task.
    """
    mock_dj_refresh = mocker.patch("requests.post")
    mock_dj_refresh.return_value.status = 201

    reflect_source.apply(
        args=("postgres.test.revenue",),
    ).get()

    assert mock_dj_refresh.call_args_list == [
        call("http://dj:8000/nodes/postgres.test.revenue/refresh/", timeout=30),
    ]
