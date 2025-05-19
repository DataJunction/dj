"""Reflection service celery tasks."""

from abc import ABC

import celery
import requests
from celery import shared_task
from celery.utils.log import get_task_logger

from datajunction_reflection.worker.app import celery_app
from datajunction_reflection.worker.utils import get_settings

logger = get_task_logger(__name__)


class ReflectionServiceTask(celery.Task, ABC):
    """
    Base reflection service task.
    """

    abstract = True

    def on_failure(self, exc, task_id, *args, **kwargs):
        logger.exception("%s failed: %s", task_id, exc)  # pragma: no cover


@shared_task(
    queue="celery",
    name="datajunction_reflection.worker.app.refresh",
    base=ReflectionServiceTask,
)
def refresh():
    """
    Find available DJ nodes and kick off reflection tasks for
    nodes with associated tables.
    """
    settings = get_settings()
    response = requests.get(
        f"{settings.core_service}/nodes/?node_type=source",
        timeout=30,
    )
    response.raise_for_status()
    source_nodes = response.json()

    tasks = []
    for node_name in source_nodes:
        task = celery_app.send_task(
            "datajunction_reflection.worker.tasks.reflect_source",
            (node_name,),
        )
        tasks.append(task)


@shared_task(
    queue="celery",
    name="datajunction_reflection.worker.tasks.reflect_source",
    base=ReflectionServiceTask,
)
def reflect_source(
    node_name: str,
):
    """
    This reflects the state of the node's associated table, whether
    external or materialized, back to the DJ core service.
    """
    logger.info(f"Refreshing source node={node_name} in DJ core")
    settings = get_settings()

    # Call the source node's refresh endpoint
    response = requests.post(
        f"{settings.core_service}/nodes/{node_name}/refresh/",
        timeout=30,
    )

    logger.info(
        "Finished refreshing source node `%s`. Response: %s",
        node_name,
        response.reason,
    )

    # pylint: disable=fixme
    # TODO: Post actual availability state when information available
