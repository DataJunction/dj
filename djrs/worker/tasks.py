"""Reflection service celery tasks."""
import datetime
import json
from abc import ABC

import celery
import requests
from celery import shared_task
from celery.utils.log import get_task_logger
from djopenapi.model.availability_state_base import AvailabilityStateBase
from djopenapi.model.update_node import UpdateNode

from djrs.worker.app import celery_app
from djrs.worker.utils import get_dj_client, get_settings

logger = get_task_logger(__name__)


class ReflectionServiceTask(celery.Task, ABC):
    """
    Base reflection service task.
    """

    abstract = True

    def on_failure(self, exc, task_id, *args, **kwargs):
        logger.exception("%s failed: %s", task_id, exc)


@shared_task(queue="celery", name="djrs.worker.app.refresh", base=ReflectionServiceTask)
def refresh():
    """
    Find available DJ nodes and kick off reflection tasks for
    nodes with associated tables.
    """
    dj_api = get_dj_client()
    all_nodes = {
        node["name"]: node
        for node in json.loads(
            dj_api.read_nodes_nodes_get(skip_deserialization=True).response.data,
        )
    }

    tasks = []
    for node_name, node in all_nodes.items():
        if node["catalog"] and node["schema_"] and node["table"]:
            task = celery_app.send_task(
                "djrs.worker.tasks.reflect",
                (
                    node_name,
                    node["catalog"]["name"],
                    node["schema_"],
                    node["table"],
                ),
            )
            tasks.append(task)


@shared_task(
    queue="celery",
    name="djrs.worker.tasks.reflect",
    base=ReflectionServiceTask,
)
def reflect(node_name: str, catalog: str, schema: str, table: str):
    """
    This reflects the state of the node's associated table, whether
    external or materialized, back to the DJ core service.
    """
    logger.info("Reflecting node={node_name}, table={table} to DJ core")
    settings = get_settings()
    dj_api = get_dj_client()

    # Update table columns
    response = requests.get(
        f"{settings.query_service}/table/{catalog}.{schema}.{table}/columns/",
        timeout=30,
    )
    table_columns = response.json()["columns"]
    update_columns_response = dj_api.update_node_nodes_name_patch(
        body=UpdateNode(
            columns={col["name"]: {"type": col["type"]} for col in table_columns},
        ),
        path_params={"name": node_name},
        skip_deserialization=True,
    ).response

    logger.info(
        f"Update node columns for `{node_name}` response: "
        f"{update_columns_response.reason}",
    )

    # pylint: disable=fixme
    # TODO: Post actual availability state when information available
    availability_state_response = (
        dj_api.add_availability_data_node_name_availability_post(
            body=AvailabilityStateBase(
                catalog=catalog,
                max_partition=[],
                min_partition=[],
                table=table,
                valid_through_ts=int(datetime.datetime.now().timestamp()),
                schema_=schema,
            ),
            path_params={"node_name": node_name},
            skip_deserialization=True,
        ).response
    )

    logger.info(
        "Post availability state for `%s` response: %s",
        node_name,
        availability_state_response.reason,
    )
