"""
APIs related to generating client code used for performing various actions in DJ.
"""

import json
import logging

from fastapi import Depends
from sqlalchemy.orm import Session

from datajunction_server.api.helpers import get_node_by_name
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.materialization import MaterializationJobTypeEnum
from datajunction_server.models.node import NodeOutput
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import get_session, get_settings

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["client"])


@router.get("/datajunction-clients/python/new_node/{node_name}", response_model=str)
def client_code_for_creating_node(
    node_name: str, *, session: Session = Depends(get_session)
) -> str:
    """
    Generate the Python client code used for creating this node
    """
    node_short_name = node_name.split(".")[-1]
    node = get_node_by_name(session, node_name)

    # Generic user-configurable node creation params
    params = NodeOutput.from_orm(node).current.dict(
        exclude={
            "id",
            "version",
            "type",
            "catalog_id",
            "lineage",
            "status",
            "metric_metadata_id",
            "mode",
            "node_id",
            "updated_at",
            "materializations",
            "columns",
            "catalog",
            "parents",
            "metric_metadata",
            "query" if node.type == NodeType.CUBE else "",
            "dimension_links",
        },
        exclude_none=True,
    )

    params["primary_key"] = [col.name for col in node.current.primary_key()]

    for key in params:
        if not isinstance(params[key], list) and key != "query" and key != "lineage":
            params[key] = f'"{params[key]}"'
        if key == "query":
            params[key] = f'"""{params[key]}"""'

    # Cube-specific params
    cube_params = []
    if node.type == NodeType.CUBE:
        ordering = {col.name: col.order for col in node.current.columns}
        metrics_list = sorted(
            [
                elem.node_revisions[-1].name
                for elem in node.current.cube_elements
                if elem.node_revisions[-1].type == NodeType.METRIC
            ],
            key=lambda x: ordering[x],
        )
        dimensions_list = sorted(
            [
                elem.node_revisions[-1].name + "." + elem.name
                for elem in node.current.cube_elements
                if elem.node_revisions[-1].type == NodeType.DIMENSION
            ],
            key=lambda x: ordering[x],
        )
        cube_metrics = ", ".join([f'"{metric}"' for metric in metrics_list])
        cube_dimensions = ", ".join([f'"{dim}"' for dim in dimensions_list])
        cube_params = [
            f"    metrics=[{cube_metrics}]",
            f"    dimensions=[{cube_dimensions}]",
        ]

    formatted_params = ",\n".join(
        [f"    {k}={params[k]}" for k in sorted(params.keys())] + cube_params,
    )

    client_code = f"""dj = DJBuilder(DJ_URL)

{node_short_name} = dj.create_{node.type}(
{formatted_params}
)"""
    return client_code  # type: ignore


@router.get(
    "/datajunction-clients/python/add_materialization/{node_name}/{materialization_name}",
    response_model=str,
)
def client_code_for_adding_materialization(
    node_name: str,
    materialization_name: str,
    *,
    session: Session = Depends(get_session),
) -> str:
    """
    Generate the Python client code used for adding this materialization
    """
    node_short_name = node_name.split(".")[-1]
    node = get_node_by_name(session, node_name)
    materialization = [
        materialization
        for materialization in node.current.materializations
        if materialization.name == materialization_name
    ][0]
    user_modified_config = {
        key: materialization.config[key]
        for key in materialization.config
        if key in ("partitions", "spark", "druid", "")
    }
    with_b = "\n".join(
        [
            f"    {line}"
            for line in json.dumps(user_modified_config, indent=4).split("\n")
        ],
    )
    client_code = f"""dj = DJBuilder(DJ_URL)

{node_short_name} = dj.{node.type}(
    "{node.name}"
)
materialization = MaterializationConfig(
    job="{MaterializationJobTypeEnum.find_match(materialization.job).name.lower()}",
    strategy="{materialization.strategy}",
    schedule="{materialization.schedule}",
    config={with_b.strip()},
)
{node_short_name}.add_materialization(
    materialization
)"""
    return client_code  # type: ignore


@router.get(
    "/datajunction-clients/python/link_dimension/{node_name}/{column}/{dimension}/",
    response_model=str,
)
def client_code_for_linking_dimension_to_node(
    node_name: str,
    column: str,
    dimension: str,
    *,
    session: Session = Depends(get_session),
) -> str:
    """
    Generate the Python client code used for linking this node's column to a dimension
    """
    node_short_name = node_name.split(".")[-1]
    node = get_node_by_name(session, node_name)
    client_code = f"""dj = DJBuilder(DJ_URL)
{node_short_name} = dj.{node.type}(
    "{node.name}"
)
{node_short_name}.link_dimension(
    "{column}",
    "{dimension}",
)"""
    return client_code  # type: ignore
