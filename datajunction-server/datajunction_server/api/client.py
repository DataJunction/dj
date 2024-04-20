"""
APIs related to generating client code used for performing various actions in DJ.
"""

import json
import logging

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from datajunction_server.api.helpers import get_node_by_name
from datajunction_server.database import Node, NodeRevision
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.materialization import MaterializationJobTypeEnum
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import get_session, get_settings

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["client"])


@router.get("/datajunction-clients/python/new_node/{node_name}", response_model=str)
async def client_code_for_creating_node(
    node_name: str, *, session: AsyncSession = Depends(get_session)
) -> str:
    """
    Generate the Python client code used for creating this node
    """
    node_short_name = node_name.split(".")[-1]
    node = await Node.get_by_name(
        session,
        node_name,
        options=[
            joinedload(Node.current).options(
                *NodeRevision.default_load_options(),
                joinedload(NodeRevision.cube_elements),
            ),
        ],
        raise_if_not_exists=True,
    )
    # Generic user-configurable node creation params
    params = {
        "name": node.name,  # type: ignore
        "display_name": node.current.display_name,  # type: ignore
        "description": node.current.description,  # type: ignore
        "mode": node.current.mode,  # type: ignore
        "query": node.current.query,  # type: ignore
        "schema_": node.current.schema_,  # type: ignore
        "table": node.current.table,  # type: ignore
        "primary_key": [col.name for col in node.current.primary_key()],  # type: ignore
    }

    for key in params:  # pylint: disable=consider-using-dict-items
        if (
            not isinstance(params[key], list)
            and key != "query"
            and key != "lineage"
            and params[key]
        ):
            params[key] = f'"{params[key]}"'
        if key == "query" and params[key]:
            params[key] = f'"""{params[key]}"""'

    # Cube-specific params
    cube_params = []
    if node.type == NodeType.CUBE:  # type: ignore
        node = await Node.get_cube_by_name(session, node_name)
        ordering = {
            col.name: col.order or idx
            for idx, col in enumerate(node.current.columns)  # type: ignore
        }
        metrics_list = sorted(
            [
                elem.node_revisions[-1].name
                for elem in node.current.cube_elements  # type: ignore
                if elem.node_revisions[-1].type == NodeType.METRIC
            ],
            key=lambda x: ordering[x],
        )
        dimensions_list = sorted(
            [
                elem.node_revisions[-1].name + "." + elem.name
                for elem in node.current.cube_elements  # type: ignore
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
        [f"    {k}={params[k]}" for k in sorted(params.keys()) if params[k]]
        + cube_params,
    )

    node_type = node.type  # type: ignore
    client_code = f"""dj = DJBuilder(DJ_URL)

{node_short_name} = dj.create_{node_type}(
{formatted_params}
)"""
    return client_code  # type: ignore


@router.get(
    "/datajunction-clients/python/add_materialization/{node_name}/{materialization_name}",
    response_model=str,
)
async def client_code_for_adding_materialization(
    node_name: str,
    materialization_name: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> str:
    """
    Generate the Python client code used for adding this materialization
    """
    node_short_name = node_name.split(".")[-1]
    node = await Node.get_by_name(
        session,
        node_name,
        options=[
            joinedload(Node.current).options(
                selectinload(NodeRevision.materializations),
            ),
        ],
    )
    materialization = [
        materialization
        for materialization in node.current.materializations  # type: ignore
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

{node_short_name} = dj.{node.type if node else ""}(
    "{node.name if node else ""}"
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
async def client_code_for_linking_dimension_to_node(
    node_name: str,
    column: str,
    dimension: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> str:
    """
    Generate the Python client code used for linking this node's column to a dimension
    """
    node_short_name = node_name.split(".")[-1]
    node = await get_node_by_name(session, node_name)
    client_code = f"""dj = DJBuilder(DJ_URL)
{node_short_name} = dj.{node.type}(
    "{node.name}"
)
{node_short_name}.link_dimension(
    "{column}",
    "{dimension}",
)"""
    return client_code  # type: ignore
