"""
APIs related to generating client code used for performing various actions in DJ.
"""
import json
import logging
import os
import tempfile
from datetime import datetime
from typing import Dict, List, Optional, cast

from fastapi import BackgroundTasks, Depends, Request
from nbformat.v4 import new_code_cell, new_markdown_cell, new_notebook
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload
from starlette.responses import FileResponse

from datajunction_server.api.helpers import get_node_by_name
from datajunction_server.database import Node, NodeNamespace, NodeRevision
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.client import (
    export_nodes_notebook_cells,
    python_client_create_node,
    python_client_initialize,
)
from datajunction_server.models.materialization import MaterializationJobTypeEnum
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import get_upstream_nodes, topological_sort
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
    client_code = await python_client_create_node(session, node_name)
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


def notebook_file_response(notebook: Dict, background_tasks: BackgroundTasks):
    """
    Write the notebook contents to a temporary file and prepare a file response
    with appropriate headers so that the API returns a downloadable .ipynb file.
    """
    file_descriptor, path = tempfile.mkstemp(suffix=".ipynb")
    with os.fdopen(file_descriptor, "w") as file:
        file.write(json.dumps(notebook))

    background_tasks.add_task(os.unlink, path)
    headers = {
        "Content-Disposition": 'attachment; filename="export.ipynb"',
    }
    return FileResponse(path, headers=headers)


async def build_export_notebook(
    session: AsyncSession,
    nodes: List[Node],
    introduction: str,
    request_url: str,
):
    """
    Builds a notebook with Python client code for exporting the list of provided nodes.
    """
    notebook = new_notebook()
    notebook.cells.append(new_markdown_cell(introduction))
    notebook.cells.append(new_code_cell(python_client_initialize(str(request_url))))
    sorted_nodes = topological_sort(nodes)
    cells = await export_nodes_notebook_cells(session, sorted_nodes)
    notebook.cells.extend(cells)
    return notebook


@router.get(
    "/datajunction-clients/python/notebook",
)
async def notebook_for_exporting_nodes(
    *,
    namespace: Optional[str] = None,
    cube: Optional[str] = None,
    include_dimensions: bool = False,
    include_sources: bool = False,
    session: AsyncSession = Depends(get_session),
    background_tasks: BackgroundTasks,
    request: Request,
) -> FileResponse:
    """
    Generate the Python client code used for exporting multiple nodes. There are two options:
    - namespace
        If `namespace` is specified, the generated notebook will contain Python client
        code to export all nodes in the namespace.
    - cube
        If `cube` is specified, the generated notebook will contain Python client code
        used for exporting a cube, including all metrics and dimensions referenced in the cube.
    """
    if namespace and cube:
        raise DJInvalidInputException(
            "Cannot only specify export of either a namespace or a cube.",
        )

    if namespace:
        nodes = await NodeNamespace.list_node_namespace_dag(session, namespace)
        introduction = (
            f"## DJ Namespace Export\n\n"
            f"Exported `{namespace}`\n\n(As of {datetime.now()})"
        )
    else:
        nodes = await get_upstream_nodes(session, cast(str, cube))
        cube_node = cast(
            Node,
            await Node.get_by_name(session, cast(str, cube), raise_if_not_exists=True),
        )
        nodes += [cube_node]
        if not include_dimensions:
            nodes = [node for node in nodes if node.type != NodeType.DIMENSION]
        if not include_sources:
            nodes = [node for node in nodes if node.type != NodeType.SOURCE]
        introduction = (
            f"## DJ Cube Export\n\n"
            f"Exported `{cube}` {cube_node.current_version}\n\n(As of {datetime.now()})"
        )

    notebook = await build_export_notebook(session, nodes, introduction, request.url)
    return notebook_file_response(notebook, background_tasks)
