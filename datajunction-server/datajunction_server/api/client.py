"""
APIs related to generating client code used for performing various actions in DJ.
"""
import json
import logging
import os
import tempfile
from datetime import datetime
from typing import Dict, Optional, cast

from fastapi import BackgroundTasks, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload
from starlette.responses import FileResponse

from datajunction_server.database import Node, NodeNamespace, NodeRevision
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.client import (
    build_export_notebook,
    python_client_code_for_linking_complex_dimension,
    python_client_create_node,
    python_client_initialize,
)
from datajunction_server.models.materialization import MaterializationJobTypeEnum
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import get_upstream_nodes
from datajunction_server.utils import SEPARATOR, get_session, get_settings

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["client"])


@router.get("/datajunction-clients/python/new_node/{node_name}", response_model=str)
async def client_code_for_creating_node(
    node_name: str,
    *,
    include_client_setup: bool = True,
    session: AsyncSession = Depends(get_session),
    replace_namespace: Optional[str] = None,
    request: Request,
) -> str:
    """
    Generate the Python client code used for creating this node
    """
    client_setup = (
        python_client_initialize(str(request.url)) if include_client_setup else ""
    )
    client_code = await python_client_create_node(session, node_name, replace_namespace)
    return client_setup + "\n\n" + client_code  # type: ignore


@router.get(
    "/datajunction-clients/python/dimension_links/{node_name}",
    response_model=str,
)
async def client_code_for_dimension_links_on_node(
    node_name: str,
    *,
    include_client_setup: bool = True,
    session: AsyncSession = Depends(get_session),
    replace_namespace: Optional[str] = None,
    request: Request,
) -> str:
    """
    Generate the Python client code used for creating this node
    """
    node = await Node.get_by_name(
        session,
        node_name,
        options=[
            joinedload(Node.current).options(
                selectinload(NodeRevision.dimension_links),
            ),
        ],
    )
    code = [python_client_initialize(str(request.url))] if include_client_setup else []
    short_name = node_name.split(SEPARATOR)[-1]
    code += [f'{short_name} = dj.node("{node_name}")']
    for link in node.current.dimension_links:  # type: ignore
        code.append(
            python_client_code_for_linking_complex_dimension(
                node_name,
                link,
                replace_namespace,
            ),
        )
    return "\n\n".join(code)


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
    * namespace: If `namespace` is specified, the generated notebook will contain Python client
    code to export all nodes in the namespace.
    * cube: If `cube` is specified, the generated notebook will contain Python client code
    used for exporting a cube, including all metrics and dimensions referenced in the cube.
    """
    if namespace and cube:
        raise DJInvalidInputException(
            "Can only specify export of either a namespace or a cube.",
        )

    if namespace:
        nodes = await NodeNamespace.list_all_nodes(session, namespace)
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
