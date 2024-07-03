"""Helper functions to generate client code."""
import os
import urllib
from typing import List, Optional, cast

from jinja2 import Environment, FileSystemLoader
from nbformat.v4 import new_code_cell, new_markdown_cell, new_notebook
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from datajunction_server.construction.utils import to_namespaced_name
from datajunction_server.database import DimensionLink, Node, NodeRevision
from datajunction_server.database.column import Column
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import topological_sort
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.utils import SEPARATOR

jinja_env = Environment(
    loader=FileSystemLoader(
        os.path.join(os.path.dirname(__file__), "templates"),
    ),
)


def python_client_initialize(request_url: str):
    """
    Returns the python client code to initialize the client. This function can be overridden
    for different servers, based on how the client should be setup.
    """
    parsed_url = urllib.parse.urlparse(str(request_url))
    server_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    template = jinja_env.get_template("client_setup.j2")
    return template.render(request_url=server_url)


def python_client_code_for_linking_complex_dimension(
    node_name: str,
    dimension_link: DimensionLink,
    replace_namespace: Optional[str] = None,
):
    """
    Returns the python client code to create a complex dimension link.
    """
    node_short_name = node_name.split(SEPARATOR)[-1]
    node_namespace = SEPARATOR.join(node_name.split(SEPARATOR)[:-1])
    if replace_namespace:
        join_on_ast = dimension_link.join_sql_ast()
        for col in join_on_ast.find_all(ast.Column):
            col_node_namespace = str(
                SEPARATOR.join(col.identifier().split(SEPARATOR)[:-2]),
            )
            col_short_name = str(SEPARATOR.join(col.identifier().split(SEPARATOR)[-2:]))
            if (
                replace_namespace
                and col_node_namespace == node_namespace  # pragma: no cover
            ):
                col.name = to_namespaced_name(f"{replace_namespace}.{col_short_name}")
        join_on = str(
            join_on_ast.select.from_.relations[-1].extensions[0].criteria.on,  # type: ignore
        )
    else:
        join_on = dimension_link.join_sql

    dimension_node_name = (
        dimension_link.dimension.name.replace(
            node_namespace,
            replace_namespace,
        )
        if replace_namespace
        else dimension_link.dimension.name
    )

    template = jinja_env.get_template("link_dimension.j2")
    return template.render(
        node_short_name=node_short_name,
        dimension_node=dimension_node_name,
        join_on=join_on,
        join_type=dimension_link.join_type.value,
        role=dimension_link.role,
    )


async def python_client_code_for_setting_column_attributes(
    session: AsyncSession,
    node_name: str,
):
    """
    Returns the python client code to set column attributes.
    """
    node_short_name = node_name.split(SEPARATOR)[-1]
    node = await Node.get_by_name(
        session,
        node_name,
        options=[
            joinedload(Node.current).options(
                selectinload(NodeRevision.columns).options(
                    selectinload(Column.attributes),
                ),
            ),
        ],
    )

    template = jinja_env.get_template("set_column_attributes.j2")
    snippets = [
        template.render(
            node_short_name=node_short_name,
            column_name=col.name,
            attributes=[
                attr.attribute_type
                for attr in col.attributes
                if attr.attribute_type.name != "primary_key"
            ],
        )
        for col in node.current.columns  # type: ignore
        if col.has_attributes_besides("primary_key")
    ]
    return "\n\n".join(snippets)


async def python_client_create_node(
    session: AsyncSession,
    node_name: str,
    replace_namespace: Optional[str] = None,
):
    """
    Returns the python client code for creating this node

    replace_namespace: a string to replace the node namespace with
    """
    node_short_name = node_name.split(SEPARATOR)[-1]
    node = cast(
        Node,
        await Node.get_by_name(
            session,
            node_name,
            options=[
                joinedload(Node.current).options(
                    *NodeRevision.default_load_options(),
                    selectinload(NodeRevision.cube_elements)
                    .selectinload(Column.node_revisions)
                    .options(
                        selectinload(NodeRevision.node),
                    ),
                ),
                joinedload(Node.tags),
            ],
            raise_if_not_exists=True,
        ),
    )

    if node.type == NodeType.SOURCE:
        template = jinja_env.get_template("register_table.j2")
        return template.render(
            catalog=node.current.catalog.name,
            schema=node.current.schema_,
            table=node.current.table,
        )

    template = jinja_env.get_template(f"create_{node.type}.j2")
    query = (
        node.current.query
        if not node.current.query or not replace_namespace
        else move_node_references_namespace(
            SEPARATOR.join(node.name.split(SEPARATOR)[:-1]),
            node.current.query,
            replace_namespace,
        )
    ).strip()

    return template.render(
        short_name=node_short_name,
        name=(
            node.name
            if not replace_namespace
            else f"{replace_namespace}.{node_short_name}"
        ),
        display_name=node.current.display_name,
        description=node.current.description.strip(),
        mode=node.current.mode,
        **(
            {"primary_key": [col.name for col in node.current.primary_key()]}
            if node.type != NodeType.METRIC
            else {
                "required_dimensions": [  # type: ignore
                    col.name for col in node.current.required_dimensions
                ],
                **(
                    {
                        "direction": (  # type: ignore
                            f"MetricDirection.{node.current.metric_metadata.direction.upper()}"
                        ),
                    }
                    if node.current.metric_metadata
                    and node.current.metric_metadata.direction
                    else {}
                ),
                **(
                    {"unit": node.current.metric_metadata.unit}
                    if node.current.metric_metadata
                    and node.current.metric_metadata.unit
                    else {}
                ),
            }
        ),
        **(
            {
                "metrics": [
                    (
                        f"{replace_namespace}{SEPARATOR}{metric.split(SEPARATOR)[-1]}"
                        if replace_namespace
                        else metric
                    )
                    for metric in node.current.cube_node_metrics
                ],
                "dimensions": node.current.cube_node_dimensions,
            }
            if node.type == NodeType.CUBE
            else {"query": query}
        ),
        tags=[tag.name for tag in node.tags],
    )


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


def move_node_references_namespace(namespace: str, query: str, replacement: str) -> str:
    """
    Moves all node references in this query to a different namespace but keeps
    the node short names intact.

    Example:
        move_node_references_namespace("SELECT a, b FROM default.one.c", "default.two")
    The above will yield this modified query:
        SELECT a, b FROM default.two.c
    """
    query_ast = parse(query)
    tables = query_ast.find_all(ast.Table)
    for tbl in tables:
        if str(tbl.name.namespace) == namespace:
            tbl.name.namespace = to_namespaced_name(replacement)
    return str(query_ast)


async def export_nodes_notebook_cells(session: AsyncSession, nodes: List[Node]):
    """
    Returns notebook cells used for exporting the list of nodes.
    A node export means the following:
    - Client code to create the node and set the right tags
    - Client code to link all dimensions set on the node
    - Client code to set all column attributes on the node
    """
    cells = []
    cells.append(
        new_markdown_cell(
            "### Upserting Nodes:\n" + "\n".join([f"* {node.name}" for node in nodes]),
        ),
    )

    # Set up a namespace mapping between current namespaces and where they should be moved
    # to. This is modifiable by the exported notebook user and can be used to move nodes
    namespaces = {SEPARATOR.join(node.name.split(SEPARATOR)[:-1]) for node in nodes}
    template = jinja_env.get_template("namespace_mapping.j2")
    template.render(namespaces=namespaces)
    cells.append(new_code_cell(template.render(namespaces=namespaces)))

    for node in nodes:
        # Add cell for creating node
        namespace = SEPARATOR.join(node.name.split(SEPARATOR)[:-1])
        cells.append(
            new_code_cell(
                await python_client_create_node(
                    session,
                    node.name,
                    replace_namespace=f"{{NAMESPACE_MAPPING['{namespace}']}}",
                ),
            ),
        )

        # Add cell for linking dimensions if needed
        if node.current.dimension_links:
            cells.append(
                new_markdown_cell(
                    f"Linking dimensions for {node.type} node `{node.name}`:",
                ),
            )
            link_dimensions = "\n".join(
                [
                    python_client_code_for_linking_complex_dimension(
                        node.name,
                        link,
                        replace_namespace=f"{{NAMESPACE_MAPPING['{namespace}']}}",
                    )
                    for link in node.current.dimension_links
                ],
            )
            cells.append(new_code_cell(link_dimensions))

        # Add cell for setting column attributes if needed
        if any(
            col.has_attributes_besides("primary_key") for col in node.current.columns
        ):
            cells.append(
                new_code_cell(
                    await python_client_code_for_setting_column_attributes(
                        session,
                        node.name,
                    ),
                ),
            )
    return cells
