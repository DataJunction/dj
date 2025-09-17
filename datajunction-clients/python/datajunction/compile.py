# pylint: disable=too-many-lines
"""
Compile a metrics repository.

This will:

    1. Build graph of nodes.
    2. Retrieve the schema of source nodes.
    3. Infer the schema of downstream nodes.
    4. Save everything to the DB.

"""

import asyncio
import logging
import os
import random
import string
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import yaml
from rich import box
from rich.align import Align
from rich.console import Console
from rich.live import Live
from rich.table import Table
from yaml.resolver import BaseResolver

from datajunction import DJBuilder, DJClient
from datajunction._base import SerializableMixin
from datajunction.exceptions import (
    DJClientException,
    DJDeploymentFailure,
    DJNamespaceAlreadyExists,
)
from datajunction.models import (
    ColumnAttribute,
    MetricDirection,
    MetricUnit,
    NodeMode,
    NodeType,
)
from datajunction.tags import Tag

_logger = logging.getLogger(__name__)

CONFIG_FILENAME = "dj.yaml"


def str_presenter(dumper, data):
    """
    YAML representer that uses the | (pipe) character for multiline strings
    """
    if len(data.splitlines()) > 1 or "\n" in data:
        text_list = [line.rstrip() for line in data.splitlines()]
        fixed_data = "\n".join(text_list)
        return dumper.represent_scalar(
            BaseResolver.DEFAULT_SCALAR_TAG,
            fixed_data,
            style="|",
        )
    return dumper.represent_scalar(BaseResolver.DEFAULT_SCALAR_TAG, data)


yaml.add_representer(str, str_presenter)


def _parent_dir(path: Union[str, Path]):
    """
    Returns the parent directory
    """
    return os.path.dirname(os.path.abspath(path))


def _conf_exists(path: Union[str, Path]):
    """
    Returns True if a config exists in the Path
    """
    return os.path.isfile(os.path.join(path, CONFIG_FILENAME))


def find_project_root(directory: Optional[str] = None):
    """
    Returns the project root, identified by a root config file
    """
    if directory and not os.path.isdir(directory):
        raise DJClientException(f"Directory {directory} does not exist")
    checked_dir = directory or os.getcwd()
    while not _conf_exists(checked_dir):
        checked_dir = _parent_dir(checked_dir)
        if checked_dir == "/" and not _conf_exists(checked_dir):
            raise DJClientException(
                "Cannot find project root, make sure you've "
                f"defined a project in a {CONFIG_FILENAME} file",
            )

    return checked_dir


@dataclass
class TagYAML:
    """
    YAML representation of a tag
    """

    name: str
    description: str = ""
    tag_type: str = ""
    tag_metadata: Optional[Dict] = None


class JoinType(str, Enum):
    """
    Join type
    """

    LEFT = "left"
    RIGHT = "right"
    INNER = "inner"
    FULL = "full"
    CROSS = "cross"


class LinkType(str, Enum):
    """
    There are two types of dimensions links supported: join links or reference links
    """

    JOIN = "join"
    REFERENCE = "reference"


@dataclass
class DimensionJoinLinkYAML(
    SerializableMixin,
):  # pylint: disable=too-many-instance-attributes
    """
    YAML representation of a dimension join link

    If a custom `join_on` clause is not specified, DJ will automatically set
    this clause to be on the selected column and the dimension node's primary key
    """

    dimension_node: str
    type: LinkType = LinkType.JOIN

    node_column: Optional[str] = None
    join_type: JoinType = JoinType.LEFT
    join_on: Optional[str] = None
    role: Optional[str] = None

    @classmethod
    def from_dict(
        cls,
        dj_client: Optional[DJClient],
        data: Dict[str, Any],
    ) -> "DimensionJoinLinkYAML":
        """
        Create an instance of the given dataclass `cls` from a dictionary `data`.
        This will handle nested dataclasses and optional types.
        """
        if LinkType(data["type"].lower()) != LinkType.JOIN:
            raise TypeError("Wrong dimension link type: " + data["type"])
        return super().from_dict(dj_client, data)


@dataclass
class DimensionReferenceLinkYAML(
    SerializableMixin,
):  # pylint: disable=too-many-instance-attributes
    """
    YAML representation of a dimension reference link

    The `dimension` input should be a fully qualified dimension attribute name,
    e.g., "<dimension_node>.<column>"
    """

    node_column: str
    dimension: str
    type: LinkType = LinkType.REFERENCE
    role: Optional[str] = None

    @classmethod
    def from_dict(
        cls,
        dj_client: Optional[DJClient],
        data: Dict[str, Any],
    ) -> "DimensionReferenceLinkYAML":
        """
        Create an instance of the given dataclass `cls` from a dictionary `data`.
        This will handle nested dataclasses and optional types.
        """
        if LinkType(data["type"].lower()) != LinkType.REFERENCE:
            raise TypeError(
                "Wrong dimension link type: " + data["type"],
            )  # pragma: no cover
        return super().from_dict(dj_client, data)


@dataclass
class NodeYAML(SerializableMixin):
    """
    YAML represention of a node
    """

    deploy_order: int = 0


@dataclass
class ColumnYAML(SerializableMixin):
    """
    Represents a column
    """

    name: str
    type: str
    display_name: str | None = None
    description: str | None = None
    attributes: list[str] | None = None


@dataclass
class LinkableNodeYAML(NodeYAML):
    """
    YAML represention of a node type that can be linked to dimension nodes:
    source, transform, dimension
    """

    columns: list[ColumnYAML] | None = None
    dimension_links: list[DimensionJoinLinkYAML | DimensionReferenceLinkYAML] | None = (
        None
    )

    def _deploy_column_settings(self, node):
        """
        Deploy any column-level settings (e.g., attributes or display name) for the
        columns on this node.
        """
        if not self.columns:
            return

        for column in self.columns:
            # Deploy column attributes if present
            if column.attributes:
                node.set_column_attributes(
                    column_name=column.name,
                    attributes=[
                        ColumnAttribute(name=attr) for attr in column.attributes
                    ],
                )
            # Deploy display name if present
            if column.display_name:
                node.set_column_display_name(
                    column_name=column.name,
                    display_name=column.display_name,
                )
            # Deploy description if present (empty string counts as present)
            if column.description is not None:
                node.set_column_description(
                    column_name=column.name,
                    description=column.description,
                )

    def _deploy_dimension_links(  # pylint: disable=too-many-locals
        self,
        name: str,
        node_init,
        prefix: str,
        table: Table,
    ):
        """
        Deploy any links from columns on this node to columns on dimension nodes
        """
        prefixed_name = f"{prefix}.{name}"
        node = node_init(prefixed_name)
        existing_join_links = {link.dimension.name for link in node.dimension_links}
        existing_reference_links = {col.name for col in node.columns if col.dimension}
        if self.dimension_links:
            for link in self.dimension_links:
                prefixed_dimension = render_prefixes(
                    link.dimension_node
                    if isinstance(link, DimensionJoinLinkYAML)
                    else link.dimension,
                    prefix,
                )
                if isinstance(link, DimensionJoinLinkYAML):
                    if prefixed_dimension in existing_join_links:
                        existing_join_links.remove(  # pragma: no cover
                            prefixed_dimension,
                        )
                    if link.join_on:
                        prefixed_join_on = render_prefixes(link.join_on, prefix)
                        node.link_complex_dimension(
                            dimension_node=prefixed_dimension,
                            join_type=link.join_type or JoinType.LEFT,
                            join_on=prefixed_join_on,
                            role=link.role,
                        )
                    else:
                        node.link_dimension(
                            link.node_column,
                            prefixed_dimension,
                        )
                else:
                    if link.node_column in existing_reference_links:
                        existing_reference_links.remove(  # pragma: no cover
                            link.node_column,
                        )
                    split_dim = prefixed_dimension.rsplit(".", 1)
                    node.add_reference_dimension_link(
                        node_column=link.node_column,
                        dimension_node=split_dim[0],
                        dimension_column=split_dim[1],
                        role=link.role,
                    )

                message = f"[green]Dimension {link.type} link created between " + (
                    f"{prefixed_name} and {prefixed_dimension}."
                    if link.type == LinkType.JOIN
                    else f"{link.node_column} and {prefixed_dimension}"
                )
                table.add_row(*[prefixed_name, "[b]link", message])

        for dim_node in existing_join_links:  # pragma: no cover
            node.remove_complex_dimension_link(dim_node)
            message = f"[i][yellow]Dimension join link removed to {dim_node}"
            table.add_row(*[prefixed_name, "[b]link", message])
        for node_col in existing_reference_links:  # pragma: no cover
            node.remove_reference_dimension_link(node_col)
            message = f"[i][yellow]Dimension reference link removed on {node_col}"
            table.add_row(*[prefixed_name, "[b]link", message])


@dataclass
class SourceYAML(LinkableNodeYAML):  # pylint: disable=too-many-instance-attributes
    """
    YAML representation of a source node
    """

    node_type: Literal[NodeType.SOURCE] = NodeType.SOURCE
    display_name: Optional[str] = None
    table: str = ""
    description: Optional[str] = None
    primary_key: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    mode: NodeMode = NodeMode.PUBLISHED
    query: Optional[str] = None
    deploy_order: int = 1

    def __post_init__(self):
        """
        Validate that the table name is fully qualified
        """
        if (
            self.table.count(".") != 2
            or not self.table.replace(".", "").replace("_", "").isalnum()
        ):
            raise DJClientException(
                f"Invalid table name {self.table}, table "
                "name must be fully qualified: "
                "<catalog>.<schema>.<table>",
            )

    def deploy(self, name: str, prefix: str, client: DJBuilder):
        """
        Validate a node by deploying it to a temporary system space
        """
        catalog, schema, table = self.table.split(".")
        node = client.create_source(
            display_name=self.display_name,
            name=f"{prefix}.{name}",
            catalog=catalog,
            schema=schema,
            table=table,
            columns=self.columns,
            description=self.description,
            primary_key=self.primary_key,
            tags=self.tags,
            mode=self.mode,
            update_if_exists=True,
        )
        self._deploy_column_settings(node)
        return node

    def deploy_dimension_links(
        self,
        name: str,
        prefix: str,
        client: DJBuilder,
        table: Table,
    ):
        """
        Deploy any links from columns on this node to columns on dimension nodes
        """
        return self._deploy_dimension_links(name, client.source, prefix, table)


@dataclass
class TransformYAML(LinkableNodeYAML):  # pylint: disable=too-many-instance-attributes
    """
    YAML representation of a transform node
    """

    node_type: Literal[NodeType.TRANSFORM] = NodeType.TRANSFORM
    query: str = ""
    display_name: Optional[str] = None
    description: Optional[str] = None
    primary_key: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    mode: NodeMode = NodeMode.PUBLISHED
    custom_metadata: Optional[Dict] = None
    deploy_order: int = 2

    def deploy(self, name: str, prefix: str, client: DJBuilder):
        """
        Validate a node by deploying it to a temporary system space
        """
        node = client.create_transform(
            name=f"{prefix}.{name}",
            display_name=self.display_name,
            query=self.query,
            description=self.description,
            primary_key=self.primary_key,
            tags=self.tags,
            mode=self.mode,
            custom_metadata=self.custom_metadata,
            update_if_exists=True,
        )
        self._deploy_column_settings(node)
        return node

    def deploy_dimension_links(
        self,
        name: str,
        prefix: str,
        client: DJBuilder,
        table: Table,
    ):
        """
        Deploy any links from columns on this node to columns on dimension nodes
        """
        return self._deploy_dimension_links(name, client.transform, prefix, table)


@dataclass
class DimensionYAML(LinkableNodeYAML):  # pylint: disable=too-many-instance-attributes
    """
    YAML representation of a dimension node
    """

    node_type: Literal[NodeType.DIMENSION] = NodeType.DIMENSION
    query: str = ""
    display_name: Optional[str] = None
    description: Optional[str] = None
    primary_key: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    mode: NodeMode = NodeMode.PUBLISHED
    deploy_order: int = 3

    def deploy(self, name: str, prefix: str, client: DJBuilder):
        """
        Validate a node by deploying it to a temporary system space
        """
        node = client.create_dimension(
            name=f"{prefix}.{name}",
            display_name=self.display_name,
            query=self.query,
            description=self.description,
            primary_key=self.primary_key,
            tags=self.tags,
            mode=self.mode,
            update_if_exists=True,
        )
        self._deploy_column_settings(node)
        return node

    def deploy_dimension_links(
        self,
        name: str,
        prefix: str,
        client: DJBuilder,
        table: Table,
    ):
        """
        Deploy any links from columns on this node to columns on dimension nodes
        """
        return self._deploy_dimension_links(name, client.dimension, prefix, table)


@dataclass
class MetricYAML(NodeYAML):  # pylint: disable=too-many-instance-attributes
    """
    YAML representation of a metric node
    """

    node_type: Literal[NodeType.METRIC] = NodeType.METRIC
    query: str = ""
    display_name: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    required_dimensions: list[str] | None = None
    direction: MetricDirection | None = None
    unit: MetricUnit | None = None
    mode: NodeMode = NodeMode.PUBLISHED
    deploy_order: int = 4

    def deploy(self, name: str, prefix: str, client: DJBuilder):
        """
        Validate a node by deploying it to a temporary system space
        """
        node = client.create_metric(
            name=f"{prefix}.{name}",
            display_name=self.display_name,
            query=self.query,
            description=self.description,
            required_dimensions=self.required_dimensions,
            direction=self.direction,
            unit=self.unit,
            tags=self.tags,
            mode=self.mode,
            update_if_exists=True,
        )
        return node


@dataclass
class CubeYAML(NodeYAML):  # pylint: disable=too-many-instance-attributes
    """
    YAML representation of a cube node
    """

    node_type: Literal[NodeType.CUBE] = NodeType.CUBE
    display_name: Optional[str] = None
    metrics: List[str] = field(default_factory=list)
    dimensions: List[str] = field(default_factory=list)
    filters: Optional[List[str]] = None
    description: Optional[str] = None
    mode: NodeMode = NodeMode.PUBLISHED
    tags: Optional[List[str]] = None
    deploy_order: int = 5

    def deploy(self, name: str, prefix: str, client: DJBuilder):
        """
        Validate a node by deploying it to a temporary system space
        """
        prefixed_metrics = [
            render_prefixes(metric_name, prefix) for metric_name in self.metrics
        ]
        prefixed_dimensions = [
            render_prefixes(dimension_name, prefix)
            for dimension_name in self.dimensions
        ]
        node = client.create_cube(
            name=f"{prefix}.{name}",
            display_name=self.display_name,
            metrics=prefixed_metrics,
            dimensions=prefixed_dimensions,
            filters=self.filters,
            description=self.description,
            mode=self.mode,
            tags=self.tags,
            update_if_exists=True,
        )
        return node


@dataclass
class NodeConfig:
    """
    A single node configuration
    """

    name: str
    definition: Union[SourceYAML, TransformYAML, DimensionYAML, MetricYAML, CubeYAML]
    path: str


@dataclass
class BuildConfig:
    """
    A build configuration for a project
    """

    priority: List[str] = field(default_factory=list[str])


@dataclass
class Project:
    """
    A project configuration
    """

    name: str
    prefix: str
    root_path: str = ""
    description: str = ""
    build: BuildConfig = field(default_factory=BuildConfig)
    tags: List[TagYAML] = field(default_factory=list[TagYAML])
    mode: NodeMode = NodeMode.PUBLISHED

    @classmethod
    def load_current(cls):
        """
        Return's the nearest project configuration
        """
        return cls.load()

    @classmethod
    def load(cls, directory: Optional[str] = None):
        """
        Return's the nearest project configuration
        """
        root = find_project_root(directory)
        config_file_path = os.path.join(root, CONFIG_FILENAME)
        with open(config_file_path, encoding="utf-8") as f_config:
            config_dict = yaml.safe_load(f_config)
            config = cls(**config_dict)
            config.root_path = root
            config.build = (
                BuildConfig(**config.build)  # pylint: disable=not-a-mapping
                if isinstance(config.build, dict)
                else config.build
            )
            config.tags = (
                [
                    TagYAML(**tag) if isinstance(tag, dict) else tag
                    for tag in config.tags
                ]
                if config.tags
                else []
            )
            return config

    def compile(self) -> "CompiledProject":
        """
        Compile a loaded project by reading all of the node definition files
        """
        definitions = load_node_configs_notebook_safe(
            repository=Path(self.root_path),
            priority=self.build.priority,
        )
        compiled = asdict(self)
        compiled.update(
            {"namespaces": collect_namespaces(definitions), "definitions": definitions},
        )
        compiled_project = CompiledProject(**compiled)
        compiled_project.build = self.build
        compiled_project.tags = self.tags
        return compiled_project

    @staticmethod
    def pull(
        client: DJBuilder,
        namespace: str,
        target_path: Union[str, Path],
        ignore_existing_files: bool = False,
    ):
        """
        Pull down a namespace to a local project.
        """
        path = Path(target_path)
        if any(path.iterdir()) and not ignore_existing_files:
            raise DJClientException("The target path must be empty")
        node_definitions = client._export_namespace(  # pylint: disable=protected-access
            namespace=namespace,
        )
        with open(
            path / Path("dj.yaml"),
            "w",
            encoding="utf-8",
        ) as yaml_file:
            yaml.dump(
                {
                    "name": f"Project {namespace} (Autogenerated)",
                    "description": f"This is an autogenerated project for namespace {namespace}",
                    "prefix": namespace,
                    "build": {
                        "priority": [node["build_name"] for node in node_definitions],
                    },
                },
                yaml_file,
                sort_keys=False,
            )
        for node in node_definitions:
            del node["build_name"]
            node_definition_dir = path / Path(node.pop("directory"))
            Path.mkdir(node_definition_dir, parents=True, exist_ok=True)
            if (
                node["filename"].endswith(".dimension.yaml")
                or node["filename"].endswith(".transform.yaml")
                or node["filename"].endswith(".metric.yaml")
            ):
                node["query"] = inject_prefixes(node["query"], namespace)
            elif node["filename"].endswith(".cube.yaml"):
                node["metrics"] = [
                    inject_prefixes(metric, namespace) for metric in node["metrics"]
                ]
                node["dimensions"] = [
                    inject_prefixes(dimension, namespace)
                    for dimension in node["dimensions"]
                ]
            if node.get("dimension_links"):
                for link in node["dimension_links"]:  # pragma: no cover
                    if "dimension_node" in link:
                        link["dimension_node"] = inject_prefixes(
                            link["dimension_node"],
                            namespace,
                        )
                    if "join_on" in link:
                        link["join_on"] = inject_prefixes(link["join_on"], namespace)
                    if "dimension" in link:
                        link["dimension"] = inject_prefixes(
                            link["dimension"],
                            namespace,
                        )
            with open(
                node_definition_dir / Path(node.pop("filename")),
                "w",
                encoding="utf-8",
            ) as yaml_file:
                yaml.dump(node, yaml_file, sort_keys=False)


def collect_namespaces(node_configs: List[NodeConfig], prefix: str = ""):
    """
    Collect all namespaces that are needed to define a set of nodes
    """
    namespaces = set()
    prefixed_node_names = (
        [f"{prefix}.{config.name}" for config in node_configs]
        if prefix
        else [config.name for config in node_configs]
    )
    for name in prefixed_node_names:
        parts = name.split(".")
        num_parts = len(parts)
        for i in range(1, num_parts):
            namespace = ".".join(parts[:i])
            namespaces.add(namespace)
    return namespaces


def render_prefixes(parameterized_string: str, prefix: str):
    """
    Replaces ${prefix} in a string
    """
    return parameterized_string.replace("${prefix}", f"{prefix}.")


def inject_prefixes(unparameterized_string: str, prefix: str) -> str:
    """
    Replaces a namespace in a string with ${prefix}
    """
    return unparameterized_string.replace(f"{prefix}.", "${prefix}")


@dataclass
class CompiledProject(Project):
    """
    A compiled project with all node definitions loaded
    """

    namespaces: List[str] = field(default_factory=list)
    definitions: List[NodeConfig] = field(default_factory=list)
    validated: bool = False
    errors: List[dict] = field(default_factory=list)

    def _deploy_tags(self, prefix: str, table: Table, client: DJBuilder):
        """
        Deploy tags
        """
        if not self.tags:
            return table
        for tag in self.tags:
            prefixed_name = f"{prefix}.{tag.name}"
            try:
                new_tag = Tag(
                    name=prefixed_name,
                    description=tag.description,
                    tag_type=tag.tag_type,
                    tag_metadata=tag.tag_metadata,
                    dj_client=client,
                )
                new_tag.save()
                table.add_row(
                    *[
                        prefixed_name,
                        "[b][#3A4F6C]tag",
                        f"[green]Tag {prefixed_name} successfully created (or updated)",
                    ],
                )
            except DJClientException as exc:  # pragma: no cover
                table.add_row(*[tag.name, "tag", f"[i][red]{str(exc)}"])
                self.errors.append(
                    {"name": prefixed_name, "type": "tag", "error": str(exc)},
                )
        return table

    def _deploy_namespaces(self, prefix: str, table: Table, client: DJBuilder):
        """
        Deploy namespaces
        """
        namespaces_to_create = self.namespaces
        if prefix:  # pragma: no cover
            namespaces_to_create = [prefix] + [
                f"{prefix}.{ns}" for ns in list(self.namespaces)
            ]
        for namespace in namespaces_to_create:
            try:
                client.create_namespace(
                    namespace=namespace,
                )
                table.add_row(
                    *[
                        namespace,
                        "[b][#3A4F6C]namespace",
                        f"[green]Namespace {namespace} successfully created",
                    ],
                )
            except DJNamespaceAlreadyExists:
                table.add_row(
                    *[
                        namespace,
                        "namespace",
                        f"[i][yellow]Namespace {namespace} already exists",
                    ],
                )
            except DJClientException as exc:
                # This is a just-in-case code for some older client versions.
                if "already exists" in str(exc):
                    table.add_row(
                        *[
                            namespace,
                            "namespace",
                            f"[i][yellow]Namespace {namespace} already exists",
                        ],
                    )
                else:
                    # pragma: no cover
                    table.add_row(*[namespace, "namespace", f"[i][red]{str(exc)}"])
                    self.errors.append(
                        {
                            "name": namespace,
                            "type": "namespace",
                            "error": str(exc),
                        },
                    )
        return table

    def _deploy_nodes(
        self,
        node_configs: List[NodeConfig],
        prefix: str,
        table: Table,
        client: DJBuilder,
    ):
        """
        Deploy nodes
        """
        for node_config in node_configs:
            style = (
                "[b][#01B268]"
                if isinstance(node_config.definition, SourceYAML)
                else "[b][#0162B4]"
                if isinstance(node_config.definition, TransformYAML)
                else "[b][#A96622]"
                if isinstance(node_config.definition, DimensionYAML)
                else "[b][#A2293E]"
                if isinstance(node_config.definition, MetricYAML)
                else "[b][#580075]"
                if isinstance(node_config.definition, CubeYAML)
                else ""
            )
            try:
                rendered_node_config = deepcopy(node_config)
                prefixed_name = f"{prefix}.{node_config.name}"
                # pre-fix the query
                if isinstance(
                    node_config.definition,
                    (TransformYAML, DimensionYAML, MetricYAML),
                ):
                    rendered_node_config.definition.query = render_prefixes(  # type: ignore
                        rendered_node_config.definition.query or "",  # type: ignore
                        prefix,
                    )
                # pre-fix the tags
                project_tags = [tag.name for tag in self.tags]
                if node_config.definition.tags:
                    rendered_node_config.definition.tags = [
                        f"{prefix}.{tag}" if tag in project_tags else tag
                        for tag in node_config.definition.tags
                    ]
                created_node = rendered_node_config.definition.deploy(
                    name=rendered_node_config.name,
                    prefix=prefix,
                    client=client,
                )
                table.add_row(
                    *[
                        prefixed_name,
                        f"{style}{created_node.type}",
                        f"[green]Node {created_node.name} successfully created (or updated)",
                    ],
                )
            except DJClientException as exc:
                table.add_row(
                    *[
                        prefixed_name,
                        f"{style}{node_config.definition.node_type}",
                        f"[i][red]{str(exc)}",
                    ],
                )
                self.errors.append(
                    {"name": prefixed_name, "type": "node", "error": str(exc)},
                )

    def _deploy_dimension_links(self, prefix: str, table: Table, client: DJBuilder):
        """
        Deploy any dimension links defined within any node definition
        """
        for node_config in self.definitions:
            if isinstance(
                node_config.definition,
                (SourceYAML, TransformYAML, DimensionYAML),
            ):
                try:
                    node_config.definition.deploy_dimension_links(
                        name=node_config.name,
                        prefix=prefix,
                        client=client,
                        table=table,
                    )
                except DJClientException as exc:
                    table.add_row(
                        *[node_config.name, "[b]link[/]", f"[i][red]{str(exc)}"],
                    )
                    self.errors.append(
                        {"name": node_config.name, "type": "link", "error": str(exc)},
                    )

    def _deploy(
        self,
        client: DJBuilder,
        prefix: str,
        console: Console = Console(),
    ):
        """
        Deploy the compiled project
        """
        self.errors = []

        # Split out cube nodes to be deployed after dimensional graph
        cubes = [
            node_config
            for node_config in self.definitions
            if isinstance(node_config.definition, CubeYAML)
        ]
        non_cubes = [
            node_config
            for node_config in self.definitions
            if not isinstance(node_config.definition, CubeYAML)
        ]

        table = Table(show_footer=False)
        table_centered = Align.center(table)
        with Live(table_centered, console=console, screen=False, refresh_per_second=20):
            table.title = f"{self.name}\nDeployment for Prefix: [bold green]{prefix}[/ bold green]"
            table.box = box.SIMPLE_HEAD
            table.add_column("Name", no_wrap=True)
            table.add_column("Type", no_wrap=True)
            table.add_column("Message", no_wrap=False)
            self._deploy_tags(prefix=prefix, table=table, client=client)
            self._deploy_namespaces(prefix=prefix, table=table, client=client)
            self._deploy_nodes(
                node_configs=non_cubes,
                prefix=prefix,
                table=table,
                client=client,
            )
            self._deploy_dimension_links(prefix=prefix, table=table, client=client)
            self._deploy_nodes(
                node_configs=cubes,
                prefix=prefix,
                table=table,
                client=client,
            )

    def _cleanup_namespace(
        self,
        client: DJBuilder,
        prefix: str,
        console: Console = Console(),
    ):
        """
        Cleanup a prefix
        """
        table = Table(show_footer=False)
        table_centered = Align.center(table)
        with Live(table_centered, console=console, screen=False, refresh_per_second=20):
            table.title = (
                f"{self.name}\nCleanup for Prefix: [bold red]{prefix}[/ bold red]"
            )
            table.box = box.SIMPLE_HEAD
            table.add_column("Name", no_wrap=True)
            table.add_column("Type", no_wrap=True)
            table.add_column("Message", no_wrap=False)
            try:
                client.delete_namespace(namespace=prefix, cascade=True)
                table.add_row(
                    *[
                        prefix,
                        "[b][#3A4F6C]namespace",
                        f"[green]Namespace {prefix} successfully deleted.",
                    ],
                )
            except DJClientException as exc:
                table.add_row(*[prefix, "namespace", f"[i][red]{str(exc)}"])
                self.errors.append(
                    {
                        "name": prefix,
                        "type": "namespace",
                        "error": str(exc),
                    },
                )

    def validate(self, client, console: Console = Console(), with_cleanup: bool = True):
        """
        Validate the compiled project
        """
        self.errors = []
        console.clear()
        validation_id = "".join(random.choices(string.ascii_letters, k=16))
        system_prefix = f"system.temp.{validation_id}.{self.prefix}"
        self._deploy(client=client, prefix=system_prefix, console=console)
        if with_cleanup:  # pragma: no cover
            self._cleanup_namespace(
                client=client,
                prefix=system_prefix,
                console=console,
            )
        if self.errors:
            raise DJDeploymentFailure(project_name=self.name, errors=self.errors)
        self.validated = True

    def deploy(self, client: DJBuilder, console: Console = Console()):
        """
        Validate and deploy the compiled project
        """
        console.clear()
        if not self.validated:
            self.validate(client=client, console=console)
        self._deploy(client=client, prefix=self.prefix, console=console)
        if self.errors:  # pragma: no cover
            # .deploy() requires .validate() to have been called first so
            # theoretically this exception should never or rarely ever be
            # hit. This is just a safe fallback in cases where deploying
            # worked during validation but failed by a subsequent deployment
            # of the same set of definitions
            raise DJDeploymentFailure(project_name=self.name, errors=self.errors)


def get_name_from_path(repository: Path, path: Path) -> str:
    """
    Compute the name of a node given its path and the repository path.
    """
    # strip anything before the repository
    relative_path = path.relative_to(repository).with_suffix("")

    # Check that there are no additional dots in the node filename
    if relative_path.stem.count("."):
        raise DJClientException(
            f"Invalid node definition filename stem {relative_path.stem}, "
            "stem must only have a single dot separator and end with a node type "
            "i.e. my_node.source.yaml",
        )
    name = str(relative_path).split(".", maxsplit=1)[0]
    return name.replace(os.path.sep, ".")


async def load_data(
    repository: Path,
    path: Path,
) -> Optional[NodeConfig]:
    """
    Load data from a YAML file.
    """
    yaml_cls = (
        SourceYAML
        if path.stem.endswith(".source")
        else TransformYAML
        if path.stem.endswith(".transform")
        else DimensionYAML
        if path.stem.endswith(".dimension")
        else MetricYAML
        if path.stem.endswith(".metric")
        else CubeYAML
        if path.stem.endswith(".cube")
        else None
    )
    if not yaml_cls:
        raise DJClientException(
            f"Invalid node definition filename {path.stem}, "
            "node definition filename must end with a node type i.e. my_node.source.yaml",
        )
    with open(path, encoding="utf-8") as f_yaml:
        yaml_dict = yaml.safe_load(f_yaml)
        definition = yaml_cls.from_dict(None, yaml_dict)

        return NodeConfig(
            name=get_name_from_path(repository=repository, path=path),
            definition=definition,
            path=str(path),
        )


def load_node_configs_notebook_safe(repository: Path, priority: List[str]):
    """
    Notebook safe wrapper for load_node_configs function
    """
    try:
        asyncio.get_running_loop()
        with ThreadPoolExecutor(1) as pool:  # pragma: no cover
            node_configs = pool.submit(
                lambda: asyncio.run(
                    load_node_configs(
                        repository=repository,
                        priority=priority,
                    ),
                ),
            ).result()
    except RuntimeError:
        node_configs = asyncio.run(
            load_node_configs(repository=repository, priority=priority),
        )
    return node_configs


async def load_node_configs(
    repository: Path,
    priority: List[str],
) -> List[Optional[NodeConfig]]:
    """
    Load all configs from a repository.
    """

    # load all nodes and their dependencies, exclude CONFIG_FILENAME
    paths = {
        get_name_from_path(repository=repository, path=path): path
        for path in (
            set(repository.glob("**/*.yaml")) - set(repository.glob(CONFIG_FILENAME))
        )
    }
    node_configs = []
    for node_name in priority:
        try:
            node_configs.append(
                await load_data(repository=repository, path=paths.pop(node_name)),
            )
        except KeyError as exc:
            raise DJClientException(
                f"Build priority list includes node name {node_name} "
                "which has no corresponding definition "
                f"{paths.keys()}",
            ) from exc

    tasks = [load_data(repository=repository, path=path) for _, path in paths.items()]
    non_prioritized_nodes = [node for node in await asyncio.gather(*tasks) if node]
    non_prioritized_nodes.sort(key=lambda config: config.definition.deploy_order)
    node_configs.extend(non_prioritized_nodes)
    return node_configs
