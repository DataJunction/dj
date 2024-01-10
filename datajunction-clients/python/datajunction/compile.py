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
from pathlib import Path
from typing import Dict, List, Literal, Optional, Union

import yaml
from pydantic import BaseModel, validator
from pydantic_yaml import parse_yaml_raw_as
from rich import box
from rich.align import Align
from rich.console import Console
from rich.live import Live
from rich.table import Table

from datajunction import DJBuilder
from datajunction.exceptions import DJClientException, DJDeploymentFailure
from datajunction.models import Column, NodeMode, NodeType
from datajunction.nodes import Cube, Dimension, Metric, Source, Transform
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
        return dumper.represent_scalar("tag:yaml.org,2002:str", fixed_data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


yaml.add_representer(str, str_presenter)


def _parent_dir(path: Union[str, Path]):
    """
    Returns the parent directory
    """
    return os.path.dirname(path)


def _conf_exists(path: Union[str, Path]):
    """
    Returns True if a config exists in the Path
    """
    return os.path.isfile(os.path.join(path, CONFIG_FILENAME))


def find_project_root(directory: Optional[str] = None):
    """
    Returns the project root, identified by a root config file
    """
    checked_dir = directory or os.getcwd()
    while not _conf_exists(checked_dir):
        checked_dir = _parent_dir(checked_dir)
        if checked_dir == "/" and not _conf_exists(checked_dir):
            raise DJClientException(
                "Cannot find project root, make sure you've "
                f"defined a project in a {CONFIG_FILENAME} file",
            )

    return checked_dir


class TagYAML(BaseModel):
    """
    YAML representation of a tag
    """

    name: str
    description: str = ""
    tag_type: str
    tag_metadata: Optional[Dict] = None


class NodeYAML(BaseModel):
    """
    YAML represention of a node
    """

    deploy_order: int = 0


class SourceYAML(NodeYAML):
    """
    YAML representation of a source node
    """

    node_type: Literal[NodeType.SOURCE] = NodeType.SOURCE
    display_name: Optional[str]
    table: str
    columns: List[Column]
    description: Optional[str] = None
    primary_key: Optional[List[str]] = None
    tags: Optional[List[Tag]] = None
    mode: NodeMode = NodeMode.PUBLISHED
    dimension_links: Optional[dict] = None
    deploy_order: int = 1

    @validator("table")
    def table_is_qualified(cls, value: str) -> str:  # pylint: disable=no-self-argument
        """
        Validate that the table name is fully qualified
        """
        if (
            value.count(".") != 2
            or not value.replace(".", "").replace("_", "").isalnum()
        ):
            raise DJClientException(
                f"Invalid table name {value}, table "
                "name must be fully qualified: "
                "<catalog>.<schema>.<table>",
            )
        return value

    def deploy(self, name: str, prefix: str, client: DJBuilder):
        """
        Validate a node by deploying it to a temporary system space
        """
        catalog, schema, table = self.table.split(".")
        node = Source(
            display_name=self.display_name,
            name=f"{prefix}.{name}",
            catalog=catalog,
            schema_=schema,
            table=table,
            columns=self.columns,
            description=self.description,
            primary_key=self.primary_key,
            tags=self.tags,
            mode=self.mode,
            dj_client=client,
        )
        node.save()
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
        if self.dimension_links:
            prefixed_name = f"{prefix}.{name}"
            node = client.source(prefixed_name)
            for column, dimension_column in self.dimension_links.items():
                prefixed_dimension = render_prefixes(
                    dimension_column["dimension"],
                    prefix,
                )
                node.link_dimension(
                    column,
                    prefixed_dimension,
                )
                table.add_row(
                    *[
                        prefixed_name,
                        "[b]link[/]",
                        (
                            f"[green]Column {column} linked to dimension {prefixed_dimension}"
                        ),
                    ]
                )


class TransformYAML(NodeYAML):
    """
    YAML representation of a transform node
    """

    node_type: Literal[NodeType.TRANSFORM] = NodeType.TRANSFORM
    query: str
    display_name: Optional[str]
    description: Optional[str] = None
    primary_key: Optional[List[str]] = None
    tags: Optional[List[Tag]] = None
    mode: NodeMode = NodeMode.PUBLISHED
    dimension_links: Optional[dict] = None
    deploy_order: int = 2

    def deploy(self, name: str, prefix: str, client: DJBuilder):
        """
        Validate a node by deploying it to a temporary system space
        """
        node = Transform(
            name=f"{prefix}.{name}",
            display_name=self.display_name,
            query=self.query,
            description=self.description,
            primary_key=self.primary_key,
            tags=self.tags,
            mode=self.mode,
            dj_client=client,
        )
        node.save()
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
        if self.dimension_links:
            prefixed_name = f"{prefix}.{name}"
            node = client.transform(prefixed_name)
            for column, dimension_column in self.dimension_links.items():
                prefixed_dimension = render_prefixes(
                    dimension_column["dimension"],
                    prefix,
                )
                node.link_dimension(
                    column,
                    prefixed_dimension,
                )
                table.add_row(
                    *[
                        prefixed_name,
                        "[b]link[/]",
                        (
                            f"[green]Column {column} linked to column {dimension_column} "
                            f"on dimension {prefixed_dimension}"
                        ),
                    ]
                )


class DimensionYAML(NodeYAML):
    """
    YAML representation of a dimension node
    """

    node_type: Literal[NodeType.DIMENSION] = NodeType.DIMENSION
    query: str
    display_name: Optional[str]
    description: Optional[str]
    primary_key: Optional[List[str]]
    tags: Optional[List[Tag]]
    mode: NodeMode = NodeMode.PUBLISHED
    dimension_links: Optional[dict] = None
    deploy_order: int = 3

    def deploy(self, name: str, prefix: str, client: DJBuilder):
        """
        Validate a node by deploying it to a temporary system space
        """
        node = Dimension(
            name=f"{prefix}.{name}",
            display_name=self.display_name,
            query=self.query,
            description=self.description,
            primary_key=self.primary_key,
            tags=self.tags,
            mode=self.mode,
            dj_client=client,
        )
        node.save()
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
        if self.dimension_links:
            prefixed_name = f"{prefix}.{name}"
            node = client.dimension(prefixed_name)
            for column, dimension_column in self.dimension_links.items():
                prefixed_dimension = render_prefixes(
                    dimension_column["dimension"],
                    prefix,
                )
                node.link_dimension(
                    column,
                    prefixed_dimension,
                )
                table.add_row(
                    *[
                        prefixed_name,
                        "[b]link[/]",
                        (
                            f"[green]Column {column} linked to column {dimension_column} "
                            f"on dimension {prefixed_dimension}"
                        ),
                    ]
                )


class MetricYAML(NodeYAML):
    """
    YAML representation of a metric node
    """

    node_type: Literal[NodeType.METRIC] = NodeType.METRIC
    query: str
    display_name: Optional[str]
    description: Optional[str]
    tags: Optional[List[Tag]]
    mode: NodeMode = NodeMode.PUBLISHED
    deploy_order: int = 4

    def deploy(self, name: str, prefix: str, client: DJBuilder):
        """
        Validate a node by deploying it to a temporary system space
        """
        node = Metric(
            name=f"{prefix}.{name}",
            display_name=self.display_name,
            query=self.query,
            description=self.description,
            tags=self.tags,
            mode=self.mode,
            dj_client=client,
        )
        node.save()
        return node


class CubeYAML(NodeYAML):
    """
    YAML representation of a cube node
    """

    node_type: Literal[NodeType.CUBE] = NodeType.CUBE
    display_name: Optional[str]
    metrics: List[str]
    dimensions: List[str]
    filters: Optional[List[str]] = None
    description: Optional[str] = None
    mode: NodeMode = NodeMode.PUBLISHED
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
        node = Cube(
            name=f"{prefix}.{name}",
            display_name=self.display_name,
            metrics=prefixed_metrics,
            dimensions=prefixed_dimensions,
            filters=self.filters,
            description=self.description,
            mode=self.mode,
            dj_client=client,
        )
        node.save()
        return node


class NodeConfig(BaseModel):
    """
    A single node configuration
    """

    name: str
    definition: NodeYAML
    path: str


class BuildConfig(BaseModel):
    """
    A build configuration for a project
    """

    priority: List[str] = []


class Project(BaseModel):
    """
    A project configuration
    """

    name: str
    prefix: str
    build: BuildConfig = BuildConfig()
    root_path: str = ""
    tags: Optional[List[TagYAML]] = []
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
            config = parse_yaml_raw_as(cls, f_config)
            config.root_path = root
            return config

    def compile(self) -> "CompiledProject":
        """
        Compile a loaded project by reading all of the node definition files
        """
        definitions = load_node_configs_notebook_safe(
            repository=Path(self.root_path),
            priority=self.build.priority,
        )
        compiled = self.dict()
        compiled.update(
            {"namespaces": collect_namespaces(definitions), "definitions": definitions},
        )
        return CompiledProject(**compiled)

    @staticmethod
    def pull(
        client: DJBuilder,
        namespace: str,
        target_path: Union[str, Path],
        ignore_existing_files: bool = False,
    ):
        """
        Pull down a namespace to a local project
        """
        path = Path(target_path)
        if any(path.iterdir()) and not ignore_existing_files:
            raise DJClientException("The target path must be empty")
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
                },
                yaml_file,
            )
        node_definitions = client._export_namespace(  # pylint: disable=protected-access
            namespace=namespace,
        )
        for node in node_definitions:
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
                for _, dim in node["dimension_links"].items():
                    dim["dimension"] = inject_prefixes(dim["dimension"], namespace)
            with open(
                node_definition_dir / Path(node.pop("filename")),
                "w",
                encoding="utf-8",
            ) as yaml_file:
                yaml.dump(node, yaml_file)


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


def inject_prefixes(unparameterized_string: str, prefix: str):
    """
    Replaces a namespace in a string with ${prefix}
    """
    return unparameterized_string.replace(f"{prefix}.", "${prefix}")


class CompiledProject(Project):
    """
    A compiled project with all node definitions loaded
    """

    namespaces: List[str]
    definitions: List[NodeConfig]
    validated: bool = False
    errors: List[dict] = []

    def _deploy_tags(self, prefix: str, table: Table, client: DJBuilder):
        """
        Deploy tags
        """
        if self.tags:
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
                            f"[green]Tag {prefixed_name} successfully created",
                        ]
                    )
                except DJClientException as exc:  # pragma: no cover
                    table.add_row(*[tag.name, "tag", f"[i][red]{str(exc)}"])
                    self.errors.append(
                        {
                            "name": prefixed_name,
                            "type": "tag",
                            "error": exc,
                        },
                    )
        return table

    def _deploy_namespaces(self, prefix: str, table: Table, client: DJBuilder):
        """
        Deploy namespaces
        """
        for namespace in self.namespaces + [prefix]:
            prefixed_name = f"{prefix}.{namespace}" if namespace != prefix else prefix
            try:
                client.create_namespace(
                    namespace=prefixed_name,
                )
                table.add_row(
                    *[
                        prefixed_name,
                        "[b][#3A4F6C]namespace",
                        f"[green]Namespace {prefixed_name} successfully created",
                    ]
                )
            except DJClientException as exc:
                if "already exists" in str(exc):
                    table.add_row(
                        *[
                            prefixed_name,
                            "[b][#3A4F6C]namespace",
                            f"[green]Namespace {prefixed_name} successfully created",
                        ]
                    )
                else:  # pragma: no cover
                    table.add_row(*[namespace, "namespace", f"[i][red]{str(exc)}"])
                    self.errors.append(
                        {
                            "name": prefixed_name,
                            "type": "namespace",
                            "error": exc,
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
                rendered_node_config = node_config.copy(deep=True)
                if isinstance(
                    node_config.definition,
                    (TransformYAML, DimensionYAML, MetricYAML),
                ):
                    rendered_node_config.definition.query = render_prefixes(
                        node_config.definition.query,
                        prefix,
                    )
                created_node = rendered_node_config.definition.deploy(
                    name=node_config.name,
                    prefix=prefix,
                    client=client,
                )
                table.add_row(
                    *[
                        node_config.name,
                        f"{style}{created_node.type}",
                        f"[green]Node {created_node.name} successfully created",
                    ]
                )
            except DJClientException as exc:
                table.add_row(
                    *[
                        node_config.name,
                        f"{style}{node_config.definition.node_type}",
                        f"[i][red]{str(exc)}",
                    ]
                )
                self.errors.append(
                    {
                        "name": node_config.name,
                        "type": "node",
                        "error": exc,
                    },
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
                        *[node_config.name, "[b]link[/]", f"[i][red]{str(exc)}"]
                    )
                    self.errors.append(
                        {"name": node_config.name, "type": "link", "error": exc},
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
            table.title = (
                f"{self.name}\nDeployment Prefix: [bold green]{prefix}[/ bold green]"
            )
            table.box = box.SIMPLE_HEAD
            table.add_column("Name", no_wrap=True)
            table.add_column("Type", no_wrap=True)
            table.add_column("", no_wrap=True)
            self._deploy_tags(prefix=prefix, table=table, client=client)
            self._deploy_namespaces(prefix=prefix, table=table, client=client)
            self._deploy_nodes(
                node_configs=non_cubes,
                prefix=prefix,
                table=table,
                client=client,
            )
            # Deploy dimensional graph before deploying cubes
            self._deploy_dimension_links(prefix=prefix, table=table, client=client)
            self._deploy_nodes(
                node_configs=cubes,
                prefix=prefix,
                table=table,
                client=client,
            )

    def validate(self, client, console: Console = Console()):
        self.errors = []
        console.clear()
        validation_id = "".join(random.choices(string.ascii_letters, k=16))
        system_prefix = f"system.{validation_id}.{self.prefix}"
        self._deploy(client=client, prefix=system_prefix, console=console)
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
    if yaml_cls:
        with open(path, encoding="utf-8") as f_yaml:
            definition = parse_yaml_raw_as(yaml_cls, f_yaml)
            return NodeConfig(
                name=get_name_from_path(repository=repository, path=path),
                definition=definition,
                path=str(path),
            )
    return None


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
