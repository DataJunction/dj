import asyncio
import logging
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Coroutine, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload, defer

from datajunction_server.api.helpers import (
    get_attribute_type,
    get_node_namespace,
    COLUMN_NAME_REGEX,
    map_dimensions_to_roles,
)
from datajunction_server.construction.build_v2 import FullColumnName
from datajunction_server.database import Node, NodeRevision
from datajunction_server.database.attributetype import AttributeType
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.column import Column, ColumnAttribute
from datajunction_server.database.dimensionlink import DimensionLink, JoinType
from datajunction_server.database.history import History
from datajunction_server.database.metricmetadata import MetricMetadata
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import NodeRelationship
from datajunction_server.database.partition import Partition
from datajunction_server.database.tag import Tag
from datajunction_server.database.user import User, OAuthProvider
from datajunction_server.errors import (
    DJError,
    DJInvalidDeploymentConfig,
    DJInvalidInputException,
    DJWarning,
    ErrorCode,
)
from datajunction_server.internal.deployment.utils import (
    extract_node_graph,
    topological_levels,
    DeploymentContext,
)
from datajunction_server.internal.deployment.validation import (
    NodeValidationResult,
    CubeValidationData,
    bulk_validate_node_data,
)
from datajunction_server.internal.history import EntityType
from datajunction_server.internal.nodes import (
    hard_delete_node,
    validate_complex_dimension_link,
)
from datajunction_server.models.attribute import ColumnAttributes
from datajunction_server.models.base import labelize
from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DeploymentResult,
    DeploymentSpec,
    DeploymentStatus,
    DimensionJoinLinkSpec,
    DimensionReferenceLinkSpec,
    LinkableNodeSpec,
    MetricSpec,
    NodeSpec,
    SourceSpec,
    TagSpec,
)
from datajunction_server.models.dimensionlink import (
    JoinLinkInput,
    LinkType,
)
from datajunction_server.models.history import ActivityType
from datajunction_server.models.node import (
    DEFAULT_DRAFT_VERSION,
    DEFAULT_PUBLISHED_VERSION,
    NodeMode,
    NodeStatus,
    NodeType,
)
from datajunction_server.utils import (
    SEPARATOR,
    Version,
    get_namespace_from_name,
    get_settings,
)


logger = logging.getLogger(__name__)


@dataclass
class ResourceRegistry:
    """
    Tracks all resources created/used during deployment
    """

    namespaces: list[NodeNamespace] = field(default_factory=list)
    tags: dict[str, Tag] = field(default_factory=dict)
    owners: dict[str, User] = field(default_factory=dict)
    catalogs: dict[str, Catalog] = field(default_factory=dict)
    nodes: dict[str, Node] = field(default_factory=dict)
    attributes: dict[str, AttributeType] = field(default_factory=dict)

    def add_tags(self, tags: dict[str, Tag]):
        self.tags.update(tags)

    def add_owners(self, owners: dict[str, User]):
        self.owners.update(owners)

    def add_catalogs(self, catalogs: dict[str, Catalog]):
        self.catalogs.update(catalogs)

    def add_nodes(self, nodes: dict[str, Node]):
        self.nodes.update(nodes)

    def set_namespaces(self, namespaces: list[NodeNamespace]):
        self.namespaces = namespaces

    def add_attributes(self, attributes: dict[str, AttributeType]):
        self.attributes.update(attributes)


@dataclass
class DeploymentPlan:
    to_deploy: list[NodeSpec]
    to_skip: list[NodeSpec]
    to_delete: list[NodeSpec]
    existing_specs: dict[str, NodeSpec]
    node_graph: dict[str, list[str]]
    external_deps: set[str]

    def is_empty(self) -> bool:
        return not self.to_deploy and not self.to_delete

    @property
    def linked_dimension_nodes(self) -> set[str]:
        return {
            link.rendered_dimension_node
            for node_spec in self.to_deploy
            if isinstance(node_spec, LinkableNodeSpec) and node_spec.dimension_links
            for link in node_spec.dimension_links
        }


class DeploymentOrchestrator:
    """
    Handles validation and deployment of all resources in a deployment specification
    """

    def __init__(
        self,
        deployment_spec: DeploymentSpec,
        deployment_id: str,
        session: AsyncSession,
        context: DeploymentContext,
    ):
        # Deployment context
        self.deployment_spec = deployment_spec
        self.deployment_id = deployment_id
        self.session = session
        self.context = context

        # Deployment state tracking
        self.registry = ResourceRegistry()
        self.errors: list[DJError] = []
        self.warnings: list[DJError] = []
        self.deployed_results: list[DeploymentResult] = []

    async def execute(self) -> list[DeploymentResult]:
        """
        Validate and deploy all resources and nodes into the specified namespace.
        """
        start_total = time.perf_counter()
        logger.info(
            "Starting deployment of %d nodes in namespace %s",
            len(self.deployment_spec.nodes),
            self.deployment_spec.namespace,
        )
        await self._setup_deployment_resources()
        await self._validate_deployment_resources()

        deployment_plan = await self._create_deployment_plan()
        if deployment_plan.is_empty():
            return await self._handle_no_changes()

        await self._execute_deployment_plan(deployment_plan)
        logger.info(
            "Completed deployment for %s [%s] in %.3fs",
            self.deployment_spec.namespace,
            self.deployment_id,
            time.perf_counter() - start_total,
        )
        return self.deployed_results

    async def _update_deployment_status(self):
        """
        Update deployment status with current results
        """
        from datajunction_server.api.deployments import InProcessExecutor

        await InProcessExecutor.update_status(
            self.deployment_id,
            DeploymentStatus.RUNNING,
            self.deployed_results,
        )

    async def _setup_deployment_resources(self):
        """
        Setup all deployment-level resources
        """
        self.registry.set_namespaces(await self._setup_namespaces())
        self.registry.add_tags(await self._setup_tags())
        self.registry.add_owners(await self._setup_owners())
        self.registry.add_catalogs(await self._setup_catalogs())
        self.registry.add_attributes(await self._setup_attributes())
        logger.info(
            "Set up deployment resources: %d namespaces, %d tags, %d owners, %d catalogs, %d attributes",
            len(self.registry.namespaces),
            len(self.registry.tags),
            len(self.registry.owners),
            len(self.registry.catalogs),
            len(self.registry.attributes),
        )

    async def _validate_deployment_resources(self):
        """Validate deployment configuration and fail fast if invalid"""
        # Check for duplicate node specs
        node_names = [node.rendered_name for node in self.deployment_spec.nodes]
        if len(node_names) != len(set(node_names)):  # pragma: no branch
            duplicates = [
                name for name, count in Counter(node_names).items() if count > 1
            ]
            self.errors.append(
                DJError(
                    code=ErrorCode.ALREADY_EXISTS,
                    message=f"Duplicate nodes in deployment spec: {', '.join(duplicates)}",
                ),
            )

        if self.errors:
            raise DJInvalidDeploymentConfig(
                message="Invalid deployment configuration",
                errors=self.errors,
                warnings=self.warnings,
            )

    async def _auto_register_sources(
        self,
        missing_nodes: list[str],
    ) -> list[SourceSpec]:
        """
        Auto-register missing source nodes by introspecting catalog tables.

        This method takes a list of missing node names and attempts to register
        any that match the catalog.schema.table pattern by introspecting the
        catalog to get the table schema.

        Args:
            missing_nodes: List of node names that are missing from both
                          the deployment and the existing system

        Returns:
            List of SourceSpec objects that were successfully auto-registered
        """
        if not self.deployment_spec.auto_register_sources:
            return []

        if not missing_nodes:
            logger.info("No missing nodes to auto-register")
            return []

        settings = get_settings()
        source_prefix = settings.source_node_namespace

        logger.info("Attempting to auto-register missing sources...")
        auto_registered_sources: list[SourceSpec] = []

        # Filter to only nodes that look like catalog.schema.table,
        # stripping the configured source namespace prefix if present.
        missing_nodes_to_check: list[str] = []
        for missing_node_name in missing_nodes:
            parts = missing_node_name.split(".")
            if source_prefix and parts[0] == source_prefix:
                parts = parts[1:]
                missing_node_name = ".".join(parts)

            # Only consider tables that look like catalog.schema.table (3 parts)
            if len(parts) == 3:
                missing_nodes_to_check.append(missing_node_name)

        if not missing_nodes_to_check:
            logger.info("No missing nodes match catalog.schema.table pattern")
            return []

        # Check if query service client is available
        if not self.context.query_service_client:
            logger.warning(
                "Query service client not available, cannot auto-register sources",
            )
            return []

        # Load all catalogs that match the first part of each candidate name.
        # Only attempt auto-registration for nodes whose catalog actually exists.
        candidate_catalog_names = {
            name.split(".")[0] for name in missing_nodes_to_check
        }
        available_catalogs = await Catalog.get_by_names(
            self.session,
            list(candidate_catalog_names),
        )
        available_catalog_map = {
            catalog.name: catalog for catalog in available_catalogs
        }
        missing_nodes_to_check = [
            name
            for name in missing_nodes_to_check
            if name.split(".")[0] in available_catalog_map
        ]

        if not missing_nodes_to_check:
            return []

        # Each SQL query runs against a single engine, so all missing source tables
        # must belong to the same catalog. If multiple catalogs are referenced, that
        # indicates a cross-catalog query which no engine can execute.
        if len(available_catalog_map) > 1:
            self.errors.append(
                DJError(
                    code=ErrorCode.UNKNOWN_ERROR,
                    message=(
                        "Auto-registration requires all missing source tables to belong to "
                        "the same catalog, but found tables from multiple catalogs: "
                        + ", ".join(sorted(available_catalog_map))
                        + ". Each SQL query runs against a single engine and cannot "
                        "reference tables from different catalogs."
                    ),
                ),
            )
            return []

        catalog_name = next(iter(available_catalog_map))
        _catalog = available_catalog_map[catalog_name]

        logger.info(
            "Found %d potential sources to auto-register in catalog %s: %s",
            len(missing_nodes_to_check),
            catalog_name,
            ", ".join(sorted(missing_nodes_to_check)),
        )

        # Build the flat list of (catalog, schema, table) tuples and a lookup back to names
        tables = []
        node_info = {}
        for missing_node_name in missing_nodes_to_check:
            catalog, schema, table = missing_node_name.split(".")
            tables.append((catalog, schema, table))
            node_info[(catalog, schema, table)] = missing_node_name

        request_headers = (
            dict(self.context.request.headers) if self.context.request else {}
        )
        engine = _catalog.engines[0] if _catalog.engines else None

        try:
            columns_by_table = (
                self.context.query_service_client.get_columns_for_tables_batch(
                    tables=tables,
                    request_headers=request_headers,
                    engine=engine,
                )
            )

            for (catalog, schema, table), columns in columns_by_table.items():
                if not columns:
                    logger.warning(
                        "No columns found for %s.%s.%s, skipping auto-registration",
                        catalog,
                        schema,
                        table,
                    )
                    continue

                missing_node_name = node_info[(catalog, schema, table)]
                source_spec = SourceSpec(
                    name=missing_node_name,
                    catalog=catalog,
                    schema_=schema,
                    table=table,
                    columns=[
                        ColumnSpec(name=col.name, type=str(col.type)) for col in columns
                    ],
                    description=(
                        f"Auto-registered source from {catalog}.{schema}.{table}"
                    ),
                )
                auto_registered_sources.append(source_spec)
                logger.info(
                    "Auto-registered source %s with %d columns",
                    missing_node_name,
                    len(columns),
                )

        except Exception as exc:
            for catalog, schema, table in tables:
                missing_node_name = node_info[(catalog, schema, table)]
                self.errors.append(
                    DJError(
                        code=ErrorCode.UNKNOWN_ERROR,
                        message=(
                            f"Failed to auto-register source `{missing_node_name}`: {str(exc)}"
                        ),
                    ),
                )
            logger.error(
                "Failed to auto-register sources in catalog %s: %s",
                catalog_name,
                exc,
            )

        if auto_registered_sources:
            logger.info(
                "Successfully auto-registered %d sources",
                len(auto_registered_sources),
            )

        return auto_registered_sources

    async def _handle_no_changes(self) -> list[DeploymentResult]:
        """Handle case where no deployment changes are needed"""
        logger.info("No changes detected, skipping deployment")
        await self._update_deployment_status()
        return self.deployed_results

    async def _find_namespaces_to_create(self) -> set[str]:
        """
        Identify all namespaces that need to be created based on nodes in the deployment.
        """
        deployment_namespace = self.deployment_spec.namespace
        all_namespaces = {deployment_namespace}  # Start with deployment namespace

        def add_namespace_hierarchy(node_name: str) -> bool:
            """
            Helper to add a node's namespace and all parent namespaces.
            Returns True if the node is under the deployment namespace, False otherwise.
            """
            if SEPARATOR not in node_name:
                return True  # Root-level nodes are considered valid

            # Get the node's direct namespace (everything except the leaf name)
            node_namespace = node_name.rsplit(SEPARATOR, 1)[0]

            # Ensure node is actually under deployment namespace
            if not node_namespace.startswith(deployment_namespace):
                return False

            # Add all parent namespaces from deployment root to node's namespace
            parts = node_namespace.split(SEPARATOR)
            deployment_parts = deployment_namespace.split(SEPARATOR)

            # Build all namespace levels from deployment root to node's namespace
            for i in range(len(deployment_parts), len(parts) + 1):
                if parent_namespace := SEPARATOR.join(parts[:i]):  # pragma: no branch
                    all_namespaces.add(parent_namespace)

            return True

        # Process each node in the deployment spec
        for node in self.deployment_spec.nodes:
            node_name = node.rendered_name
            if not add_namespace_hierarchy(node_name):
                self.errors.append(
                    DJError(
                        code=ErrorCode.INVALID_NAMESPACE,
                        message=f"Node '{node_name}' is not under deployment namespace '{deployment_namespace}'",
                        context="namespace validation",
                    ),
                )
                continue

            # Also add namespaces for dimension link targets
            if isinstance(node, LinkableNodeSpec) and node.dimension_links:
                for link in node.dimension_links:
                    add_namespace_hierarchy(link.rendered_dimension_node)

        return all_namespaces

    async def _setup_namespaces(self) -> list[NodeNamespace]:
        namespace_start = time.perf_counter()
        to_create = await self._find_namespaces_to_create()
        node_namespaces = []
        for namespace in to_create:
            node_namespace = await get_node_namespace(  # pragma: no cover
                session=self.session,
                namespace=namespace,
                raise_if_not_exists=False,
            )
            if not node_namespace:
                logger.info("Creating namespace `%s`", namespace)
                node_namespace = NodeNamespace(namespace=namespace)
                self.session.add(node_namespace)
            node_namespaces.append(node_namespace)
        await self.session.flush()
        logger.info(
            "Created %d namespaces in %.3fs",
            len(to_create),
            time.perf_counter() - namespace_start,
        )
        return node_namespaces

    async def _setup_tags(
        self,
    ) -> dict[str, Tag]:
        """
        Validate and upsert all tags defined in the deployment spec and used by nodes.
        """
        deployment_tag_specs = {
            tag_spec.name: tag_spec for tag_spec in self.deployment_spec.tags
        }
        used_tag_names = {
            tag for spec in self.deployment_spec.nodes for tag in spec.tags
        }
        all_tag_names = deployment_tag_specs.keys() | used_tag_names
        existing_tags = {
            tag.name: tag
            for tag in (
                await Tag.find_tags(self.session, tag_names=list(all_tag_names))
                if all_tag_names
                else []
            )
        }

        # Validate all used tags are defined, either in the deployment spec or already exist
        undefined_tags = (
            used_tag_names
            - set(deployment_tag_specs.keys())
            - set(existing_tags.keys())
        )
        if undefined_tags:
            self.errors.append(
                DJError(
                    code=ErrorCode.TAG_NOT_FOUND,
                    message=f"Tags used by nodes but not defined: {', '.join(undefined_tags)}",
                ),
            )

        # Upsert tags
        for tag_name, tag_spec in deployment_tag_specs.items():
            if tag_name in existing_tags:
                tag = existing_tags[tag_name]
                if tag_needs_update(tag, tag_spec):
                    tag.tag_type = tag_spec.tag_type
                    tag.description = tag_spec.description
                    tag.display_name = tag_spec.display_name or labelize(tag_name)
                    self.session.add(tag)
            else:
                tag = Tag(
                    name=tag_name,
                    tag_type=tag_spec.tag_type,
                    description=tag_spec.description,
                    display_name=tag_spec.display_name or labelize(tag_name),
                    created_by=self.context.current_user,
                )
                self.session.add(tag)
            existing_tags[tag_name] = tag

        await self.session.flush()  # Get IDs but don't commit
        return existing_tags

    async def _setup_owners(self):
        """
        Validate that all owners defined in the deployment spec exist.
        """
        usernames = [
            owner
            for node_spec in self.deployment_spec.nodes
            for owner in node_spec.owners
            if node_spec.owners
        ]
        existing_users = await User.get_by_usernames(
            self.session,
            usernames,
            raise_if_not_exists=False,
        )
        existing_usernames = {user.username for user in existing_users}
        missing_usernames = set(usernames) - existing_usernames
        new_users = []
        if missing_usernames:
            new_users = [
                User(username=username, oauth_provider=OAuthProvider.BASIC)
                for username in missing_usernames
            ]
            self.session.add_all(new_users)
            self.warnings.append(
                DJWarning(
                    code=ErrorCode.USER_NOT_FOUND,
                    message=(
                        f"The following owners do not exist and will be created: "
                        f"{', '.join(missing_usernames)}"
                    ),
                ),
            )
        await self.session.flush()
        return {user.username: user for user in existing_users + new_users}

    async def _setup_attributes(self) -> dict[str, AttributeType]:
        """
        Validate and load all attributes defined in the deployment spec and used by nodes.
        """
        return {
            attr_name.value: cast(
                AttributeType,
                await get_attribute_type(
                    self.session,
                    name=attr_name.value,
                ),
            )
            for attr_name in ColumnAttributes
        }

    async def _setup_catalogs(self) -> dict[str, Catalog]:
        """
        Validate that all catalogs defined in the deployment spec exist.
        """
        catalog_names = {
            node_spec.catalog
            for node_spec in self.deployment_spec.nodes
            if node_spec.node_type == NodeType.SOURCE and node_spec.catalog
        }
        if not catalog_names:
            return {}
        existing_catalogs = await Catalog.get_by_names(
            self.session,
            list(catalog_names),
        )
        existing_catalog_names = {catalog.name for catalog in existing_catalogs}
        missing_catalogs = catalog_names - existing_catalog_names
        if missing_catalogs:
            self.errors.append(
                DJError(
                    code=ErrorCode.CATALOG_NOT_FOUND,
                    message=(
                        f"The following catalogs do not exist: {', '.join(missing_catalogs)}"
                    ),
                ),
            )
        return {catalog.name: catalog for catalog in existing_catalogs}

    async def _create_deployment_plan(self) -> DeploymentPlan:
        """Analyze existing nodes and create deployment plan"""
        nodes_start = time.perf_counter()

        # Load existing nodes
        all_nodes = await NodeNamespace.list_all_nodes(
            self.session,
            self.deployment_spec.namespace,
            options=Node.cube_load_options(),
        )
        self.registry.add_nodes({node.name: node for node in all_nodes})
        existing_specs = {
            node.name: await node.to_spec(self.session) for node in all_nodes
        }

        logger.info(
            "Fetched %d existing nodes in %.3fs",
            len(existing_specs),
            time.perf_counter() - nodes_start,
        )

        # Determine what to deploy/skip/delete
        to_deploy, to_skip, to_delete = self.filter_nodes_to_deploy(existing_specs)

        # Add skipped nodes to results
        self.deployed_results.extend(
            [
                DeploymentResult(
                    name=node_spec.rendered_name,
                    deploy_type=DeploymentResult.Type.NODE,
                    status=DeploymentResult.Status.SKIPPED,
                    operation=DeploymentResult.Operation.NOOP,
                    message=f"Node {node_spec.rendered_name} is unchanged.",
                )
                for node_spec in to_skip
            ],
        )

        # Build deployment graph if needed
        node_graph = {}
        external_deps: set[str] = set()
        if to_deploy or to_delete:
            node_graph = extract_node_graph(
                [node for node in to_deploy if not isinstance(node, CubeSpec)],
            )
            external_deps, auto_registered_sources = await self.check_external_deps(
                node_graph,
            )

            # If sources were auto-registered, check which ones don't exist yet
            if auto_registered_sources:
                logger.info(
                    "Checking if %d auto-registered sources already exist",
                    len(auto_registered_sources),
                )

                # Check which auto-registered sources actually need to be created
                auto_source_names = [
                    src.rendered_name for src in auto_registered_sources
                ]
                existing_auto_sources = await Node.get_by_names(
                    self.session,
                    auto_source_names,
                )
                existing_auto_source_names = {
                    node.name for node in existing_auto_sources
                }

                # Only include sources that don't already exist
                sources_to_create = [
                    src
                    for src in auto_registered_sources
                    if src.rendered_name not in existing_auto_source_names
                ]

                if sources_to_create:
                    logger.info(
                        "Adding %d new auto-registered sources to deployment spec (skipping %d that already exist)",
                        len(sources_to_create),
                        len(auto_registered_sources) - len(sources_to_create),
                    )
                    # Prepend auto-registered sources so they're created first
                    self.deployment_spec.nodes = (
                        sources_to_create + self.deployment_spec.nodes
                    )

                    # Rebuild the deployment plan with the new sources included
                    logger.info(
                        "Rebuilding deployment plan with auto-registered sources",
                    )
                    to_deploy = sources_to_create + to_deploy
                else:
                    logger.info("All auto-registered sources already exist, skipping")
                    # All sources already exist, just rebuild graph to include them
                    sources_to_create = []

                # Add existing auto-registered sources to existing_specs so they can be found during link validation
                for existing_node in existing_auto_sources:
                    if existing_node.name not in existing_specs:  # pragma: no branch
                        existing_specs[
                            existing_node.name
                        ] = await existing_node.to_spec(self.session)
                        logger.info(
                            "Added existing auto-registered source %s to registry",
                            existing_node.name,
                        )

                node_graph = extract_node_graph(
                    [node for node in to_deploy if not isinstance(node, CubeSpec)],
                )

                # Re-check external deps (should be empty now, or raise error)
                external_deps, more_auto_registered = await self.check_external_deps(
                    node_graph,
                )
                if more_auto_registered:  # pragma: no cover
                    # This shouldn't happen, but handle it gracefully
                    raise DJInvalidDeploymentConfig(
                        message="Unexpected second round of auto-registration needed",
                    )

        return DeploymentPlan(
            to_deploy=to_deploy,
            to_skip=to_skip,
            to_delete=to_delete,
            existing_specs=existing_specs,
            node_graph=node_graph,
            external_deps=external_deps,
        )

    async def _execute_deployment_plan(self, plan: DeploymentPlan):
        """Execute the actual deployment based on the plan"""
        if plan.to_deploy:
            deployed_results, deployed_nodes = await self._deploy_nodes(plan)
            self.deployed_results.extend(deployed_results)
            self.registry.add_nodes(deployed_nodes)
            await self._update_deployment_status()

            deployed_links = await self._deploy_links(plan)
            self.deployed_results.extend(deployed_links)
            await self._update_deployment_status()

            deployed_cubes = await self._deploy_cubes(plan)
            self.deployed_results.extend(deployed_cubes)
            await self._update_deployment_status()

        if plan.to_delete:
            delete_results = await self._delete_nodes(plan.to_delete)
            self.deployed_results.extend(delete_results)
            await self._update_deployment_status()

    async def _deploy_nodes(
        self,
        plan: DeploymentPlan,
    ) -> tuple[list[DeploymentResult], dict[str, Node]]:
        """Deploy nodes in the plan"""

        deployed_results, deployed_nodes = [], {}

        # Order nodes topologically based on dependencies
        levels = topological_levels(plan.node_graph, ascending=False)
        logger.info(
            "Deploying nodes in topological order with %d levels",
            len(levels),
        )

        # Deploy them level by level (excluding cubes which are handled separately)
        name_to_node_specs = {
            node_spec.rendered_name: node_spec
            for node_spec in plan.to_deploy
            if not isinstance(node_spec, CubeSpec)
        }
        for level in levels:
            node_specs = [
                name_to_node_specs[node_name]
                for node_name in level
                if node_name in name_to_node_specs
                and node_name not in plan.external_deps
            ]
            if node_specs:
                level_results, nodes = await self.bulk_deploy_nodes_in_level(
                    node_specs,
                    plan.node_graph,
                )
                deployed_results.extend(level_results)
                deployed_nodes.update(nodes)
                self.registry.add_nodes(nodes)

        logger.info("Finished deploying %d non-cube nodes", len(deployed_nodes))
        return deployed_results, deployed_nodes

    async def _deploy_links(self, plan: DeploymentPlan) -> list[DeploymentResult]:
        """Deploy dimension links for nodes in the plan"""
        deployed_links = []

        # Load dimension nodes once
        dimensions_map = {
            node.name: node
            for node in await Node.get_by_names(
                self.session,
                list(plan.linked_dimension_nodes),
            )
        }
        self.registry.add_nodes(dimensions_map)

        validation_results = await self.validate_dimension_links(plan)

        for node_spec in plan.to_deploy:
            if not isinstance(node_spec, LinkableNodeSpec):
                continue
            existing_node_spec = cast(
                LinkableNodeSpec,
                plan.existing_specs.get(node_spec.rendered_name),
            )
            existing_node_links = (
                existing_node_spec.links_mapping if existing_node_spec else {}
            )
            desired_node_links = node_spec.links_mapping

            # Delete removed links
            to_delete = {
                existing_node_links[(dim, role)]
                for (dim, role) in existing_node_links
                if (dim, role) not in desired_node_links
            }
            self.deployed_results.extend(
                await self._bulk_delete_links(to_delete, node_spec),
            )

            # Create or update links
            for link_spec in node_spec.dimension_links or []:
                link_result = await self._process_node_dimension_link(
                    node_spec=node_spec,
                    link_spec=link_spec,
                    validation_results=validation_results,
                )
                deployed_links.append(link_result)
        await self.session.commit()
        logger.info("Finished deploying %d dimension links", len(deployed_links))
        return deployed_links

    async def _bulk_delete_links(self, to_delete, node_spec) -> list[DeploymentResult]:
        """Bulk delete dimension links"""
        link_ids_to_delete = []
        delete_results = []

        for delete_link in to_delete:
            node = self.registry.nodes.get(node_spec.rendered_name)
            if not node:
                continue  # pragma: no cover

            # Delete the dimension link if one exists
            for link in node.current.dimension_links:
                link_name = f"{node.name} -> {delete_link.rendered_dimension_node}"
                if (  # pragma: no cover
                    link.dimension.name == delete_link.rendered_dimension_node
                    and link.role == delete_link.role
                ):
                    link_ids_to_delete.append(link.id)  # type: ignore
                    delete_results.append(
                        DeploymentResult(
                            name=link_name,
                            deploy_type=DeploymentResult.Type.LINK,
                            status=DeploymentResult.Status.SUCCESS,
                            operation=DeploymentResult.Operation.DELETE,
                        ),
                    )

                    # Track history for link deletion
                    self.session.add(
                        History(
                            entity_type=EntityType.LINK,
                            entity_name=node.name,
                            node=node.name,
                            activity_type=ActivityType.DELETE,
                            details={
                                "dimension_node": delete_link.rendered_dimension_node,
                                "role": delete_link.role,
                                "deployment_id": self.deployment_id,
                            },
                            user=self.context.current_user.username,
                        ),
                    )

        if link_ids_to_delete:
            await self.session.execute(
                DimensionLink.__table__.delete().where(
                    DimensionLink.id.in_(link_ids_to_delete),
                ),
            )
            await self.session.flush()
        return delete_results

    async def _process_node_dimension_link(
        self,
        node_spec: NodeSpec,
        link_spec: DimensionJoinLinkSpec | DimensionReferenceLinkSpec,
        validation_results: dict[tuple[str, str, str | None], Exception | None],
    ) -> DeploymentResult:
        link_name = f"{node_spec.rendered_name} -> {link_spec.rendered_dimension_node}"
        node = self.registry.nodes.get(node_spec.rendered_name)
        dimension_node = self.registry.nodes.get(link_spec.rendered_dimension_node)

        if not node:
            return self._create_missing_node_link_result(
                link_name,
                node_spec.rendered_name,
            )
        if not dimension_node:
            return self._create_missing_node_link_result(  # pragma: no cover
                link_name,
                link_spec.rendered_dimension_node,
            )

        result = validation_results.get(
            (node.name, dimension_node.name, link_spec.role),
        )
        if isinstance(result, Exception):
            logger.error(
                "Dimension link validation failed for %s: %s",
                link_name,
                result,
            )
            return DeploymentResult(
                name=link_name,
                deploy_type=DeploymentResult.Type.LINK,
                status=DeploymentResult.Status.FAILED,
                operation=DeploymentResult.Operation.CREATE,
                message=(
                    f"Dimension link from {node.name} to"
                    f" {dimension_node.name} is invalid: {result}"
                ),
            )

        return await self._create_or_update_dimension_link(
            link_spec=link_spec,
            new_revision=node.current,
            dimension_node=dimension_node,
        )

    def _create_missing_node_link_result(
        self,
        link_name: str,
        missing_name: str,
    ) -> DeploymentResult:
        message = f"A node with name `{missing_name}` does not exist."
        return DeploymentResult(
            name=link_name,
            deploy_type=DeploymentResult.Type.LINK,
            status=DeploymentResult.Status.FAILED,
            operation=DeploymentResult.Operation.CREATE,
            message=message,
        )

    async def _create_or_update_dimension_link(
        self,
        link_spec: DimensionJoinLinkSpec | DimensionReferenceLinkSpec,
        new_revision: NodeRevision,
        dimension_node: Node,
    ) -> DeploymentResult:
        activity_type = ActivityType.CREATE
        if link_spec.type == LinkType.JOIN:
            join_link = cast(DimensionJoinLinkSpec, link_spec)
            link_input = JoinLinkInput(
                dimension_node=join_link.rendered_dimension_node,
                join_type=join_link.join_type,
                join_on=join_link.rendered_join_on,
                role=join_link.role,
                default_value=join_link.default_value,
            )
            (
                dimension_link,
                activity_type,
            ) = await self.create_or_update_dimension_join_link(
                node_revision=new_revision,
                dimension_node=dimension_node,
                link_input=link_input,
                join_type=join_link.join_type,
            )
            self.session.add(dimension_link)
            self.session.add(new_revision)
        else:
            reference_link = cast(DimensionReferenceLinkSpec, link_spec)
            target_column = [
                col
                for col in new_revision.columns
                if col.name == reference_link.node_column
            ][0]
            if target_column.dimension_id is not None:
                activity_type = ActivityType.UPDATE  # pragma: no cover
            target_column.dimension_id = dimension_node.id  # type: ignore
            target_column.dimension_column = (
                f"{reference_link.dimension_attribute}[{reference_link.role}]"
                if reference_link.role
                else reference_link.dimension_attribute
            )
        role_suffix = f"[{link_spec.role}]" if link_spec.role else ""
        await self.session.flush()

        # Track history for dimension link create/update (skip REFRESH/NOOP)
        if activity_type in (ActivityType.CREATE, ActivityType.UPDATE):
            link_details = {
                "dimension_node": dimension_node.name,
                "link_type": link_spec.type,
                "role": link_spec.role,
                "deployment_id": self.deployment_id,
            }
            if link_spec.type == LinkType.JOIN:
                join_link = cast(DimensionJoinLinkSpec, link_spec)
                link_details["join_type"] = join_link.join_type
                link_details["join_on"] = join_link.rendered_join_on

            self.session.add(
                History(
                    entity_type=EntityType.LINK,
                    entity_name=new_revision.name,
                    node=new_revision.name,
                    activity_type=activity_type,
                    details=link_details,
                    user=self.context.current_user.username,
                ),
            )

        return DeploymentResult(
            name=f"{new_revision.name} -> {dimension_node.name}" + role_suffix,
            deploy_type=DeploymentResult.Type.LINK,
            status=DeploymentResult.Status.SUCCESS
            if activity_type in (ActivityType.CREATE, ActivityType.UPDATE)
            else DeploymentResult.Status.SKIPPED,
            operation=(
                DeploymentResult.Operation.CREATE
                if activity_type == ActivityType.CREATE
                else DeploymentResult.Operation.UPDATE
                if activity_type == ActivityType.UPDATE
                else DeploymentResult.Operation.NOOP
            ),
            message=(f"{link_spec.type.title()} link successfully deployed"),
        )

    async def _deploy_cubes(self, plan: DeploymentPlan) -> list[DeploymentResult]:
        """Deploy cubes for nodes in the plan using bulk approach"""
        cubes_to_deploy = [
            node for node in plan.to_deploy if isinstance(node, CubeSpec)
        ]

        if not cubes_to_deploy:
            return []

        logger.info("Starting bulk deployment of %d cubes", len(cubes_to_deploy))
        start = time.perf_counter()

        # Bulk validate cubes
        validation_results = await self._bulk_validate_cubes(cubes_to_deploy)

        # Bulk create cubes from validation results
        nodes, revisions, deployment_results = await self._create_cubes_from_validation(
            validation_results,
        )

        # Commit all cubes
        self.session.add_all(nodes)
        self.session.add_all(revisions)
        await self.session.commit()

        # Refresh all deployed cube nodes
        all_nodes = await self.refresh_nodes(
            [cube.rendered_name for cube in cubes_to_deploy],
        )
        self.registry.add_nodes(all_nodes)

        logger.info(
            "Deployed %d cubes in %.3fs",
            len(nodes),
            time.perf_counter() - start,
        )
        return deployment_results

    async def _bulk_validate_cubes(
        self,
        cube_specs: list[CubeSpec],
    ) -> list[NodeValidationResult]:
        """Bulk validate cube specifications efficiently with batched DB queries"""
        if not cube_specs:
            return []  # pragma: no cover

        # Collect all unique metrics and dimensions across all cubes
        all_metric_names, all_dimension_names = self._collect_cube_dependencies(
            cube_specs,
        )

        # Batch load all metrics and dimensions
        metric_nodes_map, missing_metrics = await self._batch_load_metrics(
            all_metric_names,
        )
        dimension_mapping, missing_dimensions = await self._batch_load_dimensions(
            all_dimension_names,
        )

        # Validate each cube using the batch-loaded data
        validation_results = []
        for cube_spec in cube_specs:
            validation_result = await self._validate_single_cube(
                cube_spec,
                metric_nodes_map,
                missing_metrics,
                missing_dimensions,
                dimension_mapping,
            )
            validation_results.append(validation_result)
        return validation_results

    def _collect_cube_dependencies(
        self,
        cube_specs: list[CubeSpec],
    ) -> tuple[set[str], set[str]]:
        """Collect all unique metrics and dimensions across all cubes"""
        all_metric_names = set()
        all_dimension_names = set()

        for cube_spec in cube_specs:
            all_metric_names.update(cube_spec.rendered_metrics or [])
            all_dimension_names.update(cube_spec.rendered_dimensions or [])

        return all_metric_names, all_dimension_names

    async def _batch_load_metrics(
        self,
        all_metric_names: set[str],
    ) -> tuple[dict[str, Node], set[str]]:
        """Batch load all metrics"""
        missing_metrics = set()
        metric_nodes_map = {}
        all_metric_nodes = await Node.get_by_names(
            self.session,
            list(all_metric_names),
            options=[
                joinedload(Node.current).options(
                    selectinload(NodeRevision.columns),
                    joinedload(NodeRevision.catalog),
                    selectinload(NodeRevision.parents),
                ),
            ],
            include_inactive=False,
        )
        metric_nodes_map = {node.name: node for node in all_metric_nodes}
        missing_metrics = set(all_metric_names) - {
            metric.name for metric in all_metric_nodes
        }
        return metric_nodes_map, missing_metrics

    async def _batch_load_dimensions(
        self,
        all_dimension_names: set[str],
    ) -> tuple[dict[str, Node], set[str]]:
        """Batch load all dimension attributes"""
        missing_dimensions = set()
        dimension_mapping = {}
        dimension_attributes: list[FullColumnName] = [
            FullColumnName(dimension_attribute)
            for dimension_attribute in all_dimension_names
        ]
        dimension_node_names = [attr.node_name for attr in dimension_attributes]
        dimension_nodes: dict[str, Node] = {
            node.name: node
            for node in await Node.get_by_names(
                self.session,
                dimension_node_names,
                options=[
                    joinedload(Node.current).options(
                        selectinload(NodeRevision.columns).options(
                            selectinload(Column.node_revision),
                        ),
                        defer(NodeRevision.query_ast),
                    ),
                ],
            )
        }
        for attr in dimension_attributes:
            if attr.node_name not in dimension_nodes:
                missing_dimensions.add(attr.name)
                continue
            if not any(
                col.name == attr.column_name
                for col in dimension_nodes[attr.node_name].current.columns
            ):
                missing_dimensions.add(attr.name)

        dimension_mapping = {
            attr.name: dimension_nodes[attr.node_name]
            for attr in dimension_attributes
            if attr.node_name in dimension_nodes
        }
        return dimension_mapping, missing_dimensions

    def _collect_cube_errors(
        self,
        cube_spec: CubeSpec,
        missing_metrics: set[str],
        missing_dimensions: set[str],
        catalogs: list[Catalog],
    ):
        errors = []
        if missing_for_cube := set(cube_spec.rendered_metrics).intersection(
            missing_metrics,
        ):
            errors.append(
                DJError(
                    code=ErrorCode.INVALID_CUBE,
                    message=(
                        f"One or more metrics not found for cube "
                        f"{cube_spec.rendered_name}: {', '.join(missing_for_cube)}"
                    ),
                ),
            )
        if missing_for_cube := set(cube_spec.rendered_dimensions).intersection(
            missing_dimensions,
        ):
            errors.append(
                DJError(
                    code=ErrorCode.INVALID_CUBE,
                    message=(
                        f"One or more dimensions not found for cube "
                        f"{cube_spec.rendered_name}: {', '.join(missing_for_cube)}"
                    ),
                ),
            )

        if len(set(catalogs)) > 1:
            errors.append(  # pragma: no cover
                DJError(
                    code=ErrorCode.INVALID_CUBE,
                    message=(
                        f"Metrics for cube {cube_spec.rendered_name} belong to "
                        f"different catalogs: {', '.join(cat.name for cat in catalogs if cat)}"
                    ),
                ),
            )
        return errors

    async def _validate_single_cube(
        self,
        cube_spec: CubeSpec,
        metric_nodes_map: dict[str, Node],
        missing_metrics: set[str],
        missing_dimensions: set[str],
        dimension_mapping: dict[str, Node],
    ) -> NodeValidationResult:
        cube_metric_nodes = [
            metric_nodes_map[metric_name]
            for metric_name in cube_spec.rendered_metrics or []
            if metric_name in metric_nodes_map
        ]

        # Extract metric columns and catalogs
        metric_columns = [node.current.columns[0] for node in cube_metric_nodes]
        catalogs = [metric.current.catalog for metric in cube_metric_nodes]
        catalog = catalogs[0] if catalogs else None

        # Collect errors for missing metrics/dimensions or catalog mismatches
        errors = self._collect_cube_errors(
            cube_spec,
            missing_metrics,
            missing_dimensions,
            catalogs,
        )
        if errors:
            return NodeValidationResult(
                spec=cube_spec,
                status=NodeStatus.INVALID,
                inferred_columns=[],
                errors=errors,
                dependencies=[],
            )

        # Get dimensions for this cube from batch-loaded data
        cube_dimension_nodes = []
        cube_dimensions = []
        dimension_attributes = [
            dimension_attribute.rsplit(SEPARATOR, 1)
            for dimension_attribute in (cube_spec.rendered_dimensions or [])
        ]
        for node_name, column_name in dimension_attributes:
            full_key = f"{node_name}{SEPARATOR}{column_name}"
            dimension_node = dimension_mapping[full_key]
            if dimension_node not in cube_dimension_nodes:  # pragma: no cover
                cube_dimension_nodes.append(dimension_node)

            # Get the actual column
            columns = {col.name: col for col in dimension_node.current.columns}
            column_name_without_role = column_name
            match = re.fullmatch(COLUMN_NAME_REGEX, column_name)
            if match:  # pragma: no cover
                column_name_without_role = match.groups()[0]

            if column_name_without_role in columns:  # pragma: no cover
                cube_dimensions.append(columns[column_name_without_role])

        return NodeValidationResult(
            spec=cube_spec,
            status=(  # Check if all dependencies are valid
                NodeStatus.VALID
                if (
                    all(
                        metric.current.status == NodeStatus.VALID
                        for metric in cube_metric_nodes
                    )
                    and all(
                        dim.current.status == NodeStatus.VALID
                        for dim in cube_dimension_nodes
                    )
                )
                else NodeStatus.INVALID
            ),
            inferred_columns=cube_spec.rendered_columns,
            errors=[],
            dependencies=[],
            _cube_validation_data=CubeValidationData(
                metric_columns=metric_columns,
                metric_nodes=cube_metric_nodes,
                dimension_nodes=cube_dimension_nodes,
                dimension_columns=cube_dimensions,
                catalog=catalog,
            ),
        )

    async def _create_cubes_from_validation(
        self,
        validation_results: list[NodeValidationResult],
    ) -> tuple[list[Node], list[NodeRevision], list[DeploymentResult]]:
        """Create cube nodes and revisions from validation results without re-validation"""
        nodes, revisions = [], []
        deployment_results = []

        for result in validation_results:
            if result.status == NodeStatus.INVALID:
                deployment_results.append(self._process_invalid_node_deploy(result))
            else:
                cube_spec = cast(CubeSpec, result.spec)
                existing = self.registry.nodes.get(cube_spec.rendered_name)
                operation = (
                    DeploymentResult.Operation.UPDATE
                    if existing
                    else DeploymentResult.Operation.CREATE
                )

                # Get pre-computed validation data to avoid re-validation
                if not result._cube_validation_data:
                    raise DJInvalidDeploymentConfig(  # pragma: no cover
                        f"Missing validation data for cube {cube_spec.rendered_name}",
                    )
                changelog = await self._generate_changelog(result)
                if existing:
                    logger.info("Updating cube node %s", cube_spec.rendered_name)
                    new_node = existing
                    new_node.current_version = str(
                        Version.parse(new_node.current_version).next_major_version(),
                    )
                    new_node.display_name = cube_spec.display_name
                    new_node.owners = [
                        self.registry.owners[owner_name]
                        for owner_name in cube_spec.owners
                        if owner_name in self.registry.owners
                    ]
                    new_node.tags = [
                        self.registry.tags[tag_name] for tag_name in cube_spec.tags
                    ]
                    # Create new revision for update
                    # Use no_autoflush to prevent premature flushing of columns without node_revision_id
                    with self.session.no_autoflush:
                        new_revision = (
                            await self._create_cube_node_revision_from_validation_data(
                                cube_spec=cube_spec,
                                validation_data=result._cube_validation_data,
                                new_node=new_node,
                            )
                        )
                else:
                    logger.info("Creating cube node %s", cube_spec.rendered_name)
                    namespace = get_namespace_from_name(cube_spec.rendered_name)

                    # Use no_autoflush to prevent premature flushing of columns without node_revision_id
                    # (columns will be created in _create_cube_node_revision_from_validation_data)
                    with self.session.no_autoflush:
                        await get_node_namespace(
                            session=self.session,
                            namespace=namespace,
                        )

                        new_node = Node(
                            name=cube_spec.rendered_name,
                            namespace=namespace,
                            type=NodeType.CUBE,
                            display_name=cube_spec.display_name,
                            current_version=(
                                str(DEFAULT_DRAFT_VERSION)
                                if cube_spec.mode == NodeMode.DRAFT
                                else str(DEFAULT_PUBLISHED_VERSION)
                            ),
                            tags=[
                                self.registry.tags[tag_name]
                                for tag_name in cube_spec.tags
                            ],
                            created_by_id=self.context.current_user.id,
                            owners=[
                                self.registry.owners[owner_name]
                                for owner_name in cube_spec.owners
                                if owner_name in self.registry.owners
                            ],
                        )

                        # Create node revision using pre-computed validation data (no re-validation)
                        new_revision = (
                            await self._create_cube_node_revision_from_validation_data(
                                cube_spec=cube_spec,
                                validation_data=result._cube_validation_data,
                                new_node=new_node,
                            )
                        )

                # Track history for cube create/update operations
                activity_type = ActivityType.UPDATE if existing else ActivityType.CREATE
                self.session.add(
                    History(
                        entity_type=EntityType.NODE,
                        entity_name=cube_spec.rendered_name,
                        node=cube_spec.rendered_name,
                        activity_type=activity_type,
                        details={
                            "version": new_node.current_version,
                            "deployment_id": self.deployment_id,
                            "metrics": cube_spec.rendered_metrics,
                            "dimensions": cube_spec.rendered_dimensions,
                        },
                        user=self.context.current_user.username,
                    ),
                )

                # Create deployment result
                deployment_result = DeploymentResult(
                    name=cube_spec.rendered_name,
                    deploy_type=DeploymentResult.Type.NODE,
                    status=DeploymentResult.Status.SUCCESS,
                    operation=operation,
                    message=f"{operation.value.title()}d {new_node.type} ({new_node.current_version})"
                    + ("\n".join([""] + changelog)),
                )

                deployment_results.append(deployment_result)
                nodes.append(new_node)
                revisions.append(new_revision)

        return nodes, revisions, deployment_results

    async def _create_cube_node_revision_from_validation_data(
        self,
        cube_spec: CubeSpec,
        validation_data: CubeValidationData,
        new_node: Node,
    ) -> NodeRevision:
        """Create cube node revision using pre-computed validation data"""
        # Build the "columns" for this node based on the cube elements
        node_columns = []

        dimension_to_roles_mapping = map_dimensions_to_roles(
            cube_spec.rendered_dimensions or [],
        )

        # Build a mapping from column name to column spec for partition lookups
        column_spec_map = {}
        if cube_spec.columns:
            for col_spec in cube_spec.rendered_columns:
                column_spec_map[col_spec.name] = col_spec

        for idx, col in enumerate(
            validation_data.metric_columns + validation_data.dimension_columns,
        ):
            await self.session.refresh(col, ["node_revision"])
            referenced_node = col.node_revision
            full_element_name = (
                referenced_node.name  # type: ignore
                if referenced_node.type == NodeType.METRIC  # type: ignore
                else f"{referenced_node.name}.{col.name}"  # type: ignore
            )
            node_column = Column(
                name=full_element_name,
                display_name=referenced_node.display_name
                if referenced_node.type == NodeType.METRIC
                else col.display_name,
                type=col.type,
                attributes=[
                    ColumnAttribute(attribute_type_id=attr.attribute_type_id)
                    for attr in col.attributes
                ],
                order=idx,
            )
            if full_element_name in dimension_to_roles_mapping:
                node_column.dimension_column = dimension_to_roles_mapping[
                    full_element_name
                ]

            # Apply partition from column spec if specified
            if full_element_name in column_spec_map:
                col_spec = column_spec_map[full_element_name]
                if col_spec.partition:  # pragma: no branch
                    partition = Partition(
                        column=node_column,
                        type_=col_spec.partition.type,
                        granularity=col_spec.partition.granularity,
                        format=col_spec.partition.format,
                    )
                    self.session.add(partition)

            node_columns.append(node_column)

        node_revision = NodeRevision(
            name=cube_spec.rendered_name,
            display_name=cube_spec.display_name
            or labelize(cube_spec.rendered_name.split(SEPARATOR)[-1]),
            description=cube_spec.description,
            type=NodeType.CUBE,
            query="",
            columns=node_columns,
            cube_elements=validation_data.metric_columns
            + validation_data.dimension_columns,
            parents=list(
                set(validation_data.dimension_nodes + validation_data.metric_nodes),
            ),
            status=NodeStatus.VALID,  # Already validated
            catalog=validation_data.catalog,
            created_by_id=self.context.current_user.id,
            node=new_node,
            version=new_node.current_version,
            mode=cube_spec.mode,
        )
        return node_revision

    async def _validate_node_deletion(
        self,
        to_delete: list[NodeSpec],
    ) -> dict[str, list[str]]:
        """
        Check if nodes being deleted are referenced by other nodes.
        Checks both within the namespace and across all namespaces.
        Returns a dict mapping node names to lists of nodes that reference them.
        Empty dict means all nodes can be safely deleted.
        """
        if not to_delete:
            return {}

        nodes_to_delete = {node_spec.rendered_name for node_spec in to_delete}
        # Map of deleted_node to a list of nodes that reference it
        references: dict[str, list[str]] = {}

        # Query just IDs and names of nodes being deleted (more efficient than loading full objects)
        stmt = select(Node.id, Node.name).where(Node.name.in_(list(nodes_to_delete)))
        result = await self.session.execute(stmt)
        id_to_name = {node_id: node_name for node_id, node_name in result}
        deleted_node_ids = set(id_to_name.keys())
        if not deleted_node_ids:
            return {}  # No nodes found to delete

        # Find nodes that have any of the to-delete nodes as parents
        # NodeRelationship: parent_id -> Node.id, child_id -> NodeRevision.id
        # We need to find NodeRevisions that depend on deleted nodes, then get their Node
        from datajunction_server.database.node import NodeRevision

        stmt = (
            select(NodeRevision, NodeRelationship)
            .join(
                NodeRelationship,
                NodeRevision.id == NodeRelationship.child_id,
            )
            .options(joinedload(NodeRevision.node).joinedload(Node.current))
            .where(NodeRelationship.parent_id.in_(deleted_node_ids))
        )
        result = await self.session.execute(stmt)
        for child_revision, relationship in result:
            # Get the node that owns this revision
            child_node = child_revision.node
            # Only count if this is the current revision of the node
            if child_node.current != child_revision:
                continue  # pragma: no cover
            if child_node.name in nodes_to_delete:
                continue
            # Get the parent node name from id_to_name mapping
            parent_name = id_to_name.get(relationship.parent_id)
            if parent_name:  # pragma: no branch
                if parent_name not in references:
                    references[parent_name] = []
                references[parent_name].append(child_node.name)

        # Find dimension links that reference any of the to-delete nodes
        # DimensionLink: node_revision_id -> NodeRevision.id, dimension_id -> Node.id
        stmt = (
            select(NodeRevision, DimensionLink)
            .join(
                DimensionLink,
                NodeRevision.id == DimensionLink.node_revision_id,
            )
            .options(joinedload(NodeRevision.node).joinedload(Node.current))
            .where(DimensionLink.dimension_id.in_(deleted_node_ids))
        )
        result = await self.session.execute(stmt)
        for node_revision, link in result:
            # Get the node that owns this revision
            node = node_revision.node
            # Only count if this is the current revision of the node
            if node.current != node_revision:
                continue  # pragma: no cover
            if node.name in nodes_to_delete:
                continue  # pragma: no cover
            # Get the dimension node name from id_to_name mapping
            dim_name = id_to_name.get(link.dimension_id)
            if dim_name:  # pragma: no branch
                if dim_name not in references:
                    references[dim_name] = []
                references[dim_name].append(f"{node.name} (dimension link)")

        return references

    async def _delete_nodes(self, to_delete: list[NodeSpec]) -> list[DeploymentResult]:
        logger.info("Starting deletion of %d nodes", len(to_delete))

        # Check which nodes have references that would prevent deletion
        references = await self._validate_node_deletion(to_delete)

        results = []
        for node_spec in to_delete:
            node_name = node_spec.rendered_name
            if node_name in references:
                # Node has references - skip deletion and return FAILED result
                referencing_nodes = references[node_name]
                error_msg = (
                    f"Cannot delete '{node_name}' - referenced by: "
                    f"{', '.join(referencing_nodes)}"
                )
                logger.warning(error_msg)
                results.append(
                    DeploymentResult(
                        name=node_name,
                        deploy_type=DeploymentResult.Type.NODE,
                        status=DeploymentResult.Status.FAILED,
                        operation=DeploymentResult.Operation.DELETE,
                        message=error_msg,
                    ),
                )
            else:
                # Node can be safely deleted
                results.append(await self._deploy_delete_node(node_name))

        return results

    async def _deploy_delete_node(self, name: str) -> DeploymentResult:
        async def add_history(event, session):
            """Add history to session without committing"""
            session.add(event)

        try:
            await hard_delete_node(
                name=name,
                session=self.session,
                current_user=self.context.current_user,
                save_history=add_history,
            )
            return DeploymentResult(
                name=name,
                deploy_type=DeploymentResult.Type.NODE,
                status=DeploymentResult.Status.SUCCESS,
                operation=DeploymentResult.Operation.DELETE,
                message=f"Node {name} has been removed.",
            )
        except Exception as exc:
            logger.exception(exc)
            return DeploymentResult(
                name=name,
                deploy_type=DeploymentResult.Type.NODE,
                status=DeploymentResult.Status.FAILED,
                operation=DeploymentResult.Operation.DELETE,
                message=str(exc),
            )

    def filter_nodes_to_deploy(
        self,
        existing_nodes_map: dict[str, NodeSpec],
    ):
        filter_nodes_start = time.perf_counter()

        to_create: list[NodeSpec] = []
        to_update: list[NodeSpec] = []
        to_skip: list[NodeSpec] = []
        for node_spec in self.deployment_spec.nodes:
            existing_spec = existing_nodes_map.get(node_spec.rendered_name)
            if not existing_spec:
                to_create.append(node_spec)
            elif node_spec != existing_spec:
                to_update.append(node_spec)
            else:
                to_skip.append(node_spec)

        desired_node_names = {n.rendered_name for n in self.deployment_spec.nodes}
        to_delete = [
            existing
            for name, existing in existing_nodes_map.items()
            if name not in desired_node_names
        ]

        logger.info("Creating %d new nodes", len(to_create))
        logger.info("Updating %d existing nodes", len(to_update))
        logger.info("Skipping %d nodes as they are unchanged", len(to_skip))
        logger.info("Deleting %d nodes: %s", len(to_delete), to_delete)
        logger.info(
            "Filtered nodes to deploy in %.3fs",
            time.perf_counter() - filter_nodes_start,
        )
        return to_create + to_update, to_skip, to_delete

    async def check_external_deps(
        self,
        node_graph: dict[str, list[str]],
    ) -> tuple[set[str], list[SourceSpec]]:
        """
        Find any dependencies that are not in the deployment but are already in the system.
        If any dependencies are missing and auto_register_sources is enabled, attempt to
        register them. Otherwise raise an error.

        Returns:
            Tuple of (external_deps, auto_registered_sources)
        """
        settings = get_settings()
        source_prefix = settings.source_node_namespace
        source_prefix_dot = f"{source_prefix}." if source_prefix else None

        dimension_link_deps = [
            link.rendered_dimension_node
            for node in self.deployment_spec.nodes
            if isinstance(node, LinkableNodeSpec) and node.dimension_links
            for link in node.dimension_links
        ]

        # Helper to check if a dependency is in the deployment, accounting for the
        # configured source namespace prefix (e.g. "source.catalog.schema.table").
        def is_in_deployment(dep: str) -> bool:
            if dep in node_graph:
                return True
            if source_prefix_dot and dep.startswith(source_prefix_dot):
                normalized = dep[len(source_prefix_dot) :]
                return normalized in node_graph
            return False

        deps_not_in_deployment = {
            dep
            for deps in list(node_graph.values())
            for dep in deps
            if not is_in_deployment(dep)
        }.union({dep for dep in dimension_link_deps if not is_in_deployment(dep)})
        if deps_not_in_deployment:
            logger.warning(
                "The following dependencies are not defined in the deployment: %s. "
                "They must pre-exist in the system before this deployment can succeed.",
                deps_not_in_deployment,
            )
            # Search for nodes with both original and normalized names
            names_to_search = list(deps_not_in_deployment)
            for dep in deps_not_in_deployment:
                if source_prefix_dot and dep.startswith(source_prefix_dot):
                    normalized = dep[len(source_prefix_dot) :]
                    if normalized not in names_to_search:
                        names_to_search.append(normalized)

            external_node_deps = await Node.get_by_names(
                self.session,
                names_to_search,
            )
            found_dep_names = {node.name for node in external_node_deps}
            missing_nodes = []
            for dep in deps_not_in_deployment:
                # Normalize by stripping the configured source namespace prefix if present
                normalized_dep = dep
                if source_prefix_dot and dep.startswith(source_prefix_dot):
                    normalized_dep = dep[len(source_prefix_dot) :]

                # Check against both original and normalized names
                if dep in found_dep_names or normalized_dep in found_dep_names:
                    continue
                # Check if this is a dimension.column reference (parent was found)
                parent = normalized_dep.rsplit(SEPARATOR, 1)[0]
                if SEPARATOR in parent and parent in found_dep_names:
                    continue

                # Check if this is a namespace prefix (some found node starts with dep.)
                # This happens when rsplit of a metric gives its namespace
                # Check both: nodes already in DB (found_dep_names) AND nodes in current deployment (node_graph)
                deployment_node_names = set(node_graph.keys())
                if (
                    dep == self.deployment_spec.namespace
                    or normalized_dep == self.deployment_spec.namespace
                    or any(
                        name.startswith(dep + SEPARATOR)
                        or name.startswith(normalized_dep + SEPARATOR)
                        for name in found_dep_names | deployment_node_names
                    )
                ):
                    continue  # pragma: no cover
                missing_nodes.append(dep)

            if missing_nodes:
                # Try to auto-register missing sources
                auto_registered = await self._auto_register_sources(missing_nodes)

                # If we failed to auto-register (or it's disabled), raise an error
                if self.errors or not auto_registered:
                    if self.errors:
                        raise DJInvalidDeploymentConfig(
                            message="Failed to auto-register sources",
                            errors=self.errors,
                            warnings=self.warnings,
                        )
                    raise DJInvalidDeploymentConfig(
                        message=(
                            "The following dependencies are not in the deployment and do not"
                            " pre-exist in the system: "
                            + ", ".join(sorted(missing_nodes))
                        ),
                    )

                # Successfully auto-registered, return them so caller can rebuild graph
                return deps_not_in_deployment, auto_registered

            logger.info(
                "All %d external dependencies pre-exist in the system",
                len(external_node_deps),
            )
        return deps_not_in_deployment, []

    async def validate_dimension_links(self, plan: DeploymentPlan):
        """
        Validate all dimension links for nodes in the deployment plan.
        Returns:
            - Dictionary mapping (node_name, dimension_node_name, role) -> join result
        """
        start_validation = time.perf_counter()
        validation_data = []
        validation_tasks: list[Coroutine] = []
        logger.info("Validating %d dimension links", len(validation_tasks))

        for node_spec in plan.to_deploy:
            if hasattr(node_spec, "dimension_links") and node_spec.dimension_links:
                for link in node_spec.dimension_links:
                    validation_data.append(
                        {
                            "node_name": node_spec.rendered_name,
                            "dimension_node_name": link.rendered_dimension_node,
                            "role": link.role,
                        },
                    )
                    validation_tasks.append(
                        self.validate_dimension_link(node_spec.rendered_name, link),
                    )

        results = await asyncio.gather(*validation_tasks, return_exceptions=True)
        link_mapping = {
            (
                validation_data[idx]["node_name"],
                validation_data[idx]["dimension_node_name"],
                validation_data[idx]["role"],
            ): result
            for idx, result in enumerate(results)
        }
        logger.info(
            "Finished validating %d dimension links in %.3fs",
            len(link_mapping),
            time.perf_counter() - start_validation,
        )
        return link_mapping

    async def bulk_deploy_nodes_in_level(
        self,
        node_specs: list[NodeSpec],
        node_graph: dict[str, list[str]],
    ) -> tuple[list[DeploymentResult], dict[str, Node]]:
        """
        Bulk deploy a list of nodes in a single transaction.
        For these nodes, we know that:
        1. They do not have any dependencies on each other
        2. They are either new or have changes compared to existing nodes
        """
        start = time.perf_counter()
        logger.info("Starting bulk deployment of %d nodes", len(node_specs))

        dependency_nodes = await self.get_dependencies(node_graph)

        # Validate all node queries to determine columns, types, and dependencies
        validation_results = await bulk_validate_node_data(
            node_specs,
            node_graph,
            self.session,
            dependency_nodes,
        )

        # Process validation results and create nodes
        nodes, revisions, deployment_results = await self.create_nodes_from_validation(
            validation_results,
            dependency_nodes,
            node_graph,
        )
        # Check for duplicates
        node_keys = [(n.name, n.namespace) for n in nodes]
        if len(node_keys) != len(set(node_keys)):
            duplicates = [  # pragma: no cover
                k[0] for k, v in Counter(node_keys).items() if v > 1
            ]
            raise DJInvalidDeploymentConfig(  # pragma: no cover
                message=f"Duplicate nodes in deployment spec: {', '.join(duplicates)}",
            )
        self.session.add_all(nodes)
        self.session.add_all(revisions)
        await self.session.commit()

        # Refresh nodes for latest state
        all_nodes = await self.refresh_nodes(
            [node.rendered_name for node in node_specs],
        )

        logger.info(
            f"Deployed {len(nodes)} nodes in bulk in {time.perf_counter() - start:.2f}s",
        )
        return deployment_results, all_nodes

    async def get_dependencies(
        self,
        node_graph: dict[str, list[str]],
    ) -> dict[str, Node]:
        all_required_nodes = node_graph.keys() | {
            dep for deps in node_graph.values() for dep in deps
        }
        dependency_nodes = {
            node.name: node
            for node in await Node.get_by_names(self.session, list(all_required_nodes))
        }
        for _, dep_node in dependency_nodes.items():
            if dep_node.current and dep_node.current.columns:  # pragma: no cover
                for column in dep_node.current.columns:
                    if isinstance(column.type, str):
                        try:
                            from datajunction_server.sql.parsing.backends.antlr4 import (
                                parse_rule,
                            )

                            column.type = parse_rule(column.type, "dataType")
                        except Exception:  # pragma: no cover
                            pass  # pragma: no cover
        return dependency_nodes

    async def refresh_nodes(self, node_names: list[str]) -> dict[str, Node]:
        refresh_start = time.perf_counter()
        all_nodes = {
            node.name: node
            for node in await Node.get_by_names(self.session, node_names)
        }
        for node in all_nodes.values():
            await self.session.refresh(node, ["current"])
        logger.info(
            "Refreshed %d nodes in %.2fs",
            len(all_nodes),
            time.perf_counter() - refresh_start,
        )
        return all_nodes

    async def create_nodes_from_validation(
        self,
        validation_results: list[NodeValidationResult],
        dependency_nodes: dict[str, Node],
        node_graph: dict[str, list[str]],
    ) -> tuple[list[Node], list[NodeRevision], list[DeploymentResult]]:
        nodes, revisions = [], []
        deployment_results = []
        for result in validation_results:
            if result.status == NodeStatus.INVALID:
                deployment_results.append(self._process_invalid_node_deploy(result))
            else:
                (
                    deployment_result,
                    new_node,
                    new_revision,
                ) = await self._process_valid_node_deploy(
                    result,
                    dependency_nodes,
                    node_graph,
                )
                deployment_results.append(deployment_result)
                nodes.append(new_node)
                revisions.append(new_revision)
        return nodes, revisions, deployment_results

    def _process_invalid_node_deploy(
        self,
        result: NodeValidationResult,
    ) -> DeploymentResult:
        """Create deployment result for failed validation"""
        logger.error(
            f"Node {result.spec.rendered_name} failed: {result.errors}",
        )
        existing = self.registry.nodes.get(result.spec.rendered_name)
        operation = (
            DeploymentResult.Operation.UPDATE
            if existing
            else DeploymentResult.Operation.CREATE
        )

        return DeploymentResult(
            name=result.spec.rendered_name,
            deploy_type=DeploymentResult.Type.NODE,
            status=DeploymentResult.Status.FAILED,
            operation=operation,
            message="; ".join(error.message for error in result.errors),
        )

    async def _process_valid_node_deploy(
        self,
        result: NodeValidationResult,
        dependency_nodes: dict[str, Node],
        node_graph: dict[str, list[str]],
    ) -> tuple[DeploymentResult, Node, NodeRevision]:
        existing = self.registry.nodes.get(result.spec.rendered_name)  # is not None
        operation = (
            DeploymentResult.Operation.UPDATE
            if existing
            else DeploymentResult.Operation.CREATE
        )
        changelog = await self._generate_changelog(result)
        new_node = self._create_or_update_node(result.spec, existing)
        new_revision = await self._create_node_revision(
            new_node,
            result,
            dependency_nodes,
            node_graph,
        )
        self.session.add(new_node)
        self.session.add(new_revision)
        await self.session.flush()

        # Track history for create/update operations
        activity_type = ActivityType.UPDATE if existing else ActivityType.CREATE
        self.session.add(
            History(
                entity_type=EntityType.NODE,
                entity_name=result.spec.rendered_name,
                node=result.spec.rendered_name,
                activity_type=activity_type,
                details={
                    "version": new_node.current_version,
                    "deployment_id": self.deployment_id,
                },
                user=self.context.current_user.username,
            ),
        )

        deployment_result = DeploymentResult(
            name=result.spec.rendered_name,
            deploy_type=DeploymentResult.Type.NODE,
            status=DeploymentResult.Status.SUCCESS,
            operation=operation,
            message=f"{operation.value.title()}d {new_node.type} ({new_node.current_version})"
            + ("\n".join([""] + changelog)),
        )
        return deployment_result, new_node, new_revision

    async def _generate_changelog(self, result: NodeValidationResult) -> list[str]:
        """Generate changelog entries for a node update"""
        changelog: list[str] = []

        # No changes if the node is new
        existing = self.registry.nodes.get(result.spec.rendered_name)
        if not existing:
            return changelog

        # Track changes to node columns
        old_revision = existing.current if existing else None
        existing_columns_map = {
            col.name: col for col in (old_revision.columns if old_revision else [])
        }
        changed_count = [
            column_changed(new_col, existing_columns_map.get(new_col.name))
            for new_col in result.inferred_columns
        ]
        if sum(changed_count) > 0:
            changelog.append(
                f"└─ Set properties for {sum(changed_count)} columns",
            )

        # Track changes to other node fields
        existing_node_spec = await existing.to_spec(self.session)
        changed_fields = existing_node_spec.diff(result.spec) if existing else []
        changelog.append(
            ("└─ Updated " + ", ".join(changed_fields)),
        ) if changed_fields else ""
        return changelog

    def _create_or_update_node(
        self,
        node_spec: NodeSpec,
        existing: Node | None,
    ) -> Node:
        """Create or update a Node object based on the spec and existing node"""
        new_node = (
            Node(
                name=node_spec.rendered_name,
                type=node_spec.node_type,
                display_name=node_spec.display_name,
                namespace=".".join(node_spec.rendered_name.split(".")[:-1]),
                current_version=(
                    str(DEFAULT_DRAFT_VERSION)
                    if node_spec.mode == NodeMode.DRAFT
                    else str(DEFAULT_PUBLISHED_VERSION)
                ),
                tags=[self.registry.tags[tag_name] for tag_name in node_spec.tags],
                created_by_id=self.context.current_user.id,
                owners=[
                    self.registry.owners[owner_name]
                    for owner_name in node_spec.owners
                    if owner_name in self.registry.owners
                ],
            )
            if not existing
            else existing
        )
        if existing:
            new_node.current_version = str(
                Version.parse(new_node.current_version).next_major_version(),
            )
            new_node.display_name = node_spec.display_name
            new_node.owners = [
                self.registry.owners[owner_name]
                for owner_name in node_spec.owners
                if owner_name in self.registry.owners
            ]
        if set(node_spec.tags) != set([tag.name for tag in new_node.tags]):
            tags = [self.registry.tags.get(tag) for tag in node_spec.tags]
            new_node.tags = tags  # type: ignore
        return new_node

    async def _create_node_revision(
        self,
        new_node: Node,
        result: NodeValidationResult,
        dependency_nodes: dict[str, Node],
        node_graph: dict[str, list[str]],
    ):
        """Create node revision with inferred columns and dependencies"""
        existing = self.registry.nodes.get(result.spec.rendered_name)
        old_node_revision = existing.current if existing else None
        parents = [
            dependency_nodes.get(parent)
            for parent in node_graph.get(result.spec.rendered_name, [])
            if parent in dependency_nodes
        ]
        if result.spec.node_type != NodeType.SOURCE:
            if parents:
                catalog = parents[0].current.catalog  # type: ignore
            else:
                # Fall back to virtual catalog for nodes with no parents
                # (e.g., hardcoded dimensions)
                catalog = await Catalog.get_virtual_catalog(  # pragma: no cover
                    self.session,
                )
        else:
            catalog = self.registry.catalogs.get(result.spec.catalog)

        new_revision = NodeRevision(
            name=result.spec.rendered_name,
            display_name=result.spec.display_name,
            type=result.spec.node_type,
            description=result.spec.description,
            mode=result.spec.mode,
            version=new_node.current_version,
            node=new_node,
            catalog=catalog,
            status=result.status,
            parents=[
                dependency_nodes.get(parent)
                for parent in node_graph.get(result.spec.rendered_name, [])
                if parent in dependency_nodes
            ],
            created_by_id=self.context.current_user.id,
            custom_metadata=result.spec.custom_metadata,
        )
        new_revision.version = new_node.current_version

        if isinstance(result.spec, LinkableNodeSpec) and old_node_revision:
            for link in old_node_revision.dimension_links:
                new_revision.dimension_links.append(
                    DimensionLink(
                        node_revision=new_revision,
                        dimension_id=link.dimension_id,
                        join_sql=link.join_sql,
                        join_type=link.join_type,
                        join_cardinality=link.join_cardinality,
                        materialization_conf=link.materialization_conf,
                        role=link.role,
                        default_value=link.default_value,
                    ),
                )
        pk_columns = (
            result.spec.primary_key if isinstance(result.spec, LinkableNodeSpec) else []
        )

        if result.spec.node_type in (
            NodeType.TRANSFORM,
            NodeType.DIMENSION,
            NodeType.METRIC,
        ):
            new_revision.query = result.spec.rendered_query
            new_revision.columns = [
                self._create_column_from_spec(
                    col,
                    pk_columns,
                    order=col.order if col.order is not None else idx,
                )
                for idx, col in enumerate(result.inferred_columns)
            ]

        if result.spec.node_type == NodeType.SOURCE:
            source_spec = cast(SourceSpec, result.spec)
            catalog, schema, table = (
                source_spec.catalog,
                source_spec.schema_,
                source_spec.table,
            )
            new_revision.schema_ = schema
            new_revision.table = table
            new_revision.columns = [
                self._create_column_from_spec(
                    col,
                    pk_columns,
                    order=col.order if col.order is not None else idx,
                )
                for idx, col in enumerate(result.spec.columns)
            ]

        if result.spec.node_type == NodeType.METRIC:
            metric_spec = cast(MetricSpec, result.spec)
            new_revision.columns[0].display_name = new_revision.display_name
            if (
                metric_spec.unit_enum
                or metric_spec.direction
                or metric_spec.significant_digits
                or metric_spec.max_decimal_exponent
                or metric_spec.min_decimal_exponent
            ):
                new_revision.metric_metadata = MetricMetadata(
                    unit=metric_spec.unit_enum,
                    direction=metric_spec.direction,
                    significant_digits=metric_spec.significant_digits,
                    max_decimal_exponent=metric_spec.max_decimal_exponent,
                    min_decimal_exponent=metric_spec.min_decimal_exponent,
                )

            # Assign required dimensions if specified and present in columns
            if metric_spec.required_dimensions:
                required_dimensions = []
                origin_node = new_revision.parents[0].current
                columns_mapping = {col.name: col for col in origin_node.columns}
                for dim in metric_spec.required_dimensions:
                    if dim in columns_mapping:
                        required_dimensions.append(
                            columns_mapping[dim],
                        )  # pragma: no cover
                new_revision.required_dimensions = required_dimensions
        return new_revision

    def _create_column_from_spec(
        self,
        col: ColumnSpec,
        pk_columns: list[str],
        order: int = 0,
    ) -> Column:
        return Column(
            name=col.name,
            type=col.type,
            display_name=col.display_name,
            description=col.description,
            order=order,
            attributes=[
                ColumnAttribute(
                    attribute_type=self.registry.attributes.get(attr),
                )
                for attr in set(
                    list(
                        col.attributes
                        + (["primary_key"] if col.name in pk_columns else []),
                    ),
                )
                if attr in self.registry.attributes
            ],
            partition=Partition(
                type_=col.partition.type,
                format=col.partition.format,
                granularity=col.partition.granularity,
            )
            if col.partition
            else None,
        )

    async def create_or_update_dimension_join_link(
        self,
        node_revision: NodeRevision,
        dimension_node: Node,
        link_input: JoinLinkInput,
        join_type: JoinType,
    ) -> tuple[DimensionLink, ActivityType]:
        """
        Create or update a dimension link on a node revision.
        """
        # Find an existing dimension link if there is already one defined for this node
        existing_link = [
            link  # type: ignore
            for link in node_revision.dimension_links  # type: ignore
            if link.dimension.name == dimension_node.name
            and link.role == link_input.role  # type: ignore
        ]
        activity_type = ActivityType.CREATE

        if existing_link:
            if len(existing_link) >= 1:  # pragma: no cover
                for dup_link in existing_link[1:]:
                    await self.session.delete(dup_link)
            # Update the existing dimension link
            activity_type = ActivityType.UPDATE
            dimension_link = existing_link[0]
            if (
                dimension_link.join_sql == link_input.join_on
                and dimension_link.join_type == join_type
                and dimension_link.join_cardinality == link_input.join_cardinality
                and dimension_link.default_value == link_input.default_value
            ):
                return dimension_link, ActivityType.REFRESH
            dimension_link.join_sql = link_input.join_on
            dimension_link.join_type = join_type
            dimension_link.join_cardinality = link_input.join_cardinality
            dimension_link.default_value = link_input.default_value
        else:
            # If there is no existing link, create new dimension link object
            dimension_link = DimensionLink(
                node_revision_id=node_revision.id,  # type: ignore
                dimension_id=dimension_node.id,  # type: ignore
                join_sql=link_input.join_on,
                join_type=join_type,
                join_cardinality=link_input.join_cardinality,
                role=link_input.role,
                default_value=link_input.default_value,
            )
            node_revision.dimension_links.append(dimension_link)  # type: ignore
        return dimension_link, activity_type

    async def validate_dimension_link(
        self,
        node_name: str,
        link: DimensionJoinLinkSpec | DimensionReferenceLinkSpec,
    ):
        """Validate a single dimension link specification"""
        dimension_node_name = link.rendered_dimension_node
        if node_name not in self.registry.nodes:
            raise DJInvalidInputException(
                message=f"Node {node_name} does not exist for linking.",
            )
        if dimension_node_name not in self.registry.nodes:
            raise DJInvalidInputException(  # pragma: no cover
                message=(
                    f"Dimension node {dimension_node_name} does not"
                    f" exist for linking to {node_name}"
                ),
            )
        if link.type == LinkType.JOIN:
            await validate_complex_dimension_link(
                self.session,
                self.registry.nodes.get(node_name),  # type: ignore
                self.registry.nodes.get(dimension_node_name),  # type: ignore
                JoinLinkInput(
                    dimension_node=dimension_node_name,
                    join_type=link.join_type,
                    join_on=link.rendered_join_on,
                    role=link.role,
                    default_value=link.default_value,
                ),
                self.registry.nodes,
            )
        elif link.type == LinkType.REFERENCE:  # pragma: no cover
            await validate_reference_dimension_link(
                link,
                self.registry.nodes.get(node_name),  # type: ignore
                self.registry.nodes.get(dimension_node_name),  # type: ignore
            )


def tag_needs_update(existing_tag: Tag, tag_spec: TagSpec) -> bool:
    """Check if tag actually needs updating"""
    return (
        existing_tag.tag_type != tag_spec.tag_type
        or existing_tag.description != tag_spec.description
        or existing_tag.display_name
        != (tag_spec.display_name or labelize(tag_spec.name))
    )


async def validate_reference_dimension_link(
    link: DimensionReferenceLinkSpec,
    node: Node,
    dim_node: Node,
) -> None:
    """
    Placeholder for validating reference dimension links
    """
    if not any(
        col.name == link.dimension_attribute for col in dim_node.current.columns
    ):
        raise DJInvalidInputException(
            message=(
                f"Dimension attribute '{link.dimension_attribute}' not found in"
                f" dimension node '{link.rendered_dimension_node}' for link in node"
                f" '{node.name}'."
            ),
        )


def column_changed(desired_col: ColumnSpec, col: Column | None) -> bool:
    if col is None:
        return False
    if (
        col.display_name != desired_col.display_name
        and desired_col.display_name is not None
    ):
        return True
    if col.description != desired_col.description:
        return True
    if (desired_col.partition or col.partition) and desired_col.partition != (
        col.partition.to_spec() if col.partition else None
    ):
        return True
    if (set(desired_col.attributes) - {"primary_key"}) != (
        set(col.attribute_names()) - {"primary_key"}
    ):
        return True
    return False
