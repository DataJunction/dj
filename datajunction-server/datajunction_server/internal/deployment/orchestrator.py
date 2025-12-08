import asyncio
import logging
from dataclasses import dataclass, field
import re
import time
from typing import Coroutine, cast

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v2 import FullColumnName
from datajunction_server.api.helpers import (
    get_attribute_type,
    get_node_namespace,
    COLUMN_NAME_REGEX,
    map_dimensions_to_roles,
)
from datajunction_server.database.attributetype import AttributeType
from datajunction_server.database.partition import Partition
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.tag import Tag
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.user import User, OAuthProvider
from datajunction_server.database.node import Node
from datajunction_server.models.node import NodeType
from datajunction_server.internal.deployment.validation import (
    NodeValidationResult,
    CubeValidationData,
)
from dataclasses import dataclass
from datajunction_server.internal.nodes import (
    hard_delete_node,
    validate_complex_dimension_link,
)
from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DeploymentResult,
    DeploymentSpec,
    DimensionReferenceLinkSpec,
    LinkableNodeSpec,
    NodeSpec,
    TagSpec,
)
from datajunction_server.errors import (
    DJError,
    DJInvalidDeploymentConfig,
    DJInvalidInputException,
    DJWarning,
    ErrorCode,
)
from datajunction_server.models.base import labelize
from datajunction_server.internal.deployment.utils import (
    extract_node_graph,
    topological_levels,
)
from datajunction_server.internal.deployment.utils import DeploymentContext
from datajunction_server.database.user import User
from datajunction_server.database import Node, NodeRevision
from datajunction_server.database.metricmetadata import MetricMetadata
from datajunction_server.api.helpers import get_attribute_type
from datajunction_server.models.base import labelize
from datajunction_server.models.node import (
    DEFAULT_DRAFT_VERSION,
    DEFAULT_PUBLISHED_VERSION,
    NodeMode,
)
from datajunction_server.internal.deployment.validation import (
    bulk_validate_node_data,
)
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.column import Column, ColumnAttribute
from datajunction_server.database.dimensionlink import DimensionLink, JoinType
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.tag import Tag
from datajunction_server.models.attribute import ColumnAttributes
from datajunction_server.models.deployment import (
    CubeSpec,
    DeploymentResult,
    DeploymentSpec,
    DeploymentStatus,
    DimensionJoinLinkSpec,
    LinkableNodeSpec,
    MetricSpec,
    SourceSpec,
    NodeSpec,
)
from datajunction_server.models.dimensionlink import (
    JoinLinkInput,
    LinkType,
)
from datajunction_server.models.history import ActivityType
from datajunction_server.database.history import History
from datajunction_server.internal.history import EntityType
from datajunction_server.models.node import (
    NodeStatus,
    NodeType,
)
from datajunction_server.errors import (
    DJInvalidDeploymentConfig,
)
from datajunction_server.utils import SEPARATOR, Version, get_namespace_from_name

from sqlalchemy.orm import joinedload, selectinload, defer


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
        if self.errors:
            raise DJInvalidDeploymentConfig(
                message="Invalid deployment configuration",
                errors=self.errors,
                warnings=self.warnings,
            )

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

        # Process each node to extract namespace hierarchy
        for node in self.deployment_spec.nodes:
            node_name = node.rendered_name
            if SEPARATOR not in node_name:
                continue  # pragma: no cover

            # Get the node's direct namespace (everything except the root name)
            node_namespace = node_name.rsplit(SEPARATOR, 1)[0]

            # Generate all parent namespaces up to deployment root
            namespace_parts = node_namespace.split(SEPARATOR)
            deployment_parts = deployment_namespace.split(SEPARATOR)

            # Ensure node is actually under deployment namespace
            if not node_namespace.startswith(deployment_namespace):
                self.errors.append(
                    DJError(
                        code=ErrorCode.INVALID_NAMESPACE,
                        message=f"Node '{node_name}' is not under deployment namespace '{deployment_namespace}'",
                        context="namespace validation",
                    ),
                )
                continue

            # Build all namespace levels from deployment root to node's namespace
            for i in range(len(deployment_parts), len(namespace_parts) + 1):
                if parent_namespace := SEPARATOR.join(
                    namespace_parts[:i],
                ):  # pragma: no cover
                    all_namespaces.add(parent_namespace)
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
        external_deps = set()
        if to_deploy or to_delete:
            node_graph = extract_node_graph(
                [node for node in to_deploy if not isinstance(node, CubeSpec)],
            )
            external_deps = await self.check_external_deps(node_graph)

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
                else:
                    logger.info("Creating cube node %s", cube_spec.rendered_name)
                    namespace = get_namespace_from_name(cube_spec.rendered_name)
                    await get_node_namespace(session=self.session, namespace=namespace)

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
                            self.registry.tags[tag_name] for tag_name in cube_spec.tags
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
        )
        return node_revision

    async def _delete_nodes(self, to_delete: list[NodeSpec]) -> list[DeploymentResult]:
        logger.info("Starting deletion of %d nodes", len(to_delete))
        return [
            await self._deploy_delete_node(node_spec.rendered_name)
            for node_spec in to_delete
        ]

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
    ) -> set[str]:
        """
        Find any dependencies that are not in the deployment but are already in the system.
        If any dependencies are not in the deployment and not in the system, raise an error.
        """
        dimension_link_deps = [
            link.rendered_dimension_node
            for node in self.deployment_spec.nodes
            if isinstance(node, LinkableNodeSpec) and node.dimension_links
            for link in node.dimension_links
        ]

        deps_not_in_deployment = {
            dep
            for deps in list(node_graph.values())
            for dep in deps
            if dep not in node_graph
        }.union({dep for dep in dimension_link_deps if dep not in node_graph})
        if deps_not_in_deployment:
            logger.warning(
                "The following dependencies are not defined in the deployment: %s. "
                "They must pre-exist in the system before this deployment can succeed.",
                deps_not_in_deployment,
            )
            external_node_deps = await Node.get_by_names(
                self.session,
                list(deps_not_in_deployment),
            )
            if len(external_node_deps) != len(deps_not_in_deployment):
                missing_nodes = sorted(
                    set(deps_not_in_deployment)
                    - {node.name for node in external_node_deps},
                )
                raise DJInvalidDeploymentConfig(
                    message=(
                        "The following dependencies are not in the deployment and do not"
                        " pre-exist in the system: " + ", ".join(missing_nodes)
                    ),
                )
            logger.info(
                "All %d external dependencies pre-exist in the system",
                len(external_node_deps),
            )
        return deps_not_in_deployment

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
            catalog = parents[0].current.catalog if parents else None  # type: ignore
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
                self._create_column_from_spec(col, pk_columns)
                for col in result.inferred_columns
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
                self._create_column_from_spec(col, pk_columns)
                for col in result.spec.columns
            ]

        if result.spec.node_type == NodeType.METRIC:
            metric_spec = cast(MetricSpec, result.spec)
            new_revision.columns[0].display_name = new_revision.display_name
            if (
                metric_spec.unit
                or metric_spec.direction
                or metric_spec.significant_digits
                or metric_spec.max_decimal_exponent
                or metric_spec.min_decimal_exponent
            ):
                new_revision.metric_metadata = MetricMetadata(
                    unit=metric_spec.unit,
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
    ) -> Column:
        return Column(
            name=col.name,
            type=col.type,
            display_name=col.display_name,
            description=col.description,
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
            ):
                return dimension_link, ActivityType.REFRESH
            dimension_link.join_sql = link_input.join_on
            dimension_link.join_type = join_type
            dimension_link.join_cardinality = link_input.join_cardinality
        else:
            # If there is no existing link, create new dimension link object
            dimension_link = DimensionLink(
                node_revision_id=node_revision.id,  # type: ignore
                dimension_id=dimension_node.id,  # type: ignore
                join_sql=link_input.join_on,
                join_type=join_type,
                join_cardinality=link_input.join_cardinality,
                role=link_input.role,
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
