import logging
import re
import time
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import cast

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload, defer, load_only, noload

from datajunction_server.api.helpers import (
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
from datajunction_server.database.node import MissingParent, NodeRelationship
from datajunction_server.database.partition import Partition
from datajunction_server.database.tag import Tag
from datajunction_server.database.user import User, OAuthProvider
from datajunction_server.instrumentation.provider import get_metrics_provider
from datajunction_server.errors import (
    DJError,
    DJInvalidDeploymentConfig,
    DJInvalidInputException,
    DJWarning,
    ErrorCode,
)
from datajunction_server.internal.deployment.utils import (
    classify_parents,
    extract_node_graph,
    topological_levels,
    DeploymentContext,
)
from datajunction_server.internal.impact import propagate_impact
from datajunction_server.internal.deployment.dimension_reachability import (
    DimensionReachability,
)
from datajunction_server.internal.deployment.validation import (
    NodeValidationResult,
    CubeValidationData,
    bulk_validate_node_data,
)
from datajunction_server.internal.history import EntityType
from datajunction_server.sql.dag import get_metric_parents_map
from datajunction_server.internal.nodes import (
    derive_frozen_measures_bulk,
    hard_delete_node,
)
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
    render_prefixes,
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


def _extract_dimension_refs_from_filters(
    filters: list[str],
) -> list[tuple[str, str]]:
    """Extract (node_name, column_name) pairs from filter expressions.

    Parses all filters as a single WHERE clause (joined with AND) and
    collects namespaced column references.  For example,
    ``ns.hard_hat.state = 'CA'`` yields ``('ns.hard_hat', 'state')``.

    Returns a list of (node_name, column_name) tuples.  Dimension node
    names are identified by having at least one SEPARATOR in the namespace.
    """
    from datajunction_server.sql.parsing.backends.antlr4 import parse, ast

    if not filters:
        return []
    combined = " AND ".join(f"({f})" for f in filters)
    try:
        tree = parse(f"SELECT 1 WHERE {combined}")
    except Exception:
        return []  # Unparseable — skip, will be caught at query time
    refs: list[tuple[str, str]] = []
    for col in tree.find_all(ast.Column):
        if col.namespace and len(col.namespace) >= 1:
            node_name = SEPARATOR.join(n.name for n in col.namespace)
            if SEPARATOR in node_name:  # pragma: no branch
                refs.append((node_name, col.name.name))
    return refs


def _diff_column_metadata(
    new_cols: "list | None",
    old_cols: "list | None",
) -> list[str]:
    """Return human-readable notes for column metadata changes (attributes, display_name, etc.).

    Ignores type differences since column types are inferred by DJ, not user-specified.
    Returns an empty list when nothing changed.
    """

    new_map = {c.name: c for c in (new_cols or [])}
    old_map = {c.name: c for c in (old_cols or [])}
    notes: list[str] = []

    added = sorted(set(new_map) - set(old_map))
    removed = sorted(set(old_map) - set(new_map))
    if added:
        notes.append(f"Column added: {', '.join(added)}")
    if removed:
        notes.append(f"Column removed: {', '.join(removed)}")

    for col_name in sorted(set(new_map) & set(old_map)):
        new_col = new_map[col_name]
        old_col = old_map[col_name]
        col_notes: list[str] = []

        new_attrs = set(new_col.attributes or []) - {"primary_key"}
        old_attrs = set(old_col.attributes or []) - {"primary_key"}
        if new_attrs != old_attrs:
            added_a = sorted(new_attrs - old_attrs)
            removed_a = sorted(old_attrs - new_attrs)
            parts = []
            if added_a:
                parts.append(f"+{', '.join(added_a)}")
            if removed_a:
                parts.append(f"-{', '.join(removed_a)}")
            col_notes.append("attributes " + " ".join(parts))

        new_dn = new_col.display_name or ""
        old_dn = old_col.display_name or ""
        if new_dn != old_dn:
            col_notes.append(f"display_name {old_dn!r} → {new_dn!r}")

        new_desc = new_col.description or ""
        old_desc = old_col.description or ""
        if new_desc != old_desc:
            col_notes.append("description changed")

        if col_notes:
            notes.append(f"Column '{col_name}': {'; '.join(col_notes)}")

    return notes


class _DryRunRollback(Exception):
    """Sentinel exception that triggers a SAVEPOINT rollback in dry_run mode."""


class DeploymentTimer:
    """Accumulates phase timings and logs a summary table at the end."""

    def __init__(self):
        self._phases: list[tuple[str, float, str]] = []  # (name, ms, detail)
        self._start = time.perf_counter()

    def record(self, name: str, elapsed_ms: float, detail: str = ""):
        self._phases.append((name, elapsed_ms, detail))

    @contextmanager
    def phase(self, name: str):
        """Context manager that records the elapsed time for a named phase.

        Yields a mutable list — append a single string to set the detail:
            with timer.phase("deploy nodes") as p:
                results = await do_work()
                p.append(f"{len(results)} nodes")
        """
        detail: list[str] = []
        t = time.perf_counter()
        yield detail
        self.record(name, (time.perf_counter() - t) * 1000, detail[0] if detail else "")

    def log_summary(self, namespace: str, deployment_id: str):
        total_ms = (time.perf_counter() - self._start) * 1000
        accounted = sum(ms for _, ms, _ in self._phases)
        overhead = total_ms - accounted

        lines = [f"Deployment timing summary for {namespace} [{deployment_id}]:"]
        lines.append(f"  {'Phase':<40} {'Time':>10}  {'Detail'}")
        lines.append(f"  {'─' * 40} {'─' * 10}  {'─' * 30}")
        for name, ms, detail in self._phases:
            pct = (ms / total_ms * 100) if total_ms > 0 else 0
            lines.append(
                f"  {name:<40} {ms:>8.0f}ms  {detail}  ({pct:.0f}%)",
            )
        if overhead > 100:  # Only show if meaningful
            pct = (overhead / total_ms * 100) if total_ms > 0 else 0
            lines.append(
                f"  {'(unaccounted overhead)':<40} {overhead:>8.0f}ms  ({pct:.0f}%)",
            )
        lines.append(f"  {'─' * 40} {'─' * 10}")
        lines.append(f"  {'TOTAL':<40} {total_ms:>8.0f}ms")
        logger.info("\n".join(lines))


@dataclass
class DeploymentExecuteResult:
    """Return value of DeploymentOrchestrator.execute()."""

    results: list  # list[DeploymentResult]
    downstream_impacts: list  # list[ImpactedNode]


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
        dry_run: bool = False,
    ):
        # Deployment context
        self.deployment_spec = deployment_spec
        self.deployment_id = deployment_id
        self.session = session
        self.context = context
        self.dry_run = dry_run

        # Deployment state tracking
        self.registry = ResourceRegistry()
        self.errors: list[DJError] = []
        self.warnings: list[DJError] = []
        self.deployed_results: list[DeploymentResult] = []
        self._timer = DeploymentTimer()

    @property
    def _history_user(self) -> str:
        """
        Returns the username to record in History events.

        For git-backed deployments, uses the commit author (name or email) so that
        node revision history reflects the person who edited the YAML, not the CI
        service account that ran `dj push`.  Falls back to the authenticated user.
        """
        source = self.deployment_spec.source
        if source is not None and source.type == "git":
            author = source.commit_author_email or source.commit_author_name
            if author:
                return author
        return self.context.current_user.username

    async def execute(self) -> DeploymentExecuteResult:
        """
        Validate and deploy all resources and nodes into the specified namespace.

        A single SAVEPOINT wraps setup, plan, and execution so that for
        ``dry_run=True`` every DB write is rolled back — including setup-phase
        writes (namespaces, tags, owners, catalogs, attributes) that would
        otherwise leak. The outer transaction is committed only for wet-runs.

        Returned ``results`` and ``downstream_impacts`` are populated inside
        the SAVEPOINT; Python references stay live after rollback, so dry-run
        callers still get the full impact analysis.
        """
        start_total = time.perf_counter()
        self._timer = DeploymentTimer()
        logger.info(
            "Starting deployment of %d nodes in namespace %s",
            len(self.deployment_spec.nodes),
            self.deployment_spec.namespace,
        )

        result = DeploymentExecuteResult(results=[], downstream_impacts=[])
        try:
            async with self.session.begin_nested():
                result = await self._plan_and_execute()
                if self.dry_run:  # pragma: no branch
                    raise _DryRunRollback()
        except _DryRunRollback:
            pass
        else:
            # `else` only fires when no _DryRunRollback was raised, which
            # implies wet-run — commit the outer transaction.
            await self.session.commit()

        elapsed_ms = (time.perf_counter() - start_total) * 1000
        _metrics_tags = {
            "namespace": self.deployment_spec.namespace,
            "dry_run": str(self.dry_run),
        }
        get_metrics_provider().timer(
            "dj.deployments.execute_latency_ms",
            elapsed_ms,
            _metrics_tags,
        )
        get_metrics_provider().gauge(
            "dj.deployments.node_count",
            len(self.deployed_results),
            _metrics_tags,
        )
        self._timer.log_summary(self.deployment_spec.namespace, self.deployment_id)
        logger.info(
            "Completed deployment for %s [%s] in %.3fs",
            self.deployment_spec.namespace,
            self.deployment_id,
            elapsed_ms / 1000,
        )
        return result

    async def _plan_and_execute(self) -> DeploymentExecuteResult:
        """Inner body of ``execute`` — runs inside the orchestrator's SAVEPOINT.

        Split out so that the caller (``execute``) owns transaction control:
        wrapping setup + plan + execute in one ``begin_nested()`` ensures
        dry-runs roll back all DB writes, including setup-phase writes
        (namespaces, tags, owners, catalogs, attributes).
        """
        with self._timer.phase("setup resources"):
            await self._setup_deployment_resources()

        with self._timer.phase("validate resources"):
            await self._validate_deployment_resources()

        if await self._is_copy_fast_path():
            with self._timer.phase("build plan (copy fast-path)"):
                deployment_plan = await self._build_copy_plan()
        else:
            with self._timer.phase("build plan") as p:
                deployment_plan, pre_results = await self._create_deployment_plan()
                p.append(
                    f"{len(deployment_plan.to_deploy)} to deploy, "
                    f"{len(deployment_plan.to_delete)} to delete",
                )
            self.deployed_results.extend(pre_results)

        if deployment_plan.is_empty():
            return DeploymentExecuteResult(
                results=await self._handle_no_changes(),
                downstream_impacts=[],
            )

        downstream = await self._execute_deployment_plan(deployment_plan)
        return DeploymentExecuteResult(
            results=self.deployed_results,
            downstream_impacts=downstream,
        )

    async def _update_deployment_status(self):
        """
        Update deployment status with current results (skipped for dry runs).
        """
        if self.dry_run:
            return
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
            return []

        settings = get_settings()
        source_prefix = settings.source_node_namespace

        logger.info("Attempting to auto-register missing sources...")
        auto_registered_sources: list[SourceSpec] = []

        # Filter to only nodes that look like [source.]catalog.schema.table.
        # The catalog.schema.table parts are used for catalog lookup and column introspection;
        # the registered source node name preserves the original reference (including any prefix).
        missing_nodes_to_check: list[str] = []
        # Maps (catalog, schema, table) -> original full node name (may include source prefix)
        original_node_names: dict[tuple, str] = {}
        for original_name in missing_nodes:
            parts = original_name.split(".")
            stripped_parts = parts
            if source_prefix and parts[0] == source_prefix:
                stripped_parts = parts[1:]

            # Only consider tables that look like catalog.schema.table (3 parts)
            if len(stripped_parts) == 3:
                stripped_name = ".".join(stripped_parts)
                missing_nodes_to_check.append(stripped_name)
                original_node_names[tuple(stripped_parts)] = original_name

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

        # Build the flat list of (catalog, schema, table) tuples and a lookup back to names.
        # Use the original name (with any source prefix) as the registered node name.
        tables = []
        node_info = {}
        for missing_node_name in missing_nodes_to_check:
            catalog, schema, table = missing_node_name.split(".")
            tables.append((catalog, schema, table))
            node_info[(catalog, schema, table)] = original_node_names.get(
                (catalog, schema, table),
                missing_node_name,
            )

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
                await Tag.find_tags(
                    self.session,
                    tag_names=list(all_tag_names),
                    options=[
                        load_only(
                            Tag.id,
                            Tag.name,
                            Tag.tag_type,
                            Tag.description,
                            Tag.display_name,
                            Tag.created_by_id,
                        ),
                        noload(Tag.created_by),
                        noload(Tag.nodes),
                    ],
                )
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
        tags_modified = False
        for tag_name, tag_spec in deployment_tag_specs.items():
            if tag_name in existing_tags:
                tag = existing_tags[tag_name]
                if tag_needs_update(tag, tag_spec):
                    tag.tag_type = tag_spec.tag_type
                    tag.description = tag_spec.description
                    tag.display_name = tag_spec.display_name or labelize(tag_name)
                    self.session.add(tag)
                    tags_modified = True
            else:
                tag = Tag(
                    name=tag_name,
                    tag_type=tag_spec.tag_type,
                    description=tag_spec.description,
                    display_name=tag_spec.display_name or labelize(tag_name),
                    created_by_id=self.context.current_user.id,
                )
                self.session.add(tag)
                tags_modified = True
            existing_tags[tag_name] = tag

        if tags_modified:
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
            options=[
                load_only(User.id, User.username),
                noload(User.owned_nodes),
                noload(User.owned_associations),
                noload(User.created_by),
                noload(User.created_node_revisions),
                noload(User.group_members),
                noload(User.member_of),
                noload(User.role_assignments),
            ],
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
        Load all attribute types from the database (built-in and custom).
        """
        all_attributes = (
            (await self.session.execute(select(AttributeType))).scalars().all()
        )
        return {attr.name: attr for attr in all_attributes}

    async def _setup_catalogs(self) -> dict[str, Catalog]:
        """
        Load all catalogs from the database and validate that any catalogs
        explicitly referenced by source nodes in the deployment spec exist.
        """
        all_catalogs = (
            (
                await self.session.execute(
                    select(Catalog).options(selectinload(Catalog.engines)),
                )
            )
            .scalars()
            .all()
        )
        all_catalogs_map = {catalog.name: catalog for catalog in all_catalogs}

        # Validate that catalogs explicitly named in the deployment spec are present
        spec_catalog_names = {
            node_spec.catalog
            for node_spec in self.deployment_spec.nodes
            if node_spec.node_type == NodeType.SOURCE and node_spec.catalog
        }

        # Also validate that configured default catalogs exist
        if self.deployment_spec.default_catalog:
            spec_catalog_names.add(self.deployment_spec.default_catalog)
        settings = get_settings()
        if settings.seed_setup.default_catalog_name:
            spec_catalog_names.add(settings.seed_setup.default_catalog_name)

        missing_catalogs = spec_catalog_names - all_catalogs_map.keys()
        if missing_catalogs:
            self.errors.append(
                DJError(
                    code=ErrorCode.CATALOG_NOT_FOUND,
                    message=(
                        f"The following catalogs do not exist: {', '.join(missing_catalogs)}"
                    ),
                ),
            )
        return all_catalogs_map

    async def _is_copy_fast_path(self) -> bool:
        """
        Return True when all specs are pre-validated copies targeted at an empty
        namespace. In that case we can skip the full planning phase (diff, external
        dep resolution) and go straight to bulk inserts.
        """
        if not self.deployment_spec.nodes:
            return False
        if not all(spec._skip_validation for spec in self.deployment_spec.nodes):
            return False
        count = await self.session.scalar(
            select(func.count())
            .select_from(Node)
            .where(
                or_(
                    Node.namespace == self.deployment_spec.namespace,
                    Node.namespace.like(f"{self.deployment_spec.namespace}.%"),
                ),
            ),
        )
        return (count or 0) == 0

    async def _build_copy_plan(self) -> DeploymentPlan:
        """
        Build a DeploymentPlan directly for a copy-into-empty-namespace operation.

        Skips ``check_external_deps`` and diff computation entirely — the specs
        are from already-validated nodes so there is nothing to validate or diff.
        External dependency nodes are loaded into the registry so that
        ``_create_node_revision`` can infer the correct catalog from parents.
        """
        non_cube_specs = [
            s for s in self.deployment_spec.nodes if not isinstance(s, CubeSpec)
        ]
        # Fast path: if all specs have pre-computed upstream names (populated during
        # export from source node DB parents), build the graph directly without
        # re-parsing SQL queries.
        if all(s._upstream_names is not None for s in non_cube_specs):
            node_graph = {
                spec.rendered_name: [
                    render_prefixes(n, spec.namespace)
                    for n in (spec._upstream_names or [])
                ]
                for spec in non_cube_specs
            }
        else:
            node_graph = extract_node_graph(non_cube_specs)

        incoming_names = {s.rendered_name for s in self.deployment_spec.nodes}
        all_referenced = set(node_graph.keys()) | {
            dep for deps in node_graph.values() for dep in deps
        }
        external_dep_names = all_referenced - incoming_names

        if external_dep_names:
            # Need: catalog (for catalog inference in _create_node_revision) and
            # columns (with attributes) for the copy fast-path cube validation
            # in _build_copy_cube_validation_data — that function is sync and
            # would otherwise lazy-load columns, triggering MissingGreenlet.
            ext_nodes = await Node.get_by_names(
                self.session,
                list(external_dep_names),
                options=[
                    selectinload(Node.current).options(
                        joinedload(NodeRevision.catalog),
                        selectinload(NodeRevision.columns).options(
                            joinedload(Column.attributes).joinedload(
                                ColumnAttribute.attribute_type,
                            ),
                        ),
                    ),
                    selectinload(Node.tags),
                ],
            )
            self.registry.add_nodes({n.name: n for n in ext_nodes})

        logger.info(
            "Copy fast-path: deploying %d specs with %d external deps (skipping planning)",
            len(self.deployment_spec.nodes),
            len(external_dep_names),
        )
        return DeploymentPlan(
            to_deploy=list(self.deployment_spec.nodes),
            to_skip=[],
            to_delete=[],
            existing_specs={},
            node_graph=node_graph,
            external_deps=external_dep_names,
        )

    async def _create_deployment_plan(
        self,
    ) -> tuple[DeploymentPlan, list[DeploymentResult]]:
        """Analyze existing nodes and create deployment plan.

        Returns (plan, pre_results) where pre_results are skip/invalid results
        that should be added to deployed_results by the caller.
        """
        pre_results: list[DeploymentResult] = []

        with self._timer.phase("  plan: load existing nodes") as p:
            all_nodes = await NodeNamespace.list_all_nodes(
                self.session,
                self.deployment_spec.namespace,
                options=Node.cube_load_options(),
            )
            p.append(f"{len(all_nodes)} nodes")
        self.registry.add_nodes({node.name: node for node in all_nodes})

        with self._timer.phase("  plan: to_spec conversion") as p:
            existing_specs = {
                node.name: await node.to_spec(self.session) for node in all_nodes
            }
            p.append(f"{len(existing_specs)} specs")

        with self._timer.phase("  plan: diff/filter") as p:
            to_deploy, to_skip, to_delete = self.filter_nodes_to_deploy(existing_specs)
            p.append(
                f"{len(to_deploy)} deploy, {len(to_skip)} skip, {len(to_delete)} delete",
            )

        # Add skipped nodes to results - flag nodes that are still invalid
        for node_spec in to_skip:
            existing_node = self.registry.nodes.get(node_spec.rendered_name)
            is_invalid = (
                existing_node
                and existing_node.current
                and existing_node.current.status == NodeStatus.INVALID
            )
            pre_results.append(
                DeploymentResult(
                    name=node_spec.rendered_name,
                    deploy_type=DeploymentResult.Type.NODE,
                    status=DeploymentResult.Status.INVALID
                    if is_invalid
                    else DeploymentResult.Status.SKIPPED,
                    operation=DeploymentResult.Operation.NOOP,
                    message="Unchanged, still INVALID" if is_invalid else "Unchanged",
                ),
            )

        # Build deployment graph if needed
        node_graph: dict[str, list[str]] = {}
        external_deps: set[str] = set()
        if to_deploy or to_delete:
            with self._timer.phase("  plan: extract node graph") as p:
                node_graph = extract_node_graph(
                    [node for node in to_deploy if not isinstance(node, CubeSpec)],
                )
                p.append(f"{len(node_graph)} nodes in graph")
            with self._timer.phase("  plan: check external deps") as p:
                (
                    external_deps,
                    auto_registered_sources,
                    missing_nodes,
                ) = await self.check_external_deps(node_graph)
                p.append(f"{len(external_deps)} external")

            # Mark nodes whose deps or dimension links are missing as INVALID
            if missing_nodes:
                to_deploy, invalid_from_missing = self._mark_missing_dep_nodes_invalid(
                    to_deploy,
                    node_graph,
                    missing_nodes,
                )
                pre_results.extend(invalid_from_missing)

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

                # Re-check external deps (should be empty now)
                (
                    external_deps,
                    more_auto_registered,
                    second_missing,
                ) = await self.check_external_deps(node_graph)
                if more_auto_registered:  # pragma: no cover
                    # This shouldn't happen, but handle it gracefully
                    raise DJInvalidDeploymentConfig(
                        message="Unexpected second round of auto-registration needed",
                    )
                if second_missing:  # pragma: no cover
                    to_deploy, invalid_from_missing = (
                        self._mark_missing_dep_nodes_invalid(
                            to_deploy,
                            node_graph,
                            second_missing,
                        )
                    )
                    pre_results.extend(invalid_from_missing)

        return (
            DeploymentPlan(
                to_deploy=to_deploy,
                to_skip=to_skip,
                to_delete=to_delete,
                existing_specs=existing_specs,
                node_graph=node_graph,
                external_deps=external_deps,
            ),
            pre_results,
        )

    async def _execute_deployment_plan(self, plan: DeploymentPlan) -> list:
        """Execute the actual deployment based on the plan.

        Runs inside the caller's SAVEPOINT (see ``execute``). The caller
        rolls back on dry-run and commits on wet-run.

        ``propagate_impact`` is called after nodes are deployed but before
        deletions, so it sees post-deploy DB state and deleted nodes'
        children are still reachable via NodeRelationship.

        Returns:
            Downstream impact list reflecting the post-deploy state.
        """
        downstream: list = []
        timer = self._timer
        if plan.to_deploy:
            with timer.phase("deploy nodes") as p:
                deployed_results, deployed_nodes = await self._deploy_nodes(plan)
                p.append(f"{len(deployed_nodes)} nodes")
            self.deployed_results.extend(deployed_results)
            self.registry.add_nodes(deployed_nodes)
            await self._update_deployment_status()

            with timer.phase("deploy links") as p:
                deployed_links = await self._deploy_links(plan)
                p.append(f"{len(deployed_links)} links")
            self.deployed_results.extend(deployed_links)
            await self._update_deployment_status()

            with timer.phase("deploy cubes") as p:
                deployed_cubes = await self._deploy_cubes(plan)
                p.append(f"{len(deployed_cubes)} cubes")
            self.deployed_results.extend(deployed_cubes)
            await self._update_deployment_status()

            # Derive frozen measures for deployed metrics inline so
            # derived_expression and FrozenMeasure rows are atomic with
            # the rest of the deployment.
            with timer.phase("derive measures") as p:
                derived = await self._derive_measures_for_deployed_metrics()
                p.append(f"{derived} metrics")

        # Run impact propagation before deletions so deleted nodes'
        # children are still reachable via NodeRelationship.
        changed_names = {
            r.name
            for r in self.deployed_results
            if r.deploy_type == DeploymentResult.Type.NODE
            and r.status != DeploymentResult.Status.SKIPPED
        }
        changed_link_names = {
            r.name.split(" -> ")[0]
            for r in self.deployed_results
            if r.deploy_type == DeploymentResult.Type.LINK
            and r.status != DeploymentResult.Status.SKIPPED
        }
        with timer.phase("propagate impact") as p:
            downstream = await propagate_impact(
                session=self.session,
                namespace=self.deployment_spec.namespace,
                changed_node_names=changed_names,
                deleted_node_names=frozenset(
                    spec.rendered_name for spec in plan.to_delete
                ),
                changed_link_node_names=changed_link_names,
            )
            p.append(f"{len(downstream)} downstream")

        # Hard-delete after impact propagation (cascade-deletes
        # NodeRelationship rows that were needed for the BFS above).
        if plan.to_delete:
            with timer.phase("delete nodes") as p:
                delete_results = await self._delete_nodes(plan.to_delete)
                p.append(f"{len(delete_results)} deleted")
            self.deployed_results.extend(delete_results)
            await self._update_deployment_status()

        return downstream

    async def _derive_measures_for_deployed_metrics(self) -> int:
        """Run ``derive_frozen_measures`` inline for every metric that was
        created or updated in this deployment.

        Single-node create/update schedules this as a FastAPI background task;
        bulk deployment has no such background-task machinery available for
        a durable post-commit gap (timeouts, restarts, disconnected clients
        drop the task). Running inline inside the orchestrator's SAVEPOINT
        makes derivation atomic with the rest of the deployment:

          * wet-run: committed with the deployment
          * dry-run: rolled back along with the SAVEPOINT

        Order matters: a derived metric's extractor reads its base metrics'
        already-derived measures. ``self.deployed_results`` is appended in the
        order ``_deploy_nodes`` processes topological levels, so iterating
        it in append order produces correct base-before-derived ordering
        within a single deployment.
        """
        metric_spec_names = {
            spec.rendered_name
            for spec in self.deployment_spec.nodes
            if spec.node_type == NodeType.METRIC
        }
        touched_metric_names = [
            r.name
            for r in self.deployed_results
            if r.deploy_type == DeploymentResult.Type.NODE
            and r.name in metric_spec_names
            and r.status != DeploymentResult.Status.SKIPPED
            and r.operation
            in (
                DeploymentResult.Operation.CREATE,
                DeploymentResult.Operation.UPDATE,
            )
        ]
        if not touched_metric_names:
            return 0

        nodes = await Node.get_by_names(
            self.session,
            touched_metric_names,
            options=[joinedload(Node.current)],
        )
        # Iterate in the deployed order (base metrics before derived metrics)
        # so a derived metric's extractor sees its upstream base metrics'
        # already-derived measures in-session.
        name_to_node = {n.name: n for n in nodes}
        revision_ids = [name_to_node[name].current.id for name in touched_metric_names]
        await derive_frozen_measures_bulk(self.session, revision_ids)
        return len(touched_metric_names)

    async def _deploy_nodes(
        self,
        plan: DeploymentPlan,
    ) -> tuple[list[DeploymentResult], dict[str, Node]]:
        """Deploy nodes in the plan"""
        start = time.perf_counter()
        timer = self._timer

        deployed_results, deployed_nodes = [], {}

        # Order nodes topologically based on dependencies
        levels = topological_levels(plan.node_graph, ascending=False)
        logger.info(
            "Deploying nodes in topological order with %d levels",
            len(levels),
        )

        # Load all dependencies once upfront (not per-level).
        # The registry is checked first, so only external deps hit the DB.
        t = time.perf_counter()
        is_copy = all(s._skip_validation for s in plan.to_deploy)
        dependency_nodes = await self.get_dependencies(
            plan.node_graph,
            skip_type_reparsing=is_copy,
        )
        timer.record(
            "    nodes: load dependencies (once)",
            (time.perf_counter() - t) * 1000,
            f"{len(dependency_nodes)} deps",
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
                    dependency_nodes=dependency_nodes,
                )
                deployed_results.extend(level_results)
                deployed_nodes.update(nodes)
                self.registry.add_nodes(nodes)
                # Update dependency_nodes with freshly deployed nodes
                # so subsequent levels can see them
                dependency_nodes.update(nodes)

        logger.info("Finished deploying %d non-cube nodes", len(deployed_nodes))
        get_metrics_provider().timer(
            "dj.deployment.deploy_nodes_ms",
            (time.perf_counter() - start) * 1000,
        )
        get_metrics_provider().gauge(
            "dj.deployment.deploy_nodes_count",
            len(deployed_nodes),
        )
        return deployed_results, deployed_nodes

    async def _deploy_links(self, plan: DeploymentPlan) -> list[DeploymentResult]:
        """Deploy dimension links for nodes in the plan"""
        start = time.perf_counter()
        timer = self._timer
        deployed_links = []

        # Load dimension nodes — serve from registry if already deployed, DB otherwise
        missing_dim_names = [
            name
            for name in plan.linked_dimension_nodes
            if name not in self.registry.nodes
        ]
        if missing_dim_names:
            fetched = await Node.get_by_names(self.session, missing_dim_names)
            self.registry.add_nodes({n.name: n for n in fetched})

        with timer.phase("    links: process") as p:
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
                deployed_links.extend(
                    await self._bulk_delete_links(to_delete, node_spec),
                )

                # Create or update links
                for link_spec in node_spec.dimension_links or []:
                    link_result = await self._process_node_dimension_link(
                        node_spec=node_spec,
                        link_spec=link_spec,
                    )
                    deployed_links.append(link_result)
            p.append(f"{len(deployed_links)} links")

        with timer.phase("    links: DB flush"):
            await self.session.flush()
        logger.info("Finished deploying %d dimension links", len(deployed_links))
        get_metrics_provider().timer(
            "dj.deployment.deploy_links_ms",
            (time.perf_counter() - start) * 1000,
        )
        get_metrics_provider().gauge(
            "dj.deployment.deploy_links_count",
            len(deployed_links),
        )
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
                            user=self._history_user,
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
    ) -> DeploymentResult:
        link_name = f"{node_spec.rendered_name} -> {link_spec.rendered_dimension_node}"
        node = self.registry.nodes.get(node_spec.rendered_name)
        dimension_node = self.registry.nodes.get(link_spec.rendered_dimension_node)

        if not node:  # pragma: no cover
            return self._create_missing_node_link_result(
                link_name,
                node_spec.rendered_name,
            )
        if not dimension_node:
            return self._create_missing_node_link_result(  # pragma: no cover
                link_name,
                link_spec.rendered_dimension_node,
            )

        if node.current and node.current.status == NodeStatus.INVALID:
            # Node is INVALID (no columns / bad SQL). Write the link aspirationally
            # so it's already present once the node is fixed.
            # Reference links can't be created without a real column to point at.
            if link_spec.type == LinkType.JOIN:
                result = await self._create_or_update_dimension_link(
                    link_spec=link_spec,
                    new_revision=node.current,
                    dimension_node=dimension_node,
                )
                result.message += (
                    f"\n[invalid] Node '{node.name}' is INVALID — "
                    "link may not function until the node is fixed"
                )
                return result
            return DeploymentResult(
                name=link_name,
                deploy_type=DeploymentResult.Type.LINK,
                status=DeploymentResult.Status.FAILED,
                operation=DeploymentResult.Operation.CREATE,
                message=(
                    f"Dimension link from {node.name} to {dimension_node.name} was not"
                    f" created because {node.name} is INVALID and has no columns"
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
                spark_hints=join_link.spark_hints,
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
                    user=self._history_user,
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
        timer = self._timer

        with timer.phase("    cubes: validate"):
            if all(spec._skip_validation for spec in cubes_to_deploy):
                validation_results = [
                    self._build_copy_cube_validation_data(cube_spec)
                    for cube_spec in cubes_to_deploy
                ]
            else:
                validation_results = await self._bulk_validate_cubes(cubes_to_deploy)

        with timer.phase("    cubes: create ORM objects"):
            (
                nodes,
                revisions,
                deployment_results,
            ) = await self._create_cubes_from_validation(validation_results)

        with timer.phase("    cubes: DB flush"):
            self.session.add_all(nodes)
            self.session.add_all(revisions)
            await self.session.flush()

        # Wire node.current directly — no refresh needed since nothing
        # downstream accesses cube_elements from the registry.
        for node_obj, revision in zip(nodes, revisions):
            node_obj.current = revision
        self.registry.add_nodes({n.name: n for n in nodes})

        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "Deployed %d cubes in %.3fs",
            len(nodes),
            elapsed_ms / 1000,
        )
        get_metrics_provider().timer(
            "dj.deployment.deploy_cubes_ms",
            elapsed_ms,
        )
        get_metrics_provider().gauge(
            "dj.deployment.deploy_cubes_count",
            len(nodes),
        )
        return deployment_results

    async def _bulk_validate_cubes(
        self,
        cube_specs: list[CubeSpec],
    ) -> list[NodeValidationResult]:
        """Bulk validate cube specifications with batched dimension reachability."""
        if not cube_specs:
            return []  # pragma: no cover

        # Collect all unique metrics and dimensions across all cubes
        all_metric_names, all_dimension_names = self._collect_cube_dependencies(
            cube_specs,
        )

        # Batch load (registry-first, DB fallback)
        metric_nodes_map, missing_metrics = await self._batch_load_metrics(
            all_metric_names,
        )
        dimension_mapping, missing_dimensions = await self._batch_load_dimensions(
            all_dimension_names,
        )

        # Resolve metric → non-metric parent mapping (batched).
        # Replace DB-returned nodes with registry versions where available
        # (registry nodes have .current eagerly loaded; DB nodes may not).
        all_metric_nodes = list(metric_nodes_map.values())
        raw_metric_to_parents = await get_metric_parents_map(
            self.session,
            all_metric_nodes,
        )
        metric_to_parents: dict[str, list[Node]] = {}
        parents_needing_current: list[str] = []
        for metric_name, parents in raw_metric_to_parents.items():
            resolved = []
            for p in parents:
                registry_node = self.registry.nodes.get(p.name)
                if registry_node:
                    resolved.append(registry_node)
                else:
                    parents_needing_current.append(p.name)
                    resolved.append(p)
            metric_to_parents[metric_name] = resolved

        # Batch-load .current for any parents not in the registry
        if parents_needing_current:
            loaded = await Node.get_by_names(
                self.session,
                parents_needing_current,
                options=[joinedload(Node.current)],
            )
            loaded_map = {n.name: n for n in loaded}
            for metric_name, parents in metric_to_parents.items():
                metric_to_parents[metric_name] = [
                    loaded_map.get(p.name, p) if p.name in loaded_map else p
                    for p in parents
                ]

        # One batched BFS for dimension reachability across ALL cubes.
        # local_names maps rev_id → node_name so each parent is always
        # reachable from itself (local dimensions like hard_hat.state).
        local_names: dict[int, str] = {}
        all_parent_rev_ids: set[int] = set()
        for parents in metric_to_parents.values():
            for p in parents:
                if p.current:  # pragma: no branch
                    all_parent_rev_ids.add(p.current.id)
                    local_names[p.current.id] = p.name
        all_dim_node_names = {
            dim.rsplit(SEPARATOR, 1)[0]
            for cube in cube_specs
            for dim in (cube.rendered_dimensions or [])
        }
        # Also include dimension nodes referenced in filters
        for cube in cube_specs:
            if cube.rendered_filters:
                all_dim_node_names |= {
                    node_name
                    for node_name, _ in _extract_dimension_refs_from_filters(
                        cube.rendered_filters,
                    )
                }
        reachability = await DimensionReachability.build(
            self.session,
            all_parent_rev_ids,
            all_dim_node_names,
            local_names=local_names,
        )

        # Per-cube validation — dimension checks are now pure in-memory
        validation_results = []
        for cube_spec in cube_specs:
            validation_result = self._validate_single_cube(
                cube_spec,
                metric_nodes_map,
                missing_metrics,
                missing_dimensions,
                dimension_mapping,
                reachability,
                metric_to_parents,
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
        """Batch load all metrics, serving from registry when available."""
        metric_nodes_map: dict[str, Node] = {}
        names_to_fetch: list[str] = []

        for name in all_metric_names:
            if name in self.registry.nodes:
                metric_nodes_map[name] = self.registry.nodes[name]
            else:
                names_to_fetch.append(name)

        if names_to_fetch:
            db_nodes = await Node.get_by_names(
                self.session,
                names_to_fetch,
                options=[
                    joinedload(Node.current).options(
                        selectinload(NodeRevision.columns),
                        joinedload(NodeRevision.catalog),
                        selectinload(NodeRevision.parents),
                    ),
                ],
                include_inactive=False,
            )
            metric_nodes_map.update({node.name: node for node in db_nodes})

        missing_metrics = all_metric_names - set(metric_nodes_map.keys())
        return metric_nodes_map, missing_metrics

    async def _batch_load_dimensions(
        self,
        all_dimension_names: set[str],
    ) -> tuple[dict[str, Node], set[str]]:
        """Batch load all dimension attributes, serving from registry when available."""
        missing_dimensions = set()
        dimension_attributes: list[FullColumnName] = [
            FullColumnName(dimension_attribute)
            for dimension_attribute in all_dimension_names
        ]
        dimension_node_names = [attr.node_name for attr in dimension_attributes]

        # Serve from registry first, only fetch missing from DB
        dimension_nodes: dict[str, Node] = {}
        names_to_fetch: list[str] = []
        for name in dimension_node_names:
            if name in self.registry.nodes:
                dimension_nodes[name] = self.registry.nodes[name]
            elif name not in dimension_nodes:  # pragma: no branch
                names_to_fetch.append(name)

        if names_to_fetch:
            db_nodes = await Node.get_by_names(
                self.session,
                names_to_fetch,
                options=[
                    joinedload(Node.current).options(
                        selectinload(NodeRevision.columns).options(
                            selectinload(Column.node_revision),
                        ),
                        defer(NodeRevision.query_ast),
                    ),
                ],
            )
            dimension_nodes.update({node.name: node for node in db_nodes})
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

    def _validate_single_cube(
        self,
        cube_spec: CubeSpec,
        metric_nodes_map: dict[str, Node],
        missing_metrics: set[str],
        missing_dimensions: set[str],
        dimension_mapping: dict[str, Node],
        reachability: "DimensionReachability",
        metric_to_parents: dict[str, list[Node]],
    ) -> NodeValidationResult:
        cube_metric_nodes = [
            metric_nodes_map[metric_name]
            for metric_name in cube_spec.rendered_metrics or []
            if metric_name in metric_nodes_map
        ]

        # Collect errors but continue building validation data so the cube
        # revision preserves its metrics/dimensions even when INVALID.
        early_errors: list[DJError] = []

        # Check for INVALID metric nodes — filter them out for column
        # extraction but keep track of the error.
        invalid_metrics = [
            m for m in cube_metric_nodes if m.current.status == NodeStatus.INVALID
        ]
        valid_metric_nodes = [
            m for m in cube_metric_nodes if m.current.status != NodeStatus.INVALID
        ]
        if invalid_metrics:
            early_errors.append(
                DJError(
                    code=ErrorCode.INVALID_CUBE,
                    message=(
                        f"One or more metrics are INVALID for cube "
                        f"{cube_spec.rendered_name}: "
                        + ", ".join(m.name for m in invalid_metrics)
                    ),
                ),
            )

        # Extract metric columns and catalogs from valid metrics only
        metric_columns = [
            node.current.columns[0]
            for node in valid_metric_nodes
            if node.current.columns
        ]
        catalogs = [metric.current.catalog for metric in valid_metric_nodes]
        catalog = catalogs[0] if catalogs else None

        # Collect errors for missing metrics/dimensions or catalog mismatches
        early_errors.extend(
            self._collect_cube_errors(
                cube_spec,
                missing_metrics,
                missing_dimensions,
                catalogs,
            ),
        )

        # Collect parent revision IDs for this cube's metrics (used by both
        # dimension and filter reachability checks below).
        cube_parent_rev_ids = {
            p.current.id
            for metric_name in (cube_spec.rendered_metrics or [])
            for p in metric_to_parents.get(metric_name, [])
            if p.current
        }
        rev_id_to_parent = {
            p.current.id: p.name
            for parents in metric_to_parents.values()
            for p in parents
            if p.current
        }

        # Check dimension reachability using the pre-computed batched BFS
        dim_compat_errors: list[DJError] = []
        if cube_spec.rendered_dimensions:  # pragma: no branch
            requested_dim_nodes = {
                dim.rsplit(SEPARATOR, 1)[0] for dim in cube_spec.rendered_dimensions
            }
            unreachable = reachability.unreachable_dimensions(
                cube_parent_rev_ids,
                requested_dim_nodes,
            )
            for dim_name, missing_from_ids in unreachable.items():
                parent_names = sorted(
                    rev_id_to_parent.get(rid, str(rid)) for rid in missing_from_ids
                )
                dim_compat_errors.append(
                    DJError(
                        code=ErrorCode.INVALID_DIMENSION,
                        message=(
                            f"The dimension attribute `{dim_name}` is not "
                            f"reachable from parent node(s): {', '.join(parent_names)}. "
                            f"Add a dimension link to make it available."
                        ),
                    ),
                )

        # Validate that dimensions referenced in filters are reachable
        # and that the specific columns exist on those dimension nodes.
        if cube_spec.rendered_filters and cube_parent_rev_ids:
            filter_refs = _extract_dimension_refs_from_filters(
                cube_spec.rendered_filters,
            )
            filter_dim_nodes = {node_name for node_name, _ in filter_refs}
            unreachable_filter_dims = reachability.unreachable_dimensions(
                cube_parent_rev_ids,
                filter_dim_nodes,
            )
            for dim_name, missing_from_ids in unreachable_filter_dims.items():
                parent_names = sorted(
                    rev_id_to_parent.get(rid, str(rid)) for rid in missing_from_ids
                )
                dim_compat_errors.append(
                    DJError(
                        code=ErrorCode.INVALID_DIMENSION,
                        message=(
                            f"Filter references dimension `{dim_name}` which is not "
                            f"reachable from parent node(s): {', '.join(parent_names)}. "
                            f"Add a dimension link to make it available."
                        ),
                    ),
                )
            # Check that filter columns exist on the dimension nodes
            for node_name, col_name in filter_refs:
                dim_node = self.registry.nodes.get(node_name) or dimension_mapping.get(
                    f"{node_name}{SEPARATOR}{col_name}",
                )
                if dim_node and dim_node.current:  # pragma: no branch
                    col_names = {c.name for c in dim_node.current.columns}
                    if col_name not in col_names:
                        dim_compat_errors.append(
                            DJError(
                                code=ErrorCode.INVALID_COLUMN,
                                message=(
                                    f"Filter references column `{col_name}` on "
                                    f"dimension `{node_name}`, but that column does "
                                    f"not exist. Available: {sorted(col_names)}"
                                ),
                            ),
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
            dimension_node = dimension_mapping.get(full_key)
            if not dimension_node:
                continue
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

        invalid_dims = [
            d for d in cube_dimension_nodes if d.current.status == NodeStatus.INVALID
        ]
        dim_errors = (
            [
                DJError(
                    code=ErrorCode.INVALID_CUBE,
                    message=(
                        f"One or more dimensions are INVALID for cube "
                        f"{cube_spec.rendered_name}: "
                        + ", ".join(d.name for d in invalid_dims)
                    ),
                ),
            ]
            if invalid_dims
            else []
        )
        all_errors = early_errors + dim_compat_errors + dim_errors
        return NodeValidationResult(
            spec=cube_spec,
            status=NodeStatus.INVALID if all_errors else NodeStatus.VALID,
            inferred_columns=cube_spec.rendered_columns,
            errors=all_errors,
            dependencies=[],
            _cube_validation_data=CubeValidationData(
                metric_columns=metric_columns,
                metric_nodes=cube_metric_nodes,
                dimension_nodes=cube_dimension_nodes,
                dimension_columns=cube_dimensions,
                catalog=catalog,
            ),
        )

    def _build_copy_cube_validation_data(
        self,
        cube_spec: CubeSpec,
    ) -> NodeValidationResult:
        """
        Build a NodeValidationResult for a copy fast-path cube entirely from the
        registry — no DB queries.

        Metric and dimension nodes are already in the registry with their columns set
        in-memory (via node.current = revision after flush).  All column attributes
        needed by _create_cube_node_revision_from_validation_data (type, display_name,
        attributes, attribute_type_id) are set on the in-session Column objects.
        validate_shared_dimensions is skipped because the source cube was already valid.
        """
        cube_metric_nodes = [
            self.registry.nodes[m]
            for m in (cube_spec.rendered_metrics or [])
            if m in self.registry.nodes
        ]

        metric_columns = [
            node.current.columns[0]
            for node in cube_metric_nodes
            if node.current and node.current.columns
        ]
        col_to_node: dict = {
            node.current.columns[0]: node
            for node in cube_metric_nodes
            if node.current and node.current.columns
        }

        cube_dimension_nodes: list[Node] = []
        cube_dimensions: list[Column] = []
        for dim_attr in cube_spec.rendered_dimensions or []:
            node_name, col_name_raw = dim_attr.rsplit(SEPARATOR, 1)
            node = self.registry.nodes.get(node_name)
            if not (node and node.current):
                continue  # pragma: no cover
            if node not in cube_dimension_nodes:  # pragma: no branch
                cube_dimension_nodes.append(node)
            match = re.fullmatch(COLUMN_NAME_REGEX, col_name_raw)
            col_name = match.groups()[0] if match else col_name_raw
            for col in node.current.columns or []:  # pragma: no branch
                if col.name == col_name:  # pragma: no branch
                    cube_dimensions.append(col)
                    col_to_node[col] = node
                    break

        catalogs = [m.current.catalog for m in cube_metric_nodes if m.current]
        catalog = catalogs[0] if catalogs else None

        return NodeValidationResult(
            spec=cube_spec,
            status=cube_spec._source_status or NodeStatus.VALID,
            inferred_columns=cube_spec.rendered_columns,
            errors=[],
            dependencies=[],
            _cube_validation_data=CubeValidationData(
                metric_columns=metric_columns,
                metric_nodes=cube_metric_nodes,
                dimension_nodes=cube_dimension_nodes,
                dimension_columns=cube_dimensions,
                catalog=catalog,
                col_to_node=col_to_node,
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
            cube_spec = cast(CubeSpec, result.spec)
            existing = self.registry.nodes.get(cube_spec.rendered_name)
            operation = (
                DeploymentResult.Operation.UPDATE
                if existing
                else DeploymentResult.Operation.CREATE
            )

            # Get pre-computed validation data to avoid re-validation
            assert result._cube_validation_data is not None
            validation_data = result._cube_validation_data
            changelog, changed_fields = await self._generate_changelog(result)
            if existing:
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
                            validation_data=validation_data,
                            new_node=new_node,
                            status=result.status,
                        )
                    )
            else:
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
                            validation_data=validation_data,
                            new_node=new_node,
                            status=result.status,
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
                    user=self._history_user,
                ),
            )

            # Create deployment result
            invalid_note = (
                "\n[invalid] " + "; ".join(e.message for e in result.errors)
                if result.status == NodeStatus.INVALID
                else ""
            )
            deployment_result = DeploymentResult(
                name=cube_spec.rendered_name,
                deploy_type=DeploymentResult.Type.NODE,
                status=DeploymentResult.Status.INVALID
                if result.status == NodeStatus.INVALID
                else DeploymentResult.Status.SUCCESS,
                operation=operation,
                message=f"{operation.value.title()}d {new_node.type} ({new_node.current_version})"
                + ("\n".join([""] + changelog))
                + invalid_note,
                changed_fields=changed_fields,
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
        status: NodeStatus = NodeStatus.VALID,
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
            # Fast path: use pre-built col→node map (avoids a DB round-trip per column).
            # Slow path: refresh the back-reference from DB (original behaviour).
            if col in validation_data.col_to_node:
                owning_node = validation_data.col_to_node[col]
                referenced_node = owning_node.current  # type: ignore
            else:
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
                    node_column.partition = Partition(
                        type_=col_spec.partition.type,
                        granularity=col_spec.partition.granularity,
                        format=col_spec.partition.format,
                    )

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
            status=status,
            catalog=validation_data.catalog,
            created_by_id=self.context.current_user.id,
            node=new_node,
            version=new_node.current_version,
            mode=cube_spec.mode,
            cube_filters=cube_spec.rendered_filters or [],
            custom_metadata=cube_spec.custom_metadata,
            # Initialize to empty so dimension_links can be appended without
            # triggering a lazy load on the in-session revision (MissingGreenlet).
            dimension_links=[],
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
        async def add_history(event, session):  # pragma: no cover
            """Add history to session without committing"""
            session.add(event)

        try:
            await hard_delete_node(
                name=name,
                session=self.session,
                current_user=self.context.current_user,
                save_history=add_history,
                flush_only=True,
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
        to_create: list[NodeSpec] = []
        to_update: list[NodeSpec] = []
        to_skip: list[NodeSpec] = []
        force = self.deployment_spec.force
        for node_spec in self.deployment_spec.nodes:
            existing_spec = existing_nodes_map.get(node_spec.rendered_name)
            if not existing_spec:
                to_create.append(node_spec)
            elif force or node_spec != existing_spec:
                to_update.append(node_spec)
            else:
                # Re-deploy unchanged nodes that are stuck in INVALID state so
                # they get revalidated (e.g. after an upstream fix).
                existing_node = self.registry.nodes.get(node_spec.rendered_name)
                if (
                    existing_node
                    and existing_node.current
                    and existing_node.current.status == NodeStatus.INVALID
                ):
                    to_update.append(node_spec)
                else:
                    to_skip.append(node_spec)

        desired_node_names = {n.rendered_name for n in self.deployment_spec.nodes}
        to_delete = [
            existing
            for name, existing in existing_nodes_map.items()
            if name not in desired_node_names
        ]

        return to_create + to_update, to_skip, to_delete

    async def check_external_deps(
        self,
        node_graph: dict[str, list[str]],
    ) -> tuple[set[str], list[SourceSpec], list[str]]:
        """
        Find any dependencies that are not in the deployment but are already in the system.
        If any dependencies are missing and auto_register_sources is enabled, attempt to
        register them. Otherwise return the missing nodes so the caller can mark affected
        nodes as INVALID rather than aborting the whole deployment.

        Returns:
            Tuple of (external_deps, auto_registered_sources, missing_nodes)
        """
        settings = get_settings()
        source_prefix = settings.source_node_namespace
        source_prefix_dot = f"{source_prefix}." if source_prefix else None

        # Dimension link targets are node names — track them separately so we can
        # apply stricter existence checks (no parent-node fallback).
        dim_link_dep_set: set[str] = {
            link.rendered_dimension_node
            for node in self.deployment_spec.nodes
            if isinstance(node, LinkableNodeSpec) and node.dimension_links
            for link in node.dimension_links
        }
        logger.info(
            "check_external_deps: %d dim link targets across all spec nodes: %s",
            len(dim_link_dep_set),
            sorted(dim_link_dep_set),
        )

        # Helper to check if a dependency is in the deployment, accounting for the
        # configured source namespace prefix (e.g. "source.catalog.schema.table").
        def is_in_deployment(dep: str) -> bool:
            if dep in node_graph:
                return True
            if source_prefix_dot and dep.startswith(source_prefix_dot):
                normalized = dep[len(source_prefix_dot) :]
                return normalized in node_graph
            return False

        sql_deps_not_in_deployment = {
            dep
            for deps in list(node_graph.values())
            for dep in deps
            if not is_in_deployment(dep)
        }
        dim_link_deps_not_in_deployment = {
            dep for dep in dim_link_dep_set if not is_in_deployment(dep)
        }
        logger.info(
            "check_external_deps: dim link deps not in deployment: %s",
            sorted(dim_link_deps_not_in_deployment),
        )
        deps_not_in_deployment = (
            sql_deps_not_in_deployment | dim_link_deps_not_in_deployment
        )

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
            logger.info(
                "check_external_deps: DB lookup for %d names → found %d: %s",
                len(names_to_search),
                len(found_dep_names),
                sorted(found_dep_names),
            )
            missing_nodes = []
            deployment_node_names = set(node_graph.keys())
            for dep in deps_not_in_deployment:
                is_dim_link = dep in dim_link_dep_set
                # Normalize by stripping the configured source namespace prefix if present
                normalized_dep = dep
                if source_prefix_dot and dep.startswith(source_prefix_dot):
                    normalized_dep = dep[len(source_prefix_dot) :]

                # Check against both original and normalized names
                if dep in found_dep_names or normalized_dep in found_dep_names:
                    continue

                # For SQL deps only: allow a `node.column` reference to pass if the
                # parent node exists.  Dimension link targets are full node names, so
                # this parent-node shortcut must NOT apply to them (it would mask typos).
                if not is_dim_link:
                    parent = normalized_dep.rsplit(SEPARATOR, 1)[0]
                    if SEPARATOR in parent and parent in found_dep_names:
                        continue

                # Check if this is a namespace prefix (some found node starts with dep.)
                # This happens when rsplit of a metric gives its namespace
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
                logger.warning(
                    "check_external_deps: dep %r NOT found — is_dim_link=%s → will mark INVALID",
                    dep,
                    is_dim_link,
                )
                missing_nodes.append(dep)

            if missing_nodes:
                # Try to auto-register missing sources
                auto_registered = await self._auto_register_sources(missing_nodes)

                if self.errors:
                    raise DJInvalidDeploymentConfig(
                        message="Failed to auto-register sources",
                        errors=self.errors,
                        warnings=self.warnings,
                    )

                if auto_registered:
                    # Successfully auto-registered, return them so caller can rebuild graph
                    return deps_not_in_deployment, auto_registered, []

                # Could not auto-register — return missing nodes so the caller can mark
                # affected nodes INVALID rather than aborting the whole deployment.
                logger.warning(
                    "The following dependencies are not in the deployment and do not"
                    " pre-exist in the system: %s — affected nodes will be marked INVALID",
                    missing_nodes,
                )
                return deps_not_in_deployment, [], missing_nodes

            logger.info(
                "All %d external dependencies pre-exist in the system",
                len(external_node_deps),
            )
        return deps_not_in_deployment, [], []

    def _mark_missing_dep_nodes_invalid(
        self,
        to_deploy: list,
        node_graph: dict[str, list[str]],
        missing_nodes: list[str],
    ) -> tuple[list, list[DeploymentResult]]:
        """Remove stale SKIPPED results for nodes with missing deps.

        Nodes that reference a missing dep (SQL or dimension link) will fail
        validation naturally in bulk_validate_node_data and be deployed to the DB
        as INVALID.  Keeping them in to_deploy ensures they appear as BFS roots in
        _compute_downstream_impact, so their downstream nodes are correctly shown as
        impacted and marked INVALID on wet runs.

        This method only clears any pre-added SKIPPED results so the actual
        deployment result (CREATE/UPDATE + INVALID) doesn't appear twice.
        """
        missing_set = set(missing_nodes)
        logger.info(
            "_mark_missing_dep_nodes_invalid: missing_set=%s, checking %d spec nodes",
            sorted(missing_set),
            len(self.deployment_spec.nodes),
        )
        affected_names: set[str] = set()

        for node_spec in self.deployment_spec.nodes:
            name = node_spec.rendered_name
            sql_deps = set(node_graph.get(name, []))
            link_deps = {
                link.rendered_dimension_node
                for link in getattr(node_spec, "dimension_links", None) or []
            }
            blocking = (sql_deps | link_deps) & missing_set
            if link_deps:
                logger.info(
                    "_mark_missing_dep_nodes_invalid: node %r link_deps=%s blocking=%s",
                    name,
                    sorted(link_deps),
                    sorted(blocking),
                )
            if blocking:
                affected_names.add(name)

        # Remove stale SKIPPED results so the deployment result isn't duplicated.
        self.deployed_results = [
            r for r in self.deployed_results if r.name not in affected_names
        ]

        return to_deploy, []

    async def bulk_deploy_nodes_in_level(
        self,
        node_specs: list[NodeSpec],
        node_graph: dict[str, list[str]],
        dependency_nodes: dict[str, Node] | None = None,
    ) -> tuple[list[DeploymentResult], dict[str, Node]]:
        """
        Bulk deploy a list of nodes in a single transaction.
        For these nodes, we know that:
        1. They do not have any dependencies on each other
        2. They are either new or have changes compared to existing nodes
        """
        start = time.perf_counter()
        logger.info("Starting bulk deployment of %d nodes", len(node_specs))
        timer = self._timer
        is_copy = all(s._skip_validation for s in node_specs)

        if dependency_nodes is None:
            with timer.phase("    nodes: load dependencies (once)"):
                dependency_nodes = await self.get_dependencies(
                    node_graph,
                    skip_type_reparsing=is_copy,
                )

        with timer.phase("    nodes: validate") as p:
            if is_copy:
                validation_results = [
                    NodeValidationResult(
                        spec=spec,
                        status=spec._source_status or NodeStatus.VALID,
                        inferred_columns=spec.columns or [],
                        errors=[],
                        dependencies=node_graph.get(spec.rendered_name, []),
                    )
                    for spec in node_specs
                ]
            else:
                validation_results = await bulk_validate_node_data(
                    node_specs,
                    node_graph,
                    self.session,
                    dependency_nodes=dependency_nodes,
                )
            p.append(f"{len(validation_results)} results")

        with timer.phase("    nodes: create ORM objects") as p:
            (
                nodes,
                revisions,
                deployment_results,
            ) = await self.create_nodes_from_validation(
                validation_results,
                dependency_nodes,
                node_graph,
            )
            p.append(f"{len(nodes)} nodes")

        # Check for duplicates
        node_keys = [(n.name, n.namespace) for n in nodes]
        if len(node_keys) != len(set(node_keys)):
            duplicates = [  # pragma: no cover
                k[0] for k, v in Counter(node_keys).items() if v > 1
            ]
            raise DJInvalidDeploymentConfig(  # pragma: no cover
                message=f"Duplicate nodes in deployment spec: {', '.join(duplicates)}",
            )

        with timer.phase("    nodes: DB flush") as p:
            self.session.add_all(nodes)
            self.session.add_all(revisions)
            await self.session.flush()
            p.append(f"{len(nodes)} nodes + {len(revisions)} revisions")

        # Refresh dimension_links so the collection stays in sync after flush.
        for revision in revisions:
            await self.session.refresh(revision, ["dimension_links"])

        # Wire node.current directly from the just-flushed revisions — avoids a
        # SELECT + N refresh round-trips.  All attributes we need (columns, catalog,
        # status, parents) are already set on the in-session objects.
        for node_obj, revision in zip(nodes, revisions):
            node_obj.current = revision

        # Parse string column types into proper ColumnType objects so that
        # downstream impact propagation / type inference works correctly.
        # Without this, in-memory columns carry raw strings like 'bigint'
        # that haven't round-tripped through ColumnTypeDecorator.
        from datajunction_server.sql.parsing.backends.antlr4 import parse_rule

        for revision in revisions:
            for col in revision.columns:
                if isinstance(col.type, str):  # pragma: no cover
                    try:
                        col.type = parse_rule(col.type, "dataType")
                    except Exception:
                        pass

        all_nodes = {node_obj.name: node_obj for node_obj in nodes}

        logger.info(
            f"Deployed {len(nodes)} nodes in bulk in {time.perf_counter() - start:.2f}s",
        )
        return deployment_results, all_nodes

    async def get_dependencies(
        self,
        node_graph: dict[str, list[str]],
        skip_type_reparsing: bool = False,
    ) -> dict[str, Node]:
        all_required_nodes = node_graph.keys() | {
            dep for deps in node_graph.values() for dep in deps
        }
        # Serve nodes already in the registry without a DB round-trip.
        # Nodes from previous topological levels and preloaded external deps are
        # already there; only names missing from the registry hit the database.
        dependency_nodes: dict[str, Node] = {}
        missing_names: list[str] = []
        for name in all_required_nodes:
            if name in self.registry.nodes:
                dependency_nodes[name] = self.registry.nodes[name]
            else:
                missing_names.append(name)
        if missing_names:
            db_nodes = await Node.get_by_names(self.session, missing_names)
            dependency_nodes.update({n.name: n for n in db_nodes})
        if not skip_type_reparsing:
            for dep_node in dependency_nodes.values():
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

    async def create_nodes_from_validation(
        self,
        validation_results: list[NodeValidationResult],
        dependency_nodes: dict[str, Node],
        node_graph: dict[str, list[str]],
    ) -> tuple[list[Node], list[NodeRevision], list[DeploymentResult]]:
        nodes, revisions = [], []
        deployment_results = []
        # Use no_autoflush to prevent premature flushing mid-loop (columns without
        # node_revision_id, nodes without IDs). The caller (bulk_deploy_nodes_in_level)
        # does a single session.flush() after collecting all objects.
        with self.session.no_autoflush:
            for result in validation_results:
                if result.status == NodeStatus.INVALID:
                    logger.warning(
                        "Deploying node %s with INVALID status: %s",
                        result.spec.rendered_name,
                        "; ".join(e.message for e in result.errors),
                    )
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

    def _fallback_catalog(self) -> Catalog | None:
        """Return the deployment's configured default catalog, if any.

        Priority: deployment spec default_catalog > server default_catalog_name.
        Used when catalog can't be derived from parent nodes (e.g., invalid nodes
        whose parents are unresolvable). Callers should further fall back to the
        virtual catalog when this returns None.
        """
        if self.deployment_spec.default_catalog:
            return self.registry.catalogs.get(self.deployment_spec.default_catalog)
        settings = get_settings()
        if settings.seed_setup.default_catalog_name:
            return self.registry.catalogs.get(settings.seed_setup.default_catalog_name)
        return None

    def _infer_cube_catalog(
        self,
        cube_spec: "CubeSpec",
        existing: Node | None,
    ) -> Catalog | None:
        """Infer the catalog for a cube from its metrics (same logic as valid cube deployment).

        deployment_spec.default_catalog takes priority so an explicitly configured catalog
        wins even when metric nodes were previously deployed with the wrong catalog.
        Falls back to the first metric's catalog, then the existing node's catalog.
        """
        if fallback := self._fallback_catalog():
            return fallback
        for metric_name in cube_spec.rendered_metrics:
            metric_node = self.registry.nodes.get(metric_name)
            if metric_node and metric_node.current and metric_node.current.catalog:
                return metric_node.current.catalog
        if existing and existing.current and existing.current.catalog:
            return existing.current.catalog
        return None

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
        changelog, changed_fields = await self._generate_changelog(result)
        new_node = self._create_or_update_node(result.spec, existing)
        new_revision = await self._create_node_revision(
            new_node,
            result,
            dependency_nodes,
            node_graph,
        )
        self.session.add(new_node)
        self.session.add(new_revision)

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
                user=self._history_user,
            ),
        )

        invalid_note = (
            "\n[invalid] " + "; ".join(e.message for e in result.errors)
            if result.status == NodeStatus.INVALID
            else ""
        )
        deployment_result = DeploymentResult(
            name=result.spec.rendered_name,
            deploy_type=DeploymentResult.Type.NODE,
            status=DeploymentResult.Status.INVALID
            if result.status == NodeStatus.INVALID
            else DeploymentResult.Status.SUCCESS,
            operation=operation,
            message=f"{operation.value.title()}d {new_node.type} ({new_node.current_version})"
            + ("\n".join([""] + changelog))
            + invalid_note,
            changed_fields=changed_fields,
        )
        return deployment_result, new_node, new_revision

    async def _generate_changelog(
        self,
        result: NodeValidationResult,
    ) -> tuple[list[str], list[str]]:
        """Generate changelog entries for a node update.

        Returns (changelog_lines, changed_fields) where changelog_lines are
        human-readable strings for the deployment message and changed_fields
        are the raw field names (for structured display in the dry-run impact response).
        """
        changelog: list[str] = []
        changed_fields: list[str] = []

        # No changes if the node is new
        existing = self.registry.nodes.get(result.spec.rendered_name)
        if not existing:
            return changelog, changed_fields

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

        # Check if query changed (diff() ignores it, but we want to surface it)
        if hasattr(
            existing_node_spec,
            "rendered_query",
        ) and hasattr(  # pragma: no branch
            result.spec,
            "rendered_query",
        ):
            old_query = existing_node_spec.rendered_query
            new_query = result.spec.rendered_spec().rendered_query
            if old_query != new_query:
                changed_fields = ["query"] + changed_fields

        # Check if column metadata changed (diff() ignores columns)
        from datajunction_server.models.deployment import LinkableNodeSpec as LNS

        if isinstance(result.spec, LNS) and isinstance(existing_node_spec, LNS):
            col_change_notes = _diff_column_metadata(
                result.spec.rendered_spec().columns,
                existing_node_spec.columns,
            )
            if col_change_notes:
                changed_fields = changed_fields + ["columns"]
                for note in col_change_notes:
                    changelog.append(f"└─ {note}")

        if changed_fields:
            changelog.append("└─ Updated " + ", ".join(changed_fields))

        # If the node has dimension links and is being updated (even if link specs
        # didn't change), the links will be re-deployed — note this in the message.
        if (
            not changed_fields
            and isinstance(result.spec, LinkableNodeSpec)
            and result.spec.dimension_links
        ):
            changelog.append("└─ Updated dimension_links")

        return changelog, changed_fields

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

    @staticmethod
    def _classify_parents(
        spec: NodeSpec,
        dep_names: list[str],
        dependency_nodes: dict[str, Node],
    ) -> tuple[list[Node], list[str]]:
        """Split a spec's dependency names into resolved parents and missing names.

        Thin wrapper around the shared utils.classify_parents helper — derives
        the is_derived_metric flag from the spec so callers don't have to.
        """
        is_derived_metric = (
            spec.node_type == NodeType.METRIC
            and spec.query_ast is not None
            and spec.query_ast.select.from_ is None
        )
        return classify_parents(is_derived_metric, dep_names, dependency_nodes)

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
        parents, missing_parent_names = self._classify_parents(
            result.spec,
            node_graph.get(result.spec.rendered_name, []),
            dependency_nodes,
        )
        if result.spec.node_type != NodeType.SOURCE:
            # Pick the first parent with a non-virtual catalog to assign as the
            # catalog inherited from source parents.
            virtual_catalog_name = get_settings().seed_setup.virtual_catalog_name
            parent_catalog = next(
                (
                    p.current.catalog  # type: ignore
                    for p in parents
                    if p
                    and p.current
                    and p.current.catalog  # type: ignore
                    and p.current.catalog.name != virtual_catalog_name  # type: ignore
                ),
                None,
            )
            # _fallback_catalog() (from deployment_spec.default_catalog) takes priority
            # over parent_catalog so that an explicitly configured catalog wins even when
            # parents were previously deployed with the wrong (e.g. virtual) catalog.
            catalog = (
                self._fallback_catalog()
                or parent_catalog
                or await Catalog.get_virtual_catalog(  # pragma: no cover
                    self.session,
                )
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
            parents=parents,
            missing_parents=[MissingParent(name=n) for n in missing_parent_names],
            created_by_id=self.context.current_user.id,
            custom_metadata=result.spec.custom_metadata,
            # Initialize to empty so _deploy_links can append without triggering a
            # lazy load on the in-session revision (which would cause MissingGreenlet).
            dimension_links=[],
        )
        new_revision.version = new_node.current_version

        if isinstance(result.spec, LinkableNodeSpec) and old_node_revision:
            for link in old_node_revision.dimension_links:
                new_revision.dimension_links.append(
                    DimensionLink(
                        node_revision=new_revision,
                        dimension_id=link.dimension_id,
                        dimension=link.dimension,
                        join_sql=link.join_sql,
                        join_type=link.join_type,
                        join_cardinality=link.join_cardinality,
                        materialization_conf=link.materialization_conf,
                        role=link.role,
                        default_value=link.default_value,
                        spark_hints=link.spark_hints,
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
            # Merge user-specified column metadata (display_name, description, attributes)
            # from the spec onto the inferred columns.  Inferred columns carry the type;
            # spec columns carry user intent.
            spec_col_map: dict[str, ColumnSpec] = {
                c.name: c for c in (result.spec.columns or [])
            }
            merged_cols: list[ColumnSpec] = []
            for idx, inferred in enumerate(result.inferred_columns):
                spec_col = spec_col_map.get(inferred.name)
                if spec_col:
                    merged = inferred.model_copy(
                        update={
                            "display_name": spec_col.display_name
                            if spec_col.display_name is not None
                            else inferred.display_name,
                            "description": spec_col.description
                            if spec_col.description is not None
                            else inferred.description,
                            "attributes": spec_col.attributes
                            if spec_col.attributes
                            else inferred.attributes,
                            "order": spec_col.order
                            if spec_col.order is not None
                            else (
                                inferred.order if inferred.order is not None else idx
                            ),
                        },
                    )
                else:
                    merged = inferred
                merged_cols.append(merged)
            new_revision.columns = [
                self._create_column_from_spec(
                    col,
                    pk_columns,
                    order=col.order if col.order is not None else idx,
                )
                for idx, col in enumerate(merged_cols)
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
            if new_revision.columns:  # pragma: no branch
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
        # Find an existing dimension link. Use (dimension, role, join_sql) for an
        # exact match first — this correctly handles multiple links to the same
        # dimension with different join clauses (e.g., two date links with no role).
        # Fall back to (dimension, role) only when no exact match exists.
        candidates = [
            link  # type: ignore
            for link in node_revision.dimension_links  # type: ignore
            if link.dimension.name == dimension_node.name
            and link.role == link_input.role  # type: ignore
        ]
        activity_type = ActivityType.CREATE

        # Try exact match first (all fields identical → REFRESH/noop)
        exact_match = [
            link
            for link in candidates
            if link.join_sql == link_input.join_on
            and link.join_type == join_type
            and link.join_cardinality == link_input.join_cardinality
            and link.default_value == link_input.default_value
            and link.spark_hints == link_input.spark_hints
        ]
        if exact_match:
            return exact_match[0], ActivityType.REFRESH

        # No exact match — if there's exactly one candidate with the same
        # (dimension, role), treat it as an update to that link.
        if len(candidates) == 1:
            activity_type = ActivityType.UPDATE
            dimension_link = candidates[0]
            dimension_link.join_sql = link_input.join_on
            dimension_link.join_type = join_type
            dimension_link.join_cardinality = link_input.join_cardinality
            dimension_link.default_value = link_input.default_value
            dimension_link.spark_hints = link_input.spark_hints
        else:
            # No single candidate to update — create a new dimension link object
            dimension_link = DimensionLink(
                node_revision_id=node_revision.id,  # type: ignore
                dimension_id=dimension_node.id,  # type: ignore
                dimension=dimension_node,  # populate relationship so checks work without flush
                join_sql=link_input.join_on,
                join_type=join_type,
                join_cardinality=link_input.join_cardinality,
                role=link_input.role,
                default_value=link_input.default_value,
                spark_hints=link_input.spark_hints,
            )
            node_revision.dimension_links.append(dimension_link)  # type: ignore
        return dimension_link, activity_type


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
