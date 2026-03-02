"""Building node SQL functions"""

import collections
import logging
import re
from dataclasses import dataclass
from functools import cached_property
from typing import (
    Any,
    DefaultDict,
    Optional,
    Tuple,
    Union,
    cast,
)

from sqlalchemy import text, bindparam, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from datajunction_server.internal.access.authorization import (
    AccessChecker,
    AccessDenialMode,
)
from datajunction_server.construction.utils import to_namespaced_name
from datajunction_server.database import Engine
from datajunction_server.database.attributetype import ColumnAttribute
from datajunction_server.database.column import Column
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import (
    DJException,
    DJQueryBuildError,
    DJQueryBuildException,
    ErrorCode,
)
from datajunction_server.models import access
from datajunction_server.models.column import SemanticType
from datajunction_server.models.cube_materialization import (
    MetricComponent,
)
from datajunction_server.models.engine import Dialect
from datajunction_server.models.node import BuildCriteria
from datajunction_server.models.node_type import NodeType
from datajunction_server.naming import amenable_name, from_amenable_name
from datajunction_server.sql.parsing.ast import CompileContext
from datajunction_server.sql.parsing.backends.antlr4 import ast, cached_parse, parse
from datajunction_server.utils import SEPARATOR, refresh_if_needed

logger = logging.getLogger(__name__)


@dataclass
class FullColumnName:
    """
    A fully qualified column name with the node name and the column.
    """

    name: str

    @cached_property
    def node_name(self) -> str:
        """
        Gets the node name part of the full column name.
        """
        return SEPARATOR.join(self.name.split(SEPARATOR)[:-1])

    @cached_property
    def full_column_name(self) -> str:
        """
        Gets the column name part of the full column name.
        """
        return self.name.split(SEPARATOR)[-1]

    @cached_property
    def column_name(self) -> str:
        """
        Gets the column name part of the full column name.
        """
        if self.role:
            return self.full_column_name.replace(f"[{self.role}]", "")
        return self.full_column_name

    @cached_property
    def role(self) -> Optional[str]:
        """
        Gets the role.
        """
        regex = r"\[([A-Za-z0-9_\-\>]*)\]"
        match = re.search(regex, self.full_column_name)
        if match:
            return match.group(1)
        return None

    @cached_property
    def join_key(self) -> str:
        """
        Generates a unique key for identifying shared join contexts in query building.

        For dimensions without role paths, returns the node name (e.g., "default.countries").
        For role path dimensions, includes the role to distinguish different join contexts
        for the same dimension node (e.g., "default.countries[user_birth_country]").

        This key is used by the query builder to group dimensions that can share the same
        dimension node join.
        """
        if self.role:
            return f"{self.node_name}[{self.role}]"
        return self.node_name


@dataclass
class DimensionJoin:
    """
    Info on a dimension join
    """

    join_path: list[DimensionLink]
    requested_dimensions: list[str]
    node_query: Optional[ast.Query] = None
    right_alias: Optional[ast.Name] = None

    def add_requested_dimension(self, dimension: str):
        """
        Adds a requested dimension to the join
        """
        if dimension not in self.requested_dimensions:  # pragma: no cover
            self.requested_dimensions.append(dimension)


def resolve_metric_component_against_parent(
    component: MetricComponent,
    parent_ast: ast.Query,
    parent_node: Node,
) -> ast.Query:
    """
    Parses and resolves a SQL expression (or aggregated expression) against a parent query AST.
    We resolve column references based on the parent's column mappings and apply the types
    from the parent.
    """
    # aggregation is function name or template (e.g., "SUM" or "SUM(POWER({}, 2))")
    if not component.aggregation:
        expr_sql = component.expression
    elif "{}" in component.aggregation:  # pragma: no cover
        # Template case: "SUM(POWER({}, 2))" -> "SUM(POWER(x, 2))"
        expr_sql = component.aggregation.format(component.expression)
    else:
        # Simple case: "SUM" -> "SUM(x)"
        expr_sql = f"{component.aggregation}({component.expression})"
    # Add all expressions from the metric component's aggregation level to the GROUP BY
    group_by_clause = (
        f"GROUP BY {','.join(component.rule.level)}" if component.rule.level else ""
    )
    component_ast = cached_parse(
        f"SELECT {expr_sql} AS {component.name} FROM {parent_ast.alias_or_name.name} {group_by_clause}",
    )

    parent_select = parent_ast.select
    original_columns = {
        col.name.name: col
        for col in parent_select.projection
        if isinstance(col, ast.Column)
    }
    for col in component_ast.find_all(ast.Column):
        if matching := parent_select.column_mapping.get(col.name.name):
            # Case 1: The column name matches one of the parent's select aliases directly
            col.name = matching.alias_or_name.copy()
            col.add_type(matching.type)
        elif matching := parent_select.semantic_column_mapping.get(col.identifier()):
            # Case 2: The column name is a joinable dimension and can be found by searching
            # the semantic entities of each of the parent columns
            col.name = matching.alias_or_name.copy()
            col.add_type(matching.type)
        elif matching := original_columns.get(col.identifier()):
            # Case 3: The column name has been included as a dimension and so needs to use
            # semantic entity name rather than the original column name
            col.name = matching.alias_or_name.copy()
            col.add_type(matching.type)
        else:
            # Case 4: The column is a local dimension reference and cannot be found directly
            # in the parent's select clause, but can be resolved by prefixing with the parent
            # node's name (e.g., from `entity` to `default_DOT_transform_DOT_entity`)
            alias = amenable_name(f"{parent_node.name}{SEPARATOR}{col.name.name}")
            if matching := parent_select.column_mapping.get(alias):  # pragma: no cover
                col.name.name = alias
                col.add_type(matching.type)
    return component_ast


def build_preaggregate_query(
    parent_ast: ast.Query,
    parent_node: Node,
    dimensional_columns: list[ast.Column],
    children: list[NodeRevision],
    metric_to_components: dict[str, tuple[list[MetricComponent], ast.Query]],
):
    """
    Builds a measures query preaggregated to the chosen dimensions.
    """
    existing_ctes = parent_ast.ctes
    parent_ast.ctes = []
    built_parent_ref = parent_node.name + "_built"
    parent_node_cte = parent_ast.to_cte(ast.Name(amenable_name(built_parent_ref)))
    from_table = ast.Table(ast.Name(amenable_name(built_parent_ref)))

    final_query = ast.Query(
        ctes=existing_ctes + [parent_node_cte],
        select=ast.Select(
            projection=[
                ast.Column.from_existing(col, table=from_table)
                for col in parent_ast.select.projection
                if col and col.semantic_type == SemanticType.DIMENSION  # type: ignore
            ],
            from_=ast.From(relations=[ast.Relation(primary=from_table)]),
            group_by=[
                ast.Column(dim.alias_or_name, _table=from_table)
                for dim in dimensional_columns
            ],
        ),
    )

    added_components = set()
    for metric in children:
        for component in metric_to_components[metric.name][0]:
            if component.name in added_components:
                continue
            added_components.add(component.name)
            component_ast = resolve_metric_component_against_parent(
                component,
                parent_ast,
                parent_node,
            )
            for proj in component_ast.select.projection:
                proj.set_semantic_entity(parent_node.name + SEPARATOR + component.name)  # type: ignore
                proj.set_semantic_type(SemanticType.MEASURE)  # type: ignore
            final_query.select.projection.extend(component_ast.select.projection)
            final_query.select.group_by.extend(component_ast.select.group_by or [])
    return final_query


def get_dimensions_referenced_in_metrics(metric_nodes: list[Node]) -> list[str]:
    """
    Returns a list of dimensions referenced in the metric nodes' query definitions.
    """
    dimensions = set()
    for metric in metric_nodes:
        metric_ast = parse(metric.current.query)
        for ref in metric_ast.find_all(ast.Column):
            if SEPARATOR in ref.identifier().rsplit(SEPARATOR, 1)[0]:
                dimensions.add(ref.identifier())
    return sorted(list(dimensions))


class QueryBuilder:
    """
    This class allows users to configure building node SQL by incrementally building out
    the build configuration, including adding filters, dimensions, ordering, and limit
    clauses. The builder then handles the management of CTEs, dimension joins, and error
    validation, allowing for dynamic node query generation based on runtime conditions.
    """

    def __init__(
        self,
        session: AsyncSession,
        node_revision: NodeRevision,
        use_materialized: bool = True,
    ):
        self.session = session
        self.node_revision = node_revision
        self.use_materialized = use_materialized

        self._filters: list[str] = []
        self._parameters: dict[str, ast.Value] = {}
        self._required_dimensions: list[str] = [
            required.name for required in self.node_revision.required_dimensions
        ]
        self._dimensions: list[str] = []
        self._orderby: list[str] = []
        self._limit: Optional[int] = None
        self._build_criteria: Optional[BuildCriteria] = self.get_default_criteria()
        self._access_checker: Optional[AccessChecker] = None
        self._ignore_errors: bool = False

        # The following attributes will be modified as the query gets built.
        # --
        # Track node query CTEs as they get built
        self.cte_mapping: dict[str, ast.Query] = {}  # Maps node name to its CTE
        # Keep a list of build errors
        self.errors: list[DJQueryBuildError] = []
        # The final built query AST
        self.final_ast: Optional[ast.Query] = None
        # Shared cache for DJ node lookups - reused across all compile calls
        # This avoids redundant DB queries for the same node
        self.dependencies_cache: dict[str, Node] = {}
        # Preloaded join paths keyed by (dim_name, role)
        # Populated by find_dimension_node_joins
        self._preloaded_join_paths: dict[tuple[str, str], list[DimensionLink]] = {}

    @classmethod
    async def create(
        cls,
        session: AsyncSession,
        node_revision: NodeRevision,
        use_materialized: bool = True,
    ) -> "QueryBuilder":
        """
        Create a QueryBuilder instance for the node revision.
        """
        await refresh_if_needed(
            session,
            node_revision,
            ["required_dimensions", "dimension_links"],
        )
        instance = cls(session, node_revision, use_materialized=use_materialized)
        return instance

    def ignore_errors(self):
        """Do not raise on errors in query build."""
        self._ignore_errors = True
        return self

    def raise_errors(self):
        """Raise on errors in query build."""
        self._ignore_errors = False
        return self

    def filter_by(self, filter_: str):
        """Add filter to the query builder."""
        if filter_ not in self._filters:
            self._filters.append(filter_)
        return self

    def add_filters(self, filters: Optional[list[str]] = None):
        """Add filters to the query builder."""
        for filter_ in filters or []:
            self.filter_by(filter_)
        return self

    def add_query_parameters(
        self,
        query_parameters: dict[str, ast.Value | Any] | None = None,
    ):
        """Add parameters to the query builder."""
        for param, value in (query_parameters or {}).items():
            self._parameters[param] = QueryBuilder.normalize_query_param_value(
                param,
                value,
            )
        return self

    def add_dimension(self, dimension: str):
        """Add dimension to the query builder."""
        if (
            dimension not in self._dimensions
            and dimension not in self._required_dimensions
        ):
            self._dimensions.append(dimension)
        return self

    def add_dimensions(self, dimensions: Optional[list[str]] = None):
        """Add dimensions to the query builder."""
        for dimension in dimensions or []:
            self.add_dimension(dimension)
        return self

    def order_by(self, orderby: Optional[Union[str, list[str]]] = None):
        """Set order by for the query builder."""
        if isinstance(orderby, str):
            if orderby not in self._orderby:
                self._orderby.append(orderby)
        else:
            for order in orderby or []:
                if order not in self._orderby:  # pragma: no cover
                    self._orderby.append(order)
        return self

    def limit(self, limit: Optional[int] = None):
        """Set limit for the query builder."""
        if limit:  # pragma: no cover
            self._limit = limit
        return self

    def with_build_criteria(self, build_criteria: Optional[BuildCriteria] = None):
        """Set build criteria for the query builder."""
        if build_criteria:  # pragma: no cover
            self._build_criteria = build_criteria
        return self

    def with_access_control(
        self,
        access_checker: AccessChecker,
    ):
        """
        Set access control for the query builder.
        """
        if access_checker:  # pragma: no cover
            access_checker.add_node(self.node_revision, access.ResourceAction.READ)
            self._access_checker = access_checker
        return self

    @property
    def dimensions(self) -> list[str]:
        """All dimensions"""
        return self._dimensions + self._required_dimensions

    @property
    def filters(self) -> list[str]:
        """All filters"""
        return self._filters

    @property
    def parameters(self) -> dict[str, ast.Value]:
        """
        Extracts parameters from relevant filters
        """
        return self._parameters

    @property
    def filter_asts(self) -> list[ast.Expression]:
        """
        Returns a list of filter expressions rendered as ASTs
        """
        return [filter_ast for filter_ast in to_filter_asts(self.filters) if filter_ast]

    @property
    def include_dimensions_in_groupby(self) -> bool:
        """
        Whether to include the requested dimensions in the query's GROUP BY clause.
        Defaults to true for metrics.
        """
        return self.node_revision.type == NodeType.METRIC

    @cached_property
    def physical_table(self) -> Optional[ast.Table]:
        """
        A physical table for the node, if one exists
        """
        return get_table_for_node(
            self.node_revision,
            build_criteria=self._build_criteria,
        )

    @property
    def context(self) -> dict[str, Any]:
        """
        Debug context
        """
        return {
            "node_revision": self.node_revision.name,
            "filters": self._filters,
            "required_dimensions": self._required_dimensions,
            "dimensions": self._dimensions,
            "orderby": self._orderby,
            "limit": self._limit,
            "ignore_errors": self._ignore_errors,
            "build_criteria": self._build_criteria,
        }

    async def build(self) -> ast.Query:
        """
        Builds the node SQL with the requested set of dimensions, filter expressions,
        order by, and limit clauses.

        Build Strategy
        ---------------
        1. Recursively turn node references into query ASTs + apply any filters that can
        be pushed down. If the node query has CTEs, unwind them into subqueries.
        2. Initialize the final query with the node's query AST added to it as a CTE.
        3. For any dimensions or filters requested for the node, determine if a join is
        needed to bring in the dimension or filter. Keep track of all the necessary dimension
        joins in a dict that maps dimension nodes to join paths.
        4. For each of the necessary dimension joins, build the dimension node's query in the
        same manner as above, recursively replacing any node references and pushing down requested
        filters where possible.
        5. Add each dimension node's query AST to the final query as a CTE.
        6. Build the final query using the various CTEs. This does all the joins between the node
        query AST and the dimension nodes' ASTs using the join logic from the dimension links.
        7. Add all requested dimensions to the final select.
        8. Add order by and limit to the final select (TODO)
        """
        await refresh_if_needed(
            self.session,
            self.node_revision,
            ["availability", "columns"],
        )

        node_ast = (
            await compile_node_ast(
                self.session,
                self.node_revision,
                dependencies_cache=self.dependencies_cache,
            )
            if not self.physical_table
            else self.create_query_from_physical_table(self.physical_table)
        )

        if self.physical_table and not self._filters and not self.dimensions:
            self.final_ast = node_ast
        else:
            # Pre-load dimension nodes before building the current node AST
            # This populates dependencies_cache so Table.compile() can use cached nodes
            # instead of making individual get_dj_node calls
            target_dim_names = {
                FullColumnName(dim).node_name
                for dim in self._collect_referenced_dimensions()
                if FullColumnName(dim).node_name != self.node_revision.name
            }
            if target_dim_names:
                self._preloaded_join_paths = (
                    await self.preload_join_paths_for_dimensions(target_dim_names)
                )

            node_alias, node_ast = await self.build_current_node_ast(node_ast)

            ctx = CompileContext(
                self.session,
                DJException(),
                dependencies_cache=self.dependencies_cache,
            )
            await node_ast.compile(ctx)
            self.final_ast = self.initialize_final_query_ast(node_ast, node_alias)
            await self.build_dimension_node_joins(node_ast, node_alias)
            self.set_dimension_aliases()

        self.final_ast.select.limit = self._limit  # type: ignore
        if self._orderby:
            if order := self.build_order_bys():
                self.final_ast.select.organization = ast.Organization(  # type: ignore
                    order=order,
                )

        # Replace any parameters in the final AST with their values
        for param in self.final_ast.find_all(ast.QueryParameter):  # type: ignore
            if param.name in self.parameters and param.parent:
                param.parent.replace(param, self.parameters[param.name])
            else:
                self.errors.append(
                    DJQueryBuildError(
                        code=ErrorCode.MISSING_PARAMETER,
                        message=f"Missing value for parameter: {param.name}",
                    ),
                )

        filterable_final_dims = {
            col.semantic_entity: col
            for col in self.final_ast.select.projection  # type: ignore
            if isinstance(col, ast.Column)
        }
        for filter_ast in self.filter_asts:
            resolved = False
            for filter_dim in filter_ast.find_all(ast.Column):
                filter_dim_expr = (
                    filter_dim.parent
                    if isinstance(filter_dim.parent, ast.Subscript)
                    else filter_dim
                )
                filter_key = str(filter_dim_expr)
                dim_expr = filterable_final_dims.get(filter_key)
                if dim_expr:
                    resolved = True
                    if filter_dim_expr.parent:
                        filter_dim_expr.parent.replace(
                            filter_dim_expr,
                            ast.Column(
                                name=ast.Name(dim_expr.identifier()),
                                _table=dim_expr.table,
                                _type=dim_expr.type,
                                semantic_entity=dim_expr.semantic_entity,
                            ),
                        )

            if resolved:
                self.final_ast.select.where = combine_filter_conditions(  # type: ignore
                    self.final_ast.select.where,  # type: ignore
                    filter_ast,
                )

        # Error validation
        await self.validate_access()
        if self.errors and not self._ignore_errors:
            raise DJQueryBuildException(errors=self.errors)
        return self.final_ast  # type: ignore

    def build_order_bys(self):
        """
        Build the ORDER BY clause from the provided order expressions
        """
        temp_orderbys = cached_parse(
            f"SELECT 1 ORDER BY {','.join(self._orderby)}",
        ).select.organization.order
        valid_sort_items = [
            sortitem
            for sortitem in temp_orderbys
            if amenable_name(sortitem.expr.identifier())
            in self.final_ast.select.column_mapping
        ]
        if len(valid_sort_items) < len(temp_orderbys):
            self.errors.append(
                DJQueryBuildError(
                    code=ErrorCode.INVALID_ORDER_BY,
                    message=f"{self._orderby} is not a valid ORDER BY request",
                    debug=self.context,
                ),
            )
        return [
            ast.SortItem(
                expr=self.final_ast.select.column_mapping.get(
                    amenable_name(sortitem.expr.identifier()),
                )
                .copy()
                .set_alias(None),
                asc=sortitem.asc,
                nulls=sortitem.nulls,
            )
            for sortitem in valid_sort_items
        ]

    def get_default_criteria(
        self,
        engine: Optional[Engine] = None,
    ) -> BuildCriteria:
        """
        Get the default build criteria for a node.
        Set the dialect by using the provided engine, if any. If no engine is specified,
        set the dialect by finding available engines for this node, or default to Spark
        """
        dialect = (
            engine.dialect
            if engine
            else (
                self.node_revision.catalog.engines[0].dialect
                if self.node_revision.catalog
                and self.node_revision.catalog.engines
                and self.node_revision.catalog.engines[0].dialect
                else Dialect.SPARK
            )
        )
        return BuildCriteria(
            dialect=dialect,
            target_node_name=self.node_revision.name,
        )

    async def build_current_node_ast(self, node_ast):
        """
        Build the node AST into a CTE
        """
        ctx = CompileContext(
            self.session,
            DJException(),
            dependencies_cache=self.dependencies_cache,
        )
        await node_ast.compile(ctx)
        self.errors.extend(ctx.exception.errors)
        node_alias = ast.Name(amenable_name(self.node_revision.name))
        return node_alias, await build_ast(
            self.session,
            self.node_revision,
            node_ast,
            filters=self._filters,
            build_criteria=self._build_criteria,
            ctes_mapping=self.cte_mapping,
            use_materialized=self.use_materialized,
            dependencies_cache=self.dependencies_cache,
        )

    def initialize_final_query_ast(self, node_ast, node_alias):
        """
        Initialize the final query AST structure
        """
        node_ctes = remove_duplicates(  # pragma: no cover
            node_ast.ctes,
            lambda cte: cte.alias_or_name.identifier(),
        )
        return ast.Query(
            select=ast.Select(
                projection=[
                    ast.Column(
                        ast.Name(col.alias_or_name.name),  # type: ignore
                        _table=node_ast,
                        _type=col.type,  # type: ignore
                        semantic_type=col.semantic_type,
                        semantic_entity=col.semantic_entity,
                    )
                    for col in node_ast.select.projection
                ],
                from_=ast.From(relations=[ast.Relation(node_alias)]),  # type: ignore
            ),
            ctes=[*node_ctes, node_ast],
        )

    @classmethod
    def generate_role_alias(
        cls,
        node_name: str,
        role_path: str | None,
        join_index: int,
    ) -> str | None:
        """
        Generate unique alias for a role path join
        """
        if not role_path:
            return None
        role_parts = role_path.split("->")
        role_path = "__".join(role_parts[: join_index + 1])
        return (
            f"{amenable_name(node_name)}__{join_index}"
            if not role_path
            else role_path.replace("->", "__")
        )

    async def find_join_paths_batch(
        self,
        target_dimension_names: set[str],
        max_depth: int = 5,
    ) -> dict[tuple[str, str], list[int]]:
        """
        Find join paths from this node to all target dimension nodes using a single
        recursive CTE query. Returns a dict mapping (dimension_node_name, role_path)
        to the list of DimensionLink IDs forming the path.

        The role_path is a "->" separated string of roles at each step (e.g., "birth_date->parent").
        Empty roles are represented as empty strings.

        This is O(1) database calls instead of O(nodes * depth) individual queries.
        """
        if not target_dimension_names:
            return {}  # pragma: no cover

        # Single recursive CTE to find all paths at once
        # Tracks both the link IDs and the roles at each step
        recursive_query = text("""
            WITH RECURSIVE paths AS (
                -- Base case: first level dimension links from the source node
                SELECT
                    dl.id as link_id,
                    n.name as dim_name,
                    CAST(dl.id AS TEXT) as path,
                    COALESCE(dl.role, '') as role_path,
                    1 as depth
                FROM dimensionlink dl
                JOIN node n ON dl.dimension_id = n.id
                WHERE dl.node_revision_id = :source_revision_id

                UNION ALL

                -- Recursive case: follow dimension_links from each dimension node
                SELECT
                    dl2.id as link_id,
                    n2.name as dim_name,
                    paths.path || ',' || CAST(dl2.id AS TEXT) as path,
                    paths.role_path || '->' || COALESCE(dl2.role, '') as role_path,
                    paths.depth + 1 as depth
                FROM paths
                JOIN node prev_node ON paths.dim_name = prev_node.name
                JOIN noderevision nr ON prev_node.current_version = nr.version AND nr.node_id = prev_node.id
                JOIN dimensionlink dl2 ON dl2.node_revision_id = nr.id
                JOIN node n2 ON dl2.dimension_id = n2.id
                WHERE paths.depth < :max_depth
            )
            SELECT dim_name, path, role_path, depth
            FROM paths
            WHERE dim_name IN :target_names
            ORDER BY depth ASC
        """).bindparams(bindparam("target_names", expanding=True))

        result = await self.session.execute(
            recursive_query,
            {
                "source_revision_id": self.node_revision.id,
                "max_depth": max_depth,
                "target_names": list(target_dimension_names),
            },
        )
        rows = result.fetchall()

        # Build paths dict keyed by (dim_name, role_path)
        paths: dict[tuple[str, str], list[int]] = {}
        for dim_name, path_str, role_path, depth in rows:
            key = (dim_name, role_path or "")
            if key not in paths:  # pragma: no branch
                paths[key] = [int(x) for x in path_str.split(",")]

        return paths

    async def load_dimension_links_and_nodes(
        self,
        link_ids: set[int],
    ) -> dict[int, DimensionLink]:
        """
        Batch load DimensionLinks and their associated dimension Nodes.
        Returns a dict mapping link_id to DimensionLink object.
        """
        if not link_ids:
            return {}

        # Load all dimension links with eager loading
        # Include dimension_links on the node revision for multi-hop joins
        stmt = (
            select(DimensionLink)
            .where(DimensionLink.id.in_(link_ids))
            .options(
                joinedload(DimensionLink.dimension).options(
                    joinedload(Node.current).options(
                        selectinload(NodeRevision.columns).options(
                            joinedload(Column.attributes).joinedload(
                                ColumnAttribute.attribute_type,
                            ),
                            joinedload(Column.dimension),
                            joinedload(Column.partition),
                        ),
                        joinedload(NodeRevision.catalog),
                        selectinload(NodeRevision.availability),
                        selectinload(NodeRevision.dimension_links).options(
                            joinedload(DimensionLink.dimension),
                        ),
                    ),
                ),
            )
        )
        result = await self.session.execute(stmt)
        links = result.scalars().unique().all()

        # Build lookup dict and cache nodes
        link_dict: dict[int, DimensionLink] = {}
        for link in links:
            link_dict[link.id] = link
            if link.dimension:  # pragma: no branch
                self.dependencies_cache[link.dimension.name] = link.dimension

        return link_dict

    async def preload_join_paths_for_dimensions(
        self,
        target_dimension_names: set[str],
    ) -> dict[tuple[str, str], list[DimensionLink]]:
        """
        Find and load join paths for the requested dimensions using a single recursive
        CTE query + a single batch load.

        Returns dict mapping (dimension_node_name, role_path) to list of DimensionLink objects.
        The role_path is a "->" separated string matching the roles at each step.
        """
        # Ensure the main node's dimension_links are loaded
        await refresh_if_needed(self.session, self.node_revision, ["dimension_links"])

        # Use recursive CTE to find all paths in one query (keyed by (dim_name, role_path))
        path_ids = await self.find_join_paths_batch(target_dimension_names)

        # Collect all link IDs we need to load
        all_link_ids: set[int] = set()
        for link_id_list in path_ids.values():
            all_link_ids.update(link_id_list)

        # Batch load all DimensionLinks + Nodes in one query
        link_dict = await self.load_dimension_links_and_nodes(all_link_ids)

        # Build the final paths with actual DimensionLink objects
        dimension_paths: dict[tuple[str, str], list[DimensionLink]] = {}
        for key, link_id_list in path_ids.items():
            dimension_paths[key] = [
                link_dict[lid] for lid in link_id_list if lid in link_dict
            ]

        return dimension_paths

    async def build_dimension_node_joins(self, node_ast, node_alias):
        """
        Builds the dimension joins and adding them to the CTEs
        """
        # Add node ast to CTE tracker
        node_ast.ctes = []
        node_ast = node_ast.to_cte(node_alias)
        self.cte_mapping[self.node_revision.name] = node_ast

        # Find all dimension join paths in one recursive CTE query, and then batch
        # load only the DimensionLinks/Nodes that are actually needed
        dimension_node_joins = await self.find_dimension_node_joins()

        # Track added joins to avoid duplicates
        added_joins = set()

        for join_key, dimension_join in dimension_node_joins.items():
            join_path = dimension_join.join_path
            requested_dimensions = list(
                dict.fromkeys(dimension_join.requested_dimensions),
            )

            previous_alias = None
            for idx, link in enumerate(join_path):
                link = cast(DimensionLink, link)
                if all(  # pragma: no cover
                    dim in link.foreign_keys_reversed for dim in requested_dimensions
                ):
                    continue

                # Add dimension node query to CTEs if not already present
                if not self.cte_mapping.get(link.dimension.name):
                    dimension_node_query = await build_dimension_node_query(
                        self.session,
                        self._build_criteria,
                        link,
                        self._filters,
                        self.cte_mapping,
                        use_materialized=self.use_materialized,
                        dependencies_cache=self.dependencies_cache,
                    )
                else:
                    dimension_node_query = self.cte_mapping[link.dimension.name]

                dimension_join.node_query = convert_to_cte(
                    dimension_node_query,
                    self.final_ast,
                    link.dimension.name,
                )
                if role := FullColumnName(join_key).role:
                    dimension_join.right_alias = ast.Name(role)

                # Add it to the list of CTEs
                if link.dimension.name not in self.cte_mapping:
                    self.cte_mapping[link.dimension.name] = dimension_join.node_query  # type: ignore
                    self.final_ast.ctes.append(dimension_join.node_query)  # type: ignore

                # Build the join statement
                join_key_col = FullColumnName(join_key)
                role_alias = QueryBuilder.generate_role_alias(
                    link.dimension.name,
                    join_key_col.role,
                    idx,
                )

                # Create a unique identifier for this join to avoid duplicates
                join_identifier = (
                    link.dimension.name,
                    role_alias or link.dimension.name,
                    previous_alias,
                    str(link.join_sql),
                )

                if join_identifier not in added_joins:
                    join_ast = build_join_for_link(
                        link,
                        self.cte_mapping,
                        dimension_node_query,
                        role_alias,
                        previous_alias,
                    )
                    self.final_ast.select.from_.relations[-1].extensions.append(
                        join_ast,
                    )
                    added_joins.add(join_identifier)

                # Track this alias for the next join in the chain
                previous_alias = role_alias

            # Add the requested dimensions to the final SELECT
            if join_path:  # pragma: no cover
                dimensions_columns, errors = build_requested_dimensions_columns(
                    requested_dimensions,
                    join_path[-1],
                    dimension_node_joins,
                )
                self.final_ast.select.projection.extend(dimensions_columns)
                self.errors.extend(errors)

    def create_query_from_physical_table(self, physical_table) -> ast.Query:
        """
        Initial scaffolding for a query from a physical table.
        """
        return ast.Query(
            select=ast.Select(
                projection=physical_table.columns,  # type: ignore
                from_=ast.From(relations=[ast.Relation(physical_table)]),
            ),
        )

    def set_dimension_aliases(self):
        """
        Mark any remaining requested dimensions that don't need a join with
        their canonical dimension names
        """
        for dim_name in self.dimensions:
            column_name = get_column_from_canonical_dimension(
                dim_name,
                self.node_revision,
            )
            node_col = (
                self.final_ast.select.column_mapping.get(column_name)
                if column_name
                else None
            )
            # Realias based on canonical dimension name
            new_alias = amenable_name(dim_name)
            if node_col and new_alias not in self.final_ast.select.column_mapping:
                node_col.set_alias(ast.Name(amenable_name(dim_name)))
                node_col.set_semantic_entity(dim_name)
                node_col.set_semantic_type(SemanticType.DIMENSION)

    def add_request_by_node_name(self, node_name: str):
        """Add a node request to the access control validator."""
        if self._access_checker:  # pragma: no cover
            self._access_checker.add_request_by_node_name(
                node_name,
                access.ResourceAction.READ,
            )

    async def validate_access(self):
        """Validates access"""
        if self._access_checker:
            await self._access_checker.check(on_denied=AccessDenialMode.RAISE)

    async def find_dimension_node_joins(
        self,
    ) -> dict[str, DimensionJoin]:
        """
        Uses a single recursive CTE query to find all dimension node joins to reach
        the requested dimensions and filters from the current node.
        """
        dimension_node_joins: dict[str, DimensionJoin] = {}
        necessary_dimensions = self._collect_referenced_dimensions()

        # Separate local dimensions from those needing joins
        non_local_dimensions: list[FullColumnName] = []
        target_dimension_node_names: set[str] = set()

        for dim in necessary_dimensions:
            attr = FullColumnName(dim)
            if attr.node_name == self.node_revision.name:
                continue  # Local dimension, no join needed

            # Check if it's a local dimension via column.dimension link
            is_local = False
            for col in self.node_revision.columns:
                if f"{self.node_revision.name}.{col.name}" == attr.name:
                    is_local = True  # pragma: no cover
                    break  # pragma: no cover
                if (
                    col.dimension
                    and f"{col.dimension.name}.{col.dimension_column}" == attr.name
                ):
                    is_local = True
                    break

            if not is_local:
                non_local_dimensions.append(attr)
                target_dimension_node_names.add(attr.node_name)

        # Batch find all join paths using recursive CTE
        # Returns dict keyed by (dim_name, role_path) so we can match roles too
        # Skip if already preloaded earlier
        if self._preloaded_join_paths:
            preloaded_paths = self._preloaded_join_paths
        elif target_dimension_node_names:
            preloaded_paths = await self.preload_join_paths_for_dimensions(
                target_dimension_node_names,
            )
        else:
            preloaded_paths = {}

        # Build DimensionJoin objects using preloaded paths
        for attr in non_local_dimensions:
            self.add_request_by_node_name(attr.node_name)

            if attr.join_key not in dimension_node_joins:
                # Find matching path - try exact role match first, then no-role match
                join_path = None

                # The role in attr.role is like "birth_date->parent"
                # The role_path from CTE is like "birth_date->parent" (same format!)
                role_key = attr.role or ""
                exact_key = (attr.node_name, role_key)

                if exact_key in preloaded_paths:
                    join_path = preloaded_paths[exact_key]
                elif not attr.role:  # pragma: no branch
                    # No role specified - find any path to this dimension (prefer shortest)
                    for (dim_name, role_path), path in preloaded_paths.items():
                        if dim_name == attr.node_name:
                            if join_path is None or len(path) < len(
                                join_path,
                            ):  # pragma: no cover
                                join_path = path

                if join_path is None:
                    # No path found - dimension cannot be joined
                    self.errors.append(
                        DJQueryBuildError(
                            code=ErrorCode.INVALID_DIMENSION_JOIN,
                            message=(
                                f"This dimension attribute cannot be joined in: {attr.name}. "
                                f"Please make sure that {attr.node_name} is "
                                f"linked to {self.node_revision.name}"
                            ),
                            context=str(self),
                        ),
                    )
                    continue

                # Check if this dimension actually needs a join
                if join_path and not await needs_dimension_join(
                    self.session,
                    attr.name,
                    join_path,
                ):
                    continue

                dimension_join = DimensionJoin(
                    join_path=join_path,
                    requested_dimensions=[attr.name],
                )
                dimension_node_joins[attr.join_key] = dimension_join
            else:
                dimension_node_joins[attr.join_key].add_requested_dimension(attr.name)

        return dimension_node_joins

    @classmethod
    def normalize_query_param_value(cls, param: str, value: ast.Value | Any):
        match value:
            case ast.Value():
                return value
            case bool():
                return ast.Boolean(value)
            case int() | float():
                return ast.Number(value)
            case None:
                return ast.Null()
            case str():
                return ast.String(f"'{value}'")
            case _:
                raise TypeError(
                    f"Unsupported parameter type: {type(value)} for param {param}",
                )

    def _collect_referenced_dimensions(self) -> list[str]:
        """
        Collects all dimensions referenced in filters and requested dimensions.
        """
        necessary_dimensions = self.dimensions.copy()
        for filter_ast in self.filter_asts:
            for filter_dim in filter_ast.find_all(ast.Column):
                filter_dim_id = (
                    str(filter_dim.parent)  # Handles dimensions with roles
                    if isinstance(filter_dim.parent, ast.Subscript)
                    else filter_dim.identifier()
                )
                if filter_dim_id not in necessary_dimensions:
                    necessary_dimensions.append(filter_dim_id)
        return necessary_dimensions


class CubeQueryBuilder:
    """
    This class allows users to configure building cube SQL (retrieving SQL for multiple
    metrics + dimensions) through settings like adding filters, dimensions, ordering, and limit
    clauses. The builder then handles the management of CTEs, dimension joins, and error
    validation, allowing for dynamic node query generation based on runtime conditions.
    """

    def __init__(
        self,
        session: AsyncSession,
        metric_nodes: list[Node],
        use_materialized: bool = True,
    ):
        self.session = session
        self.metric_nodes = metric_nodes
        self.use_materialized = use_materialized

        self._filters: list[str] = []
        self._required_dimensions: list[str] = [
            required.name
            for metric_node in self.metric_nodes
            for required in metric_node.current.required_dimensions
        ]
        self._dimensions: list[str] = []
        self._orderby: list[str] = []
        self._limit: Optional[int] = None
        self._parameters: dict[str, ast.Value] = {}
        self._build_criteria: BuildCriteria | None = self.get_default_criteria()
        self._access_checker: AccessChecker | None = None
        self._ignore_errors: bool = False

        # The following attributes will be modified as the query gets built.
        # --
        # Track node query CTEs as they get built
        self.cte_mapping: dict[str, ast.Query] = {}  # Maps node name to its CTE
        # Keep a list of build errors
        self.errors: list[DJQueryBuildError] = []
        # The final built query AST
        self.final_ast: Optional[ast.Query] = None
        # Cache for DJ node dependencies to avoid repeated lookups
        self.dependencies_cache: dict[str, Node] = {}

    def get_default_criteria(
        self,
        engine: Optional[Engine] = None,
    ) -> BuildCriteria:
        """
        Get the default build criteria for a node.
        Set the dialect by using the provided engine, if any. If no engine is specified,
        set the dialect by finding available engines for this node, or default to Spark
        """
        return BuildCriteria(
            dialect=engine.dialect if engine and engine.dialect else Dialect.SPARK,
        )

    @classmethod
    async def create(
        cls,
        session: AsyncSession,
        metric_nodes: list[Node],
        use_materialized: bool = True,
    ) -> "CubeQueryBuilder":
        """
        Create a QueryBuilder instance for the node revision.
        """
        for node in metric_nodes:
            await refresh_if_needed(session, node, ["current"])
            await refresh_if_needed(session, node.current, ["required_dimensions"])

        instance = cls(session, metric_nodes, use_materialized=use_materialized)
        return instance

    def ignore_errors(self):
        """Do not raise on errors in query build."""
        self._ignore_errors = True
        return self

    def raise_errors(self):
        """Raise on errors in query build."""
        self._ignore_errors = False  # pragma: no cover
        return self  # pragma: no cover

    def filter_by(self, filter_: str):
        """Add filter to the query builder."""
        if filter_ not in self._filters:  # pragma: no cover
            self._filters.append(filter_)
        return self

    def add_filters(self, filters: Optional[list[str]] = None):
        """Add filters to the query builder."""
        for filter_ in filters or []:
            self.filter_by(filter_)
        return self

    def add_dimension(self, dimension: str):
        """Add dimension to the query builder."""
        if (  # pragma: no cover
            dimension not in self._dimensions
            and dimension not in self._required_dimensions
        ):
            self._dimensions.append(dimension)
        return self

    def add_dimensions(self, dimensions: Optional[list[str]] = None):
        """Add dimensions to the query builder."""
        for dimension in dimensions or []:
            self.add_dimension(dimension)
        return self

    def add_query_parameters(self, query_parameters: dict[str, Any] | None = None):
        """Add parameters to the query builder."""
        for param, value in (query_parameters or {}).items():
            self._parameters[param] = QueryBuilder.normalize_query_param_value(
                param,
                value,
            )
        return self

    def order_by(self, orderby: Optional[Union[str, list[str]]] = None):
        """Set order by for the query builder."""
        if isinstance(orderby, str):
            if orderby not in self._orderby:  # pragma: no cover
                self._orderby.append(orderby)  # pragma: no cover
        else:
            for order in orderby or []:
                if order not in self._orderby:  # pragma: no cover
                    self._orderby.append(order)
        return self

    def limit(self, limit: Optional[int] = None):
        """Set limit for the query builder."""
        if limit:  # pragma: no cover
            self._limit = limit
        return self

    def with_build_criteria(self, build_criteria: Optional[BuildCriteria] = None):
        """Set build criteria for the query builder."""
        if build_criteria:  # pragma: no cover
            self._build_criteria = build_criteria
        return self

    def with_access_control(
        self,
        access_checker: AccessChecker,
    ):
        """
        Set access control for the query builder.
        """
        access_checker.add_nodes(self.metric_nodes, access.ResourceAction.READ)
        self._access_checker = access_checker
        return self

    @property
    def dimensions(self) -> list[str]:
        """All dimensions"""
        return self._dimensions  # TO DO: add self._required_dimensions

    @property
    def filters(self) -> list[str]:
        """All filters"""
        return self._filters

    @property
    def parameters(self) -> dict[str, ast.Value]:
        """
        Extracts parameters from relevant filters
        """
        return self._parameters

    async def build(self) -> ast.Query:
        """
        Builds SQL for multiple metrics with the requested set of dimensions,
        filter expressions, order by, and limit clauses.
        """
        self.add_dimensions(get_dimensions_referenced_in_metrics(self.metric_nodes))

        measures_queries = await self.build_measures_queries()

        # Join together the transforms on the shared dimensions and select all
        # requested metrics and dimensions in the final select projection
        parent_ctes, metric_ctes = self.extract_ctes(measures_queries)
        initial_cte = metric_ctes[0]
        self.final_ast = ast.Query(
            ctes=parent_ctes + metric_ctes,
            select=ast.Select(
                projection=[
                    ast.Function(
                        ast.Name("COALESCE"),
                        args=[
                            ast.Column(
                                name=ast.Name(proj.alias, namespace=join_cte.alias),  # type: ignore
                                _type=proj.type,  # type: ignore
                                semantic_entity=proj.semantic_entity,  # type: ignore
                                semantic_type=proj.semantic_type,  # type: ignore
                            )
                            for join_cte in metric_ctes
                        ],
                    ).set_alias(
                        proj.alias,  # type: ignore
                    )
                    for proj in initial_cte.select.projection
                ],
                from_=ast.From(
                    relations=[ast.Relation(primary=ast.Table(initial_cte.alias))],  # type: ignore
                ),
            ),
        )
        # Add metrics
        for metric_cte in metric_ctes[1:]:
            self.final_ast.select.projection.extend(
                [
                    ast.Column(
                        name=ast.Name(proj.alias, namespace=metric_cte.alias),  # type: ignore
                        _type=proj.type,  # type: ignore
                        semantic_entity=proj.semantic_entity,  # type: ignore
                        semantic_type=proj.semantic_type,  # type: ignore
                    )
                    for proj in metric_cte.select.projection
                    if from_amenable_name(proj.alias_or_name.identifier())  # type: ignore
                    not in self.dimensions
                ],
            )
            join_on = [
                ast.BinaryOp(
                    op=ast.BinaryOpKind.Eq,
                    left=ast.Column(
                        name=ast.Name(proj.alias, namespace=initial_cte.alias),  # type: ignore
                        _type=proj.type,  # type: ignore
                    ),
                    right=ast.Column(
                        name=ast.Name(proj.alias, namespace=metric_cte.alias),  # type: ignore
                        _type=proj.type,  # type: ignore
                    ),
                )
                for proj in metric_cte.select.projection  # type: ignore
                if from_amenable_name(proj.alias_or_name.identifier())  # type: ignore
                in self.dimensions
            ]
            self.final_ast.select.from_.relations[0].extensions.append(  # type: ignore
                ast.Join(
                    join_type="full",
                    right=ast.Table(metric_cte.alias),  # type: ignore
                    criteria=ast.JoinCriteria(
                        on=ast.BinaryOp.And(*join_on),
                    ),
                ),
            )

        if self._orderby:
            self.final_ast.select.organization = self.build_orderby()

        if self._limit:
            self.final_ast.select.limit = ast.Number(value=self._limit)

        # Error validation
        await self.validate_access()
        if self.errors and not self._ignore_errors:
            raise DJQueryBuildException(errors=self.errors)  # pragma: no cover
        return self.final_ast

    async def validate_access(self):
        """Validates access"""
        if self._access_checker:  # pragma: no cover
            await self._access_checker.check(on_denied=AccessDenialMode.RAISE)

    async def build_measures_queries(self):
        """
        Build the metrics' queries grouped by parent
        """
        from datajunction_server.construction.build import (
            group_metrics_by_parent,
        )

        common_parents = await group_metrics_by_parent(self.session, self.metric_nodes)
        measures_queries = {}
        for parent_node, metrics in common_parents.items():  # type: ignore
            await refresh_if_needed(self.session, parent_node, ["current"])
            query_builder = await QueryBuilder.create(self.session, parent_node.current)
            if self._ignore_errors:
                query_builder = query_builder.ignore_errors()
            parent_ast = await (
                query_builder.with_access_control(self._access_checker)
                .with_build_criteria(self._build_criteria)
                .add_dimensions(self.dimensions)
                .add_filters(self.filters)
                .add_query_parameters(self.parameters)
                .build()
            )
            self.errors.extend(query_builder.errors)

            dimension_columns = [
                expr
                for expr in parent_ast.select.projection
                if from_amenable_name(expr.alias_or_name.identifier(False))  # type: ignore
                in self.dimensions
                # or expr.semantic_entity in self.dimensions
            ]
            parent_ast.select.projection = dimension_columns
            for col in dimension_columns:
                group_by_col = col.copy()
                group_by_col.alias = None
                parent_ast.select.group_by.append(group_by_col)

            await refresh_if_needed(self.session, parent_node.current, ["columns"])

            # Generate semantic types for each
            for expr in parent_ast.select.projection:
                column_identifier = expr.alias_or_name.identifier(False)  # type: ignore
                semantic_entity = from_amenable_name(column_identifier)
                if semantic_entity in self.dimensions:  # pragma: no cover
                    expr.set_semantic_entity(semantic_entity)  # type: ignore
                    expr.set_semantic_type(SemanticType.DIMENSION)  # type: ignore

            # Add metric aggregations to select
            for metric_node in metrics:
                metric_proj = await self.build_metric_agg(
                    metric_node,
                    parent_node,
                    parent_ast,
                )
                parent_ast.select.projection.extend(metric_proj)

            ctx = CompileContext(
                self.session,
                DJException(),
                dependencies_cache=self.dependencies_cache,
            )
            await parent_ast.compile(ctx)
            measures_queries[parent_node.name] = parent_ast
        return measures_queries

    async def build_metric_agg(
        self,
        metric_node: NodeRevision,
        parent_node: Node,
        parent_ast: ast.Query,
    ):
        """
        Build the metric's aggregate expression.
        """
        if self._access_checker:  # pragma: no cover
            self._access_checker.add_node(metric_node, access.ResourceAction.READ)  # type: ignore
        metric_query_builder = await QueryBuilder.create(self.session, metric_node)
        if self._ignore_errors:
            metric_query_builder = (  # pragma: no cover
                metric_query_builder.ignore_errors()
            )
        if self._access_checker:  # pragma: no cover
            metric_query_builder = metric_query_builder.with_access_control(
                self._access_checker,
            )
        metric_query = await metric_query_builder.with_build_criteria(
            self._build_criteria,
        ).build()
        self.errors.extend(metric_query_builder.errors)
        metric_query.ctes[-1].select.projection[0].set_semantic_entity(  # type: ignore
            f"{metric_node.name}.{amenable_name(metric_node.name)}",
        )
        metric_query.ctes[-1].select.projection[0].set_alias(  # type: ignore
            ast.Name(amenable_name(metric_node.name)),
        )
        metric_query.ctes[-1].select.projection[0].set_semantic_type(  # type: ignore
            SemanticType.METRIC,
        )
        for col in metric_query.ctes[-1].select.find_all(ast.Column):
            if matching := parent_ast.select.semantic_column_mapping.get(
                col.identifier(),
            ):
                # When the column is a joinable dimension reference, find it in the parent AST and
                # point the column to the parent AST column ref
                col.name = ast.Name(name=matching.name.name)
                col._table = matching.table
                col.add_type(matching.type)
            else:
                column_identifier = SEPARATOR.join(name.name for name in col.namespace)
                node_name = (
                    column_identifier.rsplit(SEPARATOR, 1)[0]
                    if SEPARATOR in column_identifier
                    else parent_node.name
                )
                col._table = ast.Table(
                    name=ast.Name(name=amenable_name(node_name)),
                )
        return metric_query.ctes[-1].select.projection

    def extract_ctes(self, measures_queries) -> Tuple[list[ast.Query], list[ast.Query]]:
        """
        Extracts the parent CTEs and the metric CTEs from the queries
        """
        parent_ctes: list[ast.Query] = []
        metric_ctes: list[ast.Query] = []
        for parent_name, parent_query in measures_queries.items():
            existing_cte_aliases = {
                cte.alias_or_name.identifier() for cte in parent_ctes
            }
            parent_ctes += [
                cte
                for cte in parent_query.ctes
                if cte.alias_or_name.identifier() not in existing_cte_aliases
            ]
            parent_query.ctes = []
            metric_ctes += [
                parent_query.to_cte(ast.Name(amenable_name(parent_name + "_metrics"))),
            ]
        return parent_ctes, metric_ctes

    def build_orderby(self):
        """
        Creates an order by ast from the requested order bys
        """
        temp_orderbys = cached_parse(  # type: ignore
            f"SELECT 1 ORDER BY {','.join(self._orderby)}",
        ).select.organization.order
        valid_sort_items = [
            sortitem
            for sortitem in temp_orderbys
            if amenable_name(sortitem.expr.identifier())  # type: ignore
            in self.final_ast.select.column_mapping
        ]
        if len(valid_sort_items) < len(temp_orderbys):
            self.errors.append(  # pragma: no cover
                DJQueryBuildError(
                    code=ErrorCode.INVALID_ORDER_BY,
                    message=f"{self._orderby} is not a valid ORDER BY request",
                ),
            )
        return ast.Organization(
            order=[
                ast.SortItem(
                    expr=self.final_ast.select.column_mapping.get(  # type: ignore
                        amenable_name(sortitem.expr.identifier()),  # type: ignore
                    )
                    .copy()
                    .set_alias(None),
                    asc=sortitem.asc,
                    nulls=sortitem.nulls,
                )
                for sortitem in valid_sort_items
            ],
        )


def get_column_from_canonical_dimension(
    dimension_name: str,
    node: NodeRevision,
) -> Optional[str]:
    """
    Gets a column based on a dimension request on a node.
    """
    column_name = None
    dimension_attr = FullColumnName(dimension_name)
    # Dimension requested was on node
    if dimension_attr.node_name == node.name:
        column_name = dimension_attr.column_name

    # Dimension requested has reference link on node
    dimension_columns = {
        (col.dimension.name, col.dimension_column): col.name
        for col in node.columns
        if col.dimension
    }
    key = (dimension_attr.node_name, dimension_attr.column_name)
    if key in dimension_columns:
        return dimension_columns[key]

    # Dimension referenced was foreign key of dimension link
    for link in node.dimension_links:
        foreign_key_column = link.foreign_keys_reversed.get(dimension_attr.name)
        if foreign_key_column:
            return FullColumnName(foreign_key_column).column_name
    return column_name


def to_filter_asts(filters: Optional[list[str]] = None):
    """
    Converts a list of filter expresisons to ASTs
    """
    return [
        parse(f"select * where {filter_}").select.where for filter_ in filters or []
    ]


def remove_duplicates(input_list, key_func=lambda x: x):  # pragma: no cover
    """
    Remove duplicates from the list by using the key_func on each element
    to determine the "key" used for identifying duplicates.
    """
    return list(
        collections.OrderedDict((key_func(item), item) for item in input_list).values(),
    )


async def dimension_join_path(
    session: AsyncSession,
    node: NodeRevision,
    dimension: str,
    dependencies_cache: Optional[dict] = None,
) -> Optional[list[DimensionLink]]:
    """
    Find a join path between this node and the dimension attribute.
    * If there is no possible join path, returns None
    * If it is a local dimension on this node, return []
    * If it is in one of the dimension nodes on the dimensions graph, return a
    list of dimension links that represent the join path

    If dependencies_cache is provided, it will be used to look up pre-loaded nodes
    to avoid additional database queries.
    """
    if dependencies_cache is None:
        dependencies_cache = {}

    # Check if it is a local dimension
    for col in node.columns:  # pragma: no cover
        # Decide if we should restrict this to only columns marked as dimensional
        # await session.refresh(col, ["attributes"]) TODO
        # if col.is_dimensional():
        #     ...
        if f"{node.name}.{col.name}" == dimension:
            return []
        if (
            col.dimension
            and f"{col.dimension.name}.{col.dimension_column}" == dimension
        ):
            return []

    dimension_attr = FullColumnName(dimension)
    role_path = (
        [role for role in dimension_attr.role.split("->")]
        if dimension_attr.role
        else []
    )

    # If it's not a local dimension, traverse the node's dimensions graph
    # This queue tracks the dimension link being processed and the path to that link
    await refresh_if_needed(session, node, ["dimension_links"])

    # Start with first layer of linked dims
    layer_with_role = [
        (link, [link], 1)
        for link in node.dimension_links
        if not role_path or (link.role == role_path[0])
    ]
    layer_without_role = [(link, [link], 0) for link in node.dimension_links]

    processing_queue = collections.deque(
        layer_with_role if layer_with_role else layer_without_role,
    )
    visited = set()
    while processing_queue:
        current_link, join_path, role_idx = processing_queue.popleft()
        if current_link.id in visited:
            continue  # pragma: no cover
        visited.add(current_link.id)

        # Try to use cached node, fall back to ORM relationship
        dim_name = current_link.dimension.name
        dim_node = dependencies_cache.get(dim_name)

        if dim_name == dimension_attr.node_name:
            return join_path

        # Get the current revision - use cache if available
        if dim_node and dim_node.current:
            current_revision = dim_node.current  # pragma: no cover
        else:
            await refresh_if_needed(session, current_link.dimension, ["current"])
            current_revision = current_link.dimension.current

        if not current_revision:
            continue  # pragma: no cover

        # Get dimension links from current revision
        if (
            dim_node
            and dim_node.current
            and dim_node.current.dimension_links is not None
        ):
            dim_links = dim_node.current.dimension_links  # pragma: no cover
        else:
            await refresh_if_needed(session, current_revision, ["dimension_links"])
            dim_links = current_revision.dimension_links

        layer_with_role = [
            (link, join_path + [link], role_idx + 1)
            for link in dim_links
            if not role_path or (link.role == role_path[role_idx])
        ]
        if layer_with_role:
            processing_queue.extend(layer_with_role)
        else:
            processing_queue.extend(  # pragma: no cover
                [(link, join_path + [link], role_idx) for link in dim_links],
            )
    return None


async def build_dimension_node_query(
    session: AsyncSession,
    build_criteria: Optional[BuildCriteria],
    link: DimensionLink,
    filters: list[str],
    cte_mapping: dict[str, ast.Query],
    use_materialized: bool = True,
    dependencies_cache: Optional[dict] = None,
):
    """
    Builds a dimension node query with the requested filters
    """
    if dependencies_cache is None:
        dependencies_cache = {}  # pragma: no cover

    dim_node = dependencies_cache.get(link.dimension.name)
    current_revision = dim_node.current  # type: ignore
    physical_table = get_table_for_node(
        current_revision,
        build_criteria=build_criteria,
    )
    dimension_node_ast = (
        await compile_node_ast(
            session,
            current_revision,
            dependencies_cache=dependencies_cache,
        )
        if not physical_table
        else ast.Query(
            select=ast.Select(
                projection=physical_table.columns,  # type: ignore
                from_=ast.From(relations=[ast.Relation(physical_table)]),
            ),
        )
    )
    dimension_node_query = await build_ast(
        session,
        current_revision,
        dimension_node_ast,
        filters=filters,  # type: ignore
        build_criteria=build_criteria,
        ctes_mapping=cte_mapping,
        use_materialized=use_materialized,
        dependencies_cache=dependencies_cache,
    )
    return dimension_node_query


def convert_to_cte(
    inner_query: ast.Query,
    outer_query: ast.Query,
    cte_name: str,
):
    """
    Convert the query to a CTE that can be used by the outer query
    """
    # Move all the CTEs used by the inner query to the outer query,
    # deduplicating by CTE name to avoid duplicate CTEs
    existing_cte_names = {cte.alias_or_name.identifier() for cte in outer_query.ctes}
    for cte in inner_query.ctes:
        cte_identifier = cte.alias_or_name.identifier()
        if cte_identifier not in existing_cte_names:  # pragma: no branch
            cte.set_parent(outer_query, parent_key="ctes")
            outer_query.ctes.append(cte)
            existing_cte_names.add(cte_identifier)
    inner_query.ctes = []

    # Convert the dimension node query to a CTE
    inner_query = inner_query.to_cte(
        ast.Name(amenable_name(cte_name)),
        outer_query,
    )
    return inner_query


def build_requested_dimensions_columns(
    requested_dimensions: list[str],
    link: DimensionLink,
    dimension_node_joins: dict[str, DimensionJoin],
) -> Tuple[list[Union[ast.Column, ast.Alias, ast.Function]], list[DJQueryBuildError]]:
    """
    Builds the requested dimension columns for the final select layer.
    """
    dimensions_columns = []
    errors = []
    for dim in requested_dimensions:
        replacement = build_dimension_attribute(
            dim,
            dimension_node_joins,
            link,
            alias=amenable_name(dim),
        )
        if replacement:  # pragma: no cover
            dimensions_columns.append(replacement)
        else:
            errors.append(  # pragma: no cover
                DJQueryBuildError(
                    code=ErrorCode.INVALID_DIMENSION,
                    message=f"Dimension attribute {dim} does not exist!",
                ),
            )
    return dimensions_columns, errors


async def compile_node_ast(
    session,
    node_revision: NodeRevision,
    dependencies_cache: Optional[dict] = None,
) -> ast.Query:
    """
    Parses the node's query into an AST and compiles it.

    Args:
        session: Database session
        node_revision: The node revision to compile
        dependencies_cache: Optional shared cache for DJ node lookups.
                           If provided, node lookups will be cached and reused
                           across multiple compile_node_ast calls.
    """
    node_ast = parse(node_revision.query)
    ctx = CompileContext(
        session,
        DJException(),
        dependencies_cache=dependencies_cache or {},
    )
    await node_ast.compile(ctx)

    return node_ast


def build_dimension_attribute(
    full_column_name: str,
    dimension_node_joins: dict[str, DimensionJoin],
    link: DimensionLink,
    alias: Optional[str] = None,
) -> Optional[Union[ast.Column, ast.Alias, ast.Function]]:
    """
    Turn the canonical dimension attribute into a column on the query AST.

    If the dimension link has a default_value configured, wraps the column
    in COALESCE(column, default_value) to handle NULL results from LEFT JOINs.
    """
    dimension_attr = FullColumnName(full_column_name)
    node_query = (
        dimension_node_joins[dimension_attr.join_key].node_query
        if dimension_attr.join_key in dimension_node_joins
        else None
    )

    if node_query:
        foreign_key_column_name = (
            FullColumnName(
                link.foreign_keys_reversed.get(dimension_attr.name),
            ).column_name
            if dimension_attr.name in link.foreign_keys_reversed
            else None
        )
        for col in node_query.select.projection:
            if col.alias_or_name.name == dimension_attr.column_name or (  # type: ignore
                foreign_key_column_name
                and col.alias_or_name.identifier() == foreign_key_column_name  # type: ignore
            ):
                column = ast.Column(
                    name=ast.Name(col.alias_or_name.name),  # type: ignore
                    _table=(
                        node_query
                        if not dimension_attr.role
                        else ast.Table(
                            name=node_query.name,
                            alias=ast.Name(dimension_attr.role.replace("->", "__")),
                        )
                    ),
                    _type=col.type,  # type: ignore
                    semantic_entity=full_column_name,
                )

                # Apply COALESCE with default_value if configured
                if link.default_value is not None:
                    coalesce_expr = ast.Function(
                        ast.Name("COALESCE"),
                        args=[column, ast.String(f"'{link.default_value}'")],
                    )
                    if alias:
                        aliased = coalesce_expr.set_alias(ast.Name(alias))
                        aliased.set_as(True)
                        return aliased
                    return coalesce_expr  # pragma: no cover

                column.alias = ast.Name(alias) if alias else None
                return column
    return None  # pragma: no cover


async def needs_dimension_join(
    session: AsyncSession,
    dimension_attribute: str,
    join_path: list["DimensionLink"],
) -> bool:
    """
    Checks if the requested dimension attribute needs a dimension join or
    if it can be pulled from an existing column on the node.
    """
    if len(join_path) == 1:
        link = join_path[0]
        await refresh_if_needed(session, link.dimension, ["current"])
        await refresh_if_needed(session, link.dimension.current, ["columns"])
        if dimension_attribute in link.foreign_keys_reversed:
            return False
    return True


def combine_filter_conditions(
    existing_condition,
    *new_conditions,
) -> Optional[Union[ast.BinaryOp, ast.Expression]]:
    """
    Combines the existing where clause with new filter conditions.
    """
    if not existing_condition and not new_conditions:
        return None
    if not existing_condition:
        return ast.BinaryOp.And(*new_conditions)
    return ast.BinaryOp.And(existing_condition, *new_conditions)


def build_join_for_link(
    link: "DimensionLink",
    cte_mapping: dict[str, ast.Query],
    join_right: ast.Query,
    role_alias: str | None = None,
    previous_alias: str | None = None,
):
    """
    Build a join for the dimension link using the provided query table expression
    on the left and the provided query table expression on the right.
    """
    join_ast = link.joins()[0]
    join_ast.right = join_right.alias  # type: ignore
    if link.role and role_alias:
        join_ast.right = ast.Alias(  # type: ignore
            child=join_right.alias,
            alias=ast.Name(role_alias),
            as_=True,
        )

    dimension_node_columns = join_right.select.column_mapping
    join_left = cte_mapping.get(link.node_revision.name)
    node_columns = join_left.select.column_mapping  # type: ignore
    if not join_ast.criteria:
        return join_ast
    for col in join_ast.criteria.find_all(ast.Column):  # type: ignore
        full_column = FullColumnName(col.identifier())
        is_dimension_node = full_column.node_name == link.dimension.name
        if full_column.column_name not in (
            dimension_node_columns if is_dimension_node else node_columns
        ):
            raise DJQueryBuildException(  # pragma: no cover
                f"The requested column {full_column.column_name} does not exist"
                f" on {full_column.node_name}",
            )

        # Determine the correct table reference for each side
        if is_dimension_node:
            # Right side - use join_right, but create proper alias if there's a role
            if link.role and role_alias:
                table_ref = ast.Alias(  # type: ignore
                    child=join_right.alias,
                    alias=ast.Name(role_alias),
                    as_=True,
                )
            else:
                table_ref = join_right  # type: ignore
        else:
            # Left side - use previous alias if available for multi-hop joins
            table_ref = join_left  # type: ignore
            if previous_alias:
                table_ref = ast.Alias(  # type: ignore
                    child=join_left.alias,  # type: ignore
                    alias=ast.Name(previous_alias),
                    as_=True,
                )

        replacement = ast.Column(
            name=ast.Name(full_column.column_name),
            _table=table_ref,
            _type=(dimension_node_columns if is_dimension_node else node_columns)
            # type: ignore
            .get(full_column.column_name)
            .type,
        )
        col.parent.replace(col, replacement)  # type: ignore
    return join_ast


async def build_ast(
    session: AsyncSession,
    node: NodeRevision,
    query: ast.Query,
    filters: Optional[list[str]],
    build_criteria: Optional[BuildCriteria] = None,
    access_control=None,
    ctes_mapping: dict[str, ast.Query] = None,
    use_materialized: bool = True,
    dependencies_cache: Optional[dict] = None,
) -> ast.Query:
    """
    Recursively replaces DJ node references with query ASTs. These are replaced with
    materialized tables where possible (i.e., source nodes will always be replaced with a
    materialized table), but otherwise we generate the SQL of each upstream node reference.

    This function will apply any filters that can be pushed down to each referenced node's AST
    (filters are only applied if they don't require dimension node joins).
    """
    context = CompileContext(
        session=session,
        exception=DJException(),
        dependencies_cache=dependencies_cache or {},
    )

    await query.compile(context)
    query.bake_ctes()
    await refresh_if_needed(session, node, ["dimension_links"])

    new_cte_mapping: dict[str, ast.Query] = {}
    if ctes_mapping is None:
        ctes_mapping = new_cte_mapping  # pragma: no cover

    node_to_tables_mapping = get_dj_node_references_from_select(query.select)
    for referenced_node, reference_expressions in node_to_tables_mapping.items():
        await refresh_if_needed(session, referenced_node, ["dimension_links"])

        for ref_expr in reference_expressions:
            # Try to find a materialized table attached to this node, if one exists.
            physical_table = None
            if use_materialized:
                logger.debug("Checking for physical node: %s", referenced_node.name)
                physical_table = cast(
                    Optional[ast.Table],
                    get_table_for_node(
                        referenced_node,
                        build_criteria=build_criteria,
                    ),
                )

            if not physical_table:
                logger.debug("Didn't find physical node: %s", referenced_node.name)
                # Build a new CTE with the query AST if there is no materialized table
                if referenced_node.name not in ctes_mapping:
                    node_query = parse(cast(str, referenced_node.query))
                    query_ast = await build_ast(  # type: ignore
                        session,
                        referenced_node,
                        node_query,
                        filters=filters,
                        build_criteria=build_criteria,
                        access_control=access_control,
                        ctes_mapping=ctes_mapping,
                        use_materialized=use_materialized,
                        dependencies_cache=dependencies_cache,
                    )
                    cte_name = ast.Name(amenable_name(referenced_node.name))
                    query_ast = query_ast.to_cte(cte_name, parent_ast=query)
                    if referenced_node.name not in new_cte_mapping:  # pragma: no cover
                        new_cte_mapping[referenced_node.name] = query_ast

                reference_cte = (
                    ctes_mapping[referenced_node.name]
                    if referenced_node.name in ctes_mapping
                    else new_cte_mapping[referenced_node.name]
                )
                query_ast = ast.Table(  # type: ignore
                    reference_cte.alias,  # type: ignore
                    _columns=reference_cte._columns,
                    _dj_node=referenced_node,
                )
            else:
                # Otherwise use the materialized table and apply filters where possible
                alias = amenable_name(referenced_node.name)
                query_ast = ast.Query(
                    select=ast.Select(
                        projection=physical_table.columns,  # type: ignore
                        from_=ast.From(relations=[ast.Relation(physical_table)]),
                    ),
                    alias=ast.Name(alias),
                )
                query_ast.parenthesized = True
                apply_filters_to_node(
                    referenced_node,
                    query_ast,
                    to_filter_asts(filters),
                )
                if not query_ast.select.where:
                    query_ast = ast.Alias(  # type: ignore
                        ast.Name(alias),
                        child=physical_table,
                        as_=True,
                    )

            # If the user has set an alias for the node reference, reuse the
            # same alias for the built query
            if ref_expr.alias and hasattr(query_ast, "alias"):
                query_ast.alias = ref_expr.alias
            query.select.replace(
                ref_expr,
                query_ast,
                copy=False,
            )
            await query.select.compile(context)
            for col in query.select.find_all(ast.Column):
                if (
                    col.table
                    and not col.table.alias
                    and isinstance(col.table, ast.Table)
                    and col.table.dj_node
                    and col.table.dj_node.name == referenced_node.name
                ):
                    # Only update columns that are in this query's scope
                    # (not in nested subqueries where query_ast wouldn't be accessible)
                    # A column is in this query's scope if its nearest parent Query is
                    # the current query, not some nested subquery
                    col_parent_query = col.get_nearest_parent_of_type(ast.Query)
                    if col_parent_query is query:  # pragma: no branch
                        col._table = query_ast

    # Apply pushdown filters if possible
    apply_filters_to_node(node, query, to_filter_asts(filters))

    # Add new CTEs while deduplicating by CTE name
    existing_cte_names = {cte.alias_or_name.identifier() for cte in query.ctes}
    for cte in new_cte_mapping.values():
        # Add nested CTEs from this CTE, deduplicating
        for nested_cte in cte.ctes:  # pragma: no branch
            nested_cte_id = nested_cte.alias_or_name.identifier()
            if nested_cte_id not in existing_cte_names:  # pragma: no branch
                query.ctes.append(nested_cte)
                existing_cte_names.add(nested_cte_id)
        cte.ctes = []
        # Add the CTE itself if not already present
        cte_id = cte.alias_or_name.identifier()
        if cte_id not in existing_cte_names:  # pragma: no branch
            query.ctes.append(cte)
            existing_cte_names.add(cte_id)
    query.select.add_aliases_to_unnamed_columns()
    ctes_mapping.update(new_cte_mapping)
    return query


def apply_filters_to_node(
    node: NodeRevision,
    query: ast.Query,
    filters: list[ast.Expression],
):
    """
    Apply pushdown filters if possible to the node's query AST.

    A pushdown filter is defined as a filter with references to dimensions that
    already exist on the node query without any additional dimension joins. We can
    apply these filters directly to the query AST by renaming the dimension ref in
    the filter expression.
    """
    for filter_ast in filters:
        all_referenced_dimensions_can_pushdown = True
        if not filter_ast:
            continue
        for filter_dim in filter_ast.find_all(ast.Column):
            filter_dim_id = (
                str(filter_dim.parent)  # Handles dimensions with roles
                if isinstance(filter_dim.parent, ast.Subscript)
                else filter_dim.identifier()
            )
            column_name = get_column_from_canonical_dimension(
                filter_dim_id,
                node,
            )
            node_col = (
                query.select.column_mapping.get(column_name) if column_name else None
            )
            if node_col:
                replacement = (
                    node_col.child if isinstance(node_col, ast.Alias) else node_col  # type: ignore
                ).copy()
                replacement.set_alias(None)
                filter_ast.replace(filter_dim, replacement)
            else:
                all_referenced_dimensions_can_pushdown = False

        if all_referenced_dimensions_can_pushdown:
            query.select.where = combine_filter_conditions(
                query.select.where,
                filter_ast,
            )
    return query


def get_dj_node_references_from_select(
    select: ast.SelectExpression,
) -> DefaultDict[NodeRevision, list[ast.Table]]:
    """
    Extract all DJ node references (source, transform, dimensions) from the select
    expression. DJ node references are represented in the AST as table expressions
    and have an attached DJ node.
    """
    tables: DefaultDict[NodeRevision, list[ast.Table]] = collections.defaultdict(list)

    for table in select.find_all(ast.Table):
        if node := table.dj_node:  # pragma: no cover
            tables[node].append(table)
    return tables


def get_table_for_node(
    node: NodeRevision,
    build_criteria: Optional[BuildCriteria] = None,
) -> Optional[ast.Table]:
    """
    If a node has a materialized table available, return the materialized table.
    Source nodes should always have an associated table, whereas for all other nodes
    we can check the materialization type.
    """
    table = None
    can_use_materialization = (
        build_criteria and node.name != build_criteria.target_node_name
    )
    if node.type == NodeType.SOURCE:
        table_name = (
            f"{node.catalog.name}.{node.schema_}.{node.table}"
            if node.schema_ == "iceberg"
            else f"{node.schema_}.{node.table}"
        )
        name = to_namespaced_name(table_name if node.table else node.name)
        table = ast.Table(
            name,
            _columns=[
                ast.Column(name=ast.Name(col.name), _type=col.type)
                for col in node.columns
            ],
            _dj_node=node,
        )
    elif (
        can_use_materialization
        and node.availability
        and node.availability.is_available(
            criteria=build_criteria,
        )
    ):  # pragma: no cover
        table = ast.Table(
            ast.Name(
                node.availability.table,
                namespace=(
                    ast.Name(node.availability.schema_)
                    if node.availability.schema_
                    else None
                ),
            ),
            _columns=[
                ast.Column(name=ast.Name(col.name), _type=col.type)
                for col in node.columns
            ],
            _dj_node=node,
        )
    return table
