"""
Validation logic for node specifications during deployment
"""

import logging
from dataclasses import dataclass
import time
from typing import Dict, List, Optional
import asyncio
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.internal.validation import validate_metric_query
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.models.node import NodeStatus, NodeType
from datajunction_server.models.deployment import (
    LinkableNodeSpec,
    NodeSpec,
    ColumnSpec,
    SourceSpec,
)
from datajunction_server.errors import (
    DJError,
    ErrorCode,
    DJException,
)
from datajunction_server.sql.parsing.backends.antlr4 import parse, ast

logger = logging.getLogger(__name__)


@dataclass
class ValidationContext:
    """Shared context for validation operations"""

    session: AsyncSession
    node_graph: Dict[str, List[str]]
    dependency_nodes: Dict[str, Node]
    compile_context: ast.CompileContext


@dataclass
class CubeValidationData:
    """Stores validation results for a cube to avoid re-validation"""

    metric_columns: list
    metric_nodes: list
    dimension_nodes: list
    dimension_columns: list
    catalog: Optional[object]


@dataclass
class NodeValidationResult:
    """Immutable validation result for a single node"""

    spec: NodeSpec  # Original unchanged spec
    status: NodeStatus
    inferred_columns: list[ColumnSpec]
    errors: list[DJError]
    dependencies: list[str]

    # Internal use only
    _cube_validation_data: Optional[CubeValidationData] = None


class NodeSpecBulkValidator:
    """Handles validation of node specifications"""

    def __init__(self, context: ValidationContext):
        self.context = context

    async def validate(self, node_specs: list[NodeSpec]) -> List[NodeValidationResult]:
        """
        Validate a list of node specifications
        """
        parsed_results = await self.parse_queries(node_specs)
        validation_tasks = [
            self.process_validation(spec, parsed_result)
            for spec, parsed_result in zip(node_specs, parsed_results)
        ]
        return await asyncio.gather(*validation_tasks)

    async def validate_source_node(self, spec: SourceSpec) -> NodeValidationResult:
        """Handle source node validation - no query parsing needed"""
        return NodeValidationResult(
            spec=spec,
            status=NodeStatus.VALID,
            inferred_columns=spec.columns or [],
            errors=[],
            dependencies=[],
        )

    async def validate_query_node(
        self,
        spec: NodeSpec,
        parsed_ast: ast.Query,
    ) -> NodeValidationResult:
        """
        Validate nodes with queries (transform, dimension, metric)
        """
        try:
            await parsed_ast.bake_ctes().extract_dependencies(
                self.context.compile_context,
            )
            parsed_ast.select.add_aliases_to_unnamed_columns()

            inferred_columns = self._infer_columns(spec, parsed_ast)
            errors = [
                err
                for err in [
                    self._check_inferred_columns(inferred_columns),
                    self._check_primary_key(inferred_columns, spec),
                    self._check_metric_query(spec, parsed_ast),
                ]
                if err is not None
            ]
            return NodeValidationResult(
                spec=spec,
                status=NodeStatus.VALID if not errors else NodeStatus.INVALID,
                inferred_columns=inferred_columns,
                errors=errors,
                dependencies=self.context.node_graph.get(spec.rendered_name, []),
            )
        except Exception as exc:
            return self._create_error_result(spec, exc)

    def _check_inferred_columns(self, columns: List[ColumnSpec]) -> DJError | None:
        """Check that inferred columns are not empty"""
        if not columns:
            return DJError(  # pragma: no cover
                code=ErrorCode.INVALID_SQL_QUERY,
                message="No columns could be inferred from the SQL query.",
            )
        return None

    def _check_primary_key(
        self,
        inferred_columns: List[ColumnSpec],
        spec: LinkableNodeSpec,
    ) -> DJError | None:
        columns_map = {col.name: col for col in inferred_columns}
        if isinstance(spec, LinkableNodeSpec) and not all(
            key_col in columns_map for key_col in spec.primary_key
        ):
            return DJError(
                code=ErrorCode.INVALID_SQL_QUERY,
                message=(
                    f"Some columns in the primary key {spec.primary_key} "
                    "were not found in the list of available columns for the "
                    f"node {spec.rendered_name}."
                ),
            )
        return None

    def _check_metric_query(
        self,
        spec: NodeSpec,
        parsed_ast: ast.Query,
    ) -> DJError | None:
        """Check that a metric query has aggregation in projections"""
        try:
            if spec.node_type == NodeType.METRIC:
                validate_metric_query(parsed_ast, spec.rendered_name)
            return None
        except Exception as exc:
            return DJError(
                code=ErrorCode.INVALID_SQL_QUERY,
                message=str(exc),
            )

    def _infer_columns(self, spec: NodeSpec, parsed_ast: ast.Query) -> list[ColumnSpec]:
        """Infer column specifications from parsed AST"""
        columns_spec_map = {
            col.name: col
            for col in (
                spec.columns if hasattr(spec, "columns") and spec.columns else []
            )
        }
        inferred_columns = []

        for col in parsed_ast.select.projection:
            column_name = col.alias_or_name.name  # type: ignore
            col_spec = columns_spec_map.get(column_name)

            inferred_column = self._create_column_spec(
                column_name=column_name,
                ast_column=col,  # type: ignore
                existing_spec=col_spec,
            )
            inferred_columns.append(inferred_column)

        return inferred_columns

    def _create_column_spec(
        self,
        column_name: str,
        ast_column: ast.Column,
        existing_spec: Optional[ColumnSpec],
    ) -> ColumnSpec:
        """Create a ColumnSpec from AST column and existing spec"""
        try:
            column_type = str(ast_column.type)
        except Exception as e:  # pragma: no cover
            logger.error("Error inferring column %s: %s", column_name, e)
            column_type = "unknown"

        if existing_spec:
            return ColumnSpec(
                name=column_name,
                type=column_type,
                display_name=existing_spec.display_name,
                description=existing_spec.description,
                attributes=existing_spec.attributes,
                partition=existing_spec.partition,
            )
        else:
            return ColumnSpec(
                name=column_name,
                type=column_type,
            )

    def _create_error_result(
        self,
        spec: NodeSpec,
        error: Exception,
    ) -> NodeValidationResult:
        """
        Create a validation result for errors
        """
        logger.exception(
            "Error validating node %s: %s",
            spec.rendered_name,
            error,
        )

        return NodeValidationResult(
            spec=spec,
            status=NodeStatus.INVALID,
            inferred_columns=[],
            errors=[DJError(code=ErrorCode.INVALID_SQL_QUERY, message=str(error))],
            dependencies=[],
        )

    @staticmethod
    async def parse_queries(
        node_specs: List[NodeSpec],
    ) -> List[Optional[ast.Query] | Exception]:
        """Parse all node queries in parallel using thread pool"""

        def _parse_single_query(spec: NodeSpec) -> Optional[ast.Query] | Exception:
            """Parse a single node query - runs in thread pool"""
            try:
                if spec.node_type == NodeType.SOURCE:
                    return None  # Source nodes don't have queries to parse

                query = (
                    NodeRevision.format_metric_alias(
                        spec.rendered_query,  # type: ignore
                        spec.rendered_name,
                    )
                    if spec.node_type == NodeType.METRIC
                    else spec.rendered_query
                )
                return parse(query)
            except Exception as exc:  # pragma: no cover
                logger.error(
                    "Error parsing query for node %s: %s",
                    spec.rendered_name,
                    exc,
                )
                return exc

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            parse_tasks = [
                loop.run_in_executor(executor, _parse_single_query, spec)
                for spec in node_specs
            ]
            return await asyncio.gather(*parse_tasks)

    async def process_validation(
        self,
        spec: NodeSpec,
        parsed_result: Optional[ast.Query] | Exception,
    ) -> NodeValidationResult:
        """Process a single node validation"""

        # Handle parsing errors
        if isinstance(parsed_result, Exception):
            return NodeValidationResult(  # pragma: no cover
                spec=spec,
                status=NodeStatus.INVALID,
                inferred_columns=[],
                errors=[
                    DJError(
                        code=ErrorCode.INVALID_SQL_QUERY,
                        message=str(parsed_result),
                    ),
                ],
                dependencies=[],
            )

        # Handle SOURCE nodes (no query)
        if parsed_result is None and spec.node_type == NodeType.SOURCE:
            return await self.validate_source_node(spec)

        # Handle nodes with queries
        if parsed_result is not None:
            return await self.validate_query_node(spec, parsed_result)

        return NodeValidationResult(  # pragma: no cover
            spec=spec,
            status=NodeStatus.VALID,
            inferred_columns=spec.columns or [],
            errors=[],
            dependencies=[],
        )


async def bulk_validate_node_data(
    node_specs: List[NodeSpec],
    node_graph: Dict[str, List[str]],
    session: AsyncSession,
    dependency_nodes: Dict[str, Node],
) -> List[NodeValidationResult]:
    """
    Bulk validate node specifications
    """
    logger.info("Validating %d node queries", len(node_specs))
    validate_start = time.perf_counter()
    context = ValidationContext(
        session=session,
        node_graph=node_graph,
        dependency_nodes=dependency_nodes,
        compile_context=ast.CompileContext(
            session=session,
            exception=DJException(),
            dependencies_cache=dependency_nodes,
        ),
    )
    validator = NodeSpecBulkValidator(context)
    validation_results = await validator.validate(node_specs)
    logger.info(
        "Validated %d node queries in %.2fs",
        len(node_specs),
        time.perf_counter() - validate_start,
    )
    return validation_results
