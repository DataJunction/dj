"""Column identities and the projections emitted by a measures query."""

from collections.abc import Iterator
from dataclasses import dataclass, field

from datajunction_server.construction.build_v3.filters import (
    dimension_ref_of_expression,
)
from datajunction_server.construction.build_v3.types import ColumnMetadata
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.sql.parsing import ast
from datajunction_server.utils import SEPARATOR


def input_reference(expression: ast.Expression, parent_name: str) -> str:
    """Identify an input by node, column, and role, independently of SQL aliases."""
    reference = dimension_ref_of_expression(expression)
    if reference is None:
        raise DJInvalidInputException(f"Expected a column reference, got {expression}")
    if SEPARATOR not in reference:
        return f"{parent_name}{SEPARATOR}{reference}"
    return reference


def column_inputs(expression: ast.Node) -> Iterator[ast.Expression]:
    """Walk real inputs, keeping dimension roles and excluding role markers."""
    if isinstance(expression, ast.Column):
        yield expression
        return
    if (
        isinstance(expression, ast.Subscript)
        and isinstance(expression.expr, ast.Column)
        and expression.expr.name.namespace
        and dimension_ref_of_expression(expression) is not None
    ):
        yield expression
        return
    for child in expression.children:
        yield from column_inputs(child)


@dataclass
class ProjectedColumn:
    """An emitted SQL expression and the metadata describing that output."""

    expression: ast.Aliasable | ast.Expression | ast.Column
    metadata: ColumnMetadata
    source_refs: tuple[str, ...] = ()


@dataclass
class MeasuresProjection:
    """Keep emitted SQL, metadata, and reusable input bindings together."""

    columns: list[ProjectedColumn] = field(default_factory=list)
    input_aliases: dict[str, str] = field(default_factory=dict)

    def add(self, column: ProjectedColumn) -> str:
        if any(c.metadata.name == column.metadata.name for c in self.columns):
            raise DJInvalidInputException(
                f"Conflicting output column alias: {column.metadata.name}",
            )
        self.columns.append(column)
        # Explicit output dimensions can expose the same resolved input under
        # different names. Raw consumers reuse its first emitted binding.
        for reference in column.source_refs:
            self.input_aliases.setdefault(reference, column.metadata.name)
        return column.metadata.name

    @property
    def expressions(self) -> list[ast.Aliasable | ast.Expression | ast.Column]:
        return [column.expression for column in self.columns]

    @property
    def metadata(self) -> list[ColumnMetadata]:
        return [column.metadata for column in self.columns]


@dataclass
class MeasuresSelect:
    """A measures SELECT and the bindings established while projecting it."""

    query: ast.Query
    scanned_sources: list[str]
    dimension_positions: dict[str, int]
    projection: MeasuresProjection
    grain_aliases: list[str]
    component_aliases: dict[str, str]
