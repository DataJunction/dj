"""AST primitives shared by ranked group selection and bucketing."""

from copy import deepcopy
from collections.abc import Sequence

from datajunction_server.sql.parsing import ast


def ranked_column(name: str, alias: str) -> ast.Column:
    """Reference a column on one side of a ranked tuple join."""
    return ast.Column(name=ast.Name(name), _table=ast.Table(ast.Name(alias)))


def null_safe_match(left: ast.Expression, right: ast.Expression) -> ast.Expression:
    """Match tuple keys including pairs of NULL values."""
    match = ast.BinaryOp(
        left=ast.BinaryOp(left=left, right=right, op=ast.BinaryOpKind.Eq),
        right=ast.BinaryOp(
            left=ast.IsNull(expr=deepcopy(left)),
            right=ast.IsNull(expr=deepcopy(right)),
            op=ast.BinaryOpKind.And,
        ),
        op=ast.BinaryOpKind.Or,
    )
    match.parenthesized = True
    return match


def ranked_tuple_join(
    ranked_query: ast.Query,
    pairs: Sequence[tuple[ast.Expression, ast.Expression]],
    join_type: str,
) -> ast.Join:
    """Join a ranked query on the complete dimension tuple."""
    return ast.Join(
        join_type=join_type,
        right=ranked_query,
        criteria=ast.JoinCriteria(
            on=ast.BinaryOp.And(
                *(null_safe_match(left, right) for left, right in pairs),
            ),
        ),
    )
