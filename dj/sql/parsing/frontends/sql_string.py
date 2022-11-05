"""
Transform a DJ AST into a sql string
"""

from functools import singledispatch
from dj.sql.parsing.ast import (
    Node,
    Alias,
    Expression,
    Value,
    Named,
    Column,
    Table,
    From,
    Select,
    Query,
    BinaryOp,
    Join,
    UnaryOp,
    BinaryOpKind,
    JoinKind,
    Wildcard,
)


@singledispatch
def sql(node: Node) -> str:
    """
    return the ansi sql representing the sub-ast
    """
    return " ".join([sql(child) for child in node.children])


@sql.register
def _(node: UnaryOp) -> str:
    return f"{node.op} {sql(node.expr)}"


@sql.register
def _(node: BinaryOp) -> str:
    return f"{sql(node.left)} {node.op.value} {sql(node.right)}"


@sql.register
def _(node: Value) -> str:
    return str(node.value)


@sql.register
def _(node: Alias) -> str:
    return f"{sql(node.child)} AS {node.quoted_name}"


@sql.register
def _(node: Column) -> str:
    if node.table:
        return f'{node.quote_style if node.quote_style else ""}{node.table.alias_or_name()}.{node.name}{node.quote_style if node.quote_style else ""}'
    return node.quoted_name


@sql.register
def _(node: Wildcard) -> str:
    return "*"


@sql.register
def _(node: Table) -> str:
    return node.quoted_name


@sql.register
def _(node: Join) -> str:
    return f"""{node.kind.value} {sql(node.table)}
        ON {sql(node.on)}"""


@sql.register
def _(node: From) -> str:
    return (
        f"FROM {sql(node.table)}" + "\n" + "\n".join(sql(join) for join in node.joins)
    )


@sql.register
def _(node: Select) -> str:
    projection = ",\n\t".join(sql(exp) for exp in node.projection)
    return f"""SELECT {"DISTINCT " if node.distinct else ""}{projection}
{sql(node.from_)}
{"WHERE "+sql(node.where) if node.where is not None else ""}
{"GROUP BY "+", ".join(sql(exp) for exp in node.group_by) if node.group_by else ""}
{"HAVING "+sql(node.having) if node.having is not None else ""}
""".strip()


@sql.register
def _(node: Query) -> str:
    ctes = ",\n".join(f"{cte.name} AS ({sql(cte.child)})" for cte in node.ctes)
    return f"""{'WITH' if ctes else ""}
{ctes}

{sql(node.select)}
    """.strip()
