"""
Transform a DJ AST into a sql string given a dialect
"""
from dj.sql.parsing.ast import Node


def sql(node: Node, dialect: str = "ansi") -> str:
    """
    return the sql representing the sub-ast
    """
    try:
        return {"ansi": str}[dialect.lower().strip()](node)
    except KeyError:
        raise ValueError(
            f"{dialect} is not an acceptable dialect.",
        )
