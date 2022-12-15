"""
parsing backend turning sqloxide output into DJ AST
"""
from typing import List, Set, Union, cast

from sqloxide import parse_sql

from dj.sql.parsing.ast import (
    Alias,
    Between,
    BinaryOp,
    BinaryOpKind,
    Boolean,
    Case,
    Column,
    Expression,
    From,
    Function,
    Namespace,
    IsNull,
    Join,
    JoinKind,
    Name,
    Number,
    Operation,
    Query,
    Select,
    String,
    Table,
    UnaryOp,
    UnaryOpKind,
    Value,
    Wildcard,
)
from dj.sql.parsing.backends.exceptions import DJParseException


def match_keys(parse_tree: dict, *keys: Set[str]) -> bool:
    """
    match a parse tree having exact keys
    """
    return set(parse_tree.keys()) in keys


def match_keys_subset(parse_tree: dict, *keys: Set[str]) -> bool:
    """
    match a parse tree having a subset of keys
    """
    tree_keys = set(parse_tree.keys())
    return any(key <= tree_keys for key in keys)  # pragma: no cover


def parse_op(parse_tree: dict) -> Operation:
    """
    parse an unary or binary operation
    """
    if match_keys(parse_tree, {"BinaryOp"}):
        subtree = parse_tree["BinaryOp"]
        for exp in BinaryOpKind:
            binop_kind = exp.name
            if subtree["op"] == binop_kind:
                return cast(
                    BinaryOp,
                    BinaryOp(
                        parse_expression(subtree["left"]),
                        BinaryOpKind[binop_kind],
                        parse_expression(subtree["right"]),
                    ).add_self_as_parent(),
                )
        raise DJParseException(f"Unknown operator {subtree['op']}")  # pragma: no cover
    if match_keys(parse_tree, {"UnaryOp"}):
        subtree = parse_tree["UnaryOp"]
        for exp in UnaryOpKind:  # type: ignore
            uniop_kind = exp.name
            if subtree["op"] == uniop_kind:
                return cast(
                    UnaryOp,
                    UnaryOp(
                        UnaryOpKind[uniop_kind],
                        parse_expression(subtree["expr"]),
                    ).add_self_as_parent(),
                )
        raise DJParseException(f"Unknown operator {subtree['op']}")  # pragma: no cover
    if match_keys(parse_tree, {"Between"}):
        subtree = parse_tree["Between"]
        between = cast(
            Between,
            Between(
                parse_expression(subtree["expr"]),
                parse_expression(subtree["low"]),
                parse_expression(subtree["high"]),
            ).add_self_as_parent(),
        )
        if subtree["negated"]:
            return cast(UnaryOp, UnaryOp(UnaryOpKind.Not, between).add_self_as_parent())
        return between

    raise DJParseException("Failed to parse Operator")  # pragma: no cover


def parse_case(parse_tree: dict) -> Case:
    """
    parse a case expressions
    """
    if match_keys(parse_tree, {"conditions", "else_result", "operand", "results"}):
        return cast(
            Case,
            Case(
                [parse_expression(exp) for exp in parse_tree["conditions"]],
                parse_expression(parse_tree["else_result"])
                if parse_tree["else_result"] is not None
                else None,
                parse_expression(parse_tree["operand"])
                if parse_tree["operand"] is not None
                else None,
                [parse_expression(exp) for exp in parse_tree["results"]],
            ).add_self_as_parent(),
        )
    raise DJParseException("Failed to parse Case")  # pragma: no cover


def parse_expression(  # pylint: disable=R0911,R0912
    parse_tree: Union[dict, str],
) -> Expression:
    """
    parse an expression
    """
    if isinstance(parse_tree, str):
        if parse_tree == "Wildcard":
            return Wildcard()
    else:
        if match_keys(parse_tree, {"Value"}):
            return parse_value(parse_tree["Value"])
        if match_keys(parse_tree, {"Nested"}):
            return parse_expression(parse_tree["Nested"])
        if match_keys(parse_tree, {"UnaryOp"}, {"BinaryOp"}, {"Between"}):
            return parse_op(parse_tree)
        if match_keys(parse_tree, {"Unnamed"}):
            return parse_expression(parse_tree["Unnamed"])
        if match_keys(parse_tree, {"UnnamedExpr"}):
            return parse_expression(parse_tree["UnnamedExpr"])
        if match_keys(parse_tree, {"Expr"}):
            return parse_expression(parse_tree["Expr"])
        if match_keys(parse_tree, {"Case"}):
            return parse_case(parse_tree["Case"])
        if match_keys(parse_tree, {"Function"}):
            return parse_function(parse_tree["Function"])
        if match_keys(parse_tree, {"IsNull"}, {"IsNotNull"}):
            if "IsNull" in parse_tree:
                return cast(
                    IsNull,
                    IsNull(parse_expression(parse_tree["IsNull"])).add_self_as_parent(),
                )
            return cast(
                UnaryOp,
                UnaryOp(
                    UnaryOpKind.Not,
                    cast(
                        IsNull,
                        IsNull(
                            parse_expression(parse_tree["IsNotNull"]),
                        ).add_self_as_parent(),
                    ),
                ).add_self_as_parent(),
            )
        if match_keys(parse_tree, {"Identifier"}, {"CompoundIdentifier"}):
            return parse_column(parse_tree)
        if match_keys(parse_tree, {"ExprWithAlias"}):
            subtree = parse_tree["ExprWithAlias"]
            return cast(
                Alias,
                Alias(
                    parse_name(subtree["alias"]).to_identifier(),
                    parse_column(subtree["expr"]),
                ).add_self_as_parent(),
            )
        if match_keys(parse_tree, {"Subquery"}):
            return parse_query(parse_tree["Subquery"])
    raise DJParseException("Failed to parse Expression")  # pragma: no cover


def parse_value(parse_tree: dict) -> Value:
    """
    parse a primitive value
    """
    if match_keys(parse_tree, {"Value"}):
        return parse_value(parse_tree["Value"])
    if match_keys(parse_tree, {"Number"}):
        return Number(parse_tree["Number"][0])
    if match_keys(parse_tree, {"SingleQuotedString"}):
        return String(parse_tree["SingleQuotedString"])
    if match_keys(parse_tree, {"Boolean"}):
        return Boolean(parse_tree["Boolean"])
    raise DJParseException("Not a primitive")  # pragma: no cover


def parse_namespace(parse_tree: List[dict]) -> Namespace:
    """parse a namespace"""
    return Namespace([parse_name(name) for name in parse_tree])


def parse_name(parse_tree: dict) -> Name:
    """parse a name"""
    if match_keys(parse_tree, {"value", "quote_style"}):
        return Name(
            name=parse_tree["value"],
            quote_style=parse_tree["quote_style"]
            if parse_tree["quote_style"] is not None
            else "",
        )
    raise DJParseException("Failed to parse Name")  # pragma: no cover


def parse_column(parse_tree: dict):
    """
    parse a column
    """
    if match_keys(parse_tree, {"Identifier"}, {"CompoundIdentifier"}):
        if "CompoundIdentifier" in parse_tree:
            subtree = parse_tree["CompoundIdentifier"]
            return parse_namespace(subtree).to_column()
        return parse_name(parse_tree["Identifier"]).to_column()
    return parse_expression(parse_tree)


def parse_table(parse_tree: dict) -> Union[Alias, Table]:
    """
    parse a table
    """
    if match_keys(parse_tree, {"Derived"}):
        subtree = parse_tree["Derived"]
        if subtree["lateral"]:
            raise DJParseException("Parsing does not support lateral subqueries")

        alias = subtree["alias"]
        if alias["columns"]:
            raise DJParseException(  # pragma: no cover
                "Parsing does not support columns in derived from.",
            )
        return cast(
            Alias,
            Alias(
                parse_name(alias["name"]),
                parse_query(subtree["subquery"]),
            ).add_self_as_parent(),
        )
    if match_keys(parse_tree, {"Table"}):
        subtree = parse_tree["Table"]

        table = parse_namespace(subtree["name"]).to_table()
        if subtree["alias"]:
            return cast(
                Alias,
                Alias(
                    parse_name(subtree["alias"]["name"]),
                    table,
                ).add_self_as_parent(),
            )
        return table

    raise DJParseException("Failed to parse Table")  # pragma: no cover


def parse_function(parse_tree: dict) -> Function:
    """
    parse a function operating on an expression
    """
    if match_keys_subset(parse_tree, {"name", "args"}):
        args = parse_tree["args"]
        names = parse_tree["name"]

        return cast(
            Function,
            Function(
                cast(
                    Identifier,
                    Identifier(
                        [parse_name(name) for name in names],
                    ).add_self_as_parent(),
                ),
                [parse_expression(exp) for exp in args],
            ).add_self_as_parent(),
        )
    raise DJParseException("Failed to parse Function")  # pragma: no cover


def parse_join(parse_tree: dict) -> Join:
    """
    parse a join of a select
    """
    if match_keys(
        parse_tree,
        {"relation", "join_operator"},
    ):
        relation = parse_tree["relation"]
        join_operator = parse_tree["join_operator"]
        for exp in JoinKind:
            join_kind = exp.name
            if match_keys(
                join_operator,
                {join_kind},
            ):
                if "On" not in join_operator[join_kind]:
                    raise DJParseException("Join must specify ON")
                return cast(
                    Join,
                    Join(
                        JoinKind[join_kind],
                        parse_table(relation),
                        parse_expression(join_operator[join_kind]["On"]),
                    ).add_self_as_parent(),
                )

    raise DJParseException("Failed to parse Join")  # pragma: no cover


def parse_from(parse_list: List[dict]) -> From:
    """
    parse the from of a select
    """
    if len(parse_list) != 1:
        raise DJParseException("Expected single From statement")
    parse_tree = parse_list[0]
    if match_keys(
        parse_tree,
        {"relation", "joins"},
    ):
        return cast(
            From,
            From(
                parse_table(parse_tree["relation"]),
                [parse_join(join) for join in parse_tree["joins"]],
            ).add_self_as_parent(),
        )
    raise DJParseException("Failed to parse From")  # pragma: no cover


def parse_select(parse_tree: dict) -> Select:
    """
    parse the select of a query or subquery
    """
    if match_keys_subset(
        parse_tree,
        {"distinct", "from", "group_by", "having", "projection", "selection"},
    ):
        return cast(
            Select,
            Select(
                parse_tree["distinct"],
                parse_from(parse_tree["from"]),
                [parse_expression(exp) for exp in parse_tree["group_by"]],
                parse_expression(parse_tree["having"])
                if parse_tree["having"] is not None
                else None,
                [parse_expression(exp) for exp in parse_tree["projection"]],
                parse_expression(parse_tree["selection"])
                if parse_tree["selection"] is not None
                else None,
                None,
            ).add_self_as_parent(),
        )

    raise DJParseException("Failed to parse Select")  # pragma: no cover


def parse_ctes(parse_tree: dict) -> List[Alias[Select]]:
    """
    parse the ctes of a query
    """
    if match_keys_subset(parse_tree, {"cte_tables"}):
        subtree = parse_tree["cte_tables"]
        ctes = []
        for aliased_query in subtree:
            ctes.append(
                cast(
                    Alias,
                    Alias(
                        parse_name(aliased_query["alias"]["name"]).to_identifier(),
                        parse_select(aliased_query["query"]["body"]["Select"]),
                    ).add_self_as_parent(),
                ),
            )
        return ctes
    raise DJParseException("Failed to parse ctes")  # pragma: no cover


def parse_query(parse_tree: dict) -> Query:
    """
    parse a query (ctes+select) statement
    """
    if match_keys_subset(parse_tree, {"with", "body", "limit"}):
        body = parse_tree["body"]
        if match_keys(body, {"Select"}):
            select = parse_select(body["Select"])
            select.limit = None
            if parse_tree["limit"] is not None:
                limit_value = parse_value(parse_tree["limit"])
                if not isinstance(limit_value, Number):
                    raise DJParseException("limit must be a number")  # pragma: no cover
                select.limit = limit_value
            return cast(
                Query,
                Query(
                    ctes=parse_ctes(parse_tree["with"])
                    if parse_tree["with"] is not None
                    else [],
                    select=select,
                ).add_self_as_parent(),
            )

    raise DJParseException("Failed to parse query")  # pragma: no cover


def parse_oxide_tree(parse_tree: dict) -> Query:
    """take a sqloxide parsed statement ast dict and transform it into a DJ ast"""
    if match_keys(parse_tree, {"Query"}):
        return parse_query(
            parse_tree["Query"],
        )

    raise DJParseException("Failed to parse query")  # pragma: no cover


def parse(sql: str, dialect: str = "hive") -> Query:
    """
    Parse a string into a DJ ast using sqloxide backend.

    Parses only a single Select query (can include ctes)

    """
    oxide_parsed = parse_sql(sql, dialect)
    if len(oxide_parsed) != 1:
        raise DJParseException("Expected a single sql statement.")
    return parse_oxide_tree(oxide_parsed[0])
