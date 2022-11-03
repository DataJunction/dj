from typing import (
    List,
    Union,
    Set,
)

from typing_extensions import Self

from itertools import chain

import sqlalchemy

from dj.sql.parsing.ast import (
    BinaryOp,
    Named,
    Value,
    Join,
    Expression,
    UnaryOp,
    From,
    Wildcard,
    Query,
    Select,
)


class DJParseException(Exception):
    """Exception type raised upon problem creating a DJ sql ast"""


def flatten(maybe_iterable):
    try:
        for subiterator in maybe_iterable:
            if isinstance(subiterator, str):
                yield subiterator
                continue
            for element in flatten(subiterator):
                yield element
    except TypeError:
        yield maybe_iterable


def match_keys(parse_tree: dict, *keys: Set[str]) -> bool:
    return set(parse_tree.keys()) in keys


def match_keys_subset(parse_tree: dict, *keys: Set[str]) -> bool:
    tree_keys = set(parse_tree.keys())
    return any(key <= tree_keys for key in keys)


def parse_op(parse_tree: dict):
    if match_keys(parse_tree, {"BinaryOp"}):
        subtree = parse_tree["BinaryOp"]
        for e in BinaryOpKind:
            binop_kind = e.name
            if subtree["op"] == binop_kind:
                return BinaryOp(
                    parse_expression(subtree["left"]),
                    BinaryOpKind[binop_kind],
                    parse_expression(subtree["right"]),
                )
        raise DJParseException(f"Unknown operator {binop_kind}")
    elif match_keys(parse_tree, {"UnaryOp"}):
        subtree = parse_tree["UnaryOp"]
        return UnaryOp(subtree["op"], subtree["expr"])
    raise DJParseException("Failed to parse Operator")


def parse_expression(parse_tree: Union[dict, str]) -> Expression:
    if parse_tree == "Wildcard":
        return Wildcard()
    elif match_keys(parse_tree, {"Value"}):
        return parse_value(parse_tree["Value"])
    elif match_keys(parse_tree, {"UnaryOp"}, {"BinaryOp"}):
        return parse_op(parse_tree)
    elif match_keys(parse_tree, {"UnnamedExpr"}):
        return parse_expression(parse_tree["UnnamedExpr"])
    elif match_keys(parse_tree, {"Identifier"}, {"CompoundIdentifier"}):
        return parse_column(parse_tree)
    elif match_keys(parse_tree, {"ExprWithAlias"}):
        subtree = parse_tree["ExprWithAlias"]
        return Alias(
            subtree["alias"]["value"],
            subtree["alias"]["quote_style"],
            parse_column(subtree["expr"]),
        ).add_self_as_parent()

    raise DJParseException("Failed to parse Expression")


def parse_value(parse_tree: dict) -> Value:
    if match_keys(parse_tree, {"Number"}):
        return Number(parse_tree["Number"][0])
    elif match_keys(parse_tree, {"SingleQuotedString"}):
        return String(parse_tree["SingleQuotedString"][0])
    elif match_keys(parse_tree, {"Boolean"}):
        return Boolean(parse_tree["Boolean"])
    raise DJParseException("Not a primitive")


def parse_column(parse_tree: dict):
    if match_keys(parse_tree, {"Identifier"}):
        subtree = parse_tree["Identifier"]
        return Column(subtree["value"], subtree["quote_style"])
    if match_keys(parse_tree, {"CompoundIdentifier"}):
        subtree = parse_tree["CompoundIdentifier"]
        if len(subtree) != 2:
            raise DJParseException(
                "Could not handle compound identifier of more than two identifiers"
            )
        table = Table(subtree[0]["value"], subtree[0]["quote_style"])
        column = Column(subtree[1]["value"], subtree[1]["quote_style"], table)
        table.add_columns(column)
        return column

    raise DJParseException("Failed to parse Column")


def parse_table(parse_tree: dict) -> Table:
    if match_keys(parse_tree, {"Table"}):
        subtree = parse_tree["Table"]
        name = subtree["name"]
        if len(name) != 1:
            raise DJParseException(
                "Could not handle identifier for table with more than one identifiers"
            )
        table = Table(name[0]["value"], name[0]["quote_style"])
        if subtree["alias"]:
            return Alias(
                subtree["alias"]["name"]["value"],
                subtree["alias"]["name"]["quote_style"],
                table,
            ).add_self_as_parent()
        return table

    raise DJParseException("Failed to parse Table")


def parse_join(parse_tree: dict) -> Join:
    if match_keys(
        parse_tree,
        {"relation", "join_operator"},
    ):
        relation = parse_tree["relation"]
        join_operator = parse_tree["join_operator"]
        for e in JoinKind:
            join_kind = e.name
            if match_keys(
                join_operator,
                {join_kind},
            ):
                return Join(
                    JoinKind[join_kind],
                    parse_table(relation),
                    parse_expression(join_operator[join_kind]["On"]),
                ).add_self_as_parent()

    raise DJParseException("Failed to parse Join")


def parse_from(parse_tree: list) -> From:
    if len(parse_tree) != 1:
        raise DJParseException("Expected single From statement")
    parse_tree = parse_tree[0]
    if match_keys(
        parse_tree,
        {"relation", "joins"},
    ):
        return From(
            parse_table(parse_tree["relation"]),
            [parse_join(join) for join in parse_tree["joins"]],
        ).add_self_as_parent()

    raise DJParseException("Failed to parse Select")


def parse_select(parse_tree: dict) -> List[Alias[Select]]:
    if match_keys_subset(
        parse_tree,
        {"distinct", "from", "group_by", "having", "projection", "selection"},
    ):
        return Select(
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
        ).add_self_as_parent()

    raise DJParseException("Failed to parse Select")


def parse_ctes(parse_tree: dict) -> List[Alias[Select]]:
    if match_keys_subset(parse_tree, {"cte_tables"}):
        subtree = parse_tree["cte_tables"]
        ctes = []
        for aliased_query in subtree:
            ctes.append(
                Alias(
                    aliased_query["alias"]["name"]["value"],
                    aliased_query["alias"]["name"]["quote_style"],
                    parse_select(aliased_query["query"]["body"]["Select"]),
                ).add_self_as_parent()
            )
        return ctes
    raise DJParseException("Failed to parse ctes")


def parse_oxide_tree(parse_tree: dict) -> Query:
    """take a sqloxide parsed statement ast dict and transform it into a DJ ast"""
    if match_keys(parse_tree, {"Query"}):
        return parse_query(
            parse_tree["Query"],
        )

    raise DJParseException("Failed to parse query")


def parse(sql: str) -> Query:
    """
    Parse a string into a DJ ast using sqloxide backend.

    Parses only a single Select query (can include ctes)

    """
    oxide_parsed = parse_sql(sql, "ansi")
    if len(oxide_parsed) != 1:
        raise DJParseException("Expected a single sql statement.")
    return [parse_oxide_tree(parsed) for parsed in oxide_parsed]
