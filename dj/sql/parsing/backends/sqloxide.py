"""
parsing backend turning sqloxide output into DJ AST
"""
from typing import List, Optional, Set, Union

from sqloxide import parse_sql

from dj.sql.parsing import ast
from dj.sql.parsing.backends.exceptions import DJParseException
from dj.typing import PRIMITIVE_TYPES, ColumnType


def match_keys(parse_tree: dict, *keys: Set[str]) -> Optional[Set[str]]:
    """match a parse tree having exact keys"""
    tree_keys = set(parse_tree.keys())
    for key in keys:
        if key == tree_keys:
            return key
    return None


def match_keys_subset(parse_tree: dict, *keys: Set[str]) -> Optional[Set[str]]:
    """match a parse tree having a subset of keys"""
    tree_keys = set(parse_tree.keys())
    for key in keys:
        if key <= tree_keys:  # type: ignore #pragma: no cover
            return key
    return None  # type: ignore #pragma: no cover


def parse_op(parse_tree: dict) -> ast.Operation:
    """parse an unary or binary operation"""
    if match_keys(parse_tree, {"BinaryOp"}):
        subtree = parse_tree["BinaryOp"]
        for exp in ast.BinaryOpKind:
            binop_kind = exp.name
            if subtree["op"] == binop_kind:
                return ast.BinaryOp(
                    ast.BinaryOpKind[binop_kind],
                    parse_expression(subtree["left"]),
                    parse_expression(subtree["right"]),
                )
        raise DJParseException(f"Unknown operator {subtree['op']}")  # pragma: no cover
    if match_keys(parse_tree, {"UnaryOp"}):
        subtree = parse_tree["UnaryOp"]
        for exp in ast.UnaryOpKind:  # type: ignore
            uniop_kind = exp.name
            if subtree["op"] == uniop_kind:
                return ast.UnaryOp(
                    ast.UnaryOpKind[uniop_kind],
                    parse_expression(subtree["expr"]),
                )
        raise DJParseException(f"Unknown operator {subtree['op']}")  # pragma: no cover
    if match_keys(parse_tree, {"Between"}):
        subtree = parse_tree["Between"]
        between = ast.Between(
            parse_expression(subtree["expr"]),
            parse_expression(subtree["low"]),
            parse_expression(subtree["high"]),
        )
        if subtree["negated"]:
            return ast.UnaryOp(ast.UnaryOpKind.Not, between)
        return between

    raise DJParseException("Failed to parse Operator")  # pragma: no cover


def parse_case(parse_tree: dict) -> ast.Case:
    """parse a case expressions"""
    if match_keys(parse_tree, {"conditions", "else_result", "operand", "results"}):
        return ast.Case(
            [parse_expression(exp) for exp in parse_tree["conditions"]],
            parse_expression(parse_tree["else_result"])
            if parse_tree["else_result"] is not None
            else None,
            parse_expression(parse_tree["operand"])
            if parse_tree["operand"] is not None
            else None,
            [parse_expression(exp) for exp in parse_tree["results"]],
        )
    raise DJParseException("Failed to parse Case")  # pragma: no cover


def parse_cast(parse_tree: dict) -> ast.Cast:
    """parse a cast statement"""
    if match_keys(parse_tree, {"expr", "data_type"}):
        expr = parse_expression(parse_tree["expr"])
        data_type = parse_tree["data_type"]
        type_ = None
        if isinstance(data_type, str):
            type_ = ColumnType(data_type)
        elif match_keys(parse_tree["data_type"], {"Custom"}):
            type_ = ColumnType(parse_tree["data_type"]["Custom"][0]["value"])
        elif isinstance(data_type, dict) and {
            typ.upper() for typ in data_type.keys()
        }.intersection(PRIMITIVE_TYPES):
            print(data_type)
            type_ = [
                ColumnType(typ)
                for typ in data_type.keys()
                if typ.upper() in PRIMITIVE_TYPES
            ][0]
        else:
            raise DJParseException("Failed to parse CAST type.")  # pragma: no cover
        return ast.Cast(expr, type_)
    raise DJParseException("Failed to parse CAST.")  # pragma: no cover


# flake8: noqa: C901
def parse_expression(  # pylint: disable=R0911,R0912
    parse_tree: Union[dict, str],
) -> ast.Expression:
    """parse an expression"""
    if isinstance(parse_tree, str):
        if parse_tree == "Wildcard":
            return ast.Wildcard()
    else:
        if match_keys(parse_tree, {"Value"}):
            return parse_value(parse_tree["Value"])
        if match_keys(parse_tree, {"Wildcard"}):
            return parse_expression("Wildcard")
        if match := match_keys(parse_tree, {"InList"}, {"InSubquery"}):
            return parse_in(parse_tree[match.pop()])
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
        if match_keys(parse_tree, {"Cast"}):
            return parse_cast(parse_tree["Cast"])
        if match_keys(parse_tree, {"Case"}):
            return parse_case(parse_tree["Case"])
        if match_keys(parse_tree, {"Function"}):
            return parse_function(parse_tree["Function"])
        if match_keys(parse_tree, {"IsNull"}, {"IsNotNull"}):
            if "IsNull" in parse_tree:
                return ast.IsNull(parse_expression(parse_tree["IsNull"]))
            return ast.UnaryOp(
                ast.UnaryOpKind.Not,
                ast.IsNull(
                    parse_expression(parse_tree["IsNotNull"]),
                ),
            )
        if match_keys(parse_tree, {"Identifier"}, {"CompoundIdentifier"}):
            return parse_column(parse_tree)
        if match_keys(parse_tree, {"ExprWithAlias"}):
            subtree = parse_tree["ExprWithAlias"]
            return ast.Alias(
                parse_name(subtree["alias"]),
                child=parse_column(subtree["expr"]),
            )
        if match_keys(parse_tree, {"Subquery"}):
            return parse_query(  # pylint: disable=W0212
                parse_tree["Subquery"],
            )._to_select()

        if match_keys(parse_tree, {"MapAccess"}):
            subtree = parse_tree["MapAccess"]
            return ast.MapSubscript(
                parse_column(subtree["column"]),
                [key["Value"]["SingleQuotedString"] for key in subtree["keys"]],
            )
    raise DJParseException("Failed to parse Expression")  # pragma: no cover


def parse_value(parse_tree: dict) -> ast.Value:
    """parse a primitive value"""
    if parse_tree == "Null":
        return ast.Null(None)
    if match_keys(parse_tree, {"Value"}):
        return parse_value(parse_tree["Value"])
    if match_keys(parse_tree, {"Number"}):
        return ast.Number(parse_tree["Number"][0])
    if match_keys(parse_tree, {"SingleQuotedString"}):
        return ast.String(parse_tree["SingleQuotedString"])
    if match_keys(parse_tree, {"Boolean"}):
        return ast.Boolean(parse_tree["Boolean"])
    raise DJParseException("Not a primitive")  # pragma: no cover


def parse_namespace(parse_tree: List[dict]) -> ast.Namespace:
    """parse a namespace"""
    return ast.Namespace([parse_name(name) for name in parse_tree])


def parse_name(parse_tree: dict) -> ast.Name:
    """parse a name"""
    if match_keys(parse_tree, {"value", "quote_style"}):
        return ast.Name(
            name=parse_tree["value"],
            quote_style=parse_tree["quote_style"]
            if parse_tree["quote_style"] is not None
            else "",
        )
    raise DJParseException("Failed to parse Name")  # pragma: no cover


def parse_column(parse_tree: dict):
    """parse a column"""
    if match_keys(parse_tree, {"Identifier"}, {"CompoundIdentifier"}):
        if "CompoundIdentifier" in parse_tree:
            subtree = parse_tree["CompoundIdentifier"]
            return parse_namespace(subtree).to_named_type(ast.Column)
        return parse_name(parse_tree["Identifier"]).to_named_type(ast.Column)
    return parse_expression(parse_tree)


def parse_table(parse_tree: dict) -> ast.TableExpression:
    """parse a table"""
    if match_keys(parse_tree, {"Derived"}):
        subtree = parse_tree["Derived"]
        if subtree["lateral"]:
            raise DJParseException("Parsing does not support lateral subqueries")

        alias = subtree["alias"]
        subquery = parse_query(subtree["subquery"])
        if subquery.ctes:
            raise DJParseException("CTEs are not allowed in a subquery")
        subselect = subquery.select
        if alias:
            if alias["columns"]:
                raise DJParseException(  # pragma: no cover
                    "Parsing does not support columns in derived from.",
                )
            aliased: ast.Alias[ast.Select] = ast.Alias(
                parse_name(alias["name"]),
                child=subselect,
            )
            return aliased
        return subselect
    if match_keys(parse_tree, {"Table"}):
        subtree = parse_tree["Table"]

        table = parse_namespace(subtree["name"]).to_named_type(ast.Table)
        if subtree["alias"]:
            aliased: ast.Alias[ast.Table] = ast.Alias(  # type: ignore
                parse_name(subtree["alias"]["name"]),
                child=table,
            )
            return aliased
        return table

    raise DJParseException("Failed to parse Table")  # pragma: no cover


def parse_in(parse_tree: dict) -> ast.In:
    """parse an in statement"""
    if match_keys(parse_tree, {"expr", "list", "negated"}):
        source = [parse_expression(expr) for expr in parse_tree["list"]]
        return ast.In(
            parse_expression(parse_tree["expr"]),
            source,
            parse_tree["negated"],
        )
    if match_keys(parse_tree, {"expr", "subquery", "negated"}):
        subquery: dict = parse_tree["subquery"]
        subquery_select = parse_query(subquery)._to_select()  # pylint: disable=W0212
        return ast.In(
            parse_expression(parse_tree["expr"]),
            subquery_select,
            parse_tree["negated"],
        )
    raise DJParseException("Failed to parse IN")  # pragma: no cover


def parse_over(parse_tree: dict) -> ast.Over:
    """parse the over of a function"""
    if match_keys(parse_tree, {"partition_by", "order_by", "window_frame"}):
        if parse_tree["window_frame"] is not None:
            raise DJParseException(
                "window frames are not supported.",
            )  # pragma: no cover
        partition_by = [parse_expression(exp) for exp in parse_tree["partition_by"]]
        order_by = [parse_order(exp) for exp in parse_tree["order_by"]]
        return ast.Over(partition_by, order_by)
    raise DJParseException("Failed to parse OVER")  # pragma: no cover


def parse_order(parse_tree: dict) -> ast.Order:
    """parse the order parts of an order by or window function"""
    if match_keys(parse_tree, {"expr", "asc", "nulls_first"}):
        if parse_tree["nulls_first"] is not None:
            raise DJParseException("nulls first is not supported.")  # pragma: no cover
        return ast.Order(
            expr=parse_expression(parse_tree["expr"]),
            asc=bool(parse_tree["asc"]),
        )
    raise DJParseException("Failed to parse ORDER BY expression.")  # pragma: no cover


def parse_function(parse_tree: dict) -> Union[ast.Function, ast.Raw]:
    """parse a function operating on an expression"""
    if match_keys_subset(parse_tree, {"name", "args", "over", "distinct"}):
        args = parse_tree["args"]
        names = parse_tree["name"]
        namespace, name = parse_namespace(names).pop_self()
        over = parse_tree["over"] and parse_over(parse_tree["over"])
        return ast.Function(
            name,
            args=[parse_expression(exp) for exp in args],
            distinct=parse_tree["distinct"],
            over=over,
        ).add_namespace(namespace)
    raise DJParseException("Failed to parse Function")  # pragma: no cover


def parse_join(parse_tree: dict) -> ast.Join:
    """parse a join of a select"""
    if match_keys(
        parse_tree,
        {"relation", "join_operator"},
    ):
        relation = parse_tree["relation"]
        join_operator = parse_tree["join_operator"]
        for exp in ast.JoinKind:
            join_kind = exp.name
            if match_keys(
                join_operator,
                {join_kind},
            ):
                if "On" not in join_operator[join_kind]:
                    raise DJParseException("Join must specify ON")
                return ast.Join(
                    ast.JoinKind[join_kind],
                    parse_table(relation),
                    parse_expression(join_operator[join_kind]["On"]),
                )

    raise DJParseException("Failed to parse Join")  # pragma: no cover


def parse_from(parse_list: List[dict]) -> ast.From:
    """parse the from of a select"""
    tables, joins = [], []
    for parse_tree in parse_list:
        if match_keys(
            parse_tree,
            {"relation", "joins"},
        ):
            tables.append(parse_table(parse_tree["relation"]))
            joins += [parse_join(join) for join in parse_tree["joins"]]
        else:
            raise DJParseException("Failed to parse From")  # pragma: no cover
    return ast.From(tables, joins)


def parse_select(parse_tree: dict) -> ast.Select:
    """parse the select of a query or subquery"""
    if match_keys_subset(
        parse_tree,
        {"distinct", "from", "group_by", "having", "projection", "selection"},
    ):
        return ast.Select(
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
            parse_tree["distinct"],
        )

    raise DJParseException("Failed to parse Select")  # pragma: no cover


def parse_ctes(parse_tree: dict) -> List[ast.Alias[ast.Select]]:
    """parse the ctes of a query"""
    if match_keys_subset(parse_tree, {"cte_tables"}):
        subtree = parse_tree["cte_tables"]
        ctes: List[ast.Alias[ast.Select]] = []
        for aliased_query in subtree:
            ctes.append(
                ast.Alias(
                    parse_name(aliased_query["alias"]["name"]),
                    child=parse_select(aliased_query["query"]["body"]["Select"]),
                ),
            )
        return ctes
    raise DJParseException("Failed to parse ctes")  # pragma: no cover


def parse_query(parse_tree: dict) -> ast.Query:
    """parse a query (ctes+select) statement"""
    if match_keys_subset(parse_tree, {"with", "body", "limit", "order_by"}):
        body = parse_tree["body"]
        if match_keys(body, {"Select"}):
            select = parse_select(body["Select"])
            select.limit = None
            if parse_tree["limit"] is not None:
                limit_value = parse_value(parse_tree["limit"])
                if not isinstance(limit_value, ast.Number):
                    raise DJParseException("limit must be a number")  # pragma: no cover
                select.limit = limit_value
            select.order_by = [
                parse_order(order) for order in parse_tree.get("order_by", [])
            ]
            return ast.Query(
                ctes=parse_ctes(parse_tree["with"])
                if parse_tree["with"] is not None
                else [],
                select=select,
            )

    raise DJParseException("Failed to parse query")  # pragma: no cover


def parse_oxide_tree(parse_tree: dict) -> ast.Query:
    """take a sqloxide parsed statement ast dict and transform it into a DJ ast"""
    if match_keys(parse_tree, {"Query"}):
        return parse_query(
            parse_tree["Query"],
        )

    raise DJParseException("Failed to parse Query")  # pragma: no cover


def parse(
    sql: str,
    dialect: Optional[str] = None,
    process_raw: bool = True,
) -> ast.Query:
    """Parse a string into a DJ ast using sqloxide backend.

    Parses only a single ast.Select query (can include ctes)

    """
    if dialect is None:
        dialect = "ansi"
    oxide_parsed = parse_sql(sql, dialect)
    if len(oxide_parsed) != 1:
        raise DJParseException("Expected a single sql statement.")
    query_ast = parse_oxide_tree(oxide_parsed[0])
    if process_raw:
        for func in query_ast.find_all(ast.Function):
            if str(func.name).upper() == "RAW":
                query_ast.replace(
                    func,
                    func.to_raw(lambda sub_sql, _: parse(sub_sql, dialect, False)),
                )
    return query_ast
