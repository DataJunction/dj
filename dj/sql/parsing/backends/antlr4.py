# pylint: skip-file
# mypy: ignore-errors
import inspect
import logging
from typing import List, Tuple

import antlr4
from antlr4 import InputStream, RecognitionException
from antlr4.error.ErrorListener import ErrorListener
from antlr4.error.Errors import ParseCancellationException
from antlr4.error.ErrorStrategy import BailErrorStrategy

from dj.sql.parsing import ast2 as ast
from dj.sql.parsing.backends.exceptions import DJParseException
from dj.sql.parsing.backends.grammar.generated.SqlBaseLexer import SqlBaseLexer
from dj.sql.parsing.backends.grammar.generated.SqlBaseParser import SqlBaseParser
from dj.sql.parsing.backends.grammar.generated.SqlBaseParser import SqlBaseParser as sbp

logger = logging.getLogger(__name__)


class RemoveIdentifierBackticks(antlr4.ParseTreeListener):
    @staticmethod
    def exitQuotedIdentifier(ctx):  # pylint: disable=invalid-name,unused-argument
        def identity(token):
            return token

        return identity

    @staticmethod
    def enterNonReserved(ctx):  # pylint: disable=invalid-name,unused-argument
        def add_backtick(token):
            return "`{0}`".format(token)

        return add_backtick


class ParseErrorListener(ErrorListener):
    def syntaxError(
        self,
        recognizer,
        offendingSymbol,
        line,
        column,
        msg,
        e,
    ):  # pylint: disable=invalid-name,no-self-use,too-many-arguments
        raise SqlSyntaxError(f"Parse error {line}:{column}:" f"", msg)


class UpperCaseCharStream:
    """
    Make SQL token detection case insensitive and allow identifier without
    backticks to be seen as e.g. column names
    """

    def __init__(self, wrapped):
        self.wrapped = wrapped

    def getText(self, interval, *args):  # pylint: disable=invalid-name
        if args or (self.size() > 0 and (interval.b - interval.a >= 0)):
            return self.wrapped.getText(interval, *args)
        return ""

    def LA(self, i: int):  # pylint: disable=invalid-name
        token = self.wrapped.LA(i)
        if token in (0, -1):
            return token
        return ord(chr(token).upper())

    def __getattr__(self, item):
        return getattr(self.wrapped, item)


class ExplicitBailErrorStrategy(BailErrorStrategy):
    """
    Bail Error Strategy throws a ParseCancellationException,
    This strategy simply throw a more explicit exception
    """

    def recover(self, recognizer, e: RecognitionException):
        try:
            super(ExplicitBailErrorStrategy, self).recover(recognizer, e)
        except ParseCancellationException:
            raise SqlParsingError from e


class EarlyBailSqlLexer(SqlBaseLexer):
    def recover(self, re: RecognitionException):
        raise SqlLexicalError from re


def build_parser(stream, strict_mode=False, early_bail=True):
    if not strict_mode:
        stream = UpperCaseCharStream(stream)
    if early_bail:
        lexer = EarlyBailSqlLexer(stream)
    else:
        lexer = SqlBaseLexer(stream)
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener())
    token_stream = antlr4.CommonTokenStream(lexer)
    parser = SqlBaseParser(token_stream)
    parser.addParseListener(RemoveIdentifierBackticks())
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener())
    if early_bail:
        parser._errHandler = ExplicitBailErrorStrategy()
    return parser


class SqlParsingError(Exception):
    pass


class SqlLexicalError(SqlParsingError):
    pass


class SqlSyntaxError(SqlParsingError):
    pass


def string_to_ast(string, rule, *, strict_mode=False, debug=False, early_bail=False):
    parser = build_string_parser(string, strict_mode, early_bail)
    tree = getattr(parser, rule)()
    if debug:
        print_tree(tree, printer=logger.warning)
    return tree


def build_string_parser(string, strict_mode=False, early_bail=True):
    string_as_stream = InputStream(string)
    parser = build_parser(string_as_stream, strict_mode, early_bail)
    return parser


def parse_sql(string, rule, converter, debug=False):
    tree = string_to_ast(string, rule, debug=debug)
    return converter(tree) if converter else tree


def parse_statement(string, converter=None, debug=False):
    return parse_sql(string, "singleStatement", converter, debug)


def print_tree(tree, printer=print):
    for line in tree_to_strings(tree, indent=0):
        printer(line)


def tree_to_strings(tree, indent=0):
    symbol = ("[" + tree.symbol.text + "]") if hasattr(tree, "symbol") else ""
    node_as_string = type(tree).__name__ + symbol
    result = ["|" + "-" * indent + node_as_string]
    if hasattr(tree, "children") and tree.children:
        for child in tree.children:
            result += tree_to_strings(child, indent + 1)
    return result


TERMINAL_NODE = antlr4.tree.Tree.TerminalNodeImpl


class Visitor:
    def __init__(self):
        self.registry = {}

    def register(self, func):
        params = inspect.signature(func).parameters
        type_ = params[list(params.keys())[0]].annotation
        if type_ == inspect.Parameter.empty:
            raise ValueError(
                "No type annotation found for the first parameter of the visitor.",
            )
        if type_ in self.registry:
            raise ValueError(
                f"A visitor is already registered for type {type_.__name__}.",
            )
        self.registry[type_] = func
        return func

    def __call__(self, ctx):
        if type(ctx) == TERMINAL_NODE:
            return None
        func = self.registry.get(type(ctx), None)
        if func is None:
            line, col = ctx.start.line, ctx.start.column
            raise TypeError(
                f"{line}:{col} No visitor registered for type {type(ctx).__name__}",
            )
        result = func(ctx)

        if result is None:
            line, col = ctx.start.line, ctx.start.column
            raise DJParseException(f"{line}:{col} Could not parse {ctx.getText()}")
        if (
            hasattr(result, "parenthesized")
            and result.parenthesized is None
            and hasattr(ctx, "LEFT_PAREN")
        ):
            if text := ctx.getText():
                text = text.strip()
                if (text[0] == "(") and (text[-1] == ")"):
                    result.parenthesized = True
        if (
            hasattr(ctx, "AS")
            and ctx.AS()
            and hasattr(result, "as_")
            and result.as_ is None
        ):
            result = result.set_as(True)
        return result


visit = Visitor()


@visit.register
def _(ctx: list, nones=False):
    return list(
        filter(
            lambda child: child is not None if nones is False else True,
            map(visit, ctx),
        ),
    )


@visit.register
def _(ctx: sbp.SingleStatementContext):
    return visit(ctx.statement())


@visit.register
def _(ctx: sbp.StatementDefaultContext):
    return visit(ctx.query())


@visit.register
def _(ctx: sbp.QueryContext):
    ctes = []
    if ctes_ctx := ctx.ctes():
        ctes = visit(ctes_ctx)
    limit, organization = visit(ctx.queryOrganization())
    select = visit(ctx.queryTerm())

    return ast.Query(ctes=ctes, select=select, limit=limit, organization=organization)


@visit.register
def _(ctx: sbp.QueryOrganizationContext):
    order = visit(ctx.order)
    sort = visit(ctx.sort)
    org = ast.Organization(order, sort)
    limit = None
    if ctx.limit:
        limit = visit(ctx.limit)
    return limit, org


@visit.register
def _(ctx: sbp.SortItemContext):
    expr = visit(ctx.expression())
    order = ""
    if ordering := ctx.ordering:
        order = ordering.text.upper()
    nulls = ""
    if null_order := ctx.nullOrder:
        nulls = "NULLS " + null_order.text
    return ast.SortItem(expr, order, nulls)


@visit.register
def _(ctx: sbp.ExpressionContext):
    return visit(ctx.booleanExpression())


@visit.register
def _(ctx: sbp.PredicatedContext):
    if value_expr := ctx.valueExpression():
        if not ctx.predicate():
            return visit(value_expr)
    if predicate_ctx := ctx.predicate():
        return visit(predicate_ctx)


@visit.register
def _(ctx: sbp.PredicateContext) -> ast.Predicate:
    negated = True if ctx.NOT() else False
    any = True if ctx.ANY() else False
    all = True if ctx.ALL() else False
    some = True if ctx.SOME() else False
    expr = visit(ctx.parentCtx.valueExpression())
    if ctx.BETWEEN():
        low = visit(ctx.lower)
        high = visit(ctx.upper)
        return ast.Between(negated, expr, low, high)
    if ctx.DISTINCT():
        right = visit(ctx.right)
        return ast.IsDistinctFrom(negated, expr, right)
    if ctx.LIKE():
        pattern = visit(ctx.pattern)
        return ast.Like(negated, expr, all or any or some or "", pattern, ctx.ESCAPE())
    if ctx.ILIKE():
        pattern = visit(ctx.pattern)
        return ast.Like(
            negated,
            expr,
            all or any or some or "",
            pattern,
            ctx.ESCAPE(),
            case_sensitive=False,
        )
    if ctx.IN():
        if source_ctx := ctx.query():
            source = visit(source_ctx.queryTerm())
            source.parenthesized = True
        if source_ctx := ctx.expression():
            source = visit(source_ctx)
        return ast.In(negated, expr, source)
    if ctx.IS():
        if ctx.NULL():
            return ast.IsNull(negated, expr)
        if ctx.TRUE():
            return ast.IsBoolean(negated, expr, "TRUE")
        if ctx.FALSE():
            return ast.IsBoolean(negated, expr, "FALSE")
        if ctx.UNKNOWN():
            return ast.IsBoolean(negated, expr, "UNKNOWN")
    if ctx.RLIKE():
        pattern = visit(ctx.pattern)
        return ast.Rlike(negated, expr, pattern)
    return


@visit.register
def _(ctx: sbp.ValueExpressionContext):
    if primary := ctx.primaryExpression():
        return visit(primary)


@visit.register
def _(ctx: sbp.ValueExpressionDefaultContext):
    return visit(ctx.primaryExpression())


@visit.register
def _(ctx: sbp.ArithmeticBinaryContext):
    return ast.BinaryOp(ctx.operator.text, visit(ctx.left), visit(ctx.right))


@visit.register
def _(ctx: sbp.ColumnReferenceContext):
    return ast.Column(visit(ctx.identifier()))


@visit.register
def _(ctx: sbp.QueryTermDefaultContext):
    return visit(ctx.queryPrimary())


@visit.register
def _(ctx: sbp.QueryPrimaryDefaultContext):
    return visit(ctx.querySpecification())


@visit.register
def _(ctx: sbp.QueryTermContext):
    if primary_query := ctx.queryPrimary():
        return visit(primary_query)


@visit.register
def _(ctx: sbp.QueryPrimaryContext):
    return visit(ctx.querySpecification())


@visit.register
def _(ctx: sbp.RegularQuerySpecificationContext):
    quantifier, projection = visit(ctx.selectClause())
    from_ = visit(ctx.fromClause()) if ctx.fromClause() else None
    group_by = visit(ctx.aggregationClause()) if ctx.aggregationClause() else []
    where = None
    if where_clause := ctx.whereClause():
        where = visit(where_clause)
    having = None
    if having_clause := ctx.havingClause():
        having = visit(having_clause)
    return ast.Select(
        quantifier=quantifier,
        projection=projection,
        from_=from_,
        where=where,
        group_by=group_by,
        having=having,
    )


@visit.register
def _(ctx: sbp.HavingClauseContext):
    return visit(ctx.booleanExpression())


@visit.register
def _(ctx: sbp.AggregationClauseContext):
    return visit(ctx.groupByClause())


@visit.register
def _(ctx: sbp.GroupByClauseContext):
    if grouping_analytics := ctx.groupingAnalytics():
        return visit(grouping_analytics)
    if expression := ctx.expression():
        return visit(expression)


@visit.register
def _(ctx: sbp.GroupingAnalyticsContext):
    grouping_set = visit(ctx.groupingSet())
    if ctx.ROLLUP():
        return ast.Function("ROLLUP", args=grouping_set)
    if ctx.CUBE():
        return ast.Function("CUBE", args=grouping_set)
    return grouping_set


@visit.register
def _(ctx: sbp.GroupingSetContext):
    return ast.Column(ctx.getText())


@visit.register
def _(ctx: sbp.SelectClauseContext):
    quantifier = ""
    if quant := ctx.setQuantifier():
        quantifier = visit(quant)
    projection = visit(ctx.namedExpressionSeq())
    return quantifier, projection


@visit.register
def _(ctx: sbp.SetQuantifierContext):
    if ctx.DISTINCT():
        return "DISTINCT"
    if ctx.ALL():
        return "ALL"
    return ""


@visit.register
def _(ctx: sbp.NamedExpressionSeqContext):
    return visit(ctx.children)


@visit.register
def _(ctx: sbp.NamedExpressionContext):
    expr = visit(ctx.expression())
    if alias := ctx.name:
        return expr.set_alias(visit(alias))
    return expr


@visit.register
def _(ctx: sbp.ErrorCapturingIdentifierContext):
    name = visit(ctx.identifier())
    if extra := visit(ctx.errorCapturingIdentifierExtra()):
        name.name += extra
        name.quote_style = '"'
    return name


@visit.register
def _(ctx: sbp.ErrorIdentContext):
    return ctx.getText()


@visit.register
def _(ctx: sbp.RealIdentContext):
    return ""


@visit.register
def _(ctx: sbp.IdentifierContext):
    return visit(ctx.strictIdentifier())


@visit.register
def _(ctx: sbp.UnquotedIdentifierContext):
    return ast.Name(ctx.getText())


@visit.register
def _(ctx: sbp.ConstantDefaultContext):
    return visit(ctx.constant())


@visit.register
def _(ctx: sbp.NumericLiteralContext):
    return ast.Number(ctx.number().getText())


@visit.register
def _(ctx: sbp.StringLitContext):
    if string := ctx.STRING():
        return string.getSymbol().text.strip("'")
    return ctx.DOUBLEQUOTED_STRING().getSymbol().text.strip('"')


@visit.register
def _(ctx: sbp.DereferenceContext):
    base = visit(ctx.base)
    field = visit(ctx.fieldName)
    field.namespace = base.name
    base.name = field
    return base


@visit.register
def _(ctx: sbp.FunctionCallContext):
    name = visit(ctx.functionName())
    quantifier = visit(ctx.setQuantifier()) if ctx.setQuantifier() else ""
    over = visit(ctx.windowSpec()) if ctx.windowSpec() else None
    args = visit((ctx.argument))
    return ast.Function(name, args, quantifier=quantifier, over=over)


@visit.register
def _(ctx: sbp.WindowDefContext):
    partition_by = visit(ctx.partition)
    order_by = visit(ctx.sortItem())
    window_frame = visit(ctx.windowFrame()) if ctx.windowFrame() else None
    return ast.Over(
        partition_by=partition_by,
        order_by=order_by,
        window_frame=window_frame,
    )


@visit.register
def _(ctx: sbp.WindowFrameContext):
    start = visit(ctx.start)
    end = visit(ctx.end) if ctx.end else None
    return ast.Frame(ctx.frameType.text, start=start, end=end)


@visit.register
def _(ctx: sbp.FrameBoundContext):
    return ast.FrameBound(start=ctx.start.text, stop=ctx.stop.text)


@visit.register
def _(ctx: sbp.FunctionNameContext):
    if qual_name := ctx.qualifiedName():
        return visit(qual_name)
    return ast.Name(ctx.getText())


@visit.register
def _(ctx: sbp.QualifiedNameContext):
    names = visit(ctx.children)
    for i in range(len(names) - 1, 0, -1):
        names[i].namespace = names[i - 1]
    return names[-1]


@visit.register
def _(ctx: sbp.StarContext):

    namespace = None
    if qual_name := ctx.qualifiedName():
        namespace = visit(qual_name)
    star = ast.Wildcard()
    star.name.namespace = namespace
    return star


@visit.register
def _(ctx: sbp.TableNameContext):
    if ctx.temporalClause():
        return
    name = visit(ctx.multipartIdentifier())
    table_alias = ctx.tableAlias()
    alias, cols = visit(table_alias)
    table = ast.Table(name, column_list=cols)
    if alias:
        table = table.set_alias(ast.Name(alias))
        if table_alias.AS():
            table = table.set_as(True)
    return table


@visit.register
def _(ctx: sbp.MultipartIdentifierContext):
    names = visit(ctx.children)
    for i in range(len(names) - 1, 0, -1):
        names[i].namespace = names[i - 1]
    return names[-1]


@visit.register
def _(ctx: sbp.QuotedIdentifierAlternativeContext):
    return visit(ctx.quotedIdentifier())


@visit.register
def _(ctx: sbp.QuotedIdentifierContext):
    if ident := ctx.BACKQUOTED_IDENTIFIER():
        return ast.Name(ident.getText()[1:-1], quote_style="`")
    return ast.Name(ctx.DOUBLEQUOTED_STRING().getText()[1:-1], quote_style='"')


@visit.register
def _(ctx: sbp.LateralViewContext):
    outer = bool(ctx.OUTER())
    func_name = visit(ctx.qualifiedName())
    func_args = visit(ctx.expression())
    func = ast.FunctionTable(func_name, func_args)
    table = ast.Table(visit(ctx.tblName))
    columns = [ast.Column(name) for name in visit(ctx.colName)]
    return ast.LateralView(outer, func, table, columns)


@visit.register
def _(ctx: sbp.RelationContext):
    primary_relation = visit(ctx.relationPrimary())
    extensions = visit(ctx.relationExtension()) if ctx.relationExtension() else []
    return ast.Relation(primary_relation, extensions)


@visit.register
def _(ctx: sbp.RelationExtensionContext):
    if join_rel := ctx.joinRelation():
        return visit(join_rel)


@visit.register
def _(ctx: sbp.JoinTypeContext):
    anti = f"{ctx.ANTI()} " if ctx.ANTI() else ""
    cross = f"{ctx.CROSS()} " if ctx.CROSS() else ""
    full = f"{ctx.FULL()} " if ctx.FULL() else ""
    inner = f"{ctx.INNER()} " if ctx.INNER() else ""
    outer = f"{ctx.OUTER()} " if ctx.OUTER() else ""
    semi = f"{ctx.SEMI()} " if ctx.SEMI() else ""
    left = f"{ctx.LEFT()} " if ctx.LEFT() else ""
    right = f"{ctx.RIGHT()} " if ctx.RIGHT() else ""
    return f"{left}{right}{cross}{full}{semi}{outer}{inner}{anti}"


@visit.register
def _(ctx: sbp.JoinRelationContext):
    kind = visit(ctx.joinType())
    lateral = bool(ctx.LATERAL())
    natural = bool(ctx.NATURAL())
    criteria = None
    right = visit(ctx.right)
    if join_criteria := ctx.joinCriteria():
        criteria = visit(join_criteria)
    return ast.Join(kind, right, criteria=criteria, lateral=lateral, natural=natural)


@visit.register
def _(ctx: sbp.TableValuedFunctionContext):
    return visit(ctx.functionTable())


@visit.register
def _(ctx: sbp.FunctionTableContext):
    name = visit(ctx.funcName)
    args = visit(ctx.expression())
    alias, cols = visit(ctx.tableAlias())
    return ast.FunctionTable(
        name,
        args=args,
        column_list=[ast.Column(name) for name in cols],
    ).set_alias(alias)


@visit.register
def _(ctx: sbp.TableAliasContext):
    if ident := ctx.strictIdentifier():
        identifier_list = visit(ctx.identifierList()) if ctx.identifierList() else []
        return visit(ident), identifier_list
    return None, []


@visit.register
def _(ctx: sbp.IdentifierListContext):
    return visit(ctx.identifierSeq())


@visit.register
def _(ctx: sbp.IdentifierSeqContext):
    return visit(ctx.ident)


@visit.register
def _(ctx: sbp.FromClauseContext):
    relations = visit(ctx.relation())
    laterals = visit(ctx.lateralView())
    if ctx.pivotClause() or ctx.unpivotClause():
        return
    return ast.From(relations, laterals)


@visit.register
def _(ctx: sbp.JoinCriteriaContext):
    if expr := ctx.booleanExpression():
        return ast.JoinCriteria(on=visit(expr))
    return ast.JoinCriteria(using=(visit(ctx.identifierList())))


@visit.register
def _(ctx: sbp.ComparisonContext):
    left, right = visit(ctx.left), visit(ctx.right)
    op = visit(ctx.comparisonOperator())
    return ast.BinaryOp(op, left, right)


@visit.register
def _(ctx: sbp.ComparisonOperatorContext):
    return ctx.getText()


@visit.register
def _(ctx: sbp.WhereClauseContext):
    return visit(ctx.booleanExpression())


@visit.register
def _(ctx: sbp.StringLiteralContext):
    return ast.String(ctx.getText())


@visit.register
def _(ctx: sbp.CtesContext) -> List[ast.Select]:
    names = {}
    ctes = []
    for namedQuery in ctx.namedQuery():
        if namedQuery.name in names:
            raise SqlSyntaxError(f"Duplicate CTE definition names: {namedQuery.name}")
        query = visit(namedQuery.query())
        query = query.set_alias(visit(namedQuery.name))
        if namedQuery.AS():
            query = query.set_as(True)
        query.parenthesized = True
        ctes.append(query)
    return ctes


@visit.register
def _(ctx: sbp.NamedQueryContext) -> ast.Select:
    return visit(ctx.query().queryTerm())


@visit.register
def _(ctx: sbp.LogicalBinaryContext) -> ast.BinaryOp:
    return ast.BinaryOp(ctx.operator.text, visit(ctx.left), visit(ctx.right))


@visit.register
def _(ctx: sbp.SubqueryExpressionContext):
    return visit(ctx.query().queryTerm())


@visit.register
def _(ctx: sbp.SetOperationContext):
    operator = ctx.operator.text
    quantifier = f" {visit(ctx.setQuantifier())}" if ctx.setQuantifier() else ""
    return ast.SetOp(
        left=visit(ctx.left),
        kind=f"{operator}{quantifier}",
        right=visit(ctx.right),
    )


@visit.register
def _(ctx: sbp.AliasedQueryContext) -> ast.Select:
    query = visit(ctx.query())
    query.parenthesized = True
    table_alias = ctx.tableAlias()
    ident, _ = visit(table_alias)
    if ident:
        query = query.set_alias(ident)
    if table_alias.AS():
        query = query.set_as(True)
    return query


@visit.register
def _(ctx: sbp.SimpleCaseContext) -> ast.Case:
    expr = visit(ctx.value)
    conditions = []
    results = []
    for when in ctx.whenClause():
        condition, result = visit(when)
        conditions.append(condition)
        results.append(result)
    return ast.Case(
        expr,
        conditions=conditions,
        else_result=visit(ctx.elseExpression) if ctx.elseExpression else None,
        results=results,
    )


@visit.register
def _(ctx: sbp.SearchedCaseContext) -> ast.Case:
    conditions = []
    results = []
    for when in ctx.whenClause():
        condition, result = visit(when)
        conditions.append(condition)
        results.append(result)
    return ast.Case(
        None,
        conditions=conditions,
        else_result=visit(ctx.elseExpression) if ctx.elseExpression else None,
        results=results,
    )


@visit.register
def _(ctx: sbp.WhenClauseContext) -> Tuple[ast.Expression, ast.Expression]:
    condition, result = visit(ctx.condition), visit(ctx.result)
    return condition, result


@visit.register
def _(ctx: sbp.ParenthesizedExpressionContext) -> ast.Expression:
    expr = visit(ctx.expression())
    expr.parenthesized = True
    return expr


@visit.register
def _(ctx: sbp.NullLiteralContext) -> ast.Null:
    return ast.Null()


@visit.register
def _(ctx: sbp.CastContext) -> ast.Cast:
    data_type = visit(ctx.dataType())
    expression = visit(ctx.expression())
    return ast.Cast(data_type=data_type, expression=expression)


@visit.register
def _(ctx: sbp.PrimitiveDataTypeContext) -> ast.Value:
    left_paren = ctx.LEFT_PAREN() or ""
    right_paren = ctx.RIGHT_PAREN() or ""
    ident = visit(ctx.identifier())
    integer_values = (
        ",".join(str(value) for value in ctx.INTEGER_VALUE())
        if ctx.INTEGER_VALUE()
        else ""
    )
    return ast.Name(f"{ident} {left_paren}{integer_values}{right_paren}")


@visit.register
def _(ctx: sbp.ExistsContext) -> ast.UnaryOp:
    expr = visit(ctx.query().queryTerm())
    expr.parenthesized = True
    return ast.UnaryOp(op="EXISTS", expr=expr)


@visit.register
def _(ctx: sbp.LogicalNotContext) -> ast.UnaryOp:
    return ast.UnaryOp(op="NOT", expr=visit(ctx.booleanExpression()))


@visit.register
def _(ctx: sbp.IntervalLiteralContext) -> ast.Interval:
    return visit(ctx.interval())


@visit.register
def _(ctx: sbp.SubqueryContext):
    return visit(ctx.query().queryTerm())


@visit.register
def _(ctx: sbp.StructContext) -> ast.Struct:
    return ast.Struct(visit(ctx.argument))


@visit.register
def _(ctx: sbp.IntervalContext) -> ast.Interval:
    return visit(ctx.children[1])


@visit.register
def _(ctx: sbp.ErrorCapturingMultiUnitsIntervalContext) -> ast.Interval:
    from_ = visit(ctx.body)
    to = None
    if utu := ctx.unitToUnitInterval():
        to = visit(utu)
        interval = ast.Interval(from_ + to.from_, to.to)
    else:
        interval = ast.Interval(from_)
    return interval


@visit.register
def _(ctx: sbp.UnitToUnitIntervalContext) -> ast.Interval:
    value = visit(ctx.value)
    from_ = visit(ctx.from_)
    to = visit(ctx.to)
    return ast.Interval([ast.IntervalUnit(from_, value)], ast.IntervalUnit(to))


@visit.register
def _(ctx: sbp.MultiUnitsIntervalContext) -> List[ast.IntervalUnit]:
    units = []
    for pair_i in range(0, len(ctx.children), 2):
        value, unit = ctx.children[pair_i : pair_i + 2]
        value = visit(value)
        unit = visit(unit)
        units.append(ast.IntervalUnit(unit, value))
    return units


@visit.register
def _(ctx: sbp.ErrorCapturingUnitToUnitIntervalContext) -> ast.Interval:
    if ctx.error1 or ctx.error2:
        raise SqlSyntaxError(
            f"{ctx.start.line}:{ctx.start.column} Error capturing unit-to-unit interval.",
        )
    return visit(ctx.body)


@visit.register
def _(ctx: sbp.IntervalValueContext) -> ast.Number:
    if stringlit_ctx := ctx.stringLit():
        return ast.Number(visit(stringlit_ctx))
    return ast.Number(ctx.getText())


@visit.register
def _(ctx: sbp.UnitInMultiUnitsContext) -> str:
    return ctx.getText().upper().rstrip("S")


@visit.register
def _(ctx: sbp.UnitInUnitToUnitContext) -> str:
    return ctx.getText().upper()


@visit.register
def _(ctx: sbp.TrimContext) -> ast.Function:
    both = "BOTH " if ctx.BOTH() else ""
    leading = "LEADING " if ctx.LEADING() else ""
    trailing = "TRAILING " if ctx.TRAILING() else ""
    from_ = "FROM " if ctx.FROM() else ""
    return ast.Function(
        "TRIM",
        [visit(ctx.srcStr)],
        f"{both or leading or trailing}{from_}",
    )


@visit.register
def _(ctx: sbp.LambdaContext) -> ast.Function:
    identifier = visit(ctx.identifier())
    expr = visit(ctx.expression())
    return ast.Lambda(identifiers=identifier, expr=expr)


def parse(sql: str) -> ast.Query:
    """
    Parse a string into a DJ ast using the ANTLR4 backend.
    """
    tree = parse_statement(sql)
    query = visit(tree)
    return query
