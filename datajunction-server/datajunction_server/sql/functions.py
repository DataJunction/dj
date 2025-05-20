# mypy: ignore-errors

"""
SQL functions for type inference.

This file holds all the functions that we want to support in the SQL used to define
nodes. The functions are used to infer types.

Spark function reference
https://github.com/apache/spark/tree/74cddcfda3ac4779de80696cdae2ba64d53fc635/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions

Java strictmath reference
https://docs.oracle.com/javase/8/docs/api/java/lang/StrictMath.html

Databricks reference:
https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin-alpha.html
"""

import inspect
import re
from itertools import zip_longest
from typing import (
    TYPE_CHECKING,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
    get_origin,
)

import datajunction_server.sql.parsing.types as ct
from datajunction_server.errors import (
    DJError,
    DJInvalidInputException,
    DJNotImplementedException,
    ErrorCode,
)
from datajunction_server.models.engine import Dialect
from datajunction_server.sql.parsing.backends.exceptions import DJParseException
from datajunction_server.utils import get_settings

if TYPE_CHECKING:
    from datajunction_server.sql.parsing.ast import Expression


def compare_registers(types, register) -> bool:
    """
    Comparing registers
    """
    for (type_a, register_a), (type_b, register_b) in zip_longest(
        types,
        register,
        fillvalue=(-1, None),
    ):
        if type_b == -1 and register_b is None:
            if register and register[-1] and register[-1][0] == -1:  # args
                register_b = register[-1][1]
            else:
                return False  # pragma: no cover
        if type_a == -1:
            register_a = type(register_a)
        if not issubclass(register_a, register_b):  # type: ignore
            return False
    return True


class DispatchMeta(type):
    """
    Dispatch abstract class for function registry
    """

    def __getattribute__(cls, func_name):
        if func_name in type.__getattribute__(cls, "registry").get(cls, {}):

            def dynamic_dispatch(*args: "Expression"):
                return cls.dispatch(func_name, *args)(*args)

            return dynamic_dispatch
        return type.__getattribute__(cls, func_name)


class Dispatch(metaclass=DispatchMeta):
    """
    Function registry
    """

    registry: ClassVar[Dict[str, Dict[Tuple[Tuple[int, Type]], Callable]]] = {}

    @classmethod
    def register(cls, func):
        func_name = func.__name__
        params = inspect.signature(func).parameters
        spread_types = [[]]
        cls.registry[cls] = cls.registry.get(cls) or {}
        cls.registry[cls][func_name] = cls.registry[cls].get(func_name) or {}
        for i, (key, value) in enumerate(params.items()):
            name = str(value).split(":", maxsplit=1)[0]
            if name.startswith("**"):
                raise ValueError(
                    "kwargs are not supported in dispatch.",
                )  # pragma: no cover
            if name.startswith("*"):
                i = -1
            type_ = params[key].annotation
            if type_ == inspect.Parameter.empty:
                raise ValueError(  # pragma: no cover
                    "All arguments must have a type annotation.",
                )
            inner_types = [type_]
            if get_origin(type_) == Union:
                inner_types = type_.__args__
                for _ in inner_types:
                    spread_types += spread_types[:]
            temp = []
            for type_ in inner_types:
                for types in spread_types:
                    temp.append(types[:])
                    temp[-1].append((i, type_))
            spread_types = temp
        for types in spread_types:
            cls.registry[cls][func_name][tuple(types)] = func  # type: ignore
        return func

    @classmethod
    def dispatch(cls, func_name, *args: "Expression"):
        type_registry = cls.registry[cls].get(func_name)  # type: ignore
        if not type_registry:
            raise ValueError(
                f"No function registered on {cls.__name__}`{func_name}`.",
            )  # pragma: no cover

        type_list = []
        for i, arg in enumerate(args):
            type_list.append(
                (i, type(arg.type) if hasattr(arg, "type") else type(arg)),
            )

        types = tuple(type_list)

        if types in type_registry:  # type: ignore
            return type_registry[types]  # type: ignore

        for register, func in type_registry.items():  # type: ignore
            if compare_registers(types, register):
                return func

        raise TypeError(
            f"`{cls.__name__}.{func_name}` got an invalid "
            "combination of types: "
            f"{', '.join(str(t[1].__name__) for t in types)}",
        )


class Function(Dispatch):
    """
    A DJ function.
    """

    is_aggregation: ClassVar[bool] = False
    is_runtime: ClassVar[bool] = False
    dialects: List[Dialect] = [Dialect.SPARK]

    @staticmethod
    def infer_type(*args) -> ct.ColumnType:
        raise NotImplementedError()

    @staticmethod
    def compile_lambda(*args):
        """
        Compiles the lambda function used by a given Spark function that takes
        a lambda function. This allows us to evaluate the lambda's expression and
        determine the result's type.
        """


class TableFunction(Dispatch):
    """
    A DJ table-valued function.
    """

    @staticmethod
    def infer_type(*args) -> List[ct.ColumnType]:
        raise NotImplementedError()


class DjLogicalTimestamp(Function):
    """
    A special function that returns the "current" timestamp as a string based on the
    specified format. Used for incrementally materializing nodes, where "current" refers
    to the timestamp associated with the given partition that's being processed.
    """

    is_runtime = True

    @staticmethod
    def substitute():
        settings = get_settings()
        return settings.dj_logical_timestamp_format


@DjLogicalTimestamp.register  # type: ignore
def infer_type() -> ct.StringType:
    """
    Defaults to returning a timestamp in the format %Y-%m-%d %H:%M:%S
    """
    return ct.StringType()


@DjLogicalTimestamp.register  # type: ignore
def infer_type(_: ct.StringType) -> ct.StringType:
    """
    This function can optionally take a datetime format string like:
    DJ_CURRENT_TIMESTAMP('%Y-%m-%d')
    """
    return ct.StringType()


#####################
# Regular Functions #
#####################


class Abs(Function):
    """
    Returns the absolute value of the numeric or interval value.
    """

    is_aggregation = True
    dialects = [Dialect.SPARK, Dialect.DRUID]


@Abs.register
def infer_type(
    arg: ct.NumberType,
) -> ct.NumberType:
    type_ = arg.type
    return type_


class Acos(Function):
    """
    Returns the inverse cosine (a.k.a. arc cosine) of expr
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Acos.register
def infer_type(
    arg: ct.NumberType,
) -> ct.FloatType:
    return ct.FloatType()


class Aggregate(Function):
    """
    Applies a binary operator to an initial state and all elements in the array,
    and reduces this to a single state. The final state is converted into the
    final result by applying a finish function.
    """

    @staticmethod
    def compile_lambda(*args):
        """
        Compiles the lambda function used by the `aggregate` Spark function so that
        the lambda's expression can be evaluated to determine the result's type.
        """
        from datajunction_server.sql.parsing import ast

        # aggregate's lambda function can take three or four arguments, depending on whether
        # an optional finish function is provided
        if len(args) == 4:
            expr, start, merge, finish = args
        else:
            expr, start, merge = args
            finish = None

        available_identifiers = {
            identifier.name: idx for idx, identifier in enumerate(merge.identifiers)
        }
        merge_columns = list(
            merge.expr.filter(
                lambda x: isinstance(x, ast.Column)
                and x.alias_or_name.name in available_identifiers,
            ),
        )
        finish_columns = (
            list(
                finish.expr.filter(
                    lambda x: isinstance(x, ast.Column)
                    and x.alias_or_name.name in available_identifiers,
                ),
            )
            if finish
            else []
        )
        for col in merge_columns:
            if available_identifiers.get(col.alias_or_name.name) == 0:
                col.add_type(start.type)
            if available_identifiers.get(col.alias_or_name.name) == 1:
                col.add_type(expr.type.element.type)
        for col in finish_columns:
            if (
                available_identifiers.get(col.alias_or_name.name) == 0
            ):  # pragma: no cover
                col.add_type(start.type)


@Aggregate.register  # type: ignore
def infer_type(
    expr: ct.ListType,
    start: ct.ColumnType,
    merge: ct.ColumnType,
) -> ct.ColumnType:
    return merge.expr.type


class AnyValue(Function):
    """
    Returns any value of the specified expression
    """

    is_aggregation = True
    dialects = [Dialect.SPARK, Dialect.DRUID]


@AnyValue.register
def infer_type(
    expr: ct.ColumnType,
) -> ct.ColumnType:
    return expr.type  # type: ignore


class ApproxCountDistinct(Function):
    """
    approx_count_distinct(expr)
    """

    is_aggregation = True
    dialects = [Dialect.SPARK, Dialect.DRUID]


@ApproxCountDistinct.register
def infer_type(
    expr: ct.ColumnType,
) -> ct.LongType:
    return ct.LongType()


class ApproxCountDistinctDsHll(Function):
    """
    Counts distinct values of an HLL sketch column or a regular column
    """

    is_aggregation = True
    dialects = [Dialect.DRUID]


@ApproxCountDistinctDsHll.register
def infer_type(
    expr: ct.ColumnType,
) -> ct.LongType:
    return ct.LongType()


class ApproxCountDistinctDsTheta(Function):
    """
    Counts distinct values of a Theta sketch column or a regular column.
    """

    is_aggregation = True
    dialects = [Dialect.DRUID]


@ApproxCountDistinctDsTheta.register
def infer_type(
    expr: ct.ColumnType,
) -> ct.LongType:
    return ct.LongType()


class ApproxPercentile(Function):
    """
    approx_percentile(col, percentage [, accuracy]) -
    Returns the approximate percentile of the numeric or ansi interval
    column col which is the smallest value in the ordered col values
    """

    is_aggregation = True


@ApproxPercentile.register
def infer_type(
    col: ct.NumberType,
    percentage: ct.ListType,
    accuracy: Optional[ct.NumberType],
) -> ct.DoubleType:
    return ct.ListType(element_type=col.type)  # type: ignore


@ApproxPercentile.register
def infer_type(
    col: ct.NumberType,
    percentage: ct.FloatType,
    accuracy: Optional[ct.NumberType],
) -> ct.NumberType:
    return col.type  # type: ignore


class Array(Function):
    """
    Returns an array of constants
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Array.register  # type: ignore
def infer_type(
    *elements: ct.ColumnType,
) -> ct.ListType:
    types = {element.type for element in elements if element.type != ct.NullType()}
    if len(types) > 1:
        raise DJParseException(
            f"Multiple types {', '.join(sorted(str(typ) for typ in types))} passed to array.",
        )
    element_type = elements[0].type if elements else ct.NullType()
    return ct.ListType(element_type=element_type)


@Array.register  # type: ignore
def infer_type() -> ct.ListType:
    return ct.ListType(element_type=ct.NullType())


class ArrayAgg(Function):
    """
    Collects and returns a list of non-unique elements.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@ArrayAgg.register  # type: ignore
def infer_type(
    *elements: ct.ColumnType,
) -> ct.ListType:
    types = {element.type for element in elements}
    if len(types) > 1:  # pragma: no cover
        raise DJParseException(
            f"Multiple types {', '.join(sorted(str(typ) for typ in types))} passed to array.",
        )
    element_type = elements[0].type if elements else ct.NullType()
    return ct.ListType(element_type=element_type)


class ArrayAppend(Function):
    """
    Add the element at the end of the array passed as first argument
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@ArrayAppend.register  # type: ignore
def infer_type(
    array: ct.ListType,
    item: ct.ColumnType,
) -> ct.ListType:
    return ct.ListType(element_type=item.type)


class ArrayCompact(Function):
    """
    array_compact(array) - Removes null values from the array.
    """


@ArrayCompact.register
def infer_type(
    array: ct.ListType,
) -> ct.ListType:
    return array.type


class ArrayConcat(Function):
    """
    array_concat(arr1, arr2)
    Concatenates arr2 to arr1. The resulting array type is determined by the type of arr1.
    """

    dialects = [Dialect.DRUID]


@ArrayConcat.register
def infer_type(
    arr1: ct.ListType,
    arr2: ct.ListType,
) -> ct.ListType:
    return arr1.type


class ArrayContains(Function):
    """
    array_contains(array, value) - Returns true if the array contains the value.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@ArrayContains.register
def infer_type(
    array: ct.ListType,
    element: ct.ColumnType,
) -> ct.BooleanType:
    return ct.BooleanType()


class ArrayDistinct(Function):
    """
    array_distinct(array) - Removes duplicate values from the array.
    """


@ArrayDistinct.register
def infer_type(
    array: ct.ListType,
) -> ct.ListType:
    return array.type


class ArrayExcept(Function):
    """
    array_except(array1, array2) - Returns an array of the elements in
    array1 but not in array2, without duplicates.
    """


@ArrayExcept.register
def infer_type(
    array1: ct.ListType,
    array2: ct.ListType,
) -> ct.ListType:
    return array1.type


class ArrayLength(Function):
    """
    array_length(expr) - Returns the size of an array. The function returns null for null input.
    """

    dialects = [Dialect.DRUID]


@ArrayLength.register
def infer_type(
    array: ct.ListType,
) -> ct.LongType:
    return ct.LongType()


class ArrayIntersect(Function):
    """
    array_intersect(array1, array2) - Returns an array of the
    elements in the intersection of array1 and array2, without duplicates.
    """


@ArrayIntersect.register
def infer_type(
    array1: ct.ListType,
    array2: ct.ListType,
) -> ct.ListType:
    return array1.type


class ArrayJoin(Function):
    """
    array_join(array, delimiter[, nullReplacement]) - Concatenates
    the elements of the given array using the delimiter and an
    optional string to replace nulls.
    """


@ArrayJoin.register
def infer_type(
    array: ct.ListType,
    delimiter: ct.StringType,
) -> ct.StringType:
    return ct.StringType()


@ArrayJoin.register
def infer_type(
    array: ct.ListType,
    delimiter: ct.StringType,
    null_replacement: ct.StringType,
) -> ct.StringType:
    return ct.StringType()


class ArrayMax(Function):
    """
    array_max(array) - Returns the maximum value in the array. NaN is
    greater than any non-NaN elements for double/float type. NULL
    elements are skipped.
    """


@ArrayMax.register
def infer_type(
    array: ct.ListType,
) -> ct.NumberType:
    return array.type.element.type


class ArrayMin(Function):
    """
    array_min(array) - Returns the minimum value in the array. NaN is greater than
    any non-NaN elements for double/float type. NULL elements are skipped.
    """


@ArrayMin.register
def infer_type(
    array: ct.ListType,
) -> ct.NumberType:
    return array.type.element.type


class ArrayOffset(Function):
    """
    ARRAY_OFFSET(arr, long)
    Returns the array element at the 0-based index supplied
    """

    dialects = [Dialect.DRUID]


@ArrayOffset.register
def infer_type(
    array: ct.ListType,
    index: Union[ct.LongType, ct.IntegerType],
) -> ct.NumberType:
    return array.type.element.type  # type: ignore


class ArrayOrdinal(Function):
    """
    ARRAY_ORDINAL(arr, long)
    Returns the array element at the 1-based index supplied
    """

    dialects = [Dialect.DRUID]


@ArrayOrdinal.register
def infer_type(
    array: ct.ListType,
    index: Union[ct.LongType, ct.IntegerType],
) -> ct.NumberType:
    return array.type.element.type  # type: ignore


class ArrayPosition(Function):
    """
    array_position(array, element) - Returns the (1-based) index of the first
    element of the array as long.
    """


@ArrayPosition.register
def infer_type(
    array: ct.ListType,
    element: ct.ColumnType,
) -> ct.LongType:
    return ct.LongType()


class ArrayRemove(Function):
    """
    array_remove(array, element) - Remove all elements that equal to element from array.
    """


@ArrayRemove.register
def infer_type(
    array: ct.ListType,
    element: ct.ColumnType,
) -> ct.ListType:
    return array.type


class ArrayRepeat(Function):
    """
    array_repeat(element, count) - Returns the array containing element count times.
    """


@ArrayRepeat.register
def infer_type(
    element: ct.ColumnType,
    count: ct.IntegerType,
) -> ct.ListType:
    return ct.ListType(element_type=element.type)


class ArraySize(Function):
    """
    array_size(expr) - Returns the size of an array. The function returns null for null input.
    """


@ArraySize.register
def infer_type(
    array: ct.ListType,
) -> ct.LongType:
    return ct.LongType()


class ArraySort(Function):
    """
    array_sort(expr, func) - Sorts the input array
    """


@ArraySort.register
def infer_type(
    array: ct.ListType,
) -> ct.ListType:
    return array.type


@ArraySort.register
def infer_type(
    array: ct.ListType,
    sort_func: ct.LambdaType,
) -> ct.ListType:  # pragma: no cover
    return array.type


class ArrayUnion(Function):
    """
    array_union(array1, array2) - Returns an array of the elements
    in the union of array1 and array2, without duplicates.
    """


@ArrayUnion.register
def infer_type(
    array1: ct.ListType,
    array2: ct.ListType,
) -> ct.ListType:
    return array1.type


class ArraysOverlap(Function):
    """
    arrays_overlap(a1, a2) - Returns true if a1 contains at least a
    non-null element present also in a2.
    """


@ArraysOverlap.register
def infer_type(
    array1: ct.ListType,
    array2: ct.ListType,
) -> ct.ListType:
    return ct.BooleanType()


class Avg(Function):
    """
    Computes the average of the input column or expression.
    """

    is_aggregation = True
    dialects = [Dialect.SPARK, Dialect.DRUID]


@Avg.register
def infer_type(
    arg: ct.DecimalType,
) -> ct.DecimalType:
    type_ = arg.type
    return ct.DecimalType(type_.precision + 4, type_.scale + 4)


@Avg.register
def infer_type(
    arg: ct.IntervalTypeBase,
) -> ct.IntervalTypeBase:
    return type(arg.type)()


@Avg.register  # type: ignore
def infer_type(
    arg: ct.NumberType,
) -> ct.DoubleType:
    return ct.DoubleType()


@Avg.register  # type: ignore
def infer_type(
    arg: ct.DateTimeBase,
) -> ct.DateTimeBase:
    return type(arg.type)()


class Cardinality(Function):
    """
    Returns the size of an array or a map.
    """


@Cardinality.register  # type: ignore
def infer_type(
    args: ct.ListType,
) -> ct.IntegerType:
    return ct.IntegerType()


@Cardinality.register  # type: ignore
def infer_type(
    args: ct.MapType,
) -> ct.IntegerType:
    return ct.IntegerType()


class Cbrt(Function):
    """
    cbrt(expr) - Computes the cube root of the value expr.
    """


@Cbrt.register  # type: ignore
def infer_type(
    arg: ct.NumberType,
) -> ct.ColumnType:
    return ct.FloatType()


class Ceil(Function):
    """
    Computes the smallest integer greater than or equal to the input value.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


class Ceiling(Function):
    """
    ceiling(expr[, scale]) - Returns the smallest number after rounding up that is not smaller
    than expr. An optional scale parameter can be specified to control the rounding behavior.
    """


@Ceil.register
@Ceiling.register
def infer_type(
    args: ct.NumberType,
    _target_scale: ct.IntegerType,
) -> ct.DecimalType:
    target_scale = _target_scale.value
    if isinstance(args.type, ct.DecimalType):
        precision = max(args.type.precision - args.type.scale + 1, -target_scale + 1)
        scale = min(args.type.scale, max(0, target_scale))
        return ct.DecimalType(precision, scale)
    if args.type == ct.TinyIntType():
        precision = max(3, -target_scale + 1)
        return ct.DecimalType(precision, 0)
    if args.type == ct.SmallIntType():
        precision = max(5, -target_scale + 1)
        return ct.DecimalType(precision, 0)
    if args.type == ct.IntegerType():
        precision = max(10, -target_scale + 1)
        return ct.DecimalType(precision, 0)
    if args.type == ct.BigIntType():
        precision = max(20, -target_scale + 1)
        return ct.DecimalType(precision, 0)
    if args.type == ct.FloatType():
        precision = max(14, -target_scale + 1)
        scale = min(7, max(0, target_scale))
        return ct.DecimalType(precision, scale)
    if args.type == ct.DoubleType():
        precision = max(30, -target_scale + 1)
        scale = min(15, max(0, target_scale))
        return ct.DecimalType(precision, scale)

    raise DJParseException(  # pragma: no cover
        f"Unhandled numeric type in Ceil `{args.type}`",
    )


@Ceil.register
@Ceiling.register
def infer_type(
    args: ct.DecimalType,
) -> ct.DecimalType:
    return ct.DecimalType(args.type.precision - args.type.scale + 1, 0)


@Ceil.register
@Ceiling.register
def infer_type(
    args: ct.NumberType,
) -> ct.BigIntType:
    return ct.BigIntType()


class Char(Function):
    """
    char(expr) - Returns the ASCII character having the binary equivalent to expr.
    """


@Char.register  # type: ignore
def infer_type(
    arg: ct.IntegerType,
) -> ct.ColumnType:
    return ct.StringType()


class CharLength(Function):
    """
    char_length(expr) - Returns the length of the value expr.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@CharLength.register  # type: ignore
def infer_type(arg: ct.StringType) -> ct.ColumnType:
    return ct.IntegerType()


class CharacterLength(Function):
    """
    character_length(expr) - Returns the length of the value expr.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@CharacterLength.register  # type: ignore
def infer_type(arg: ct.StringType) -> ct.ColumnType:
    return ct.IntegerType()


class Chr(Function):
    """
    chr(expr) - Returns the ASCII character having the binary equivalent to expr.
    """


@Chr.register  # type: ignore
def infer_type(arg: ct.IntegerType) -> ct.ColumnType:
    return ct.StringType()


class Coalesce(Function):
    """
    Computes the average of the input column or expression.
    """

    is_aggregation = False
    dialects = [Dialect.SPARK, Dialect.DRUID]


@Coalesce.register  # type: ignore
def infer_type(
    *args: ct.ColumnType,
) -> ct.ColumnType:
    if not args:  # pragma: no cover
        raise DJInvalidInputException(
            message="Wrong number of arguments to function",
            errors=[
                DJError(
                    code=ErrorCode.INVALID_ARGUMENTS_TO_FUNCTION,
                    message="You need to pass at least one argument to `COALESCE`.",
                ),
            ],
        )
    for arg in args:
        if arg.type != ct.NullType():
            return arg.type
    return ct.NullType()


class CollectList(Function):
    """
    Collects and returns a list of non-unique elements.
    """

    is_aggregation = True


@CollectList.register
def infer_type(
    arg: ct.ColumnType,
) -> ct.ColumnType:
    return ct.ListType(element_type=arg.type)


class CollectSet(Function):
    """
    Collects and returns a list of unique elements.
    """

    is_aggregation = True


@CollectSet.register
def infer_type(
    arg: ct.ColumnType,
) -> ct.ColumnType:
    return ct.ListType(element_type=arg.type)


class Concat(Function):
    """
    concat(col1, col2, ..., colN) - Returns the concatenation of col1, col2, ..., colN.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Concat.register  # type: ignore
def infer_type(
    *strings: ct.StringType,
) -> ct.StringType:
    return ct.StringType()


@Concat.register  # type: ignore
def infer_type(
    *arrays: ct.ListType,
) -> ct.ListType:
    return arrays[0].type


@Concat.register  # type: ignore
def infer_type(
    *maps: ct.MapType,
) -> ct.MapType:
    return maps[0].type


class ConcatWs(Function):
    """
    concat_ws(separator, [str | array(str)]+) - Returns the concatenation of the
    strings separated by separator.
    """


@ConcatWs.register  # type: ignore
def infer_type(
    sep: ct.StringType,
    *strings: ct.StringType,
) -> ct.ColumnType:
    return ct.StringType()


@ConcatWs.register  # type: ignore
def infer_type(
    sep: ct.StringType,
    *strings: ct.ListType,
) -> ct.ColumnType:
    return ct.StringType()


class Contains(Function):
    """
    contains(left, right) - Returns a boolean. The value is True if right is found inside left.
    Returns NULL if either input expression is NULL. Otherwise, returns False. Both left or
    right must be of STRING or BINARY type.
    """


@Contains.register  # type: ignore
def infer_type(arg1: ct.StringType, arg2: ct.StringType) -> ct.ColumnType:
    return ct.BooleanType()


class ContainsString(Function):
    """
    contains_string(left, right) - Returns a boolean
    """

    dialects = [Dialect.DRUID]


@ContainsString.register  # type: ignore
def infer_type(arg1: ct.StringType, arg2: ct.StringType) -> ct.ColumnType:
    return ct.BooleanType()


@Contains.register  # type: ignore
def infer_type(
    arg1: ct.BinaryType,
    arg2: ct.BinaryType,
) -> ct.ColumnType:  # pragma: no cover
    return ct.BooleanType()


class Conv(Function):
    """
    conv(expr, from_base, to_base) - Convert the number expr from from_base to to_base.
    """


@Conv.register  # type: ignore
def infer_type(
    arg1: ct.NumberType,
    arg2: ct.IntegerType,
    arg3: ct.IntegerType,
) -> ct.ColumnType:
    return ct.StringType()


@Conv.register  # type: ignore
def infer_type(
    arg1: ct.StringType,
    arg2: ct.IntegerType,
    arg3: ct.IntegerType,
) -> ct.ColumnType:
    return ct.StringType()


class ConvertTimezone(Function):
    """
    convert_timezone(from_tz, to_tz, timestamp) - Convert timestamp from from_tz to to_tz.
    Spark 3.4+
    """


@ConvertTimezone.register  # type: ignore
def infer_type(
    arg1: ct.StringType,
    arg2: ct.StringType,
    arg3: ct.TimestampType,
) -> ct.ColumnType:
    return ct.TimestampType()


class Corr(Function):
    """
    corr(expr1, expr2) - Compute the correlation of expr1 and expr2.
    """


@Corr.register  # type: ignore
def infer_type(
    arg1: ct.NumberType,
    arg2: ct.NumberType,
) -> ct.ColumnType:
    return ct.FloatType()


class Cos(Function):
    """
    cos(expr) - Compute the cosine of expr.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Cos.register  # type: ignore
def infer_type(arg: ct.NumberType) -> ct.ColumnType:
    return ct.FloatType()


class Cosh(Function):
    """
    cosh(expr) - Compute the hyperbolic cosine of expr.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Cosh.register  # type: ignore
def infer_type(arg: ct.NumberType) -> ct.ColumnType:
    return ct.FloatType()


class Cot(Function):
    """
    cot(expr) - Compute the cotangent of expr.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Cot.register  # type: ignore
def infer_type(arg: ct.NumberType) -> ct.ColumnType:
    return ct.FloatType()


class Count(Function):
    """
    Counts the number of non-null values in the input column or expression.
    """

    is_aggregation = True
    dialects = [Dialect.SPARK, Dialect.DRUID]


@Count.register  # type: ignore
def infer_type(
    *args: ct.ColumnType,
) -> ct.BigIntType:
    return ct.BigIntType()


class CountIf(Function):
    """
    count_if(expr) - Returns the number of true values in expr.
    """

    is_aggregation = True


@CountIf.register  # type: ignore
def infer_type(arg: ct.BooleanType) -> ct.IntegerType:
    return ct.IntegerType()  # pragma: no cover


class CountMinSketch(Function):
    """
    count_min_sketch(col, eps, confidence, seed) - Creates a Count-Min sketch of col.
    """


@CountMinSketch.register  # type: ignore
def infer_type(
    arg1: ct.ColumnType,
    arg2: ct.FloatType,
    arg3: ct.FloatType,
    arg4: ct.IntegerType,
) -> ct.ColumnType:
    return ct.BinaryType()


class CovarPop(Function):
    """
    covar_pop(expr1, expr2) - Returns the population covariance of expr1 and expr2.
    """


@CovarPop.register  # type: ignore
def infer_type(
    arg1: ct.NumberType,
    arg2: ct.NumberType,
) -> ct.ColumnType:
    return ct.FloatType()


class CovarSamp(Function):
    """
    covar_samp(expr1, expr2) - Returns the sample covariance of expr1 and expr2.
    """


@CovarSamp.register  # type: ignore
def infer_type(arg1: ct.NumberType, arg2: ct.NumberType) -> ct.ColumnType:
    return ct.FloatType()


class Crc32(Function):
    """
    crc32(expr) - Computes a cyclic redundancy check value and returns the result as a bigint.
    """


@Crc32.register  # type: ignore
def infer_type(arg: ct.StringType) -> ct.ColumnType:
    return ct.BigIntType()


class Csc(Function):
    """
    csc(expr) - Computes the cosecant of expr.
    """


@Csc.register  # type: ignore
def infer_type(arg: ct.NumberType) -> ct.ColumnType:
    return ct.FloatType()


class CumeDist(Function):
    """
    cume_dist() - Computes the cumulative distribution of a value within a group of values.
    """


@CumeDist.register  # type: ignore
def infer_type() -> ct.ColumnType:
    return ct.FloatType()


class Curdate(Function):
    """
    curdate() - Returns the current date.
    """


@Curdate.register  # type: ignore
def infer_type() -> ct.ColumnType:
    return ct.DateType()


class CurrentCatalog(Function):
    """
    current_catalog() - Returns the current catalog.
    """


@CurrentCatalog.register  # type: ignore
def infer_type() -> ct.ColumnType:
    return ct.StringType()


class CurrentDatabase(Function):
    """
    current_database() - Returns the current database.
    """


@CurrentDatabase.register  # type: ignore
def infer_type() -> ct.ColumnType:
    return ct.StringType()


class CurrentDate(Function):
    """
    Returns the current date.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@CurrentDate.register  # type: ignore
def infer_type() -> ct.DateType:
    return ct.DateType()


class CurrentSchema(Function):
    """
    current_schema() - Returns the current schema.
    """


@CurrentSchema.register  # type: ignore
def infer_type() -> ct.ColumnType:
    return ct.StringType()


class CurrentTime(Function):
    """
    Returns the current time.
    """


@CurrentTime.register  # type: ignore
def infer_type() -> ct.TimeType:
    return ct.TimeType()


class CurrentTimestamp(Function):
    """
    Returns the current timestamp.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@CurrentTimestamp.register  # type: ignore
def infer_type() -> ct.TimestampType:
    return ct.TimestampType()


class CurrentTimezone(Function):
    """
    current_timezone() - Returns the current timezone.
    """


@CurrentTimezone.register  # type: ignore
def infer_type() -> ct.ColumnType:
    return ct.StringType()


class CurrentUser(Function):
    """
    current_user() - Returns the current user.
    """


@CurrentUser.register  # type: ignore
def infer_type() -> ct.ColumnType:
    return ct.StringType()


class Date(Function):
    """
    date(expr) - Converts expr to date.
    """


@Date.register  # type: ignore
def infer_type(arg: Union[ct.StringType, ct.TimestampType]) -> ct.ColumnType:
    return ct.DateType()


class DateAdd(Function):
    """
    date_add(date|timestamp|str, int) - Adds a specified number of days to a date.
    """

    dialects = [Dialect.SPARK]


@DateAdd.register  # type: ignore
def infer_type(
    start_date: ct.DateType,
    days: ct.IntegerBase,
) -> ct.DateType:
    return ct.DateType()


@DateAdd.register  # type: ignore
def infer_type(
    start_date: ct.TimestampType,
    days: ct.IntegerBase,
) -> ct.DateType:
    return ct.DateType()


@DateAdd.register  # type: ignore
def infer_type(
    start_date: ct.StringType,
    days: ct.IntegerBase,
) -> ct.DateType:
    return ct.DateType()


class Datediff(Function):
    """
    Computes the difference in days between two dates.
    """


@Datediff.register  # type: ignore
def infer_type(
    start_date: ct.DateType,
    end_date: ct.DateType,
) -> ct.IntegerType:
    return ct.IntegerType()


@Datediff.register  # type: ignore
def infer_type(
    start_date: ct.StringType,
    end_date: ct.StringType,
) -> ct.IntegerType:
    return ct.IntegerType()


@Datediff.register  # type: ignore
def infer_type(
    start_date: ct.TimestampType,
    end_date: ct.TimestampType,
) -> ct.IntegerType:
    return ct.IntegerType()


@Datediff.register  # type: ignore
def infer_type(
    start_date: ct.IntegerType,
    end_date: ct.IntegerType,
) -> ct.IntegerType:
    return ct.IntegerType()  # pragma: no cover


class DateFromUnixDate(Function):
    """
    date_from_unix_date(expr) - Converts the number of days from epoch (1970-01-01) to a date.
    """


@DateFromUnixDate.register  # type: ignore
def infer_type(arg: ct.IntegerType) -> ct.ColumnType:
    return ct.DateType()


class DateFormat(Function):
    """
    date_format(timestamp, fmt) - Converts timestamp to a value of string
    in the format specified by the date format fmt.
    """


@DateFormat.register  # type: ignore
def infer_type(
    timestamp: ct.TimestampType,
    fmt: ct.StringType,
) -> ct.StringType:
    return ct.StringType()


class DatePart(Function):
    """
    date_part(field, source) - Extracts a part of the date or time given.
    """


@DatePart.register  # type: ignore
def infer_type(
    arg1: ct.StringType,
    arg2: Union[ct.DateType, ct.TimestampType],
) -> ct.ColumnType:
    # The output can be integer, float, or string depending on the part extracted.
    # Here we assume the output is an integer for simplicity. Adjust as needed.
    return ct.IntegerType()


class DateSub(Function):
    """
    Subtracts a specified number of days from a date.
    """


@DateSub.register  # type: ignore
def infer_type(
    start_date: ct.DateType,
    days: ct.IntegerBase,
) -> ct.DateType:
    return ct.DateType()


@DateSub.register  # type: ignore
def infer_type(
    start_date: ct.StringType,
    days: ct.IntegerBase,
) -> ct.DateType:
    return ct.DateType()


class Day(Function):
    """
    Returns the day of the month for a specified date.
    """


@Day.register  # type: ignore
def infer_type(
    arg: Union[ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.IntegerType:  # type: ignore
    return ct.IntegerType()


class Dayofmonth(Function):
    """
    dayofmonth(date) - Extracts the day of the month of a given date.
    """


@Dayofmonth.register  # type: ignore
def infer_type(
    arg: Union[ct.DateType, ct.StringType],
) -> ct.ColumnType:
    return ct.IntegerType()


class Dayofweek(Function):
    """
    dayofweek(date) - Extracts the day of the week of a given date.
    """


@Dayofweek.register  # type: ignore
def infer_type(
    arg: Union[ct.DateType, ct.StringType],
) -> ct.ColumnType:
    return ct.IntegerType()


class Dayofyear(Function):
    """
    dayofyear(date) - Extracts the day of the year of a given date.
    """


@Dayofyear.register  # type: ignore
def infer_type(
    arg: Union[ct.DateType, ct.StringType],
) -> ct.ColumnType:
    return ct.IntegerType()


class Decimal(Function):
    """
    decimal(expr, precision, scale) - Converts expr to a decimal number.
    """


@Decimal.register  # type: ignore
def infer_type(
    arg1: Union[ct.IntegerType, ct.FloatType, ct.StringType],
) -> ct.ColumnType:
    return ct.DecimalType(8, 6)


class Decode(Function):
    """
    decode(bin, charset) - Decodes the first argument using the second argument
    character set.

    TODO: decode(expr, search, result [, search, result ] ... [, default]) - Compares
    expr to each search value in order. If expr is equal to a search value, decode
    returns the corresponding result. If no match is found, then it returns default.
    If default is omitted, it returns null.
    """


@Decode.register  # type: ignore
def infer_type(arg1: ct.BinaryType, arg2: ct.StringType) -> ct.ColumnType:
    return ct.StringType()


class Degrees(Function):
    """
    degrees(expr) - Converts radians to degrees.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Degrees.register  # type: ignore
def infer_type(arg: ct.NumberType) -> ct.ColumnType:
    return ct.FloatType()


class DenseRank(Function):
    """
    dense_rank() - Computes the dense rank of a value in a group of values.
    """


@DenseRank.register
def infer_type() -> ct.IntegerType:
    return ct.IntegerType()


@DenseRank.register
def infer_type(_: ct.ColumnType) -> ct.IntegerType:
    return ct.IntegerType()


class Div(Function):
    """
    expr1 div expr2 - Divide expr1 by expr2. It returns NULL if an operand is NULL or
    expr2 is 0. The result is casted to long.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Div.register
def infer_type(expr1: ct.NumberType, expr2: ct.NumberType) -> ct.LongType:
    return ct.LongType()


class Double(Function):
    """
    double(expr) - Converts expr to a double precision floating-point number.
    """


@Double.register  # type: ignore
def infer_type(
    arg: Union[ct.IntegerType, ct.FloatType, ct.StringType],
) -> ct.ColumnType:
    return ct.DoubleType()


class E(Function):
    """
    e() - Returns the mathematical constant e.
    """


@E.register  # type: ignore
def infer_type() -> ct.ColumnType:
    return ct.FloatType()


class ElementAt(Function):
    """
    element_at(array, index) - Returns element of array at given (1-based) index
    element_at(map, key) - Returns value for given key.
    """


@ElementAt.register
def infer_type(
    array: ct.ListType,
    _: ct.NumberType,
) -> ct.ColumnType:
    return array.type.element.type


@ElementAt.register
def infer_type(
    map_arg: ct.MapType,
    _: ct.NumberType,
) -> ct.ColumnType:
    return map_arg.type.value.type


class Elt(Function):
    """
    elt(n, input1, input2, ...) - Returns the n-th input, e.g., returns input2 when n is 2.
    """


@Elt.register  # type: ignore
def infer_type(arg1: ct.IntegerType, *args: ct.ColumnType) -> ct.ColumnType:
    return args[0].type


class Encode(Function):
    """
    encode(str, charset) - Encodes str into the provided charset.
    """


@Encode.register  # type: ignore
def infer_type(arg1: ct.StringType, arg2: ct.StringType) -> ct.ColumnType:
    return ct.StringType()


class Endswith(Function):
    """
    endswith(str, substr) - Returns true if str ends with substr.
    """


@Endswith.register  # type: ignore
def infer_type(arg1: ct.StringType, arg2: ct.StringType) -> ct.ColumnType:
    return ct.BooleanType()


class EqualNull(Function):
    """
    equal_null(expr1, expr2) - Returns true if expr1 and expr2 are equal or both are null.
    """


@EqualNull.register  # type: ignore
def infer_type(arg1: ct.ColumnType, arg2: ct.ColumnType) -> ct.ColumnType:
    return ct.BooleanType()


class Every(Function):
    """
    every(expr) - Returns true if all values are true.
    """

    is_aggregation = True


@Every.register  # type: ignore
def infer_type(arg: ct.BooleanType) -> ct.ColumnType:
    return ct.BooleanType()


class Exists(Function):
    """
    exists(expr, pred) - Tests whether a predicate holds for one or more
    elements in the array.
    """

    @staticmethod
    def compile_lambda(*args):
        """
        Compiles the lambda function used by the `filter` Spark function so that
        the lambda's expression can be evaluated to determine the result's type.
        """
        from datajunction_server.sql.parsing import ast

        expr, func = args
        if len(func.identifiers) != 1:
            raise DJParseException(  # pragma: no cover
                message="The function `exists` takes a lambda function that takes at "
                "most one argument.",
            )
        lambda_arg_col = [
            col
            for col in func.expr.find_all(ast.Column)
            if col.alias_or_name.name == func.identifiers[0].name
        ][0]
        lambda_arg_col.add_type(expr.type.element.type)


@Exists.register  # type: ignore
def infer_type(
    expr: ct.ListType,
    pred: ct.BooleanType,
) -> ct.ColumnType:
    return ct.BooleanType()


class Exp(Function):
    """
    Returns e to the power of expr.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Exp.register  # type: ignore
def infer_type(
    args: ct.ColumnType,
) -> ct.DoubleType:
    return ct.DoubleType()


class Explode(Function):
    """
    explode(expr) - Returns a new row for each element in the given array or map.
    """


@Explode.register  # type: ignore
def infer_type(arg: ct.ListType) -> ct.ColumnType:
    return arg.type.element.type  # pragma: no cover


@Explode.register  # type: ignore
def infer_type(arg: ct.MapType) -> ct.ColumnType:
    return arg.type.value.type  # pragma: no cover


class ExplodeOuter(Function):
    """
    explode_outer(expr) - Similar to explode, but returns null if the array/map is null or empty.
    """


@ExplodeOuter.register  # type: ignore
def infer_type(arg: ct.ListType) -> ct.ColumnType:
    return arg.type.element.type


@ExplodeOuter.register  # type: ignore
def infer_type(arg: ct.MapType) -> ct.ColumnType:
    return arg.type.value.type


class Expm1(Function):
    """
    expm1(expr) - Calculates e^x - 1.
    """


@Expm1.register  # type: ignore
def infer_type(arg: ct.NumberType) -> ct.ColumnType:
    return ct.FloatType()


class Extract(Function):
    """
    Returns a specified component of a timestamp, such as year, month or day.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]

    @staticmethod
    def infer_type(  # type: ignore
        field: "Expression",
        source: "Expression",
    ) -> Union[ct.DecimalType, ct.IntegerType]:
        if str(field.name) == "SECOND":  # type: ignore
            return ct.DecimalType(8, 6)
        return ct.IntegerType()


class Factorial(Function):
    """
    factorial(expr) - Returns the factorial of the number.
    """


@Factorial.register  # type: ignore
def infer_type(arg: ct.IntegerType) -> ct.ColumnType:
    return ct.IntegerType()


class Filter(Function):
    """
    Filter an array.
    """

    @staticmethod
    def compile_lambda(*args):
        """
        Compiles the lambda function used by the `filter` Spark function so that
        the lambda's expression can be evaluated to determine the result's type.
        """
        from datajunction_server.sql.parsing import ast

        expr, func = args
        if len(func.identifiers) > 2:
            raise DJParseException(
                message="The function `filter` takes a lambda function that takes at "
                "most two arguments.",
            )
        for col in func.expr.find_all(ast.Column):
            if (  # pragma: no cover
                col.alias_or_name.namespace
                and col.alias_or_name.namespace.name
                and func.identifiers[0].name == col.alias_or_name.namespace.name
            ) or func.identifiers[0].name == col.alias_or_name.name:
                col.add_type(expr.type.element.type)
            if (
                len(func.identifiers) == 2
                and col.alias_or_name.name == func.identifiers[1].name
            ):
                col.add_type(ct.LongType())


@Filter.register  # type: ignore
def infer_type(
    arg: Union[ct.ListType, ct.PrimitiveType],
    func: ct.PrimitiveType,
) -> ct.ListType:
    return arg.type  # type: ignore


class FindInSet(Function):
    """
    find_in_set(str, str_list) - Returns the index of the first occurrence of str in str_list.
    """


@FindInSet.register  # type: ignore
def infer_type(arg1: ct.StringType, arg2: ct.StringType) -> ct.ColumnType:
    return ct.IntegerType()


class First(Function):
    """
    Returns the first value of expr for a group of rows. If isIgnoreNull is
    true, returns only non-null values.
    """

    is_aggregation = True


@First.register
def infer_type(
    arg: ct.ColumnType,
) -> ct.ColumnType:
    return arg.type


@First.register
def infer_type(
    arg: ct.ColumnType,
    is_ignore_null: ct.BooleanType,
) -> ct.ColumnType:
    return arg.type


class FirstValue(Function):
    """
    Returns the first value of expr for a group of rows. If isIgnoreNull is
    true, returns only non-null values.
    """

    is_aggregation = True


@FirstValue.register
def infer_type(
    arg: ct.ColumnType,
) -> ct.ColumnType:
    return arg.type


@FirstValue.register
def infer_type(
    arg: ct.ColumnType,
    is_ignore_null: ct.BooleanType,
) -> ct.ColumnType:
    return arg.type


class Flatten(Function):
    """
    Flatten an array.
    """


@Flatten.register  # type: ignore
def infer_type(
    array: ct.ListType,
) -> ct.ListType:
    return array.type.element.type  # type: ignore


class Float(Function):
    """
    float(expr) - Casts the value expr to the target data type float.
    """


@Float.register  # type: ignore
def infer_type(arg: Union[ct.NumberType, ct.StringType]) -> ct.FloatType:
    return ct.FloatType()


class Floor(Function):
    """
    Returns the largest integer less than or equal to a specified number.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Floor.register  # type: ignore
def infer_type(
    args: ct.DecimalType,
) -> ct.DecimalType:
    return ct.DecimalType(args.type.precision - args.type.scale + 1, 0)


@Floor.register  # type: ignore
def infer_type(
    args: ct.NumberType,
) -> ct.BigIntType:
    return ct.BigIntType()


@Floor.register  # type: ignore
def infer_type(
    args: ct.NumberType,
    _target_scale: ct.IntegerType,
) -> ct.DecimalType:
    target_scale = _target_scale.value
    if isinstance(args.type, ct.DecimalType):
        precision = max(args.type.precision - args.type.scale + 1, -target_scale + 1)
        scale = min(args.type.scale, max(0, target_scale))
        return ct.DecimalType(precision, scale)
    if args.type == ct.TinyIntType():
        precision = max(3, -target_scale + 1)
        return ct.DecimalType(precision, 0)
    if args.type == ct.SmallIntType():
        precision = max(5, -target_scale + 1)
        return ct.DecimalType(precision, 0)
    if args.type == ct.IntegerType():
        precision = max(10, -target_scale + 1)
        return ct.DecimalType(precision, 0)
    if args.type == ct.BigIntType():
        precision = max(20, -target_scale + 1)
        return ct.DecimalType(precision, 0)
    if args.type == ct.FloatType():
        precision = max(14, -target_scale + 1)
        scale = min(7, max(0, target_scale))
        return ct.DecimalType(precision, scale)
    if args.type == ct.DoubleType():
        precision = max(30, -target_scale + 1)
        scale = min(15, max(0, target_scale))
        return ct.DecimalType(precision, scale)

    raise DJParseException(
        f"Unhandled numeric type in Floor `{args.type}`",
    )  # pragma: no cover


class Forall(Function):
    """
    forall(expr, predicate) - Returns true if a given predicate holds for all elements of an array.
    """

    @staticmethod
    def compile_lambda(*args):
        """
        Compiles the lambda function used by the `filter` Spark function so that
        the lambda's expression can be evaluated to determine the result's type.
        """
        from datajunction_server.sql.parsing import ast

        expr, func = args
        if len(func.identifiers) != 1:
            raise DJParseException(  # pragma: no cover
                message="The function `forall` takes a lambda function that takes at "
                "most one argument.",
            )
        lambda_arg_col = [
            col
            for col in func.expr.find_all(ast.Column)
            if col.alias_or_name.name == func.identifiers[0].name
        ][0]
        lambda_arg_col.add_type(expr.type.element.type)


@Forall.register  # type: ignore
def infer_type(arg1: ct.ListType, arg2: ct.BooleanType) -> ct.ColumnType:
    return ct.BooleanType()


class FormatNumber(Function):
    """
    format_number(x, d) - Formats the number x to a format like '#,###,###.##',
    rounded to d decimal places.
    """


@FormatNumber.register  # type: ignore
def infer_type(arg1: ct.FloatType, arg2: ct.IntegerType) -> ct.StringType:
    return ct.StringType()


@FormatNumber.register  # type: ignore
def infer_type(arg1: ct.FloatType, arg2: ct.StringType) -> ct.StringType:
    return ct.StringType()


class FormatString(Function):
    """
    format_string(format, ...) - Formats the arguments in printf-style.
    """


@FormatString.register  # type: ignore
def infer_type(arg1: ct.StringType, *args: ct.PrimitiveType) -> ct.StringType:
    return ct.StringType()


class FromCsv(Function):
    """
    from_csv(csvStr, schema, options) - Parses a CSV string and returns a struct.
    """


@FromCsv.register  # type: ignore
def infer_type(
    arg1: ct.StringType,
    schema: ct.StringType,
    arg3: Optional[ct.MapType] = None,
) -> ct.ColumnType:
    # TODO: Handle options?
    from datajunction_server.sql.parsing.backends.antlr4 import (
        parse_rule,
    )  # pragma: no cover

    return ct.StructType(
        *parse_rule(schema.value, "complexColTypeList"),
    )  # pragma: no cover


class FromJson(Function):  # pragma: no cover
    """
    Converts a JSON string to a struct or map.
    """


@FromJson.register  # type: ignore
def infer_type(
    json: ct.StringType,
    schema: ct.StringType,
    options: Optional[Function] = None,
) -> ct.StructType:
    from datajunction_server.sql.parsing.backends.antlr4 import parse_rule

    schema_type = re.sub(r"^'(.*)'$", r"\1", schema.value)
    try:
        return parse_rule(schema_type, "dataType")
    except DJParseException:
        return ct.StructType(*parse_rule(schema_type, "complexColTypeList"))


class FromUnixtime(Function):
    """
    from_unixtime(unix_time, format) - Converts the number of seconds from the Unix
    epoch to a string representing the timestamp.
    """


@FromUnixtime.register  # type: ignore
def infer_type(arg1: ct.IntegerType, arg2: ct.StringType) -> ct.ColumnType:
    return ct.StringType()


class FromUtcTimestamp(Function):
    """
    from_utc_timestamp(timestamp, timezone) - Renders that time as a timestamp
    in the given time zone.
    """


@FromUtcTimestamp.register  # type: ignore
def infer_type(arg1: ct.TimestampType, arg2: ct.StringType) -> ct.ColumnType:
    return ct.TimestampType()


@FromUtcTimestamp.register  # type: ignore
def infer_type(arg1: ct.StringType, arg2: ct.StringType) -> ct.ColumnType:
    return ct.TimestampType()


class Get(Function):
    """
    get(expr, index) - Retrieves an element from an array at the specified
    index or retrieves a value from a map for the given key.
    """


@Get.register  # type: ignore
def infer_type(arg1: ct.ListType, arg2: ct.IntegerType) -> ct.ColumnType:
    return arg1.type.element.type


class GetJsonObject(Function):
    """
    get_json_object(jsonString, path) - Extracts a JSON object from a JSON
    string based on the JSON path specified.
    """


@GetJsonObject.register  # type: ignore
def infer_type(arg1: ct.StringType, arg2: ct.StringType) -> ct.ColumnType:
    return ct.StringType()


class GetBit(Function):
    """
    getbit(expr, pos) - Returns the value of the bit (0 or 1) at the specified position.
    """


@GetBit.register  # type: ignore
def infer_type(arg1: ct.IntegerType, arg2: ct.IntegerType) -> ct.ColumnType:
    return ct.IntegerType()


class Greatest(Function):
    """
    greatest(expr, ...) - Returns the greatest value of all parameters, skipping null values.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Greatest.register  # type: ignore
def infer_type(
    *values: ct.NumberType,
) -> ct.ColumnType:
    return values[0].type


class Grouping(Function):
    """
    grouping(col) - Returns 1 if the specified column is aggregated, and 0 otherwise.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Grouping.register  # type: ignore
def infer_type(arg: ct.ColumnType) -> ct.ColumnType:
    return ct.IntegerType()


class GroupingId(Function):
    """
    grouping_id(cols) - Returns a bit vector with a bit for each grouping column.
    """


@GroupingId.register  # type: ignore
def infer_type(*args: ct.ColumnType) -> ct.ColumnType:
    return ct.BigIntType()


class Hash(Function):
    """
    hash(args) - Returns a hash value of the arguments.
    """


@Hash.register  # type: ignore
def infer_type(*args: ct.ColumnType) -> ct.ColumnType:
    return ct.IntegerType()


class Hex(Function):
    """
    hex(expr) - Converts a number or a string to a hexadecimal string.
    """


@Hex.register  # type: ignore
def infer_type(arg: Union[ct.IntegerType, ct.StringType]) -> ct.ColumnType:
    return ct.StringType()


class HistogramNumeric(Function):
    """
    histogram_numeric(col, numBins) - Generates a histogram using a series of buckets
    defined by equally spaced width intervals.
    """


@HistogramNumeric.register  # type: ignore
def infer_type(arg1: ct.ColumnType, arg2: ct.IntegerType) -> ct.ColumnType:
    # assuming that there's a StructType for the bin and frequency
    from datajunction_server.sql.parsing import ast

    return ct.ListType(
        element_type=ct.StructType(
            ct.NestedField(ast.Name("x"), ct.FloatType()),
            ct.NestedField(ast.Name("y"), ct.FloatType()),
        ),
    )


class Hour(Function):
    """
    hour(timestamp) - Extracts the hour from a timestamp.
    """


@Hour.register  # type: ignore
def infer_type(
    arg: Union[ct.TimestampType, ct.StringType],
) -> ct.ColumnType:
    return ct.IntegerType()


class Hypot(Function):
    """
    hypot(a, b) - Returns sqrt(a^2 + b^2) without intermediate overflow or underflow.
    """


@Hypot.register  # type: ignore
def infer_type(arg1: ct.NumberType, arg2: ct.NumberType) -> ct.ColumnType:
    return ct.FloatType()


class If(Function):
    """
    If statement

    if(condition, result, else_result): if condition evaluates to true,
    then returns result; otherwise returns else_result.
    """


@If.register  # type: ignore
def infer_type(
    cond: ct.BooleanType,
    then: ct.ColumnType,
    else_: ct.ColumnType,
) -> ct.ColumnType:
    if not then.type.is_compatible(else_.type):
        raise DJInvalidInputException(
            message="The then result and else result must match in type! "
            f"Got {then.type} and {else_.type}",
        )
    if then.type == ct.NullType():
        return else_.type
    return then.type


class IfNull(Function):
    """
    Returns the second expression if the first is null, else returns the first expression.
    """


@IfNull.register
def infer_type(*args: ct.ColumnType) -> ct.ColumnType:
    return args[0].type if args[1].type == ct.NullType() else args[1].type


class ILike(Function):
    """
    ilike(str, pattern) - Performs case-insensitive LIKE match.
    """


@ILike.register  # type: ignore
def infer_type(arg1: ct.StringType, arg2: ct.StringType) -> ct.ColumnType:
    return ct.BooleanType()


class InitCap(Function):
    """
    initcap(str) - Converts the first letter of each word in the string to uppercase
    and the rest to lowercase.
    """


@InitCap.register  # type: ignore
def infer_type(arg: ct.StringType) -> ct.ColumnType:
    return ct.StringType()


class Inline(Function):
    """
    inline(array_of_struct) - Explodes an array of structs into a table.
    """


@Inline.register  # type: ignore
def infer_type(arg: ct.ListType) -> ct.ColumnType:
    # The output type is the type of the struct's fields
    return arg.type.element.type


class InlineOuter(Function):
    """
    inline_outer(array_of_struct) - Similar to inline, but includes nulls if the size
    of the array is less than the size of the outer array.
    """


@InlineOuter.register  # type: ignore
def infer_type(arg: ct.ListType) -> ct.ColumnType:
    # The output type is the type of the struct's fields
    return arg.type.element.type


class InputFileBlockLength(Function):
    """
    input_file_block_length() - Returns the length of the current block being read from HDFS.
    """


@InputFileBlockLength.register  # type: ignore
def infer_type() -> ct.ColumnType:
    return ct.LongType()


class InputFileBlockStart(Function):
    """
    input_file_block_start() - Returns the start offset of the current block being read from HDFS.
    """


@InputFileBlockStart.register  # type: ignore
def infer_type() -> ct.ColumnType:
    return ct.LongType()


class InputFileName(Function):
    """
    input_file_name() - Returns the name of the current file being read from HDFS.
    """


@InputFileName.register  # type: ignore
def infer_type() -> ct.ColumnType:
    return ct.StringType()


class Instr(Function):
    """
    instr(str, substring) - Returns the position of the first occurrence of substring in string.
    """


@Instr.register  # type: ignore
def infer_type(arg1: ct.StringType, arg2: ct.StringType) -> ct.ColumnType:
    return ct.IntegerType()


class Int(Function):
    """
    int(expr) - Casts the value expr to the target data type int.
    """


@Int.register  # type: ignore
def infer_type(
    arg: ct.ColumnType,
) -> ct.ColumnType:
    return ct.IntegerType()


class Isnan(Function):
    """
    isnan(expr) - Tests if a value is NaN.
    """


@Isnan.register  # type: ignore
def infer_type(arg: ct.NumberType) -> ct.ColumnType:
    return ct.BooleanType()


class Isnotnull(Function):
    """
    isnotnull(expr) - Tests if a value is not null.
    """


@Isnotnull.register  # type: ignore
def infer_type(arg: ct.ColumnType) -> ct.ColumnType:
    return ct.BooleanType()


class Isnull(Function):
    """
    isnull(expr) - Tests if a value is null.
    """


@Isnull.register  # type: ignore
def infer_type(arg: ct.ColumnType) -> ct.ColumnType:
    return ct.BooleanType()


class JsonArrayLength(Function):
    """
    json_array_length(jsonArray) - Returns the length of the JSON array.
    """


@JsonArrayLength.register  # type: ignore
def infer_type(arg: ct.StringType) -> ct.ColumnType:
    return ct.IntegerType()


class JsonObjectKeys(Function):
    """
    json_object_keys(jsonObject) - Returns all the keys of the JSON object.
    """


@JsonObjectKeys.register  # type: ignore
def infer_type(arg: ct.StringType) -> ct.ColumnType:
    return ct.ListType(element_type=ct.StringType())


class JsonTuple(Function):
    """
    json_tuple(json_str, path1, path2, ...) - Extracts multiple values from a JSON object.
    """


@JsonTuple.register  # type: ignore
def infer_type(json_str: ct.StringType, *paths: ct.StringType) -> ct.ColumnType:
    # assuming that there's a TupleType for the extracted values
    return ct.ListType(element_type=ct.StringType())


class Kurtosis(Function):
    """
    kurtosis(expr) - Returns the kurtosis of the values in a group.
    """


@Kurtosis.register  # type: ignore
def infer_type(arg: ct.NumberType) -> ct.DoubleType:
    return ct.DoubleType()


class Lag(Function):
    """
    lag(expr[, offset[, default]]) - Returns the value that is `offset` rows
    before the current row in a window partition.
    """


@Lag.register  # type: ignore
def infer_type(
    arg: ct.ColumnType,
    offset: Optional[ct.IntegerType] = None,
    default: Optional[ct.ColumnType] = None,
) -> ct.ColumnType:
    # The output type is the same as the input expression's type
    return arg.type


class Last(Function):
    """
    last(expr[, ignoreNulls]) - Returns the last value of `expr` for a group of rows.
    """

    is_aggregation = True


@Last.register  # type: ignore
def infer_type(
    arg: ct.ColumnType,
    ignore_nulls: Optional[ct.BooleanType] = None,
) -> ct.ColumnType:
    # The output type is the same as the input expression's type
    return arg.type


class LastDay(Function):
    """
    last_day(date) - Returns the last day of the month which the date belongs to.
    """


@LastDay.register  # type: ignore
def infer_type(arg: Union[ct.DateType, ct.StringType]) -> ct.ColumnType:
    return ct.DateType()


class LastValue(Function):
    """
    last_value(expr[, ignoreNulls]) - Returns the last value in an ordered set of values.
    """

    is_aggregation = True


@LastValue.register  # type: ignore
def infer_type(
    arg: ct.ColumnType,
    ignore_nulls: Optional[ct.BooleanType] = None,
) -> ct.ColumnType:
    # The output type is the same as the input expression's type
    return arg.type


class Lcase(Function):
    """
    lcase(str) - Converts the string to lowercase.
    """


@Lcase.register  # type: ignore
def infer_type(arg: ct.StringType) -> ct.ColumnType:
    return ct.StringType()


class Lead(Function):
    """
    lead(expr[, offset[, default]]) - Returns the value that is `offset`
    rows after the current row in a window partition.
    """


@Lead.register  # type: ignore
def infer_type(
    arg: ct.ColumnType,
    offset: Optional[ct.IntegerType] = None,
    default: Optional[ct.ColumnType] = None,
) -> ct.ColumnType:
    # The output type is the same as the input expression's type
    return arg.type


class Least(Function):
    """
    least(expr1, expr2, ...) - Returns the smallest value of the list of values.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Least.register  # type: ignore
def infer_type(*args: ct.ColumnType) -> ct.ColumnType:
    # The output type is the same as the input expressions' type
    # Assuming all input expressions have the same type
    return args[0].type


class Left(Function):
    """
    left(str, len) - Returns the leftmost `len` characters from the string.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Left.register  # type: ignore
def infer_type(
    arg1: ct.StringType,
    arg2: ct.IntegerType,
) -> ct.StringType:
    return ct.StringType()  # pragma: no cover  # see test_left_func


class Len(Function):
    """
    len(str) - Returns the length of the string.
    """


@Len.register  # type: ignore
def infer_type(arg: ct.StringType) -> ct.IntegerType:
    return ct.IntegerType()


class Length(Function):
    """
    Returns the length of a string.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Length.register  # type: ignore
def infer_type(
    arg: ct.StringType,
) -> ct.IntegerType:
    return ct.IntegerType()


class Levenshtein(Function):
    """
    Returns the Levenshtein distance between two strings.
    """


@Levenshtein.register  # type: ignore
def infer_type(
    string1: ct.StringType,
    string2: ct.StringType,
) -> ct.IntegerType:
    return ct.IntegerType()


class Like(Function):
    """
    like(str, pattern) - Performs pattern matching using SQL's LIKE operator.
    """


@Like.register  # type: ignore
def infer_type(arg1: ct.StringType, arg2: ct.StringType) -> ct.ColumnType:
    return ct.BooleanType()


class Ln(Function):
    """
    Returns the natural logarithm of a number.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Ln.register  # type: ignore
def infer_type(
    args: ct.ColumnType,
) -> ct.DoubleType:
    return ct.DoubleType()


class Localtimestamp(Function):
    """
    localtimestamp() - Returns the current timestamp at the system's local time zone.
    """


@Localtimestamp.register  # type: ignore
def infer_type() -> ct.ColumnType:
    return ct.TimestampType()


class Locate(Function):
    """
    locate(substr, str[, pos]) - Returns the position of the first occurrence of substr in str.
    """


@Locate.register  # type: ignore
def infer_type(
    arg1: ct.StringType,
    arg2: ct.StringType,
    pos: Optional[ct.IntegerType] = None,
) -> ct.ColumnType:
    return ct.IntegerType()


class Log(Function):
    """
    Returns the logarithm of a number with the specified base.
    """


@Log.register  # type: ignore
def infer_type(
    base: ct.ColumnType,
    expr: ct.ColumnType,
) -> ct.DoubleType:
    return ct.DoubleType()


class Log10(Function):
    """
    Returns the base-10 logarithm of a number.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Log10.register  # type: ignore
def infer_type(
    args: ct.ColumnType,
) -> ct.DoubleType:
    return ct.DoubleType()


class Log1p(Function):
    """
    log1p(expr) - Returns the natural logarithm of the given value plus one.
    """


@Log1p.register  # type: ignore
def infer_type(arg: ct.NumberType) -> ct.DoubleType:
    return ct.DoubleType()


class Log2(Function):
    """
    Returns the base-2 logarithm of a number.
    """


@Log2.register  # type: ignore
def infer_type(
    args: ct.ColumnType,
) -> ct.DoubleType:
    return ct.DoubleType()


class Lower(Function):
    """
    Converts a string to lowercase.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]

    @staticmethod
    def infer_type(arg: "Expression") -> ct.StringType:  # type: ignore
        return ct.StringType()


class Lpad(Function):
    """
    lpad(str, len[, pad]) - Left-pads the string with pad to a length of len.
    If str is longer than len, the return value is shortened to len characters.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Lpad.register  # type: ignore
def infer_type(
    arg1: ct.StringType,
    arg2: ct.IntegerType,
    pad: Optional[ct.StringType] = None,
) -> ct.StringType:
    return ct.StringType()


class Ltrim(Function):
    """
    ltrim(str[, trimStr]) - Trims the spaces from left end of the string.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Ltrim.register  # type: ignore
def infer_type(
    arg1: ct.StringType,
    trim_str: Optional[ct.StringType] = None,
) -> ct.StringType:
    return ct.StringType()


class MakeDate(Function):
    """
    make_date(year, month, day) - Creates a date from the given year, month, and day.
    """


@MakeDate.register  # type: ignore
def infer_type(
    year: ct.IntegerType,
    month: ct.IntegerType,
    day: ct.IntegerType,
) -> ct.ColumnType:
    return ct.DateType()


class MakeDtInterval(Function):
    """
    make_dt_interval(days, hours, mins, secs) - Returns a day-time interval.
    """


@MakeDtInterval.register  # type: ignore
def infer_type(
    days: ct.IntegerType,
    hours: ct.IntegerType,
    mins: ct.IntegerType,
    secs: ct.IntegerType,
) -> ct.DayTimeIntervalType:
    return ct.DayTimeIntervalType()


class MakeInterval(Function):
    """
    make_interval(years, months) - Returns a year-month interval.
    """


@MakeInterval.register  # type: ignore
def infer_type(
    years: ct.IntegerType,
    months: ct.IntegerType,
) -> ct.YearMonthIntervalType:
    return ct.YearMonthIntervalType()


class MakeTimestamp(Function):
    """
    make_timestamp(year, month, day, hour, min, sec) - Returns a timestamp
    made from the arguments.
    """


@MakeTimestamp.register  # type: ignore
def infer_type(
    year: ct.IntegerType,
    month: ct.IntegerType,
    day: ct.IntegerType,
    hour: ct.IntegerType,
    min_: ct.IntegerType,
    sec: ct.IntegerType,
) -> ct.TimestampType:
    return ct.TimestampType()


class MakeTimestampLtz(Function):
    """
    make_timestamp_ltz(year, month, day, hour, min, sec, timezone)
    Returns a timestamp with local time zone.
    """


@MakeTimestampLtz.register  # type: ignore
def infer_type(
    year: ct.IntegerType,
    month: ct.IntegerType,
    day: ct.IntegerType,
    hour: ct.IntegerType,
    min_: ct.IntegerType,
    sec: ct.IntegerType,
    timezone: Optional[ct.StringType] = None,
) -> ct.TimestampType:
    return ct.TimestampType()


class MakeTimestampNtz(Function):
    """
    make_timestamp_ntz(year, month, day, hour, min, sec)
    Returns a timestamp without time zone.
    """


@MakeTimestampNtz.register  # type: ignore
def infer_type(
    year: ct.IntegerType,
    month: ct.IntegerType,
    day: ct.IntegerType,
    hour: ct.IntegerType,
    min_: ct.IntegerType,
    sec: ct.IntegerType,
) -> ct.TimestampType:
    return ct.TimestampType()


class MakeYmInterval(Function):
    """
    make_ym_interval(years, months) - Returns a year-month interval.
    """


@MakeYmInterval.register  # type: ignore
def infer_type(
    years: ct.IntegerType,
    months: ct.IntegerType,
) -> ct.YearMonthIntervalType:
    return ct.YearMonthIntervalType()


class Map(Function):
    """
    Returns a map of constants
    """


@Map.register  # type: ignore
def infer_type(
    *args: ct.ColumnType,
) -> ct.MapType:
    return ct.MapType(key_type=args[0].type, value_type=args[1].type)


class MapConcat(Function):
    """
    map_concat(map, ...) - Concatenates all the given maps into one.
    """


@MapConcat.register  # type: ignore
def infer_type(*args: ct.MapType) -> ct.MapType:
    return args[0].type


class MapContainsKey(Function):
    """
    map_contains_key(map, key) - Returns true if the map contains the given key.
    """


@MapContainsKey.register  # type: ignore
def infer_type(map_: ct.MapType, key: ct.ColumnType) -> ct.BooleanType:
    return ct.BooleanType()


class MapEntries(Function):
    """
    map_entries(map) - Returns an unordered array of all entries in the given map.
    """


@MapEntries.register  # type: ignore
def infer_type(map_: ct.MapType) -> ct.ColumnType:
    return ct.ListType(
        element_type=ct.StructType(
            ct.NestedField("key", field_type=map_.type.key.type),
            ct.NestedField("value", field_type=map_.type.value.type),
        ),
    )


class MapFilter(Function):
    """
    map_filter(map, function) - Returns a map that only includes the entries that match the
    given predicate.
    """

    @staticmethod
    def compile_lambda(*args):
        """
        Compiles the lambda function used by the `map_filter` Spark function so that
        the lambda's expression can be evaluated to determine the result's type.
        """
        from datajunction_server.sql.parsing import ast

        expr, func = args
        if len(func.identifiers) != 2:
            raise DJParseException(  # pragma: no cover
                message="The function `map_filter` takes a lambda function that takes "
                "exactly two arguments.",
            )
        identifiers = {iden.name: idx for idx, iden in enumerate(func.identifiers)}
        lambda_arg_cols = {
            identifiers[col.alias_or_name.name]: col
            for col in func.expr.find_all(ast.Column)
            if col.alias_or_name.name in identifiers
        }
        lambda_arg_cols[0].add_type(expr.type.key.type)
        lambda_arg_cols[1].add_type(expr.type.value.type)


@MapFilter.register  # type: ignore
def infer_type(map_: ct.MapType, function: ct.BooleanType) -> ct.MapType:
    return map_.type


class MapFromArrays(Function):
    """
    map_from_arrays(keys, values) - Creates a map from two arrays.
    """


@MapFromArrays.register  # type: ignore
def infer_type(keys: ct.ListType, values: ct.ListType) -> ct.MapType:
    return ct.MapType(
        key_type=keys.type.element.type,
        value_type=values.type.element.type,
    )


class MapFromEntries(Function):
    """
    map_from_entries(array) - Creates a map from an array of entries.
    """


@MapFromEntries.register  # type: ignore
def infer_type(array_of_entries: ct.ListType) -> ct.ColumnType:
    entry = cast(ct.StructType, array_of_entries.type.element.type)
    key, value = entry.fields
    return ct.MapType(key_type=key.type, value_type=value.type)


class MapKeys(Function):
    """
    map_keys(map) - Returns an unordered array containing the keys of the map.
    """


@MapKeys.register  # type: ignore
def infer_type(
    map_: ct.MapType,
) -> ct.ColumnType:
    return ct.ListType(element_type=map_.type.key.type)


class MapValues(Function):
    """
    map_values(map) - Returns an unordered array containing the values of the map.
    """


@MapValues.register  # type: ignore
def infer_type(map_: ct.MapType) -> ct.ColumnType:
    return ct.ListType(element_type=map_.type.value.type)


class MapZipWith(Function):
    """
    map_zip_with(map1, map2, function) - Returns a merged map of two given maps by
    applying function to the pair of values with the same key.
    """

    @staticmethod
    def compile_lambda(*args):
        """
        Compiles the lambda function used by the `map_zip_with` Spark function so that
        the lambda's expression can be evaluated to determine the result's type.
        """
        from datajunction_server.sql.parsing import ast

        map1, map2, func = args
        available_identifiers = {
            identifier.name: idx for idx, identifier in enumerate(func.identifiers)
        }
        columns = list(
            func.expr.filter(
                lambda x: isinstance(x, ast.Column)
                and x.alias_or_name.name in available_identifiers,
            ),
        )
        for col in columns:
            if available_identifiers.get(col.alias_or_name.name) == 0:
                col.add_type(map1.type.key.type)  # pragma: no cover
            if available_identifiers.get(col.alias_or_name.name) == 1:
                col.add_type(map1.type)
            if available_identifiers.get(col.alias_or_name.name) == 2:
                col.add_type(map2.type)


@MapZipWith.register  # type: ignore
def infer_type(
    map1: ct.MapType,
    map2: ct.MapType,
    function: ct.ColumnType,
) -> ct.ColumnType:
    return map1.type


class Mask(Function):
    """
    mask(input[, upperChar, lowerChar, digitChar, otherChar]) - masks the
    given string value. The function replaces characters with 'X' or 'x',
    and numbers with 'n'. This can be useful for creating copies of tables
    with sensitive information removed.
    """


@Mask.register  # type: ignore
def infer_type(
    input_: ct.StringType,
    upper: Optional[ct.StringType] = None,
    lower: Optional[ct.StringType] = None,
    digit: Optional[ct.StringType] = None,
    other: Optional[ct.StringType] = None,
) -> ct.StringType:
    return ct.StringType()


class Max(Function):
    """
    Computes the maximum value of the input column or expression.
    """

    is_aggregation = True
    dialects = [Dialect.SPARK, Dialect.DRUID]


@Max.register  # type: ignore
def infer_type(
    arg: ct.StringType,
) -> ct.StringType:
    return arg.type  # pragma: no cover


@Max.register  # type: ignore
def infer_type(
    arg: ct.NumberType,
) -> ct.NumberType:
    return arg.type  # pragma: no cover


@Max.register  # type: ignore
def infer_type(
    arg: ct.DateType,
) -> ct.DateType:
    return arg.type  # pragma: no cover


@Max.register  # type: ignore
def infer_type(
    arg: ct.TimestampType,
) -> ct.TimestampType:
    return arg.type  # pragma: no cover


class MaxBy(Function):
    """
    max_by(val, key) - Returns the value of val corresponding to the maximum value of key.
    """

    is_aggregation = True


@MaxBy.register  # type: ignore
def infer_type(val: ct.ColumnType, key: ct.ColumnType) -> ct.ColumnType:
    return val.type


class Md5(Function):
    """
    md5(expr) - Calculates the MD5 hash of the given value.
    """


@Md5.register  # type: ignore
def infer_type(arg: ct.ColumnType) -> ct.ColumnType:
    return ct.StringType()


class Mean(Function):
    """
    mean(expr) - Returns the average of the values in the group.
    """


@Mean.register  # type: ignore
def infer_type(arg: ct.ColumnType) -> ct.ColumnType:
    return ct.DoubleType()


class Median(Function):
    """
    median(expr) - Returns the median of the values in the group.
    """


@Median.register  # type: ignore
def infer_type(arg: ct.NumberType) -> ct.ColumnType:
    return ct.DoubleType()


# TODO: fix parsing of:
#   SELECT median(col) FROM VALUES (INTERVAL '0' MONTH),
#   (INTERVAL '10' MONTH) AS tab(col)
#   in order to test this
@Median.register  # type: ignore
def infer_type(arg: ct.IntervalTypeBase) -> ct.IntervalTypeBase:  # pragma: no cover
    return arg.type


class Min(Function):
    """
    Computes the minimum value of the input column or expression.
    """

    is_aggregation = True
    dialects = [Dialect.SPARK, Dialect.DRUID]


@Min.register  # type: ignore
def infer_type(
    arg: ct.StringType,
) -> ct.StringType:
    return arg.type  # pragma: no cover


@Min.register  # type: ignore
def infer_type(
    arg: ct.NumberType,
) -> ct.NumberType:
    return arg.type  # pragma: no cover


@Min.register  # type: ignore
def infer_type(
    arg: ct.DateType,
) -> ct.DateType:
    return arg.type  # pragma: no cover


@Min.register  # type: ignore
def infer_type(
    arg: ct.TimestampType,
) -> ct.TimestampType:
    return arg.type  # pragma: no cover


class MinBy(Function):
    """
    min_by(val, key) - Returns the value of val corresponding to the minimum value of key.
    """

    is_aggregation = True


@MinBy.register  # type: ignore
def infer_type(val: ct.ColumnType, key: ct.ColumnType) -> ct.ColumnType:
    return val.type


class Minute(Function):
    """
    minute(timestamp) - Returns the minute component of the string/timestamp
    """


@Minute.register  # type: ignore
def infer_type(val: Union[ct.StringType, ct.TimestampType]) -> ct.IntegerType:
    return ct.IntegerType()


class Mod(Function):
    """
    mod(expr1, expr2) - Returns the remainder after expr1/expr2.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Mod.register  # type: ignore
def infer_type(expr1: ct.NumberType, expr2: ct.NumberType) -> ct.FloatType:
    return ct.FloatType()


class Mode(Function):
    """
    mode(col) - Returns the most frequent value for the values within col.
    """


@Mode.register  # type: ignore
def infer_type(arg: ct.ColumnType) -> ct.ColumnType:
    return arg.type


class MonotonicallyIncreasingId(Function):
    """
    monotonically_increasing_id() - Returns monotonically increasing 64-bit integers
    """


@MonotonicallyIncreasingId.register  # type: ignore
def infer_type() -> ct.BigIntType:
    return ct.BigIntType()


class Month(Function):
    """
    Extracts the month of a date or timestamp.
    """


@Month.register
def infer_type(arg: Union[ct.StringType, ct.DateTimeBase]) -> ct.BigIntType:
    return ct.BigIntType()


class MonthsBetween(Function):
    """
    months_between(timestamp1, timestamp2[, roundOff])
    """


@MonthsBetween.register
def infer_type(
    arg: Union[ct.StringType, ct.TimestampType],
    arg2: Union[ct.StringType, ct.TimestampType],
    arg3: Optional[ct.BooleanType] = None,
) -> ct.BigIntType:
    return ct.FloatType()


class NamedStruct(Function):
    """
    named_struct(name, val, ...) - Creates a new struct with the given field names and values.
    """


@NamedStruct.register  # type: ignore
def infer_type(*args: ct.ColumnType) -> ct.ColumnType:
    args_iter = iter(args)
    nested_fields = [
        ct.NestedField(
            name=field_name.value.replace("'", ""),
            field_type=field_value.type,
        )
        for field_name, field_value in zip(args_iter, args_iter)
    ]
    return ct.StructType(*nested_fields)


class Nanvl(Function):
    """
    nanvl(expr1, expr2) - Returns the first argument if it is not NaN,
    or the second argument if the first argument is NaN.
    """


@Nanvl.register  # type: ignore
def infer_type(expr1: ct.NumberType, expr2: ct.NumberType) -> ct.NumberType:
    return expr1.type


class Negative(Function):
    """
    negative(expr) - Returns the negated value of the input expression.
    """


@Negative.register  # type: ignore
def infer_type(arg: ct.NumberType) -> ct.NumberType:
    return arg.type


class NextDay(Function):
    """
    next_day(start_date, day_of_week) - Returns the first date which is
    later than start_date and named as indicated.
    """


@NextDay.register  # type: ignore
def infer_type(
    date: Union[ct.DateType, ct.StringType],
    day_of_week: ct.StringType,
) -> ct.ColumnType:
    return ct.DateType()


class Not(Function):
    """
    not(expr) - Returns the logical NOT of the Boolean expression.
    """


@Not.register  # type: ignore
def infer_type(arg: ct.BooleanType) -> ct.ColumnType:
    return ct.BooleanType()  # pragma: no cover


class Now(Function):
    """
    Returns the current timestamp.
    """


@Now.register  # type: ignore
def infer_type() -> ct.TimestampType:
    return ct.TimestampType()


class NthValue(Function):
    """
    nth_value(input[, offset]) - Returns the value of input at the row
    that is the offset-th row from beginning of the window frame
    """


@NthValue.register  # type: ignore
def infer_type(expr: ct.ColumnType, offset: ct.IntegerType) -> ct.ColumnType:
    return expr.type


class Ntile(Function):
    """
    ntile(n) - Divides the rows for each window partition into n buckets
    ranging from 1 to at most n.
    """


@Ntile.register  # type: ignore
def infer_type(n_buckets: ct.IntegerType) -> ct.ColumnType:
    return ct.IntegerType()


class Nullif(Function):
    """
    nullif(expr1, expr2) - Returns null if expr1 equals expr2, or expr1 otherwise.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Nullif.register  # type: ignore
def infer_type(expr1: ct.ColumnType, expr2: ct.ColumnType) -> ct.ColumnType:
    return expr1.type


class Nvl(Function):
    """
    nvl(expr1, expr2) - Returns the first argument if it is not null, or the
    second argument if the first argument is null.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Nvl.register  # type: ignore
def infer_type(expr1: ct.ColumnType, expr2: ct.ColumnType) -> ct.ColumnType:
    return expr1.type


class Nvl2(Function):
    """
    nvl2(expr1, expr2, expr3) - Returns expr3 if expr1 is null, or expr2 otherwise.
    """


@Nvl2.register  # type: ignore
def infer_type(
    expr1: ct.ColumnType,
    expr2: ct.ColumnType,
    expr3: ct.ColumnType,
) -> ct.ColumnType:
    return expr1.type


class OctetLength(Function):
    """
    octet_length(expr) - Returns the number of bytes in the input string.
    """


@OctetLength.register  # type: ignore
def infer_type(expr: ct.StringType) -> ct.ColumnType:
    return ct.IntegerType()


class Overlay(Function):
    """
    overlay(expr1, expr2, start[, length]) - Replaces the substring of expr1
    specified by start (and optionally length) with expr2.
    """


@Overlay.register  # type: ignore
def infer_type(
    input_: ct.StringType,
    replace: ct.StringType,
    pos: ct.IntegerType,
    length: Optional[ct.IntegerType] = None,
) -> ct.ColumnType:
    return ct.StringType()


class Percentile(Function):
    """
    percentile() - Computes the percentage ranking of a value in a group of values.
    """

    is_aggregation = True


@Percentile.register
def infer_type(
    col: Union[ct.NumberType, ct.IntervalTypeBase],
    percentage: ct.NumberType,
) -> ct.FloatType:
    return ct.FloatType()  # type: ignore


@Percentile.register
def infer_type(
    col: Union[ct.NumberType, ct.IntervalTypeBase],
    percentage: ct.NumberType,
    freq: ct.IntegerType,
) -> ct.FloatType:
    return ct.FloatType()  # type: ignore


@Percentile.register
def infer_type(
    col: Union[ct.NumberType, ct.IntervalTypeBase],
    percentage: ct.ListType,
) -> ct.ListType:
    return ct.ListType(element_type=ct.FloatType())  # type: ignore


@Percentile.register
def infer_type(
    col: Union[ct.NumberType, ct.IntervalTypeBase],
    percentage: ct.ListType,
    freq: ct.IntegerType,
) -> ct.ListType:
    return ct.ListType(element_type=ct.FloatType())  # type: ignore


class PercentRank(Function):
    """
    percent_rank() - Computes the percentage ranking of a value in a group of values.
    """

    is_aggregation = True


@PercentRank.register
def infer_type(arg: ct.NumberType) -> ct.DoubleType:
    return ct.DoubleType()


class Pow(Function):
    """
    Raises a base expression to the power of an exponent expression.
    """


@Pow.register  # type: ignore
def infer_type(
    base: ct.ColumnType,
    power: ct.ColumnType,
) -> ct.DoubleType:
    return ct.DoubleType()


class Power(Function):
    """
    Raises a base expression to the power of an exponent expression.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Power.register  # type: ignore
def infer_type(
    base: ct.ColumnType,
    power: ct.ColumnType,
) -> ct.DoubleType:
    return ct.DoubleType()


class Rand(Function):
    """
    rand() - Returns a random value with independent and identically distributed
    (i.i.d.) uniformly distributed values in [0, 1).
    """


@Rand.register
def infer_type() -> ct.FloatType:
    return ct.FloatType()


@Rand.register
def infer_type(seed: ct.IntegerType) -> ct.FloatType:
    return ct.FloatType()


@Rand.register
def infer_type(seed: ct.NullType) -> ct.FloatType:
    return ct.FloatType()


class Randn(Function):
    """
    randn() - Returns a random value with independent and identically
    distributed (i.i.d.) values drawn from the standard normal distribution.
    """


@Randn.register
def infer_type() -> ct.FloatType:
    return ct.FloatType()


@Randn.register
def infer_type(seed: ct.IntegerType) -> ct.FloatType:
    return ct.FloatType()


@Randn.register
def infer_type(seed: ct.NullType) -> ct.FloatType:
    return ct.FloatType()


class Random(Function):
    """
    random() - Returns a random value with independent and identically
    distributed (i.i.d.) uniformly distributed values in [0, 1).
    """


@Random.register
def infer_type() -> ct.FloatType:
    return ct.FloatType()


@Random.register
def infer_type(seed: ct.IntegerType) -> ct.FloatType:
    return ct.FloatType()


@Random.register
def infer_type(seed: ct.NullType) -> ct.FloatType:
    return ct.FloatType()


class Rank(Function):
    """
    rank() - Computes the rank of a value in a group of values. The result is
    one plus the number of rows preceding or equal to the current row in the
    ordering of the partition. The values will produce gaps in the sequence.
    """


@Rank.register
def infer_type() -> ct.IntegerType:
    return ct.IntegerType()


@Rank.register
def infer_type(_: ct.ColumnType) -> ct.IntegerType:
    return ct.IntegerType()


class RegexpExtract(Function):
    """
    regexp_extract(str, regexp[, idx]) - Extract the first string in the str that
    match the regexp expression and corresponding to the regex group index.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@RegexpExtract.register
def infer_type(  # type: ignore
    str_: ct.StringType,
    regexp: ct.StringType,
    idx: Optional[ct.IntegerType] = 1,
) -> ct.StringType:
    return ct.StringType()


class RegexpLike(Function):
    """
    regexp_like(str, regexp) - Returns true if str matches regexp, or false otherwise
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@RegexpLike.register
def infer_type(  # type: ignore
    arg1: ct.StringType,
    arg2: ct.StringType,
) -> ct.BooleanType:
    return ct.BooleanType()


class RegexpReplace(Function):
    """
    regexp_replace(str, regexp, rep[, position]) - Replaces all substrings of str that
    match regexp with rep.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@RegexpReplace.register
def infer_type(  # type: ignore
    str_: ct.StringType,
    regexp: ct.StringType,
    rep: ct.StringType,
    position: Optional[ct.IntegerType] = 1,
) -> ct.StringType:
    return ct.StringType()


class Replace(Function):
    """
    replace(str, search[, replace]) - Replaces all occurrences of `search` with `replace`.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Replace.register
def infer_type(  # type: ignore
    string: ct.StringType,
    search: ct.StringType,
    replace: Optional[ct.StringType] = "",
) -> ct.StringType:
    return ct.StringType()


class RowNumber(Function):
    """
    row_number() - Assigns a unique, sequential number to each row, starting with
    one, according to the ordering of rows within the window partition.
    """


@RowNumber.register
def infer_type() -> ct.IntegerType:
    return ct.IntegerType()


class Round(Function):
    """
    Rounds a numeric column or expression to the specified number of decimal places.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Round.register  # type: ignore
def infer_type(
    child: ct.DecimalType,
    scale: ct.IntegerBase,
) -> ct.NumberType:
    child_type = child.type
    integral_least_num_digits = child_type.precision - child_type.scale + 1
    if scale.value < 0:
        new_precision = max(
            integral_least_num_digits,
            -scale.type.value + 1,
        )  # pragma: no cover
        return ct.DecimalType(new_precision, 0)  # pragma: no cover
    new_scale = min(child_type.scale, scale.value)
    return ct.DecimalType(integral_least_num_digits + new_scale, new_scale)


@Round.register
def infer_type(  # type: ignore
    child: ct.NumberType,
    scale: ct.IntegerBase,
) -> ct.NumberType:
    if scale.value == 0:
        return ct.IntegerType()
    return child.type


@Round.register
def infer_type(  # type: ignore
    child: ct.NumberType,
) -> ct.NumberType:
    return ct.IntegerType()


class Sequence(Function):
    """
    Generates an array of elements from start to stop (inclusive), incrementing by step.
    """


@Sequence.register
def infer_type(  # type: ignore
    start: ct.IntegerBase,
    end: ct.IntegerBase,
    step: Optional[ct.IntegerBase] = None,
) -> ct.ListType:
    return ct.ListType(element_type=start.type)


@Sequence.register
def infer_type(  # type: ignore
    start: ct.TimestampType,
    end: ct.TimestampType,
    step: ct.IntervalTypeBase,
) -> ct.ListType:
    return ct.ListType(element_type=ct.TimestampType())


@Sequence.register
def infer_type(  # type: ignore
    start: ct.DateType,
    end: ct.DateType,
    step: ct.IntervalTypeBase,
) -> ct.ListType:
    return ct.ListType(element_type=ct.DateType())


class Size(Function):
    """
    size(expr) - Returns the size of an array or a map. The function returns
    null for null input if spark.sql.legacy.sizeOfNull is set to false or
    spark.sql.ansi.enabled is set to true. Otherwise, the function returns -1
    for null input. With the default settings, the function returns -1 for null
    input.
    """


@Size.register  # type: ignore
def infer_type(
    arg: ct.ListType,
) -> ct.IntegerType:
    return ct.IntegerType()


@Size.register  # type: ignore
def infer_type(
    arg: ct.MapType,
) -> ct.IntegerType:
    return ct.IntegerType()


class Split(Function):
    """
    Splits str around occurrences that match regex and returns an
    array with a length of at most limit
    """


@Split.register
def infer_type(
    string: ct.StringType,
    regex: ct.StringType,
    limit: Optional[ct.IntegerType] = None,
) -> ct.ColumnType:
    return ct.ListType(element_type=ct.StringType())  # type: ignore


class Sqrt(Function):
    """
    Computes the square root of a numeric column or expression.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Sqrt.register
def infer_type(arg: ct.NumberType) -> ct.DoubleType:
    return ct.DoubleType()


class Stddev(Function):
    """
    Computes the sample standard deviation of a numerical column or expression.
    """

    is_aggregation = True
    dialects = [Dialect.SPARK, Dialect.DRUID]


@Stddev.register
def infer_type(arg: ct.NumberType) -> ct.DoubleType:
    return ct.DoubleType()


class StddevPop(Function):
    """
    Computes the population standard deviation of the input column or expression.
    """

    is_aggregation = True
    dialects = [Dialect.SPARK, Dialect.DRUID]


@StddevPop.register
def infer_type(arg: ct.NumberType) -> ct.DoubleType:
    return ct.DoubleType()


class StddevSamp(Function):
    """
    Computes the sample standard deviation of the input column or expression.
    """

    is_aggregation = True
    dialects = [Dialect.SPARK, Dialect.DRUID]


@StddevSamp.register
def infer_type(arg: ct.NumberType) -> ct.DoubleType:
    return ct.DoubleType()


class Strpos(Function):
    """
    strpos(string, substring) -> bigint
        Returns the starting position of the first instance of substring in string. Positions
        start with 1. If not found, 0 is returned.
    strpos(string, substring, instance) -> bigint
        Returns the position of the N-th instance of substring in string. When instance is a
        negative number the search will start from the end of string. Positions start with 1.
        If not found, 0 is returned.
    """

    dialects = [Dialect.TRINO, Dialect.DRUID]


@Strpos.register
def infer_type(
    string: ct.StringType,
    substring: ct.StringType,
) -> ct.IntegerType:
    return ct.IntegerType()


@Strpos.register
def infer_type(
    string: ct.StringType,
    substring: ct.StringType,
    instance: ct.IntegerType,
) -> ct.IntegerType:
    return ct.IntegerType()


class Struct(Function):
    """
    struct(val1, val2, ...) - Creates a new struct with the given field values.
    """


@Struct.register  # type: ignore
def infer_type(*args: ct.ColumnType) -> ct.StructType:
    return ct.StructType(
        *[
            ct.NestedField(
                name=arg.alias.name if hasattr(arg, "alias") else f"col{idx}",
                field_type=arg.type,
            )
            for idx, arg in enumerate(args)
        ],
    )


class Substring(Function):
    """
    Extracts a substring from a string column or expression.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Substring.register
def infer_type(  # type: ignore
    string: ct.StringType,
    pos: ct.IntegerType,
) -> ct.StringType:
    return ct.StringType()


@Substring.register
def infer_type(  # type: ignore
    string: ct.StringType,
    pos: ct.IntegerType,
    length: ct.IntegerType,
) -> ct.StringType:
    return ct.StringType()


class Substr(Function):
    """
    Extracts a substring from a string column or expression.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Substr.register
def infer_type(  # type: ignore
    string: ct.StringType,
    pos: ct.IntegerType,
) -> ct.StringType:
    return ct.StringType()


@Substr.register
def infer_type(  # type: ignore
    string: ct.StringType,
    pos: ct.IntegerType,
    length: ct.IntegerType,
) -> ct.StringType:
    return ct.StringType()  # pragma: no cover


class Sum(Function):
    """
    Computes the sum of the input column or expression.
    """

    is_aggregation = True
    dialects = [Dialect.SPARK, Dialect.DRUID]


@Sum.register  # type: ignore
def infer_type(
    arg: ct.IntegerBase,
) -> ct.BigIntType:
    return ct.BigIntType()


@Sum.register  # type: ignore
def infer_type(
    arg: ct.DecimalType,
) -> ct.DecimalType:
    precision = arg.type.precision
    scale = arg.type.scale
    return ct.DecimalType(precision + min(10, 31 - precision), scale)


@Sum.register  # type: ignore
def infer_type(
    arg: Union[ct.NumberType, ct.IntervalTypeBase],
) -> ct.DoubleType:
    return ct.DoubleType()


@Sum.register  # type: ignore
def infer_type(
    arg: Union[ct.DateType, ct.TimestampType],
) -> ct.DoubleType:
    return ct.DoubleType()


class ToDate(Function):  # pragma: no cover
    """
    Converts a date string to a date value.
    """


@ToDate.register  # type: ignore
def infer_type(
    expr: ct.StringType,
    fmt: Optional[ct.StringType] = None,
) -> ct.DateType:
    return ct.DateType()


class ToTimestamp(Function):  # pragma: no cover
    """
    Parses the timestamp_str expression with the fmt expression to a timestamp.
    """


@ToTimestamp.register  # type: ignore
def infer_type(
    expr: ct.StringType,
    fmt: Optional[ct.StringType] = None,
) -> ct.TimestampType:
    return ct.TimestampType()


class Transform(Function):
    """
    transform(expr, func) - Transforms elements in an array
    using the function.
    """

    @staticmethod
    def compile_lambda(*args):
        """
        Compiles the lambda function used by the `transform` Spark function so that
        the lambda's expression can be evaluated to determine the result's type.
        """
        from datajunction_server.sql.parsing import ast

        expr, func = args
        available_identifiers = {
            identifier.name: idx for idx, identifier in enumerate(func.identifiers)
        }
        columns = list(
            func.expr.filter(
                lambda x: isinstance(x, ast.Column)
                and x.alias_or_name.name in available_identifiers,
            ),
        )
        for col in columns:
            # The array element arg
            if available_identifiers.get(col.alias_or_name.name) == 0:
                col.add_type(expr.type.element.type)

            # The index arg (optional)
            if available_identifiers.get(col.alias_or_name.name) == 1:
                col.add_type(ct.IntegerType())


@Transform.register  # type: ignore
def infer_type(
    expr: ct.ListType,
    func: ct.PrimitiveType,
) -> ct.ColumnType:
    return ct.ListType(element_type=func.expr.type)


class TransformKeys(Function):
    """
    transform_keys(expr, func) - Transforms keys in a map using the function
    """

    @staticmethod
    def compile_lambda(*args):
        """
        Compiles the lambda function used by the `transform_keys` Spark function so that
        the lambda's expression can be evaluated to determine the result's type.
        """
        from datajunction_server.sql.parsing import ast

        expr, func = args
        available_identifiers = {
            identifier.name: idx for idx, identifier in enumerate(func.identifiers)
        }
        columns = list(
            func.expr.filter(
                lambda x: isinstance(x, ast.Column)
                and x.alias_or_name.name in available_identifiers,
            ),
        )
        for col in columns:
            # The map key arg
            if available_identifiers.get(col.alias_or_name.name) == 0:
                col.add_type(expr.type.key.type)

            # The map value arg
            if available_identifiers.get(col.alias_or_name.name) == 1:
                col.add_type(expr.type.value.type)


@TransformKeys.register  # type: ignore
def infer_type(
    expr: ct.MapType,
    func: ct.ColumnType,
) -> ct.MapType:
    return ct.MapType(key_type=func.expr.type, value_type=expr.type.value.type)


class TransformValues(Function):
    """
    transform_values(expr, func) - Transforms values in a map using the function
    """

    @staticmethod
    def compile_lambda(*args):
        """
        Compiles the lambda function used by the `transform_values` Spark function so that
        the lambda's expression can be evaluated to determine the result's type.
        """
        from datajunction_server.sql.parsing import ast

        expr, func = args
        available_identifiers = {
            identifier.name: idx for idx, identifier in enumerate(func.identifiers)
        }
        columns = list(
            func.expr.filter(
                lambda x: isinstance(x, ast.Column)
                and x.alias_or_name.name in available_identifiers,
            ),
        )
        for col in columns:
            # The map key arg
            if available_identifiers.get(col.alias_or_name.name) == 0:
                col.add_type(expr.type.key.type)

            # The map value arg
            if available_identifiers.get(col.alias_or_name.name) == 1:
                col.add_type(expr.type.value.type)


@TransformValues.register  # type: ignore
def infer_type(
    expr: ct.MapType,
    func: ct.ColumnType,
) -> ct.MapType:
    return ct.MapType(key_type=expr.type.key.type, value_type=func.expr.type)


class Trim(Function):
    """
    Removes leading and trailing whitespace from a string value.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Trim.register
def infer_type(arg: ct.StringType) -> ct.StringType:
    return ct.StringType()


class Timestamp(Function):
    """
    timestamp(expr) - Casts the value expr to the target data type timestamp.
    """

    # Druid has this function but it means something else: unix_millis()
    dialects = [Dialect.SPARK]


@Timestamp.register
def infer_type(expr: ct.StringType) -> ct.TimestampType:
    return ct.TimestampType()


class TimestampMicros(Function):
    """
    timestamp_micros(microseconds) - Creates timestamp from the number of microseconds
    since UTC epoch.
    """

    dialects = [Dialect.SPARK]


@TimestampMicros.register
def infer_type(microseconds: ct.BigIntType) -> ct.TimestampType:
    return ct.TimestampType()


class TimestampMillis(Function):
    """
    timestamp_millis(milliseconds) - Creates timestamp from the number of milliseconds
    since UTC epoch.
    """

    dialects = [Dialect.SPARK]


@TimestampMillis.register
def infer_type(milliseconds: ct.BigIntType) -> ct.TimestampType:
    return ct.TimestampType()


class TimestampSeconds(Function):
    """
    timestamp_seconds(seconds) - Creates timestamp from the number of seconds
    (can be fractional) since UTC epoch.
    """

    dialects = [Dialect.SPARK]


@TimestampSeconds.register
def infer_type(seconds: ct.NumberType) -> ct.TimestampType:
    return ct.TimestampType()


class Unhex(Function):
    """
    unhex(str) - Interprets each pair of characters in the input string as a
    hexadecimal number and converts it to the byte that number represents.
    The output is a binary string.
    """


@Unhex.register  # type: ignore
def infer_type(arg: ct.StringType) -> ct.ColumnType:
    return ct.BinaryType()


class UnixDate(Function):
    """
    unix_date(date) - Returns the number of days since 1970-01-01.
    """

    dialects = [Dialect.SPARK]


@UnixDate.register  # type: ignore
def infer_type(arg: ct.DateType) -> ct.IntegerType:
    return ct.IntegerType()


class UnixMicros(Function):
    """
    unix_micros(timestamp) - Returns the number of microseconds since 1970-01-01 00:00:00 UTC.
    """

    dialects = [Dialect.SPARK]


@UnixMicros.register  # type: ignore
def infer_type(arg: ct.TimestampType) -> ct.BigIntType:
    return ct.BigIntType()


class UnixMillis(Function):
    """
    unix_millis(timestamp) - Returns the number of milliseconds since 1970-01-01 00:00:00 UTC.
    Truncates higher levels of precision.
    """

    dialects = [Dialect.SPARK]


@UnixMillis.register  # type: ignore
def infer_type(arg: ct.TimestampType) -> ct.BigIntType:
    return ct.BigIntType()


class UnixSeconds(Function):
    """
    unix_seconds(timestamp) - Returns the number of seconds since 1970-01-01 00:00:00 UTC.
    Truncates higher levels of precision.
    """

    dialects = [Dialect.SPARK]


@UnixSeconds.register  # type: ignore
def infer_type(arg: ct.TimestampType) -> ct.BigIntType:
    return ct.BigIntType()


class UnixTimestamp(Function):
    """
    unix_timestamp([time_exp[, fmt]]) - Returns the UNIX timestamp of current or specified time.

    Arguments:
        time_exp - A date/timestamp or string. If not provided, this defaults to current time.
        fmt - Date/time format pattern to follow. Ignored if time_exp is not a string.
            Default value is "yyyy-MM-dd HH:mm:ss". See Datetime Patterns for valid date and time
            format patterns.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@UnixTimestamp.register  # type: ignore
def infer_type(
    time_exp: Optional[ct.StringType] = None,
    fmt: Optional[ct.StringType] = None,
) -> ct.BigIntType:
    return ct.BigIntType()


class Upper(Function):
    """
    Converts a string value to uppercase.
    """

    dialects = [Dialect.SPARK, Dialect.DRUID]


@Upper.register
def infer_type(arg: ct.StringType) -> ct.StringType:
    return ct.StringType()


class Variance(Function):
    """
    Computes the sample variance of the input column or expression.
    """

    is_aggregation = True
    dialects = [Dialect.SPARK, Dialect.DRUID]


@Variance.register
def infer_type(arg: ct.NumberType) -> ct.DoubleType:
    return ct.DoubleType()


class VarPop(Function):
    """
    Computes the population variance of the input column or expression.
    """

    is_aggregation = True
    dialects = [Dialect.SPARK, Dialect.DRUID]


@VarPop.register
def infer_type(arg: ct.NumberType) -> ct.DoubleType:
    return ct.DoubleType()


class VarSamp(Function):
    """
    Computes the sample variance of the input column or expression.
    """

    is_aggregation = True
    dialects = [Dialect.SPARK, Dialect.DRUID]


@VarSamp.register
def infer_type(arg: ct.NumberType) -> ct.DoubleType:
    return ct.DoubleType()


class Week(Function):
    """
    Returns the week number of the year of the input date value.
    Note: Trino-only
    """


@Week.register
def infer_type(arg: Union[ct.StringType, ct.DateTimeBase]) -> ct.BigIntType:
    return ct.BigIntType()


class Year(Function):
    """
    Returns the year of the input date value.
    """


@Year.register
def infer_type(
    arg: Union[ct.StringType, ct.DateTimeBase, ct.IntegerType],
) -> ct.BigIntType:
    return ct.BigIntType()


##################################################################################
# Table Functions                                                                #
# https://spark.apache.org/docs/3.3.2/sql-ref-syntax-qry-select-tvf.html#content #
##################################################################################


class Explode(TableFunction):
    """
    The Explode function is used to explode the specified array,
    nested array, or map column into multiple rows.
    The explode function will generate a new row for each
    element in the specified column.
    """


@Explode.register
def infer_type(
    arg: ct.ListType,
) -> List[ct.NestedField]:
    return [arg.element]


@Explode.register
def infer_type(
    arg: ct.MapType,
) -> List[ct.NestedField]:
    return [arg.key, arg.value]


class Unnest(TableFunction):
    """
    The unnest function is used to explode the specified array,
    nested array, or map column into multiple rows.
    It will generate a new row for each element in the specified column.
    """

    dialects = [Dialect.TRINO, Dialect.DRUID]


@Unnest.register
def infer_type(
    arg: ct.ListType,
) -> List[ct.NestedField]:
    return [arg.element]  # pragma: no cover


@Unnest.register
def infer_type(
    arg: ct.MapType,
) -> List[ct.NestedField]:
    return [arg.key, arg.value]


class FunctionRegistryDict(dict):
    """
    Custom dictionary mapping for functions
    """

    def __getitem__(self, key):
        """
        Returns a custom error about functions that haven't been implemented yet.
        """
        try:
            return super().__getitem__(key)
        except KeyError as exc:
            raise DJNotImplementedException(
                f"The function `{key}` hasn't been implemented in "
                "DJ yet. You can file an issue at https://github."
                "com/DataJunction/dj/issues/new?title=Function+"
                f"missing:+{key} to request it to be added, or use "
                "the documentation at https://github.com/DataJunct"
                "ion/dj/blob/main/docs/functions.rst to implement it.",
            ) from exc


function_registry = FunctionRegistryDict()
for cls in Function.__subclasses__():
    snake_cased = re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__)
    function_registry[cls.__name__.upper()] = cls
    function_registry[snake_cased.upper()] = cls


table_function_registry = FunctionRegistryDict()
for cls in TableFunction.__subclasses__():
    snake_cased = re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__)
    table_function_registry[cls.__name__.upper()] = cls
    table_function_registry[snake_cased.upper()] = cls
