# pylint: disable=too-many-lines, abstract-method, unused-argument, missing-function-docstring,
# pylint: disable=arguments-differ, too-many-return-statements, function-redefined
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
    get_origin,
)

import dj.sql.parsing.types as ct
from dj.errors import (
    DJError,
    DJInvalidInputException,
    DJNotImplementedException,
    ErrorCode,
)
from dj.sql.parsing.backends.exceptions import DJParseException

if TYPE_CHECKING:
    from dj.sql.parsing.ast import Expression, Lambda


def compare_registers(types, register) -> bool:
    """
    Comparing registers
    """
    for ((type_a, register_a), (type_b, register_b)) in zip_longest(
        types,
        register,
        fillvalue=(-1, None),
    ):
        print(type_a, register_a, type_b, register_b)
        if type_b == -1 and register_b is None:
            if register[-1][0] == -1:  # args
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

    def __getattribute__(cls, func_name):  # pylint: disable=redefined-outer-name
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
    def register(cls, func):  # pylint: disable=redefined-outer-name
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

    @classmethod
    def dispatch(  # pylint: disable=redefined-outer-name
        cls, func_name, *args: "Expression"
    ):
        type_registry = cls.registry[cls].get(func_name)  # type: ignore
        if not type_registry:
            raise ValueError(
                f"No function registered on {cls.__name__}`{func_name}`.",
            )  # pragma: no cover

        type_list = []
        for i, arg in enumerate(args):
            type_list.append((i, type(arg.type) if hasattr(arg, "type") else type(arg)))

        types = tuple(type_list)

        if types in type_registry:  # type: ignore
            return type_registry[types]  # type: ignore

        for register, func in type_registry.items():  # type: ignore
            if compare_registers(types, register):
                print("FOUND", types, func)
                return func

        raise TypeError(
            f"`{cls.__name__}.{func_name}` got an invalid "
            "combination of types: "
            f'{", ".join(str(t[1].__name__) for t in types)}',
        )


class Function(Dispatch):  # pylint: disable=too-few-public-methods
    """
    A DJ function.
    """

    is_aggregation: ClassVar[bool] = False

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


class TableFunction(Dispatch):  # pylint: disable=too-few-public-methods
    """
    A DJ table-valued function.
    """

    @staticmethod
    def infer_type(*args) -> List[ct.ColumnType]:
        raise NotImplementedError()


#####################
# Regular Functions #
#####################


class Abs(Function):
    """
    Returns the absolute value of the numeric or interval value.
    """

    is_aggregation = True


@Abs.register
def infer_type(
    arg: ct.NumberType,
) -> ct.NumberType:
    type_ = arg.type
    return type_


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
        from dj.sql.parsing import ast  # pylint: disable=import-outside-toplevel

        expr, start, merge = args
        available_identifiers = {
            identifier.name: idx for idx, identifier in enumerate(merge.identifiers)
        }
        columns = list(
            merge.expr.filter(
                lambda x: isinstance(x, ast.Column)
                and x.alias_or_name.name in available_identifiers,
            ),
        )
        for col in columns:
            if available_identifiers.get(col.alias_or_name.name) == 0:
                col.add_type(start.type)
            if available_identifiers.get(col.alias_or_name.name) == 1:
                col.add_type(expr.type.element.type)


@Aggregate.register  # type: ignore
def infer_type(
    expr: ct.ListType,
    start: ct.PrimitiveType,
    merge: ct.PrimitiveType,
) -> ct.ColumnType:
    return merge.expr.type


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


class ArrayAgg(Function):
    """
    Collects and returns a list of non-unique elements.
    """


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


@ArrayAppend.register  # type: ignore
def infer_type(
    array: ct.ListType,
    item: ct.ColumnType,
) -> ct.ListType:
    return ct.ListType(element_type=item.type)


class ArrayContains(Function):
    """
    array_contains(array, value) - Returns true if the array contains the value.
    """


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


class Avg(Function):
    """
    Computes the average of the input column or expression.
    """

    is_aggregation = True


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


class Ceil(Function):
    """
    Computes the smallest integer greater than or equal to the input value.
    """


@Ceil.register
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
def infer_type(
    args: ct.DecimalType,
) -> ct.DecimalType:
    return ct.DecimalType(args.type.precision - args.type.scale + 1, 0)


@Ceil.register
def infer_type(
    args: ct.NumberType,
) -> ct.BigIntType:
    return ct.BigIntType()


class Coalesce(Function):
    """
    Computes the average of the input column or expression.
    """

    is_aggregation = False


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


class Count(Function):
    """
    Counts the number of non-null values in the input column or expression.
    """

    is_aggregation = True


@Count.register  # type: ignore
def infer_type(
    *args: ct.ColumnType,
) -> ct.BigIntType:
    return ct.BigIntType()


class CurrentDate(Function):
    """
    Returns the current date.
    """


@CurrentDate.register  # type: ignore
def infer_type() -> ct.DateType:
    return ct.DateType()


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


@CurrentTimestamp.register  # type: ignore
def infer_type() -> ct.TimestampType:
    return ct.TimestampType()


class DateAdd(Function):
    """
    Adds a specified number of days to a date.
    """


@DateAdd.register  # type: ignore
def infer_type(
    start_date: ct.DateType,
    days: ct.IntegerBase,
) -> ct.DateType:
    return ct.DateType()


@DateAdd.register  # type: ignore
def infer_type(
    start_date: ct.StringType,
    days: ct.IntegerBase,
) -> ct.DateType:
    return ct.DateType()


class DateDiff(Function):
    """
    Computes the difference in days between two dates.
    """


@DateDiff.register  # type: ignore
def infer_type(
    start_date: ct.DateType,
    end_date: ct.DateType,
) -> ct.IntegerType:
    return ct.IntegerType()


@DateDiff.register  # type: ignore
def infer_type(
    start_date: ct.StringType,
    end_date: ct.StringType,
) -> ct.IntegerType:
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


class Exp(Function):
    """
    Returns e to the power of expr.
    """


@Exp.register  # type: ignore
def infer_type(
    args: ct.ColumnType,
) -> ct.DoubleType:
    return ct.DoubleType()


class Extract(Function):
    """
    Returns a specified component of a timestamp, such as year, month or day.
    """

    @staticmethod
    def infer_type(  # type: ignore
        field: "Expression",
        source: "Expression",
    ) -> Union[ct.DecimalType, ct.IntegerType]:
        if str(field.name) == "SECOND":  # type: ignore
            return ct.DecimalType(8, 6)
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


class Floor(Function):
    """
    Returns the largest integer less than or equal to a specified number.
    """


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
    if isinstance(args.type, ct.DecimalType):  # pylint: disable=R1705
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


class FromJson(Function):  # pragma: no cover
    """
    Converts a JSON string to a struct or map.
    """


@FromJson.register  # type: ignore
def infer_type(  # pragma: no cover
    json: ct.StringType,
    schema: ct.StringType,
    options: Optional[Function] = None,
) -> ct.StructType:
    # TODO: Handle options?  # pylint: disable=fixme
    # pylint: disable=import-outside-toplevel
    from dj.sql.parsing.backends.antlr4 import parse_rule  # pragma: no cover

    return ct.StructType(
        *parse_rule(schema.value, "complexColTypeList")
    )  # pragma: no cover


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

    return then.type


class IfNull(Function):
    """
    Returns the second expression if the first is null, else returns the first expression.
    """


@IfNull.register
def infer_type(*args: ct.ColumnType) -> ct.ColumnType:
    return args[0].type if args[1].type == ct.NullType() else args[1].type


class Length(Function):
    """
    Returns the length of a string.
    """


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


class Ln(Function):
    """
    Returns the natural logarithm of a number.
    """


@Ln.register  # type: ignore
def infer_type(
    args: ct.ColumnType,
) -> ct.DoubleType:
    return ct.DoubleType()


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


@Log10.register  # type: ignore
def infer_type(
    args: ct.ColumnType,
) -> ct.DoubleType:
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

    @staticmethod
    def infer_type(arg: "Expression") -> ct.StringType:  # type: ignore
        return ct.StringType()


class Map(Function):
    """
    Returns a map of constants
    """


def extract_consistent_type(elements):
    """
    Check if all elements are the same type and return that type.
    """
    if all(isinstance(element.type, ct.IntegerType) for element in elements):
        return ct.IntegerType()
    if all(isinstance(element.type, ct.DoubleType) for element in elements):
        return ct.DoubleType()
    if all(isinstance(element.type, ct.FloatType) for element in elements):
        return ct.FloatType()
    return ct.StringType()


@Map.register  # type: ignore
def infer_type(
    *elements: ct.ColumnType,
) -> ct.MapType:
    keys = elements[0::2]
    values = elements[1::2]
    if len(keys) != len(values):
        raise DJParseException("Different number of keys and values for MAP.")

    key_type = extract_consistent_type(keys)
    value_type = extract_consistent_type(values)
    return ct.MapType(key_type=key_type, value_type=value_type)


class Max(Function):
    """
    Computes the maximum value of the input column or expression.
    """

    is_aggregation = True


@Max.register  # type: ignore
def infer_type(
    arg: ct.NumberType,
) -> ct.NumberType:
    return arg.type


@Max.register  # type: ignore
def infer_type(
    arg: ct.StringType,
) -> ct.StringType:
    return arg.type


class Min(Function):
    """
    Computes the minimum value of the input column or expression.
    """

    is_aggregation = True


@Min.register  # type: ignore
def infer_type(
    arg: ct.NumberType,
) -> ct.NumberType:
    return arg.type


class Month(Function):
    """
    Extracts the month of a date or timestamp.
    """


@Month.register
def infer_type(arg: Union[ct.StringType, ct.DateTimeBase]) -> ct.BigIntType:
    return ct.BigIntType()


class Now(Function):
    """
    Returns the current timestamp.
    """


@Now.register  # type: ignore
def infer_type() -> ct.TimestampType:
    return ct.TimestampType()


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


@Power.register  # type: ignore
def infer_type(
    base: ct.ColumnType,
    power: ct.ColumnType,
) -> ct.DoubleType:
    return ct.DoubleType()


class RegexpLike(Function):
    """
    regexp_like(str, regexp) - Returns true if str matches regexp, or false otherwise
    """


@RegexpLike.register
def infer_type(  # type: ignore
    arg1: ct.StringType,
    arg2: ct.StringType,
) -> ct.BooleanType:
    return ct.BooleanType()


class Round(Function):
    """
    Rounds a numeric column or expression to the specified number of decimal places.
    """


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
    return child.type


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


@Sqrt.register
def infer_type(arg: ct.NumberType) -> ct.DoubleType:
    return ct.DoubleType()


class Stddev(Function):
    """
    Computes the sample standard deviation of a numerical column or expression.
    """

    is_aggregation = True


@Stddev.register
def infer_type(arg: ct.NumberType) -> ct.DoubleType:
    return ct.DoubleType()


class StddevPop(Function):
    """
    Computes the population standard deviation of the input column or expression.
    """

    is_aggregation = True


@StddevPop.register
def infer_type(arg: ct.NumberType) -> ct.DoubleType:
    return ct.DoubleType()


class StddevSamp(Function):
    """
    Computes the sample standard deviation of the input column or expression.
    """

    is_aggregation = True


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
    Note: Trino-only
    """


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


class Substring(Function):
    """
    Extracts a substring from a string column or expression.
    """


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


class Sum(Function):
    """
    Computes the sum of the input column or expression.
    """

    is_aggregation = True


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


class ToDate(Function):  # pragma: no cover # pylint: disable=abstract-method
    """
    Converts a date string to a date value.
    """


@ToDate.register  # type: ignore
def infer_type(
    expr: ct.StringType,
    fmt: Optional[ct.StringType] = None,
) -> ct.DateType:
    return ct.DateType()


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
        from dj.sql.parsing import ast  # pylint: disable=import-outside-toplevel

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


class Trim(Function):
    """
    Removes leading and trailing whitespace from a string value.
    """


@Trim.register
def infer_type(arg: ct.StringType) -> ct.StringType:
    return ct.StringType()


class Upper(Function):
    """
    Converts a string value to uppercase.
    """


@Upper.register
def infer_type(arg: ct.StringType) -> ct.StringType:
    return ct.StringType()


class Variance(Function):
    """
    Computes the sample variance of the input column or expression.
    """

    is_aggregation = True


@Variance.register
def infer_type(arg: ct.NumberType) -> ct.DoubleType:
    return ct.DoubleType()


class VarPop(Function):
    """
    Computes the population variance of the input column or expression.
    """

    is_aggregation = True


@VarPop.register
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
def infer_type(arg: Union[ct.StringType, ct.DateTimeBase]) -> ct.BigIntType:
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
