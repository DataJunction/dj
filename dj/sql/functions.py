# pylint: disable=too-many-lines
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

# pylint: disable=unused-argument, missing-function-docstring, arguments-differ, too-many-return-statements
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
    from dj.sql.parsing.ast import Expression


def compare_registers(types, register) -> bool:
    """
    Comparing registers
    """
    for ((type_a, register_a), (type_b, register_b)) in zip_longest(
        types,
        register,
        fillvalue=(-1, None),
    ):
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


class TableFunction(Dispatch):  # pylint: disable=too-few-public-methods
    """
    A DJ table-valued function.
    """

    @staticmethod
    def infer_type(*args) -> List[ct.ColumnType]:
        raise NotImplementedError()


class Avg(Function):  # pylint: disable=abstract-method
    """
    Computes the average of the input column or expression.
    """

    is_aggregation = True


@Avg.register
def infer_type(
    arg: ct.DecimalType,
) -> ct.DecimalType:  # noqa: F811  # pylint: disable=function-redefined
    type_ = arg.type
    return ct.DecimalType(type_.precision + 4, type_.scale + 4)


@Avg.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    arg: ct.IntervalTypeBase,
) -> ct.IntervalTypeBase:
    return type(arg.type)()


@Avg.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    arg: ct.NumberType,
) -> ct.DoubleType:
    return ct.DoubleType()


@Avg.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    arg: ct.DateTimeBase,
) -> ct.DateTimeBase:
    return type(arg.type)()


class Min(Function):  # pylint: disable=abstract-method
    """
    Computes the minimum value of the input column or expression.
    """

    is_aggregation = True


@Min.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    arg: ct.NumberType,
) -> ct.NumberType:
    return arg.type


class Max(Function):  # pylint: disable=abstract-method
    """
    Computes the maximum value of the input column or expression.
    """

    is_aggregation = True


@Max.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    arg: ct.NumberType,
) -> ct.NumberType:
    return arg.type


@Max.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    arg: ct.StringType,
) -> ct.StringType:
    return arg.type


class Sum(Function):  # pylint: disable=abstract-method
    """
    Computes the sum of the input column or expression.
    """

    is_aggregation = True


@Sum.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    arg: ct.IntegerBase,
) -> ct.BigIntType:
    return ct.BigIntType()


@Sum.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    arg: ct.DecimalType,
) -> ct.DecimalType:
    precision = arg.type.precision
    scale = arg.type.scale
    return ct.DecimalType(precision + min(10, 31 - precision), scale)


@Sum.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    arg: Union[ct.NumberType, ct.IntervalTypeBase],
) -> ct.DoubleType:
    return ct.DoubleType()


class Ceil(Function):  # pylint: disable=abstract-method
    """
    Computes the smallest integer greater than or equal to the input value.
    """


@Ceil.register
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
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
        f"Unhandled numeric type in Ceil `{args.type}`",
    )  # pragma: no cover


@Ceil.register
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    args: ct.DecimalType,
) -> ct.DecimalType:
    return ct.DecimalType(args.type.precision - args.type.scale + 1, 0)


@Ceil.register
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    args: ct.NumberType,
) -> ct.BigIntType:
    return ct.BigIntType()


class Count(Function):  # pylint: disable=abstract-method
    """
    Counts the number of non-null values in the input column or expression.
    """

    is_aggregation = True


@Count.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    *args: ct.ColumnType,
) -> ct.BigIntType:
    return ct.BigIntType()


class Coalesce(Function):  # pylint: disable=abstract-method
    """
    Computes the average of the input column or expression.
    """

    is_aggregation = False


@Coalesce.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
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


class CurrentDate(Function):  # pylint: disable=abstract-method
    """
    Returns the current date.
    """


@CurrentDate.register  # type: ignore
def infer_type() -> ct.DateType:  # noqa: F811  # pylint: disable=function-redefined
    return ct.DateType()


class CurrentDatetime(Function):  # pylint: disable=abstract-method
    """
    Returns the current date and time.
    """


@CurrentDatetime.register  # type: ignore
def infer_type() -> ct.TimestampType:  # noqa: F811  # pylint: disable=function-redefined
    return ct.TimestampType()


class CurrentTime(Function):  # pylint: disable=abstract-method
    """
    Returns the current time.
    """


@CurrentTime.register  # type: ignore
def infer_type() -> ct.TimeType:  # noqa: F811  # pylint: disable=function-redefined
    return ct.TimeType()


class CurrentTimestamp(Function):  # pylint: disable=abstract-method
    """
    Returns the current timestamp.
    """


@CurrentTimestamp.register  # type: ignore
def infer_type() -> ct.TimestampType:  # noqa: F811  # pylint: disable=function-redefined
    return ct.TimestampType()


class Now(Function):  # pylint: disable=abstract-method
    """
    Returns the current timestamp.
    """


@Now.register  # type: ignore
def infer_type() -> ct.TimestampType:  # noqa: F811  # pylint: disable=function-redefined
    return ct.TimestampType()


class DateAdd(Function):  # pylint: disable=abstract-method
    """
    Adds a specified number of days to a date.
    """


@DateAdd.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    start_date: ct.DateType,
    days: ct.IntegerBase,
) -> ct.DateType:
    return ct.DateType()


@DateAdd.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    start_date: ct.StringType,
    days: ct.IntegerBase,
) -> ct.DateType:
    return ct.DateType()


class DateSub(Function):  # pylint: disable=abstract-method
    """
    Subtracts a specified number of days from a date.
    """


@DateSub.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    start_date: ct.DateType,
    days: ct.IntegerBase,
) -> ct.DateType:
    return ct.DateType()


@DateSub.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    start_date: ct.StringType,
    days: ct.IntegerBase,
) -> ct.DateType:
    return ct.DateType()


class If(Function):  # pylint: disable=abstract-method
    """
    If statement

    if(condition, result, else_result): if condition evaluates to true,
    then returns result; otherwise returns else_result.
    """


@If.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    cond: ct.BooleanType,
    then: ct.ColumnType,
    else_: ct.ColumnType,
) -> ct.ColumnType:
    if then.type != else_.type:
        raise DJInvalidInputException(
            message="The then result and else result must match in type! "
            f"Got {then.type} and {else_.type}",
        )

    return then.type


class DateDiff(Function):  # pylint: disable=abstract-method
    """
    Computes the difference in days between two dates.
    """


@DateDiff.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    start_date: ct.DateType,
    end_date: ct.DateType,
) -> ct.IntegerType:
    return ct.IntegerType()


@DateDiff.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    start_date: ct.StringType,
    end_date: ct.StringType,
) -> ct.IntegerType:
    return ct.IntegerType()


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


class ToDate(Function):  # pragma: no cover # pylint: disable=abstract-method
    """
    Converts a date string to a date value.
    """


@ToDate.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    expr: ct.StringType,
    fmt: Optional[ct.StringType] = None,
) -> ct.DateType:
    return ct.DateType()


class Day(Function):  # pylint: disable=abstract-method
    """
    Returns the day of the month for a specified date.
    """


@Day.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    arg: Union[ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.IntegerType:  # type: ignore
    return ct.IntegerType()


class Exp(Function):  # pylint: disable=abstract-method
    """
    Returns e to the power of expr.
    """


@Exp.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    args: ct.ColumnType,
) -> ct.DoubleType:
    return ct.DoubleType()


class Floor(Function):  # pylint: disable=abstract-method
    """
    Returns the largest integer less than or equal to a specified number.
    """


@Floor.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    args: ct.DecimalType,
) -> ct.DecimalType:
    return ct.DecimalType(args.type.precision - args.type.scale + 1, 0)


@Floor.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    args: ct.NumberType,
) -> ct.BigIntType:
    return ct.BigIntType()


@Floor.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
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


class IfNull(Function):
    """
    Returns the second expression if the first is null, else returns the first expression.
    """

    @staticmethod
    def infer_type(*args: "Expression") -> ct.ColumnType:  # type: ignore
        return (  # type: ignore
            args[0].type if args[1].type == ct.NullType() else args[1].type
        )


class Length(Function):  # pylint: disable=abstract-method
    """
    Returns the length of a string.
    """


@Length.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    arg: ct.StringType,
) -> ct.IntegerType:
    return ct.IntegerType()


class Levenshtein(Function):  # pylint: disable=abstract-method
    """
    Returns the Levenshtein distance between two strings.
    """


@Levenshtein.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    string1: ct.StringType,
    string2: ct.StringType,
) -> ct.IntegerType:
    return ct.IntegerType()


class Ln(Function):  # pylint: disable=abstract-method
    """
    Returns the natural logarithm of a number.
    """


@Ln.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    args: ct.ColumnType,
) -> ct.DoubleType:
    return ct.DoubleType()


class Log(Function):  # pylint: disable=abstract-method
    """
    Returns the logarithm of a number with the specified base.
    """


@Log.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    base: ct.ColumnType,
    expr: ct.ColumnType,
) -> ct.DoubleType:
    return ct.DoubleType()


class Log2(Function):  # pylint: disable=abstract-method
    """
    Returns the base-2 logarithm of a number.
    """


@Log2.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    args: ct.ColumnType,
) -> ct.DoubleType:
    return ct.DoubleType()


class Log10(Function):  # pylint: disable=abstract-method
    """
    Returns the base-10 logarithm of a number.
    """


@Log10.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
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


class Month(Function):
    """
    Extracts the month of a date or timestamp.
    """

    @staticmethod
    def infer_type(arg: "Expression") -> ct.TinyIntType:  # type: ignore
        return ct.TinyIntType()


class Pow(Function):  # pylint: disable=abstract-method
    """
    Raises a base expression to the power of an exponent expression.
    """


@Pow.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    base: ct.ColumnType,
    power: ct.ColumnType,
) -> ct.DoubleType:
    return ct.DoubleType()


class PercentRank(Function):
    """
    Window function: returns the relative rank (i.e. percentile) of rows within a window partition
    """

    is_aggregation = True

    @staticmethod
    def infer_type() -> ct.DoubleType:
        return ct.DoubleType()


class Quantile(Function):  # pragma: no cover
    """
    Computes the quantile of a numerical column or expression.
    """

    is_aggregation = True

    @staticmethod
    def infer_type(  # type: ignore
        arg1: "Expression",
        arg2: "Expression",
    ) -> ct.DoubleType:
        return ct.DoubleType()


class ApproxQuantile(Function):  # pragma: no cover
    """
    Computes the approximate quantile of a numerical column or expression.
    """

    is_aggregation = True

    @staticmethod
    def infer_type(  # type: ignore
        arg1: "Expression",
        arg2: "Expression",
    ) -> ct.DoubleType:
        return ct.DoubleType()


class RegexpLike(Function):  # pragma: no cover
    """
    Matches a string column or expression against a regular expression pattern.
    """

    @staticmethod
    def infer_type(  # type: ignore
        arg1: "Expression",
        arg2: "Expression",
    ) -> ct.BooleanType:
        return ct.BooleanType()


class Round(Function):  # pylint: disable=abstract-method
    """
    Rounds a numeric column or expression to the specified number of decimal places.
    """


@Round.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
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
def infer_type(  # noqa: F811  # pylint: disable=function-redefined  # type: ignore
    child: ct.NumberType,
    scale: ct.IntegerBase,
) -> ct.NumberType:
    return child.type


class SafeDivide(Function):  # pragma: no cover
    """
    Divides two numeric columns or expressions and returns NULL if the denominator is 0.
    """

    @staticmethod
    def infer_type(arg1: "Expression", arg2: "Expression") -> ct.DoubleType:  # type: ignore
        return ct.DoubleType()


class Substring(Function):
    """
    Extracts a substring from a string column or expression.
    """

    @staticmethod
    def infer_type(  # type: ignore
        arg1: "Expression",
        arg2: "Expression",
        arg3: "Expression",
    ) -> ct.StringType:
        return ct.StringType()


class StrPosition(Function):  # pylint: disable=abstract-method
    """
    Returns the position of the first occurrence of a substring in a string column or expression.
    """


@StrPosition.register
def infer_type(  # noqa: F811  # pylint: disable=function-redefined  # pragma: no cover
    arg1: ct.StringType,
    arg2: ct.StringType,
) -> ct.IntegerType:
    return ct.IntegerType()  # pragma: no cover


class StrToDate(Function):  # pragma: no cover
    """
    Converts a string in a specified format to a date.
    """

    @staticmethod
    def infer_type(arg1: "Expression", arg2: "Expression") -> ct.DateType:
        return ct.DateType()


class StrToTime(Function):  # pragma: no cover
    """
    Converts a string in a specified format to a timestamp.
    """

    @staticmethod
    def infer_type(arg1: "Expression", arg2: "Expression") -> ct.TimestampType:
        return ct.TimestampType()


class Sqrt(Function):
    """
    Computes the square root of a numeric column or expression.
    """

    @staticmethod
    def infer_type(arg: "Expression") -> ct.DoubleType:
        return ct.DoubleType()


class Stddev(Function):
    """
    Computes the sample standard deviation of a numerical column or expression.
    """

    is_aggregation = True

    @staticmethod
    def infer_type(arg: "Expression") -> ct.DoubleType:
        return ct.DoubleType()


class StddevPop(Function):  # pragma: no cover
    """
    Computes the population standard deviation of the input column or expression.
    """

    is_aggregation = True

    @staticmethod
    def infer_type(arg: "Expression") -> ct.DoubleType:
        return ct.DoubleType()


class StddevSamp(Function):  # pragma: no cover
    """
    Computes the sample standard deviation of the input column or expression.
    """

    is_aggregation = True

    @staticmethod
    def infer_type(arg: "Expression") -> ct.DoubleType:
        return ct.DoubleType()


class TimeToStr(Function):  # pragma: no cover
    """
    Converts a time value to a string using the specified format.
    """

    @staticmethod
    def infer_type(arg1: "Expression", arg2: "Expression") -> ct.StringType:
        return ct.StringType()


class TimeToTimeStr(Function):  # pragma: no cover
    """
    Converts a time value to a string using the specified format.
    """

    @staticmethod
    def infer_type(arg1: "Expression", arg2: "Expression") -> ct.StringType:
        return ct.StringType()


class TimeStrToDate(Function):  # pragma: no cover
    """
    Converts a string value to a date.
    """

    @staticmethod
    def infer_type(arg: "Expression") -> ct.DateType:
        return ct.DateType()


class TimeStrToTime(Function):  # pragma: no cover
    """
    Converts a string value to a time.
    """

    @staticmethod
    def infer_type(arg: "Expression") -> ct.TimestampType:
        return ct.TimestampType()


class Trim(Function):  # pragma: no cover
    """
    Removes leading and trailing whitespace from a string value.
    """

    @staticmethod
    def infer_type(arg: "Expression") -> ct.StringType:
        return ct.StringType()


class TsOrDsToDateStr(Function):  # pragma: no cover
    """
    Converts a timestamp or date value to a string using the specified format.
    """

    @staticmethod
    def infer_type(arg1: "Expression", arg2: "Expression") -> ct.StringType:
        return ct.StringType()


class TsOrDsToDate(Function):  # pragma: no cover
    """
    Converts a timestamp or date value to a date.
    """

    @staticmethod
    def infer_type(arg: "Expression") -> ct.DateType:
        return ct.DateType()


class TsOrDiToDi(Function):  # pragma: no cover
    """
    Converts a timestamp or date value to a date.
    """

    @staticmethod
    def infer_type(arg: "Expression") -> ct.IntegerType:
        return ct.IntegerType()


class UnixToStr(Function):  # pragma: no cover
    """
    Converts a Unix timestamp to a string using the specified format.
    """

    @staticmethod
    def infer_type(arg1: "Expression", arg2: "Expression") -> ct.StringType:
        return ct.StringType()


class UnixToTime(Function):  # pragma: no cover
    """
    Converts a Unix timestamp to a time.
    """

    @staticmethod
    def infer_type(arg: "Expression") -> ct.TimestampType:
        return ct.TimestampType()


class UnixToTimeStr(Function):  # pragma: no cover
    """
    Converts a Unix timestamp to a string using the specified format.
    """

    @staticmethod
    def infer_type(arg1: "Expression", arg2: "Expression") -> ct.StringType:
        return ct.StringType()


class Upper(Function):  # pragma: no cover
    """
    Converts a string value to uppercase.
    """

    @staticmethod
    def infer_type(arg: "Expression") -> ct.StringType:
        return ct.StringType()


class Variance(Function):  # pragma: no cover
    """
    Computes the sample variance of the input column or expression.
    """

    is_aggregation = True

    @staticmethod
    def infer_type(arg: "Expression") -> ct.DoubleType:
        return ct.DoubleType()


class VariancePop(Function):  # pragma: no cover
    """
    Computes the population variance of the input column or expression.
    """

    is_aggregation = True

    @staticmethod
    def infer_type(arg: "Expression") -> ct.DoubleType:
        return ct.DoubleType()


class Array(Function):  # pylint: disable=abstract-method
    """
    Returns an array of constants
    """


@Array.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    *elements: ct.ColumnType,
) -> ct.ListType:
    types = {element.type for element in elements}
    if len(types) > 1:
        raise DJParseException(
            f"Multiple types {', '.join(sorted(str(typ) for typ in types))} passed to array.",
        )
    element_type = elements[0].type if elements else ct.NullType()
    return ct.ListType(element_type=element_type)


class Map(Function):  # pylint: disable=abstract-method
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
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    *elements: ct.ColumnType,
) -> ct.MapType:
    keys = elements[0::2]
    values = elements[1::2]
    if len(keys) != len(values):
        raise DJParseException("Different number of keys and values for MAP.")

    key_type = extract_consistent_type(keys)
    value_type = extract_consistent_type(values)
    return ct.MapType(key_type=key_type, value_type=value_type)


class Week(Function):
    """
    Returns the week number of the year of the input date value.
    """

    @staticmethod
    def infer_type(arg: "Expression") -> ct.TinyIntType:
        return ct.TinyIntType()


class Year(Function):
    """
    Returns the year of the input date value.
    """

    @staticmethod
    def infer_type(arg: "Expression") -> ct.TinyIntType:
        return ct.TinyIntType()


class FromJson(Function):  # pragma: no cover  # pylint: disable=abstract-method
    """
    Converts a JSON string to a struct or map.
    """


@FromJson.register  # type: ignore
def infer_type(  # noqa: F811  # pylint: disable=function-redefined  # pragma: no cover
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


# https://spark.apache.org/docs/3.3.2/sql-ref-syntax-qry-select-tvf.html#content
class Explode(TableFunction):  # pylint: disable=abstract-method
    """
    The Explode function is used to explode the specified array,
    nested array, or map column into multiple rows.
    The explode function will generate a new row for each
    element in the specified column.
    """


@Explode.register
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    arg: ct.ListType,
) -> List[ct.NestedField]:
    return [arg.element]


@Explode.register
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    arg: ct.MapType,
) -> List[ct.NestedField]:
    return [arg.key, arg.value]


class Unnest(TableFunction):  # pylint: disable=abstract-method
    """
    The unnest function is used to explode the specified array,
    nested array, or map column into multiple rows.
    It will generate a new row for each element in the specified column.
    """


@Unnest.register
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    arg: ct.ListType,
) -> List[ct.NestedField]:
    return [arg.element]  # pragma: no cover


@Unnest.register
def infer_type(  # noqa: F811  # pylint: disable=function-redefined
    arg: ct.MapType,
) -> List[ct.NestedField]:
    return [arg.key, arg.value]


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
