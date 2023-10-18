import re
from typing import Union

import datajunction_server.sql.parsing.types as ct

from datajunction_server.sql.functions import function_registry, Function


class NflxFunctions(Function):
    """
    All Netflix-specific UDFs to be registered into the DJ backend.
    """


class NfDateint(NflxFunctions):
    """
    nf_dateint
    Returns the date as an integer in the format yyyyMMdd.

    Examples:

    select nf_dateint(20180531) -> 20180531
    select nf_dateint(‘2018-05-31’) -> 20180531
    select nf_dateint('20180531') -> 20180531
    select nf_dateint('2018-31-05', 'yyyy-dd-MM') -> 20180531
    select nf_dateint(1527806973000) -> 20180531
    select nf_dateint(1527806973) -> 20180531
    select nf_dateint(date '2018-05-31') -> 20180531
    select nf_dateint(timestamp '2018-05-31 12:20:21.010') -> 20180531
    select nf_dateint('2018-05-31T12:20:21.010') -> 20180531
    """


@NfDateint.register  # type: ignore
def infer_type(  # noqa: F811
    arg: Union[ct.IntegerType, ct.StringType, ct.TimestampType, ct.DateType],
) -> ct.IntegerType:
    return ct.IntegerType()


class NfDateintToday(NflxFunctions):
    """
    Returns the dateint value equivalent to the current_date.

    Examples:

    select nf_dateint_today() -> 20180531 // Returns the UTC date
    """


@NfDateintToday.register  # type: ignore
def infer_type() -> ct.IntegerType:  # noqa: F811
    return ct.IntegerType()


class NfDatestr(NflxFunctions):
    """
    Returns the date as a string in the format ‘yyyy-MM-dd’

    Examples:

    select nf_datestr(20180531) -> 2018-05-31
    select nf_datestr(‘2018-05-31’) -> 2018-05-31
    select nf_datestr('20180531') -> 2018-05-31
    select nf_datestr('2018-31-05', 'yyyy-dd-MM') -> 2018-05-31
    select nf_datestr(1527806973000) -> 2018-05-31
    select nf_datestr(1527806973) -> 2018-05-31
    select nf_datestr(date '2018-05-31') -> 2018-05-31
    select nf_datestr(timestamp '2018-05-31 12:20:21.010') -> 2018-05-31
    select nf_datestr('2018-05-31T12:20:21.010') -> 2018-05-31
    """


@NfDatestr.register  # type: ignore
def infer_type(  # noqa: F811
    arg: Union[ct.IntegerType, ct.StringType, ct.TimestampType, ct.DateType],
) -> ct.StringType:
    return ct.StringType()


class NfDatestrToday(NflxFunctions):
    """
    Returns the datestr value in the format 'yyyy-MM-dd' at the start of the query session
    """


@NfDatestrToday.register  # type: ignore
def infer_type() -> ct.StringType:  # noqa: F811
    return ct.StringType()


class NfFromUnixtime(NflxFunctions):
    """
    Converts epoch seconds or milliseconds to timestamp. There's an optional second
    parameter to specify the timestamp format or the reference date in epoch.

    select nf_from_unixtime(1527745543) -> 2018-05-31 05:45:43.0
    select nf_from_unixtime(1527745543000) -> 2018-05-31 05:45:43.0
    select nf_from_unixtime(1527745543, 'yyyy/MM/dd') -> 2018/05/31
    """


@NfDatestrToday.register  # type: ignore
def infer_type(  # noqa: F811
    arg: ct.IntegerType,
    format: Optional[ct.StringType] = None,
) -> ct.TimestampType:
    return ct.TimestampType()


class NfFromUnixtimeMs(NflxFunctions):
    """
    Converts epoch milliseconds to timestamp. There's an optional second parameter to specify
    the timestamp format
    """


@NfFromUnixtime.register  # type: ignore
def infer_type(  # noqa: F811
    arg: ct.IntegerType,
    format: Optional[ct.StringType] = None,
) -> ct.TimestampType:
    return ct.TimestampType()


class NfFromUnixtimeTz(NflxFunctions):
    """
    Converts epoch seconds or milliseconds to timestamp in a given timezone.

    select nf_from_unixtime_tz(1527745543, 'GMT+05:00') -> 2018-05-31 10:45:43.0
    select nf_from_unixtime_tz(1527745543000, 'GMT+05:00') -> 2018-05-31 10:45:43.0
    select nf_from_unixtime_tz(1527745543, 'Europe/Paris') ->  2018-05-31 07:45:43.0
    """


@NfFromUnixtimeTz.register  # type: ignore
def infer_type(  # noqa: F811
    arg: ct.IntegerType,
    timezone: ct.StringType,
) -> ct.TimestampType:  # noqa: F811
    return ct.TimestampType()


class NfFromUnixtimeMsTz(NflxFunctions):
    """
    Converts epoch milliseconds to timestamp in a given timezone.
    """


@NfFromUnixtimeMsTz.register  # type: ignore
def infer_type(  # noqa: F811
    arg: ct.IntegerType,
    timezone: ct.StringType,
) -> ct.TimestampType:  # noqa: F811
    return ct.TimestampType()


class NfFromUnixtimeTzFormat(NflxFunctions):
    """
    Converts epoch seconds or milliseconds to timestamp in a given timezone and
    returns the output in the specified format

    select nf_from_unixtime_tz_format(1527745543, 'Europe/Paris',
    'yyyy-MM-dd HH:mm:ss.SSSZZ') -> 2018-05-31 07:45
    """


@NfFromUnixtimeTzFormat.register  # type: ignore
def infer_type(  # noqa: F811
    arg: ct.IntegerType,
    timezone: ct.StringType,
    format: ct.StringType,
) -> ct.TimestampType:
    return ct.TimestampType()


class NfFromUnixtimeMsTzFormat(NflxFunctions):
    """
    Converts epoch milliseconds to timestamp in a given timezone in a given timezone
    and returns the output in the specified format.
    """


@NfFromUnixtimeMsTzFormat.register  # type: ignore
def infer_type(  # noqa: F811
    arg: ct.IntegerType,
    timezone: ct.StringType,
    format: ct.StringType,
) -> ct.TimestampType:
    return ct.TimestampType()


class NfToUnixtime(NflxFunctions):
    """
    Converts the input to epoch seconds.

    select nf_to_unixtime(20180531) ->  1527724800
    select nf_to_unixtime('2018-05-31') ->  1527724800
    select nf_to_unixtime('20180531') ->  1527724800
    select nf_to_unixtime('2018-31-05', 'yyyy-dd-MM') -> 1527724800
    select nf_to_unixtime(date '2018-05-31') -> 1527724800
    select nf_to_unixtime(timestamp '2018-05-31 12:20:21.010') ->  1527769221
    select nf_to_unixtime('2018-05-31T12:20:21.010') -> 2018-05-31
    select nf_to_unixtime(nf_from_unixtime_tz(1527745543, 'GMT+05:00')) -> 1527745543
    """


@NfToUnixtime.register  # type: ignore
def infer_type(  # noqa: F811
    arg: Union[ct.IntegerType, ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.IntegerType:
    return ct.IntegerType()


class NfToUnixtimeMs(NflxFunctions):
    """
    Converts the input to epoch milliseconds.
    """


@NfToUnixtimeMs.register  # type: ignore
def infer_type(  # noqa: F811
    arg: Union[ct.IntegerType, ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.IntegerType:
    return ct.IntegerType()


class NfToUnixtimeNow(NflxFunctions):
    """
    Returns epoch seconds at the start of the session.
    """


@NfToUnixtimeNow.register  # type: ignore
def infer_type() -> ct.IntegerType:  # noqa: F811
    return ct.IntegerType()


class NfToUnixtimeNowMs(NflxFunctions):
    """
    Returns epoch milliseconds at the start of the session.
    """


@NfToUnixtimeNowMs.register  # type: ignore
def infer_type() -> ct.IntegerType:  # noqa: F811
    return ct.IntegerType()


class NfDate(NflxFunctions):
    """
    Converts the input to Spark date type in the format 'yyyy-MM-dd'

    Examples:

    select nf_date(20180531) -> 2018-05-31
    select nf_date(‘2018-05-31’) -> 2018-05-31
    select nf_date('20180531') -> 2018-05-31
    select nf_date('2018-31-05', 'yyyy-dd-MM') -> 2018-05-31
    select nf_date(1527806973000) -> 2018-05-31
    select nf_date(date '2018-05-31') -> 2018-05-31
    select nf_date(timestamp '2018-05-31 12:20:21.010') -> 2018-05-31
    select nf_date('2018-05-31T12:20:21.010') -> 2018-05-31
    """


@NfDate.register  # type: ignore
def infer_type(  # noqa: F811
    arg: Union[ct.IntegerType, ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.DateType:
    return ct.DateType()


class NfDateToday(NflxFunctions):
    """
    Returns the date value at the start of the query session
    """


@NfDateToday.register  # type: ignore
def infer_type() -> ct.DateType:  # noqa: F811
    return ct.DateType()


class NfTimestamp(NflxFunctions):
    """
    Converts the input to Spark timestamp type in the format 'yyyy-MM-dd HH:mm:ss.SSSSSS'

    Examples:

    select nf_timestamp(20180531) -> 2018-05-31 00:00:00.000
    select nf_timestamp(‘2018-05-31’) -> 2018-05-31 00:00:00.000
    select nf_timestamp('20180531') -> 2018-05-31 00:00:00.000
    select nf_timestamp('2018-31-05 12:20:21.010', 'yyyy-dd-MM HH:mm:ss.SSS') -> 2018-05-31 12:20:21.01
    select nf_timestamp(1527806973012) ->  2018-05-31 22:49:33.012
    select nf_timestamp(date '2018-05-31') -> 2018-05-31 00:00:00.000
    select nf_timestamp(timestamp '2018-05-31 12:20:21') -> 2018-05-31 12:20:21.000
    select nf_timestamp('2018-05-31T12:20:21.010+05:00') -> 2018-05-31 07:20:21.010
    """


@NfTimestamp.register  # type: ignore
def infer_type(  # noqa: F811
    arg: Union[ct.IntegerType, ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.TimestampType:
    return ct.TimestampType()


class NfTimestampNow(NflxFunctions):
    """
    Returns the timestamp value at the start of the query session
    """


@NfTimestampNow.register  # type: ignore
def infer_type() -> ct.TimestampType:  # noqa: F811
    return ct.TimestampType()


class NfDateadd(NflxFunctions):
    """
    The result returned is of the same type as input.

    Supported units for dates: Year, month, day, week, quarter Supported units for timestamps:
    Year, month, day, week, quarter, hour, minute, second, millisecond

    Examples:

    select nf_dateadd(20180531, 2) -> 20180602 as an integer
    select nf_dateadd(‘2018-05-31’, -10) -> 2018-05-21
    select nf_dateadd('20180531', 2) -> 20180602 as a string
    select nf_dateadd('20180531', '-3M') -> 20180228
    select nf_dateadd('quarter', 1, timestamp '2018-05-31 12:20:21') -> 2018-08-31 12:20:21.000
    """


@NfDateadd.register  # type: ignore
def infer_type(  # noqa: F811
    input: Union[ct.IntegerType, ct.StringType, ct.DateType],
    num_days: ct.IntegerBase,
) -> Union[ct.IntegerType, ct.StringType, ct.DateType]:
    return input.type


@NfDateadd.register  # type: ignore
def infer_type(  # noqa: F811
    input: Union[ct.IntegerType, ct.StringType, ct.DateType],
    offset: ct.StringType,
) -> Union[ct.IntegerType, ct.StringType, ct.DateType]:
    return input.type


@NfDateadd.register  # type: ignore
def infer_type(  # noqa: F811
    unit: ct.StringType,
    value: ct.IntegerType,
    input: Union[ct.IntegerType, ct.StringType, ct.DateType],
) -> Union[ct.IntegerType, ct.StringType, ct.DateType]:
    return input.type


class NfDatediff(NflxFunctions):
    """
    nf_datediff(input1, input2): Returns the number of days (input2-input1).
                                 Input1 and Input2 must be of the same type.
    nf_datediff(unit, input1, input2): Returns the difference between the input
                                       dates in terms of the specified unit
    Supported intervals: year, quarter,month, week, day, hour, minute, second, millisecond

    Examples:

    select nf_datediff(20180531, 20180604); -> 4
    select nf_datediff('2018-06-04', '2018-05-31');-> -4
    select nf_datediff('month', '20180201', '20180531') -> 3
    select nf_datediff('second', timestamp '2018-05-31 12:20:21',
        timestamp '2018-05-31 02:20:21') -> -36000
    """


@NfDatediff.register  # type: ignore
def infer_type(  # noqa: F811
    input1: Union[ct.IntegerType, ct.StringType, ct.DateType],
    input2: Union[ct.IntegerType, ct.StringType, ct.DateType],
) -> ct.IntegerType:
    return ct.IntegerType()


@NfDatediff.register  # type: ignore
def infer_type(  # noqa: F811
    unit: ct.StringType,
    input1: Union[ct.IntegerType, ct.StringType, ct.DateType],
    input2: Union[ct.IntegerType, ct.StringType, ct.DateType],
) -> ct.IntegerType:
    return ct.IntegerType()


class NfDatetrunc(NflxFunctions):
    """
    nf_datetrunc(unit, input): Returns the input truncated to the given unit

    Examples:

    Given input as timestamp 2018-05-31 12:20:21.321
    unit    truncated value
    second  2018-05-31 12:20:21.000
    minute  2018-05-31 12:20:00.000
    hour    2018-05-31 12:00:00.000
    day     2018-05-31 00:00:00.000
    week    2018-05-28 00:00:00.000
    month   2018-05-01 00:00:00.000
    quarter 2018-04-01 00:00:00.000
    year    2018-01-01 00:00:00.000

    select nf_datetrunc('month', 20180531) -> 20180501
    """


@NfDatetrunc.register  # type: ignore
def infer_type(  # noqa: F811
    unit: ct.StringType,
    input: Union[ct.IntegerType, ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.ColumnType:
    return input.type


class NfYear(NflxFunctions):
    """
    Extracts year as an integer from the input

    Examples:

    select nf_year(20180531) -> 2018
    select nf_year(‘2018-05-31’) -> 2018
    select nf_year('20180531') -> 2018
    select nf_year(date '2018-05-31') -> 2018
    select nf_year(timestamp '2018-05-31 12:20:21.010') -> 2018
    select nf_year('2018-05-31T12:20:21.010') -> 2018
    """


@NfYear.register  # type: ignore
def infer_type(  # noqa: F811
    input: Union[ct.IntegerType, ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.IntegerType:
    return ct.IntegerType()


class NfMonth(NflxFunctions):
    """
    Extracts month of year as an integer from the input (1 to 12)
    """


@NfMonth.register  # type: ignore
def infer_type(  # noqa: F811
    input: Union[ct.IntegerType, ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.IntegerType:
    return ct.IntegerType()


class NfDay(NflxFunctions):
    """
    Extracts day of month as an integer from the input (1 to 31)
    """


@NfDay.register  # type: ignore
def infer_type(  # noqa: F811
    input: Union[ct.IntegerType, ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.IntegerType:
    return ct.IntegerType()


class NfWeek(NflxFunctions):
    """
    Extracts week of year as an integer from the input (1 to 53).
    The value is incremented every Monday.
    """


@NfWeek.register  # type: ignore
def infer_type(  # noqa: F811
    input: Union[ct.IntegerType, ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.IntegerType:
    return ct.IntegerType()


class NfDayOfWeek(NflxFunctions):
    """
    Returns ISO day of week.
    """


@NfDayOfWeek.register  # type: ignore
def infer_type(  # noqa: F811
    input: Union[ct.IntegerType, ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.IntegerType:
    return ct.IntegerType()


class NfQuarter(NflxFunctions):
    """
    Extracts quarter.
    """


@NfQuarter.register  # type: ignore
def infer_type(  # noqa: F811
    input: Union[ct.IntegerType, ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.IntegerType:
    return ct.IntegerType()


class NfHour(NflxFunctions):
    """
    Extracts hour.
    """


@NfHour.register  # type: ignore
def infer_type(  # noqa: F811
    input: Union[ct.IntegerType, ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.IntegerType:
    return ct.IntegerType()


class NfMinute(NflxFunctions):
    """
    Extracts minute.
    """


@NfMinute.register  # type: ignore
def infer_type(  # noqa: F811
    input: Union[ct.IntegerType, ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.IntegerType:
    return ct.IntegerType()


class NfSecond(NflxFunctions):
    """
    Extracts week of year as an integer from the input (1 to 53).
    The value is incremented every Monday.
    """


@NfSecond.register  # type: ignore
def infer_type(  # noqa: F811
    input: Union[ct.IntegerType, ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.IntegerType:
    return ct.IntegerType()


class NfMillisecond(NflxFunctions):
    """
    Extracts week of year as an integer from the input (1 to 53).
    The value is incremented every Monday.
    """


@NfMillisecond.register  # type: ignore
def infer_type(  # noqa: F811
    input: Union[ct.IntegerType, ct.StringType, ct.DateType, ct.TimestampType],
) -> ct.IntegerType:
    return ct.IntegerType()


class NfDateformat(NflxFunctions):
    """
    Returns a string representing the input in the given Joda format

    select nf_dateformat(20180531, 'yyyyddMM') -> 20183105
    select nf_dateformat(timestamp '2018-05-31 12:20:21.010', 'yyyy MMM dd HH:mm:ss.SSS zzz')
        -> 2018 May 31 12:20:21.010 UTC
    select nf_dateformat('2018-05-31T12:20:21.010', 'yyyy MMM dd HH:mm:ss.SSS zzz')
        -> 2018 May 31 12:20:21.010 UTC
    """


@NfDateformat.register  # type: ignore
def infer_type(  # noqa: F811
    input: Union[ct.IntegerType, ct.StringType, ct.DateType, ct.TimestampType],
    format: ct.StringType,
) -> ct.StringType:
    return ct.StringType()


class NfUnixtimeNow(NflxFunctions):
    """
    nf_unixtime_now()
    """


@NfUnixtimeNow.register  # type: ignore
def infer_type() -> ct.IntegerType:
    return ct.IntegerType()


for cls in NflxFunctions.__subclasses__():
    snake_cased = re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__)
    function_registry[cls.__name__.upper()] = cls
    function_registry[snake_cased.upper()] = cls
