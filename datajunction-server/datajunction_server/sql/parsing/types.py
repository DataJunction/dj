"""DJ Column Types

    Example:
        >>> StructType( \
            NestedField("required_field", StringType(), False, "a required field"), \
            NestedField("optional_field", IntegerType(), True, "an optional field") \
        )
        StructType(NestedField(name=Name(name='required_field', quote_style='', namespace=None), \
field_type=StringType(), is_optional=False, doc='a required field'), \
NestedField(name=Name(name='optional_field', quote_style='', namespace=None), \
field_type=IntegerType(), is_optional=True, doc='an optional field'))

"""

import re
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Generator, Optional, Tuple, cast

from pydantic import BaseModel, Extra
from pydantic.class_validators import AnyCallable

from datajunction_server.enum import StrEnum

if TYPE_CHECKING:
    from datajunction_server.sql.parsing import ast


DECIMAL_REGEX = re.compile(r"(?i)decimal\((?P<precision>\d+),\s*(?P<scale>\d+)\)")
FIXED_PARSER = re.compile(r"(?i)fixed\((?P<length>\d+)\)")
VARCHAR_PARSER = re.compile(r"(?i)varchar(\((?P<length>\d+)\))?")


class Singleton:  # pylint: disable=too-few-public-methods
    """
    Singleton for types
    """

    _instance = None

    def __new__(cls, *args, **kwargs):  # pylint: disable=unused-argument
        if not isinstance(cls._instance, cls):
            cls._instance = super(Singleton, cls).__new__(cls)
        return cls._instance


class ColumnType(BaseModel):
    """
    Base type for all Column Types
    """

    _initialized = False

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        extra = Extra.allow
        arbitrary_types_allowed = True
        underscore_attrs_are_private = False

    def __init__(  # pylint: disable=keyword-arg-before-vararg
        self,
        type_string: str,
        repr_string: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._type_string = type_string
        self._repr_string = repr_string if repr_string else self._type_string
        self._initialized = True

    def __repr__(self):
        return self._repr_string

    def __str__(self):
        return self._type_string

    def __deepcopy__(self, memo):
        return self

    @classmethod
    def __get_validators__(cls) -> Generator[AnyCallable, None, None]:
        """
        One or more validators may be yielded which will be called in the
        order to validate the input, each validator will receive as an input
        the value returned from the previous validator
        """
        yield cls.validate

    @classmethod
    def validate(  # pylint: disable=too-many-return-statements
        cls,
        v: Any,
    ) -> "ColumnType":
        """
        Parses the column type
        """
        from datajunction_server.sql.parsing.backends.antlr4 import (  # pylint: disable=import-outside-toplevel
            parse_rule,
        )

        return cast(ColumnType, parse_rule(str(v), "dataType"))

    def __eq__(self, other: "ColumnType"):  # type: ignore
        """
        Equality is dependent on the string representation of the column type.
        """
        return str(other) == str(self)

    def __hash__(self):
        """
        Equality is dependent on the string representation of the column type.
        """
        return hash(str(self))

    def is_compatible(self, other: "ColumnType") -> bool:
        """
        Returns whether the two types are compatible with each other by
        checking their ancestors.
        """
        if self == NullType() or other == NullType() or self == other:
            return True  # quick return

        def has_common_ancestor(type1, type2) -> bool:
            """
            Helper function to check whether two column types have common ancestors,
            other than the highest-level ancestor types like ColumnType itself. This
            determines whether they're part of the same type group and are compatible
            with each other when performing type compatibility checks.
            """
            base_types = (ColumnType, Singleton, PrimitiveType)
            if type1 in base_types or type2 in base_types:
                return False
            if type1 == type2:
                return True
            current_has = False
            for ancestor in type1.__bases__:
                for ancestor2 in type2.__bases__:
                    current_has = current_has or has_common_ancestor(
                        ancestor,
                        ancestor2,
                    )
                    if current_has:
                        return current_has
            return False

        return has_common_ancestor(self.__class__, other.__class__)


class PrimitiveType(ColumnType):  # pylint: disable=too-few-public-methods
    """Base class for all Column Primitive Types"""


class NumberType(PrimitiveType):  # pylint: disable=too-few-public-methods
    """Base class for all Column Number Types"""


class NullType(PrimitiveType, Singleton):  # pylint: disable=too-few-public-methods
    """A data type for NULL

    Example:
        >>> NullType()
        NullType()
    """

    def __init__(self):
        super().__init__("NULL", "NullType()")


class FixedType(PrimitiveType):
    """A fixed data type.

    Example:
        >>> FixedType(8)
        FixedType(length=8)
        >>> FixedType(8)==FixedType(8)
        True
    """

    _instances: Dict[int, "FixedType"] = {}

    def __new__(cls, length: int):
        cls._instances[length] = cls._instances.get(length) or object.__new__(cls)
        return cls._instances[length]

    def __init__(self, length: int):
        if not self._initialized:
            super().__init__(f"fixed({length})", f"FixedType(length={length})")
            self._length = length

    @property
    def length(self) -> int:  # pragma: no cover
        """
        The length of the fixed type
        """
        return self._length


class LambdaType(ColumnType):
    """
    Type representing a lambda function
    """


class DecimalType(NumberType):
    """A fixed data type.

    Example:
        >>> DecimalType(32, 3)
        DecimalType(precision=32, scale=3)
        >>> DecimalType(8, 3)==DecimalType(8, 3)
        True
    """

    max_precision: ClassVar[int] = 38
    max_scale: ClassVar[int] = 38
    _instances: Dict[Tuple[int, int], "DecimalType"] = {}

    def __new__(cls, precision: int, scale: int):
        key = (
            min(precision, DecimalType.max_precision),
            min(scale, DecimalType.max_scale),
        )
        cls._instances[key] = cls._instances.get(key) or object.__new__(cls)
        return cls._instances[key]

    def __init__(self, precision: int, scale: int):
        if not self._initialized:
            super().__init__(
                f"decimal({precision}, {scale})",
                f"DecimalType(precision={precision}, scale={scale})",
            )
            self._precision = min(precision, DecimalType.max_precision)
            self._scale = min(scale, DecimalType.max_scale)

    @property
    def precision(self) -> int:  # pragma: no cover
        """
        Decimal's precision
        """
        return self._precision

    @property
    def scale(self) -> int:  # pragma: no cover
        """
        Decimal's scale
        """
        return self._scale


class NestedField(ColumnType):
    """Represents a field of a struct, a map key, a map value, or a list element.

    This is where field IDs, names, docs, and nullability are tracked.
    """

    _instances: Dict[
        Tuple[bool, str, ColumnType, Optional[str]],
        "NestedField",
    ] = {}

    def __new__(
        cls,
        name: "ast.Name",
        field_type: ColumnType,
        is_optional: bool = True,
        doc: Optional[str] = None,
    ):
        if isinstance(name, str):  # pragma: no cover
            from datajunction_server.sql.parsing.ast import (  # pylint: disable=import-outside-toplevel
                Name,
            )

            name = Name(name)

        key = (is_optional, name.name, field_type, doc)
        cls._instances[key] = cls._instances.get(key) or object.__new__(cls)
        return cls._instances[key]

    def __init__(
        self,
        name: "ast.Name",
        field_type: ColumnType,
        is_optional: bool = True,
        doc: Optional[str] = None,
    ):
        if not self._initialized:
            if isinstance(name, str):  # pragma: no cover
                from datajunction_server.sql.parsing.ast import (  # pylint: disable=import-outside-toplevel
                    Name,
                )

                name = Name(name)
            doc_string = "" if doc is None else f", doc={repr(doc)}"
            super().__init__(
                (
                    f"{name} {field_type}"
                    f"{' NOT NULL' if not is_optional else ''}"
                    + ("" if doc is None else f" {doc}")
                ),
                f"NestedField(name={repr(name)}, "
                f"field_type={repr(field_type)}, "
                f"is_optional={is_optional}"
                f"{doc_string})",
            )
            self._is_optional = is_optional
            self._name = name
            self._type = field_type
            self._doc = doc

    @property
    def is_optional(self) -> bool:
        """
        Whether the field is optional
        """
        return self._is_optional  # pragma: no cover

    @property
    def is_required(self) -> bool:
        """
        Whether the field is required
        """
        return not self._is_optional  # pragma: no cover

    @property
    def name(self) -> "ast.Name":
        """
        The name of the field
        """
        return self._name

    @property
    def doc(self) -> Optional[str]:
        """
        The docstring of the field
        """
        return self._doc  # pragma: no cover

    @property
    def type(self) -> ColumnType:
        """
        The field's type
        """
        return self._type


class StructType(ColumnType):
    """A struct type

    Example:
        >>> StructType( \
            NestedField("required_field", StringType(), False, "a required field"), \
            NestedField("optional_field", IntegerType(), True, "an optional field") \
        )
        StructType(NestedField(name=Name(name='required_field', quote_style='', namespace=None), \
field_type=StringType(), is_optional=False, doc='a required field'), \
NestedField(name=Name(name='optional_field', quote_style='', namespace=None), \
field_type=IntegerType(), is_optional=True, doc='an optional field'))
    """

    _instances: Dict[Tuple[NestedField, ...], "StructType"] = {}

    def __new__(cls, *fields: NestedField):
        cls._instances[fields] = cls._instances.get(fields) or object.__new__(cls)
        return cls._instances[fields]

    def __init__(self, *fields: NestedField):
        if not self._initialized:
            super().__init__(
                f"struct<{','.join(map(str, fields))}>",
                f"StructType{repr(fields)}",
            )
            self._fields = fields

    @property
    def fields(self) -> Tuple[NestedField, ...]:
        """
        Returns the struct's fields.
        """
        return self._fields  # pragma: no cover

    @property
    def fields_mapping(self) -> Dict[str, NestedField]:
        """
        Returns the struct's fields.
        """
        return {field.name.name: field for field in self._fields}  # pragma: no cover


class ListType(ColumnType):
    """A list type

    Example:
        >>> ListType(element_type=StringType())
        ListType(element_type=StringType())
    """

    _instances: Dict[Tuple[bool, int, ColumnType], "ListType"] = {}

    def __new__(
        cls,
        element_type: ColumnType,
    ):
        key = (element_type,)
        cls._instances[key] = cls._instances.get(key) or object.__new__(cls)  # type: ignore
        return cls._instances[key]  # type: ignore

    def __init__(
        self,
        element_type: ColumnType,
    ):
        if not self._initialized:
            super().__init__(
                f"array<{element_type}>",
                f"ListType(element_type={repr(element_type)})",
            )
            self._element_field = NestedField(
                name="col",  # type: ignore
                field_type=element_type,
                is_optional=False,  # type: ignore
            )

    @property
    def element(self) -> NestedField:
        """
        Returns the list's element
        """
        return self._element_field


class MapType(ColumnType):
    """A map type"""

    _instances: Dict[Tuple[ColumnType, ColumnType], "MapType"] = {}

    def __new__(
        cls,
        key_type: ColumnType,
        value_type: ColumnType,
    ):
        impl_key = (key_type, value_type)
        cls._instances[impl_key] = cls._instances.get(impl_key) or object.__new__(cls)
        return cls._instances[impl_key]

    def __init__(
        self,
        key_type: ColumnType,
        value_type: ColumnType,
    ):
        if not self._initialized:
            super().__init__(
                f"map<{key_type}, {value_type}>",
            )
            self._key_field = NestedField(
                name="key",  # type: ignore
                field_type=key_type,
                is_optional=False,  # type: ignore
            )
            self._value_field = NestedField(
                name="value",  # type: ignore
                field_type=value_type,
                is_optional=False,  # type: ignore
            )

    @property
    def key(self) -> NestedField:
        """
        The map's key
        """
        return self._key_field

    @property
    def value(self) -> NestedField:
        """
        The map's value
        """
        return self._value_field


class BooleanType(PrimitiveType, Singleton):
    """A boolean data type can be represented using an instance of this class.

    Example:
        >>> column_foo = BooleanType()
        >>> isinstance(column_foo, BooleanType)
        True
    """

    def __init__(self):
        super().__init__("boolean", "BooleanType()")


class IntegerBase(NumberType, Singleton):
    """Base class for all integer types"""

    max: ClassVar[int]
    min: ClassVar[int]

    def check_bounds(self, value: int) -> bool:
        """
        Check whether a value fits within the Integer min and max
        """
        return self.__class__.min < value < self.__class__.max


class IntegerType(IntegerBase):
    """An Integer data type can be represented using an instance of this class. Integers are
    32-bit signed and can be promoted to Longs.

    Example:
        >>> column_foo = IntegerType()
        >>> isinstance(column_foo, IntegerType)
        True

    Attributes:
        max (int): The maximum allowed value for Integers, inherited from the
        canonical Column implementation
          in Java (returns `2147483647`)
        min (int): The minimum allowed value for Integers, inherited from the
        canonical Column implementation
          in Java (returns `-2147483648`)
    """

    max: ClassVar[int] = 2147483647

    min: ClassVar[int] = -2147483648

    def __init__(self):
        super().__init__("int", "IntegerType()")


class TinyIntType(IntegerBase):
    """A TinyInt data type can be represented using an instance of this class. TinyInts are
    8-bit signed integers.

    Example:
        >>> column_foo = TinyIntType()
        >>> isinstance(column_foo, TinyIntType)
        True

    Attributes:
        max (int): The maximum allowed value for TinyInts (returns `127`).
        min (int): The minimum allowed value for TinyInts (returns `-128`).
    """

    max: ClassVar[int] = 127

    min: ClassVar[int] = -128

    def __init__(self):
        super().__init__("tinyint", "TinyIntType()")


class SmallIntType(IntegerBase):  # pylint: disable=R0901
    """A SmallInt data type can be represented using an instance of this class. SmallInts are
    16-bit signed integers.

    Example:
        >>> column_foo = SmallIntType()
        >>> isinstance(column_foo, SmallIntType)
        True

    Attributes:
        max (int): The maximum allowed value for SmallInts (returns `32767`).
        min (int): The minimum allowed value for SmallInts (returns `-32768`).
    """

    max: ClassVar[int] = 32767

    min: ClassVar[int] = -32768

    def __init__(self):
        super().__init__("smallint", "SmallIntType()")


class BigIntType(IntegerBase):
    """A Long data type can be represented using an instance of this class. Longs are
    64-bit signed integers.

    Example:
        >>> column_foo = BigIntType()
        >>> isinstance(column_foo, BigIntType)
        True

    Attributes:
        max (int): The maximum allowed value for Longs, inherited from the
        canonical Column implementation
          in Java. (returns `9223372036854775807`)
        min (int): The minimum allowed value for Longs, inherited from the
        canonical Column implementation
          in Java (returns `-9223372036854775808`)
    """

    max: ClassVar[int] = 9223372036854775807

    min: ClassVar[int] = -9223372036854775808

    def __init__(self):
        super().__init__("bigint", "BigIntType()")


class LongType(BigIntType):  # pylint: disable=R0901
    """A Long data type can be represented using an instance of this class. Longs are
    64-bit signed integers.

    Example:
        >>> column_foo = LongType()
        >>> column_foo == LongType()
        True

    Attributes:
        max (int): The maximum allowed value for Longs, inherited from the
        canonical Column implementation
          in Java. (returns `9223372036854775807`)
        min (int): The minimum allowed value for Longs, inherited from the
        canonical Column implementation
          in Java (returns `-9223372036854775808`)
    """

    def __new__(cls, *args, **kwargs):
        self = super().__new__(BigIntType, *args, **kwargs)
        super(BigIntType, self).__init__("long", "LongType()")
        return self


class FloatingBase(NumberType, Singleton):
    """Base class for all floating types"""


class FloatType(FloatingBase):
    """A Float data type can be represented using an instance of this class. Floats are
    32-bit IEEE 754 floating points and can be promoted to Doubles.

    Example:
        >>> column_foo = FloatType()
        >>> isinstance(column_foo, FloatType)
        True
    """

    def __init__(self):
        super().__init__("float", "FloatType()")


class DoubleType(FloatingBase):
    """A Double data type can be represented using an instance of this class. Doubles are
    64-bit IEEE 754 floating points.

    Example:
        >>> column_foo = DoubleType()
        >>> isinstance(column_foo, DoubleType)
        True
    """

    def __init__(self):
        super().__init__("double", "DoubleType()")


class DateTimeBase(PrimitiveType, Singleton):
    """
    Base class for date and time types.
    """

    # pylint: disable=invalid-name
    class Unit(StrEnum):
        """
        Units used for date and time functions and intervals
        """

        dayofyear = "DAYOFYEAR"
        year = "YEAR"
        day = "DAY"
        microsecond = "MICROSECOND"
        month = "MONTH"
        week = "WEEK"
        minute = "MINUTE"
        second = "SECOND"
        quarter = "QUARTER"
        hour = "HOUR"
        millisecond = "MILLISECOND"

    # pylint: enable=invalid-name


class DateType(DateTimeBase):
    """A Date data type can be represented using an instance of this class. Dates are
    calendar dates without a timezone or time.

    Example:
        >>> column_foo = DateType()
        >>> isinstance(column_foo, DateType)
        True
    """

    def __init__(self):
        super().__init__("date", "DateType()")


class TimeType(DateTimeBase):
    """A Time data type can be represented using an instance of this class. Times
    have microsecond precision and are a time of day without a date or timezone.

    Example:
        >>> column_foo = TimeType()
        >>> isinstance(column_foo, TimeType)
        True
    """

    def __init__(self):
        super().__init__("time", "TimeType()")


class TimestampType(DateTimeBase):
    """A Timestamp data type can be represented using an instance of this class. Timestamps in
    Column have microsecond precision and include a date and a time of day without a timezone.

    Example:
        >>> column_foo = TimestampType()
        >>> isinstance(column_foo, TimestampType)
        True
    """

    def __init__(self):
        super().__init__("timestamp", "TimestampType()")


class TimestamptzType(PrimitiveType, Singleton):
    """A Timestamptz data type can be represented using an instance of this class. Timestamptzs in
    Column are stored as UTC and include a date and a time of day with a timezone.

    Example:
        >>> column_foo = TimestamptzType()
        >>> isinstance(column_foo, TimestamptzType)
        True
    """

    def __init__(self):
        super().__init__("timestamptz", "TimestamptzType()")


class IntervalTypeBase(PrimitiveType):
    """A base class for all interval types"""


class DayTimeIntervalType(IntervalTypeBase):
    """A DayTimeIntervalType type.

    Example:
        >>> DayTimeIntervalType()==DayTimeIntervalType("DAY", "SECOND")
        True
    """

    _instances: Dict[Tuple[str, str], "DayTimeIntervalType"] = {}

    def __new__(
        cls,
        from_: DateTimeBase.Unit = DateTimeBase.Unit.day,
        to_: Optional[DateTimeBase.Unit] = DateTimeBase.Unit.second,
    ):
        key = (from_.upper(), to_.upper() if to_ else None)
        cls._instances[key] = cls._instances.get(key) or object.__new__(cls)  # type: ignore
        return cls._instances[key]  # type: ignore

    def __init__(
        self,
        from_: DateTimeBase.Unit = DateTimeBase.Unit.day,
        to_: Optional[DateTimeBase.Unit] = DateTimeBase.Unit.second,
    ):
        if not self._initialized:
            from_ = from_.upper()  # type: ignore
            to_ = to_.upper() if to_ else None  # type: ignore
            to_str = f" TO {to_}" if to_ else ""
            to_repr = f', to="{to_}"' if to_ else ""
            super().__init__(
                f"INTERVAL {from_}{to_str}",
                f'DayTimeIntervalType(from="{from_}"{to_repr})',
            )
            self._from = from_
            self._to = to_

    @property
    def from_(self) -> str:  # pylint: disable=missing-function-docstring
        return self._from  # pragma: no cover

    @property
    def to_(  # pylint: disable=missing-function-docstring
        self,
    ) -> Optional[str]:
        return self._to  # pragma: no cover


class YearMonthIntervalType(IntervalTypeBase):
    """A YearMonthIntervalType type.

    Example:
        >>> YearMonthIntervalType()==YearMonthIntervalType("YEAR", "MONTH")
        True
    """

    _instances: Dict[Tuple[str, str], "YearMonthIntervalType"] = {}

    def __new__(
        cls,
        from_: DateTimeBase.Unit = DateTimeBase.Unit.year,
        to_: Optional[DateTimeBase.Unit] = DateTimeBase.Unit.month,
    ):
        key = (from_.upper(), to_.upper() if to_ else None)
        cls._instances[key] = cls._instances.get(key) or object.__new__(cls)  # type: ignore
        return cls._instances[key]  # type: ignore

    def __init__(
        self,
        from_: DateTimeBase.Unit = DateTimeBase.Unit.year,
        to_: Optional[DateTimeBase.Unit] = DateTimeBase.Unit.month,
    ):
        if not self._initialized:
            from_ = from_.upper()  # type: ignore
            to_ = to_.upper() if to_ else None  # type: ignore
            to_str = f" TO {to_}" if to_ else ""
            to_repr = f', to="{to_}"' if to_ else ""
            super().__init__(
                f"INTERVAL {from_}{to_str}",
                f'YearMonthIntervalType(from="{from_}"{to_repr})',
            )
            self._from = from_
            self._to = to_

    @property
    def from_(self) -> str:  # pylint: disable=missing-function-docstring
        return self._from  # pragma: no cover

    @property
    def to_(  # pylint: disable=missing-function-docstring
        self,
    ) -> Optional[str]:
        return self._to  # pragma: no cover


class StringBase(PrimitiveType, Singleton):
    """Base class for all string types"""


class StringType(StringBase):
    """A String data type can be represented using an instance of this class. Strings in
    Column are arbitrary-length character sequences and are encoded with UTF-8.

    Example:
        >>> column_foo = StringType()
        >>> isinstance(column_foo, StringType)
        True
    """

    def __init__(self):
        super().__init__("string", "StringType()")


class VarcharType(StringBase):
    """A VarcharType data type can be represented using an instance of this class.
    Varchars in Column are arbitrary-length character sequences and are
    encoded with UTF-8.

    Example:
        >>> column_foo = VarcharType()
        >>> isinstance(column_foo, VarcharType)
        True
    """

    def __init__(self, length: Optional[int] = None):
        super().__init__("varchar", "VarcharType()")
        self._length = length

    def __str__(self):
        return (
            f"{self._type_string}({self._length})"
            if self._length
            else self._type_string
        )


class UUIDType(PrimitiveType, Singleton):
    """A UUID data type can be represented using an instance of this class. UUIDs in
    Column are universally unique identifiers.

    Example:
        >>> column_foo = UUIDType()
        >>> isinstance(column_foo, UUIDType)
        True
    """

    def __init__(self):
        super().__init__("uuid", "UUIDType()")


class BinaryType(PrimitiveType, Singleton):
    """A Binary data type can be represented using an instance of this class. Binarys in
    Column are arbitrary-length byte arrays.

    Example:
        >>> column_foo = BinaryType()
        >>> isinstance(column_foo, BinaryType)
        True
    """

    def __init__(self):
        super().__init__("binary", "BinaryType()")


class WildcardType(PrimitiveType, Singleton):
    """A Wildcard datatype.

    Example:
        >>> column_foo = WildcardType()
        >>> isinstance(column_foo, WildcardType)
        True
    """

    def __init__(self):
        super().__init__("wildcard", "WildcardType()")


# Define the primitive data types and their corresponding Python classes
PRIMITIVE_TYPES: Dict[str, PrimitiveType] = {
    "bool": BooleanType(),
    "boolean": BooleanType(),
    "varchar": VarcharType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "tinyint": TinyIntType(),
    "smallint": SmallIntType(),
    "bigint": BigIntType(),
    "long": BigIntType(),
    "float": FloatType(),
    "double": DoubleType(),
    "date": DateType(),
    "time": TimeType(),
    "timestamp": TimestampType(),
    "timestamptz": TimestamptzType(),
    "string": StringType(),
    "str": StringType(),
    "uuid": UUIDType(),
    "byte": BinaryType(),
    "binary": BinaryType(),
    "none": NullType(),
    "null": NullType(),
    "wildcard": WildcardType(),
}
