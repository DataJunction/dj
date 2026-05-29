"""
Tests for ``datajunction_server.models.column`` decorators.
"""

from datajunction_server.models.column import ColumnTypeDecorator
from datajunction_server.sql.parsing.types import (
    ColumnType,
    IntegerType,
    UnknownType,
)


class TestColumnTypeDecorator:
    """ColumnTypeDecorator must always return a ColumnType instance —
    callers (binop type resolution, is_compatible, etc.) assume it. Returning
    a bare string for unparseable values breaks them with cryptic
    AttributeErrors at request time."""

    def test_process_result_value_parses_known_type(self):
        result = ColumnTypeDecorator().process_result_value("int", dialect=None)
        assert isinstance(result, ColumnType)
        assert isinstance(result, IntegerType)

    def test_process_result_value_returns_none_for_falsy(self):
        # Falsy values (None, "") pass through unchanged so the column reads
        # as missing rather than being silently coerced to UnknownType.
        assert ColumnTypeDecorator().process_result_value(None, dialect=None) is None
        assert ColumnTypeDecorator().process_result_value("", dialect=None) == ""

    def test_process_result_value_unparseable_falls_back_to_unknown_type(self):
        # Legacy rows with type='unknown' (or anything else antlr can't parse)
        # used to leak the raw string through, causing AttributeError at the
        # binop type-resolution site. They must round-trip as UnknownType.
        result = ColumnTypeDecorator().process_result_value(
            "unknown",
            dialect=None,
        )
        assert isinstance(result, ColumnType)
        assert isinstance(result, UnknownType)
        # Ensure it still satisfies the contract used by binop type resolution.
        assert result.is_compatible(UnknownType()) is True
