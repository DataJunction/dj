"""
CEL operations that can be used in conditions. They are matched on overload
identifiers, defined in https://pkg.go.dev/cel.dev/cel-go/common/overloads

    node.description != ''    ->  not_equals        allowed
    node.name.startsWith('x') ->  starts_with_string  refused
"""

from collections.abc import Iterator

ALLOWED_OVERLOADS = frozenset(
    {
        "logical_and",
        "logical_or",
        "logical_not",
        "not_strictly_false",
        "conditional",
        "equals",
        "not_equals",
        "in_list",
        "greater_equals_int64",
        "greater_int64",
        "less_equals_int64",
        "less_int64",
        "size_list",
        "size_map",
        "size_string",
        "size_bytes",
        # RE2, so a pattern cannot backtrack into a hang. Needed for rules
        # about the text of a name, which have no other way to be written.
        "matches_string",
        "multiply_int64",
        "add_int64",
        "add_list",  # filter macro expansion
    },
)


# cel.expr.CheckedExpr.reference_map (cel-spec checked.proto).
_CHECKED_EXPR_REFERENCE_MAP = 2
# cel.expr.Reference fields.
_REFERENCE_OVERLOAD_ID = 3
# google.protobuf.Any.value.
_ANY_VALUE = 2

_WIRE_VARINT = 0
_WIRE_64BIT = 1
_WIRE_LENGTH_DELIMITED = 2
_WIRE_32BIT = 5


def called_overloads(serialized: bytes) -> set[str]:
    """Resolved overload ids for every function the expression calls."""
    return {
        value.decode("utf-8")
        for reference in _references(serialized)
        for field, value in _fields(reference)
        if field == _REFERENCE_OVERLOAD_ID
    }


def _references(serialized: bytes) -> Iterator[bytes]:
    checked = _unwrap_any(serialized)
    for field, entry in _fields(checked):
        if field != _CHECKED_EXPR_REFERENCE_MAP:
            continue
        # The entry's key is an int64 expr id, skipped as a varint, so only the
        # Reference value surfaces.
        for _, reference in _fields(entry):
            yield reference


def _unwrap_any(serialized: bytes) -> bytes:
    for field, payload in _fields(serialized):
        if field == _ANY_VALUE:
            return payload
    return serialized  # pragma: no cover - serialize() always wraps in an Any


def _fields(buf: bytes) -> Iterator[tuple[int, bytes]]:
    """Walk one protobuf message, yielding its length-delimited fields."""
    offset, size = 0, len(buf)
    while offset < size:
        tag, offset = _varint(buf, offset)
        field, wire = tag >> 3, tag & 0x07
        if wire == _WIRE_LENGTH_DELIMITED:
            length, offset = _varint(buf, offset)
            yield field, buf[offset : offset + length]
            offset += length
        elif wire == _WIRE_VARINT:
            _, offset = _varint(buf, offset)
        elif wire == _WIRE_32BIT:
            offset += 4
        elif wire == _WIRE_64BIT:
            offset += 8
        else:
            raise ValueError(f"unsupported protobuf wire type {wire}")


def _varint(buf: bytes, offset: int) -> tuple[int, int]:
    """Read a base-128 varint, returning its value and the offset after it."""
    result = shift = 0
    while True:
        byte = buf[offset]
        offset += 1
        # Low seven bits carry the value, least significant group first; the
        # high bit means another byte follows.
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, offset
        shift += 7


def denied_overloads(serialized: bytes) -> set[str]:
    """Overloads the expression calls that are not permitted."""
    return called_overloads(serialized) - ALLOWED_OVERLOADS
