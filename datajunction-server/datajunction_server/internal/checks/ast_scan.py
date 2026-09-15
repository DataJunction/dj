"""
Extract the functions a compiled check actually calls.

cel-expr-python accepts the CEL env-config `stdlib: include:` block that would
restrict the callable surface, but silently drops it -- it does not survive
``EnvConfig.to_yaml()`` and ``matches()`` still compiles -- so the allowlist has
to be built here instead. ``Expression.serialize()`` returns a protobuf Any
wrapping a ``cel.expr.CheckedExpr``, whose reference map holds every resolved
reference by name and overload id. That map is what we scan, by walking the wire
format directly rather than generating protos for one field path.

Recheck on each upgrade: if the library ever honours stdlib subsetting, this
module deletes.
"""

from collections.abc import Iterator

# cel.expr.CheckedExpr.reference_map (cel-spec checked.proto).
_CHECKED_EXPR_REFERENCE_MAP = 2
# cel.expr.Reference fields.
_REFERENCE_NAME = 1
_REFERENCE_OVERLOAD_ID = 3
# google.protobuf.Any.value.
_ANY_VALUE = 2

# Protobuf wire types.
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


def referenced_names(serialized: bytes) -> set[str]:
    """Every name in the reference map: identifiers and functions alike."""
    return {
        value.decode("utf-8")
        for reference in _references(serialized)
        for field, value in _fields(reference)
        if field == _REFERENCE_NAME
    }


def _references(serialized: bytes) -> Iterator[bytes]:
    checked = _unwrap_any(serialized)
    for field, entry in _fields(checked):
        if field != _CHECKED_EXPR_REFERENCE_MAP:
            continue
        # A map entry's key here is the int64 expression id, which the walker
        # skips as a varint, so the Reference value is all that surfaces.
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
    result = shift = 0
    while True:
        byte = buf[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, offset
        shift += 7
