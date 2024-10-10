"""Utils for handling GraphQL queries."""
import base64
import re
from typing import Any, Dict, Optional, Tuple

from datajunction_server.typing import UTCDatetime

CURSOR_SEPARATOR = "-"


def convert_camel_case(name):
    """
    Convert from camel case to snake case
    """
    pattern = re.compile(r"(?<!^)(?=[A-Z])")
    name = pattern.sub("_", name).lower()
    return name


def extract_subfields(selection):
    """Extract subfields"""
    subfield = {}
    for sub_selection in selection.selections:
        field_name = convert_camel_case(sub_selection.name)
        if sub_selection.selections:
            subfield[field_name] = extract_subfields(sub_selection)
        else:
            subfield[field_name] = None

    return subfield


def extract_fields(query_fields) -> Dict[str, Any]:
    """
    Extract fields from GraphQL query input into a dictionary
    """
    fields = {}

    for field in query_fields.selected_fields:
        for selection in field.selections:
            field_name = convert_camel_case(selection.name)
            if selection.selections:
                subfield = extract_subfields(selection)
                fields[field_name] = subfield
            else:
                fields[field_name] = None

    return fields


def encode_id(identifier: Tuple[UTCDatetime, int]) -> str:
    """
    Encode the identifier (a tuple of timestamp + integer ID) to a cursor
    """
    combined = (
        f"{int(identifier[0].timestamp() * 1e3)}{CURSOR_SEPARATOR}{identifier[1]}"
    )
    encoded_identifier = base64.urlsafe_b64encode(combined.encode())
    print("encode", int(identifier[0].timestamp() * 1e3), identifier[1])
    return encoded_identifier.decode()


def decode_id(cursor: Optional[str]) -> Tuple[Optional[UTCDatetime], Optional[int]]:
    """
    Decode the cursor to an identifier (a tuple of timestamp + integer ID)
    """
    if not cursor:
        return None, None

    encoded_identifier = base64.urlsafe_b64decode(cursor.encode())
    combined = encoded_identifier.decode()
    timestamp, ide = combined.split(CURSOR_SEPARATOR)
    return (UTCDatetime.fromtimestamp(int(int(timestamp) / 1e3)), int(ide))
