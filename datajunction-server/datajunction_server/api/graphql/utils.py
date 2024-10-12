"""Utils for handling GraphQL queries."""
import re
from typing import Any, Dict

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

    for query_field in query_fields.selected_fields:
        for selection in query_field.selections:
            field_name = convert_camel_case(selection.name)
            if selection.selections:
                subfield = extract_subfields(selection)
                fields[field_name] = subfield
            else:
                fields[field_name] = None

    return fields
