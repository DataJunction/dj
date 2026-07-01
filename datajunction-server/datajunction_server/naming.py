"""Naming related utils."""

from string import ascii_letters, digits
from typing import Any, Iterable, List

SEPARATOR = "."

ACCEPTABLE_CHARS = set(ascii_letters + digits + "_")
LOOKUP_CHARS = {
    ".": "DOT",
    "'": "QUOTE",
    '"': "DQUOTE",
    "`": "BTICK",
    "!": "EXCL",
    "@": "AT",
    "#": "HASH",
    "$": "DOLLAR",
    "%": "PERC",
    "^": "CARAT",
    "&": "AMP",
    "*": "STAR",
    "(": "LPAREN",
    ")": "RPAREN",
    "[": "LBRACK",
    "]": "RBRACK",
    "-": "MINUS",
    "+": "PLUS",
    "=": "EQ",
    "/": "FSLSH",
    "\\": "BSLSH",
    "|": "PIPE",
    "~": "TILDE",
    ">": "GT",
    "<": "LT",
}


def amenable_name(name: str) -> str:
    """Takes a string and makes it have only alphanumerics"""
    ret: List[str] = []
    cont: List[str] = []
    for char in name:
        if char in ACCEPTABLE_CHARS:
            cont.append(char)
        else:
            ret.append("".join(cont))
            ret.append(LOOKUP_CHARS.get(char, "UNK"))
            cont = []

    return ("_".join(ret) + "_" + "".join(cont)).strip("_")


def amenable_col_names(columns: Iterable[Any]) -> str:
    """Return underscore-joined amenable names for a sequence of SQL column nodes.

    Applies :func:`amenable_name` to ``str(col)`` for each item and joins with
    ``"_"``.  Used by both ``decompose.py`` component naming and
    ``measures.py`` grain-column alias derivation so both code paths produce
    identical identifiers for the same leaf columns.

    Repeated column references (e.g. the same predicate column appearing
    twice in ``IF(c IS NULL OR c <> 'X', ...)``) are deduped so the
    generated identifier stays stable and within identifier-length limits.

    Example::

        amenable_col_names([col("is_product_view"), col("session_id")])
        -> "is_product_view_session_id"
    """
    seen: set[str] = set()
    parts: list[str] = []
    for col in columns:
        name = amenable_name(str(col))
        if name in seen:
            continue
        seen.add(name)
        parts.append(name)
    return "_".join(parts)


def from_amenable_name(name: str) -> str:
    """
    Takes a string and converts it back to a namespaced name
    """
    for replacement, to_replace in LOOKUP_CHARS.items():
        name = name.replace(f"_{to_replace}_", replacement)
        name = name.replace(f"_{to_replace}", replacement)
        name = name.replace(f"{to_replace}_", replacement)
    return name
