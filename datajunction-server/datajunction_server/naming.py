"""Naming related utils."""
from string import ascii_letters, digits
from typing import List

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


def from_amenable_name(name: str) -> str:
    """
    Takes a string and converts it back to a namespaced name
    """
    for replacement, to_replace in LOOKUP_CHARS.items():
        name = name.replace(f"_{to_replace}_", replacement)
        name = name.replace(f"_{to_replace}", replacement)
        name = name.replace(f"{to_replace}_", replacement)
    return name
