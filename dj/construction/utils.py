"""
Utilities used around construction
"""

from string import ascii_letters, digits
from typing import List, Optional, Set

from sqlalchemy.orm.exc import NoResultFound
from sqlmodel import Session, select

from dj.construction.exceptions import CompoundBuildException
from dj.errors import DJError, ErrorCode
from dj.models.node import Node, NodeType
from dj.sql.parsing.ast import Namespace


def make_name(namespace: Optional[Namespace], name="") -> str:
    """utility taking a namespace and name to make a possible name of a DJ Node"""
    ret = ""
    if namespace:
        ret += ".".join(name.name for name in namespace.names)
    if name:
        ret += ("." if ret else "") + name
    return ret


def get_dj_node(
    session: Session,
    node_name: str,
    kinds: Optional[Set[NodeType]] = None,
) -> Optional[Node]:
    """Return the DJ Node with a given name from a set of node types"""
    query = select(Node).filter(Node.name == node_name)
    match = None
    try:
        match = session.exec(query).one()
    except NoResultFound:
        kind_msg = " or ".join(str(k) for k in kinds) if kinds else ""
        CompoundBuildException().append(
            error=DJError(
                code=ErrorCode.UNKNOWN_NODE,
                message=f"No node `{node_name}` exists of kind {kind_msg}.",
            ),
            message=f"Cannot get DJ node {node_name}",
        )
    # found a node but it's not the right kind
    if match and kinds and (match.type not in kinds):
        CompoundBuildException().append(  # pragma: no cover
            error=DJError(
                code=ErrorCode.NODE_TYPE_ERROR,
                message=(
                    f"Node `{match.name}` is of type `{str(match.type).upper()}`. "
                    f"Expected kind to be of {' or '.join(str(k) for k in kinds)}."
                ),
            ),
            message=f"Cannot get DJ node {node_name}",
        )

    return match


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

    return "_".join(ret) + "_" + "".join(cont)
