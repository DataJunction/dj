"""
Utilities used around construction
"""

from string import ascii_letters, digits
from typing import TYPE_CHECKING, List, Optional, Set

from sqlalchemy.orm.exc import NoResultFound
from sqlmodel import Session, select

from dj.errors import DJError, DJErrorException, ErrorCode
from dj.models.node import Node, NodeRevision, NodeType

if TYPE_CHECKING:
    from dj.sql.parsing.ast import Name


def get_dj_node(
    session: Session,
    node_name: str,
    kinds: Optional[Set[NodeType]] = None,
) -> NodeRevision:
    """Return the DJ Node with a given name from a set of node types"""
    query = select(Node).filter(Node.name == node_name)
    if kinds:
        query = query.filter(Node.type.in_(kinds))  # type: ignore  # pylint: disable=no-member
    match = None
    try:
        match = session.exec(query).one()
    except NoResultFound as no_result_exc:
        kind_msg = " or ".join(str(k) for k in kinds) if kinds else ""
        raise DJErrorException(
            DJError(
                code=ErrorCode.UNKNOWN_NODE,
                message=f"No node `{node_name}` exists of kind {kind_msg}.",
            ),
        ) from no_result_exc
    return match.current if match else match


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


def to_namespaced_name(name: str) -> "Name":
    """
    Builds a namespaced name from a string
    """
    from dj.sql.parsing.ast import Name  # pylint: disable=import-outside-toplevel

    chunked = name.split(".")
    chunked.reverse()
    current_name = None
    full_name = None
    for chunk in chunked:
        if not current_name:
            current_name = Name(chunk)
            full_name = current_name
        else:
            current_name.namespace = Name(chunk)
            current_name = current_name.namespace
    return full_name  # type: ignore
