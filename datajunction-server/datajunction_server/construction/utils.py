"""
Utilities used around construction
"""

from typing import TYPE_CHECKING, Optional, Set, Union

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import NoResultFound

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import DJError, DJErrorException, ErrorCode
from datajunction_server.models.node_type import NodeType

if TYPE_CHECKING:
    from datajunction_server.sql.parsing.ast import Column, Name


async def get_dj_node(
    session: AsyncSession,
    node_name: str,
    kinds: Optional[Set[NodeType]] = None,
    current: bool = True,
) -> NodeRevision:
    """Return the DJ Node with a given name from a set of node types"""
    query = select(Node).filter(Node.name == node_name)
    if kinds:
        query = query.filter(Node.type.in_(kinds))  # type: ignore  # pylint: disable=no-member
    match = None
    try:
        match = (
            (
                await session.execute(
                    query.options(
                        joinedload(Node.current).options(
                            *NodeRevision.default_load_options()
                        ),
                    ),
                )
            )
            .unique()
            .scalar_one()
        )
    except NoResultFound as no_result_exc:
        kind_msg = " or ".join(str(k) for k in kinds) if kinds else ""
        raise DJErrorException(
            DJError(
                code=ErrorCode.UNKNOWN_NODE,
                message=f"No node `{node_name}` exists of kind {kind_msg}.",
            ),
        ) from no_result_exc
    return match.current if match and current else match


async def try_get_dj_node(
    session: AsyncSession,
    name: Union[str, "Column"],
    kinds: Optional[Set[NodeType]] = None,
) -> Optional[Node]:
    "wraps get dj node to return None if no node is found"
    from datajunction_server.sql.parsing.ast import Column  # pylint: disable=C0415

    if isinstance(name, Column):
        if name.name.namespace is not None:  # pragma: no cover
            name = name.name.namespace.identifier(False)  # pragma: no cover
        else:  # pragma: no cover
            return None  # pragma: no cover
    try:
        return await get_dj_node(session, name, kinds, current=False)
    except DJErrorException:
        return None


def to_namespaced_name(name: str) -> "Name":
    """
    Builds a namespaced name from a string
    """
    from datajunction_server.sql.parsing.ast import (  # pylint: disable=import-outside-toplevel
        Name,
    )

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
