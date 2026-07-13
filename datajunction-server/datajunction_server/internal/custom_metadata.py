"""Resolution, validation, and filter translation for custom_metadata schemas."""

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.custom_metadata_schema import CustomMetadataSchema
from datajunction_server.models.node_type import NodeType


def _namespace_matches(scope: str | None, node_namespace: str | None) -> bool:
    """Return True if *scope* is applicable to *node_namespace*.

    A ``None`` scope is global and always matches.  Otherwise the node's
    namespace must equal the scope exactly or be a sub-namespace of it
    (i.e. start with ``scope + "."``).
    """
    if scope is None:
        return True  # global row applies to every namespace
    if node_namespace is None:
        return False
    return node_namespace == scope or node_namespace.startswith(scope + ".")


def _specificity(row: CustomMetadataSchema) -> int:
    """Return a score reflecting how specific *row* is.

    Scoring (additive):
      +2  namespace is set
      +1  node_type is set
    Possible values: 0 (global) … 3 (both namespace + node_type).
    """
    return (1 if row.node_type is not None else 0) + (
        2 if row.namespace is not None else 0
    )


async def resolve_schemas(
    session: AsyncSession,
    namespace: str | None,
    node_type: NodeType,
) -> dict[str, dict]:
    """Return ``{key: json_schema}`` using the most-specific active row per key.

    Rows are filtered to those that are:
    - not deactivated (``deactivated_at IS NULL``)
    - applicable to *node_type* (either global or matching exactly)
    - applicable to *namespace* via :func:`_namespace_matches`

    Among qualifying rows for the same key the one with the highest
    :func:`_specificity` score wins.
    """
    stmt = select(CustomMetadataSchema).where(
        CustomMetadataSchema.deactivated_at.is_(None),
        or_(
            CustomMetadataSchema.node_type.is_(None),
            CustomMetadataSchema.node_type == node_type.value,
        ),
    )
    rows = (await session.execute(stmt)).scalars().all()

    best: dict[str, tuple[int, dict]] = {}
    for row in rows:
        if not _namespace_matches(row.namespace, namespace):
            continue
        score = _specificity(row)
        if row.key not in best or score > best[row.key][0]:
            best[row.key] = (score, row.json_schema)
    return {key: schema for key, (_, schema) in best.items()}
