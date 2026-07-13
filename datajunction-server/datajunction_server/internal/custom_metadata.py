"""Resolution, validation, and filter translation for custom_metadata schemas."""

import jsonschema
from sqlalchemy import Numeric, cast, or_, select, type_coerce
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.custom_metadata_schema import CustomMetadataSchema
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.custom_metadata import (
    CustomMetadataFilter,
    CustomMetadataOp,
)
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

    Exception: if a key has a global row (``node_type IS NULL, namespace IS NULL``)
    with ``reserved=True``, that row's schema is always used for that key —
    any more-specific scoped rows for the same key are ignored.
    """
    stmt = select(CustomMetadataSchema).where(
        CustomMetadataSchema.deactivated_at.is_(None),
        or_(
            CustomMetadataSchema.node_type.is_(None),
            CustomMetadataSchema.node_type == node_type.value,
        ),
    )
    rows = (await session.execute(stmt)).scalars().all()

    # First pass: identify keys that have a reserved global row
    reserved_global_keys: set[str] = {
        row.key
        for row in rows
        if row.reserved
        and row.node_type is None
        and row.namespace is None
        and row.deactivated_at is None
    }

    best: dict[str, tuple[int, dict]] = {}
    for row in rows:
        if not _namespace_matches(row.namespace, namespace):
            continue
        # If this key is reserved-global, only accept the global row itself
        if row.key in reserved_global_keys:
            if row.node_type is not None or row.namespace is not None:
                continue  # skip scoped rows for reserved keys
        score = _specificity(row)
        if row.key not in best or score > best[row.key][0]:
            best[row.key] = (score, row.json_schema)
    return {key: schema for key, (_, schema) in best.items()}


async def validate_custom_metadata(
    session: AsyncSession,
    namespace: str | None,
    node_type: NodeType,
    custom_metadata: dict | None,
) -> None:
    """Lax, write-time validation: validate only keys with a resolved schema."""
    if not custom_metadata:
        return
    schemas = await resolve_schemas(session, namespace, node_type)
    if not schemas:
        return
    errors: list[str] = []
    for key, value in custom_metadata.items():
        schema = schemas.get(key)
        if schema is None:
            continue  # lax: unregistered keys pass
        validator = jsonschema.Draft202012Validator(schema)
        for err in validator.iter_errors(value):
            errors.append(f"custom_metadata.{key}: {err.message}")
    if errors:
        raise DJInvalidInputException(
            message="custom_metadata failed schema validation:\n" + "\n".join(errors),
        )


def custom_metadata_clause(col, f: CustomMetadataFilter):
    """Translate one CustomMetadataFilter into a SQLAlchemy boolean over a JSONB column."""
    jsonb = type_coerce(col, JSONB)
    if f.op == CustomMetadataOp.EXISTS:
        return jsonb.has_key(f.key)
    if f.op == CustomMetadataOp.EQ:
        # containment => GIN-servable
        return jsonb.contains({f.key: f.value})
    if f.op == CustomMetadataOp.NE:
        return ~jsonb.contains({f.key: f.value})
    if f.op == CustomMetadataOp.CONTAINS:
        return jsonb[f.key].contains(f.value)
    # range operators: extract as text and cast to numeric
    extracted = jsonb[f.key].astext
    num = cast(extracted, Numeric)
    return {
        CustomMetadataOp.GT: num > f.value,
        CustomMetadataOp.GTE: num >= f.value,
        CustomMetadataOp.LT: num < f.value,
        CustomMetadataOp.LTE: num <= f.value,
    }[f.op]
