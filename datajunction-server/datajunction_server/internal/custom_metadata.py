"""Resolution, validation, and filter translation for custom_metadata schemas."""

import re

import jsonschema
from sqlalchemy import Numeric, cast, or_, select, text, type_coerce
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


_NUMERIC_KINDS = {"number", "integer"}


async def ensure_expression_index(
    session: AsyncSession,
    key: str,
    value_kind: str | None,
) -> str | None:
    """Build a per-key expression index on noderevision for numeric-typed keys.

    For ``value_kind in {"number", "integer"}`` executes:

        CREATE INDEX IF NOT EXISTS ix_cm_<safe_key>
          ON noderevision (((custom_metadata->>'<key>')::numeric))

    The index name uses only ``[a-z0-9_]`` characters (safe for interpolation).
    The raw key is embedded as a single-quoted SQL string literal via
    ``key.replace("'", "''")`` — the standard SQL quoting escape — so that
    special characters cannot break out of the string literal context.

    Returns the index name on success, or ``None`` when no index is built.
    """
    if value_kind not in _NUMERIC_KINDS:
        return None
    safe = re.sub(r"[^a-z0-9_]", "_", key.lower())
    idx_name = f"ix_cm_{safe}"
    # Embed the key as a SQL string literal (single-quote escape only; no
    # backslash sequences needed for JSONB ->> text extraction).
    quoted_key = key.replace("'", "''")
    await session.execute(
        text(
            f"CREATE INDEX IF NOT EXISTS {idx_name} ON noderevision "
            f"(((custom_metadata->>'{quoted_key}')::numeric))",
        ),
    )
    return idx_name


def split_metadata_path(key: str) -> list[str]:
    """Split a filter key into a JSON path on unescaped dots.

    ``system.lifecycle`` addresses the nested value; ``system\\.lifecycle``
    addresses a top-level key that itself contains a dot.
    """
    parts = re.split(r"(?<!\\)\.", key)
    return [part.replace("\\.", ".") for part in parts]


def _nest(path: list[str], value) -> dict:
    """Wrap *value* in nested objects so containment matches at *path*."""
    for part in reversed(path):
        value = {part: value}
    return value


def _at_path(jsonb, path: list[str]):
    """Return the JSONB expression addressing *path*."""
    return jsonb[path[0]] if len(path) == 1 else jsonb[tuple(path)]


def custom_metadata_clause(col, f: CustomMetadataFilter):
    """Translate one CustomMetadataFilter into a SQLAlchemy boolean over a JSONB column."""
    jsonb = type_coerce(col, JSONB)
    path = split_metadata_path(f.key)
    if f.op == CustomMetadataOp.EXISTS:
        if len(path) == 1:
            return jsonb.has_key(path[0])
        parent = type_coerce(_at_path(jsonb, path[:-1]), JSONB)
        return parent.has_key(path[-1])
    if f.op == CustomMetadataOp.EQ:
        # containment => GIN-servable, at any depth
        return jsonb.contains(_nest(path, f.value))
    if f.op == CustomMetadataOp.NE:
        return ~jsonb.contains(_nest(path, f.value))
    if f.op == CustomMetadataOp.CONTAINS:
        return _at_path(jsonb, path).contains(f.value)
    # range operators: extract as text and cast to numeric
    extracted = _at_path(jsonb, path).astext
    num = cast(extracted, Numeric)
    return {
        CustomMetadataOp.GT: num > f.value,
        CustomMetadataOp.GTE: num >= f.value,
        CustomMetadataOp.LT: num < f.value,
        CustomMetadataOp.LTE: num <= f.value,
    }[f.op]
