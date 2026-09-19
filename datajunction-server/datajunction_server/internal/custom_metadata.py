"""Resolution, validation, and filter translation for custom_metadata schemas."""

import datetime
import re

import jsonschema
from sqlalchemy import Numeric, cast, or_, select, text, type_coerce
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.custom_metadata_schema import CustomMetadataSchema
from datajunction_server.errors import (
    DJAlreadyExistsException,
    DJInvalidInputException,
)
from datajunction_server.models.custom_metadata import (
    CustomMetadataFilter,
    CustomMetadataOp,
)
from datajunction_server.models.deployment import CustomMetadataSchemaSpec
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


def value_kind(json_schema: dict) -> str | None:
    """The single string ``type`` a JSON Schema declares, or None."""
    declared = json_schema.get("type")
    return declared if isinstance(declared, str) else None


def check_json_schema(key: str, json_schema: dict) -> None:
    """Raise if *json_schema* is not itself a valid JSON Schema."""
    try:
        jsonschema.Draft202012Validator.check_schema(json_schema)
    except jsonschema.exceptions.SchemaError as exc:
        raise DJInvalidInputException(
            message=(
                f"Invalid JSON Schema for custom_metadata key '{key}': {exc.message}"
            ),
        ) from exc


def scope_clause(key: str, namespace: str | None, node_type: str | None):
    """Where-clauses selecting the one row that owns (key, namespace, node_type).

    Deliberately does not filter on ``deactivated_at``: the unique index spans
    soft-deleted rows, so a caller that hides them would insert into a collision.
    """
    return [
        CustomMetadataSchema.key == key,
        CustomMetadataSchema.namespace.is_(None)
        if namespace is None
        else CustomMetadataSchema.namespace == namespace,
        CustomMetadataSchema.node_type.is_(None)
        if node_type is None
        else CustomMetadataSchema.node_type == node_type,
    ]


async def assert_not_reserved_globally(
    session: AsyncSession,
    key: str,
    namespace: str | None,
) -> None:
    """Refuse a namespace registration of a key an admin reserved globally."""
    if namespace is None:
        return
    reserved = (
        await session.execute(
            select(CustomMetadataSchema).where(
                CustomMetadataSchema.key == key,
                CustomMetadataSchema.namespace.is_(None),
                CustomMetadataSchema.reserved.is_(True),
                CustomMetadataSchema.deactivated_at.is_(None),
            ),
        )
    ).scalar_one_or_none()
    if reserved is not None:
        raise DJAlreadyExistsException(
            message=(
                f"Key '{key}' is reserved globally and cannot be registered at "
                "namespace scope."
            ),
        )


async def upsert_schema_row(
    session: AsyncSession,
    *,
    key: str,
    namespace: str | None,
    node_type: str | None,
    json_schema: dict,
    filterable: bool,
    description: str | None,
    reserved: bool,
    current_user_id: int,
) -> CustomMetadataSchema:
    """Create, update, or revive the single row owning this scope.

    Reviving rather than inserting alongside: the unique index counts
    soft-deleted rows while every read hides them, so an insert beside a
    tombstone violates the constraint. The row keeps its id and created_at, so
    a retired key that comes back is the same registration, not a new one.

    Does not commit -- the caller owns the transaction, which is what lets a
    dry-run deployment roll the registration back.
    """
    row = (
        await session.execute(
            select(CustomMetadataSchema).where(
                *scope_clause(key, namespace, node_type),
            ),
        )
    ).scalar_one_or_none()
    if row is None:
        row = CustomMetadataSchema(
            key=key,
            namespace=namespace,
            node_type=node_type,
            created_by_id=current_user_id,
        )
        session.add(row)
    row.json_schema = json_schema
    row.value_kind = value_kind(json_schema)
    row.filterable = filterable
    row.description = description
    row.reserved = reserved
    row.updated_by_id = current_user_id
    row.deactivated_at = None
    return row


async def upsert_schema_specs(
    session: AsyncSession,
    namespace: str,
    specs: list[CustomMetadataSchemaSpec],
    *,
    current_user_id: int,
    build_indexes: bool = True,
) -> None:
    """Reconcile schema rows to exactly *specs*.

    Declared keys are upserted; rows in scope the specs no longer declare are
    soft-deleted. Global rows are never touched -- no namespace owns them, so no
    deployment can retire one.

    A spec carries its own namespace, defaulted to the deployment's by
    `DeploymentSpec.set_namespaces` and constrained there to that namespace or one
    beneath it. Reconciliation covers the deploying namespace plus whatever
    sub-namespaces the specs name, and nothing else: declaring a schema for
    `shared.conformed` must not retire rows for `shared.finance`, which a different
    deployment owns, while an empty spec list still retires the deploying
    namespace's own rows.

    Every schema is checked before anything is written, so a malformed spec fails
    the deployment rather than half-applying it. ``build_indexes=False`` skips
    index DDL for a dry run.
    """
    for spec in specs:
        check_json_schema(spec.key, spec.json_schema)
        await assert_not_reserved_globally(
            session,
            spec.key,
            spec.namespace or namespace,
        )

    declared: set[tuple[str, str | None, str]] = set()
    for spec in specs:
        node_type_val = spec.node_type.value if spec.node_type is not None else None
        scope = spec.namespace or namespace
        declared.add((spec.key, node_type_val, scope))
        await upsert_schema_row(
            session,
            key=spec.key,
            namespace=scope,
            node_type=node_type_val,
            json_schema=spec.json_schema,
            filterable=spec.filterable,
            description=spec.description,
            reserved=False,
            current_user_id=current_user_id,
        )

    scopes = {namespace} | {scope for _, _, scope in declared}
    existing = (
        (
            await session.execute(
                select(CustomMetadataSchema).where(
                    CustomMetadataSchema.namespace.in_(scopes),
                    CustomMetadataSchema.deactivated_at.is_(None),
                ),
            )
        )
        .scalars()
        .all()
    )
    now = datetime.datetime.now(datetime.UTC)
    for row in existing:
        if (row.key, row.node_type, row.namespace) not in declared:
            row.deactivated_at = now

    if build_indexes:
        await session.flush()
        for spec in specs:
            if spec.filterable:
                await ensure_expression_index(
                    session,
                    spec.key,
                    value_kind(spec.json_schema),
                )


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
