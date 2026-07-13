"""REST API for the custom_metadata schema registry."""

import datetime

import jsonschema
from fastapi import Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.custom_metadata_schema import CustomMetadataSchema
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.custom_metadata import ensure_expression_index
from datajunction_server.models.custom_metadata import (
    CustomMetadataSchemaCreate,
    CustomMetadataSchemaOutput,
    ViolationReport,
    ViolationSample,
)
from datajunction_server.utils import get_session

router = SecureAPIRouter(tags=["custom-metadata"])


def _value_kind(json_schema: dict) -> str | None:
    """Extract a single-string type from a JSON Schema, or None."""
    t = json_schema.get("type")
    return t if isinstance(t, str) else None


@router.post("/custom-metadata/schemas/", response_model=CustomMetadataSchemaOutput)
async def register_schema(
    data: CustomMetadataSchemaCreate,
    *,
    session: AsyncSession = Depends(get_session),
) -> CustomMetadataSchema:
    """Create or upsert a custom_metadata schema registration."""
    # Validate that json_schema is itself a valid JSON Schema
    try:
        jsonschema.Draft202012Validator.check_schema(data.json_schema)
    except jsonschema.exceptions.SchemaError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid JSON Schema: {exc.message}",
        )

    node_type_val = data.node_type.value if data.node_type else None

    # Upsert on (key, node_type, namespace) — use select-then-update to handle NULLs
    node_type_clause = (
        CustomMetadataSchema.node_type.is_(None)
        if node_type_val is None
        else CustomMetadataSchema.node_type == node_type_val
    )
    namespace_clause = (
        CustomMetadataSchema.namespace.is_(None)
        if data.namespace is None
        else CustomMetadataSchema.namespace == data.namespace
    )
    existing = (
        await session.execute(
            select(CustomMetadataSchema).where(
                CustomMetadataSchema.key == data.key,
                node_type_clause,
                namespace_clause,
                CustomMetadataSchema.deactivated_at.is_(None),
            ),
        )
    ).scalar_one_or_none()

    if existing:
        existing.json_schema = data.json_schema
        existing.value_kind = _value_kind(data.json_schema)
        existing.filterable = data.filterable
        existing.description = data.description
        row = existing
    else:
        row = CustomMetadataSchema(
            key=data.key,
            node_type=node_type_val,
            namespace=data.namespace,
            json_schema=data.json_schema,
            value_kind=_value_kind(data.json_schema),
            filterable=data.filterable,
            description=data.description,
        )
        session.add(row)
    await session.commit()
    if row.filterable:
        await ensure_expression_index(session, row.key, row.value_kind)
        await session.commit()
    await session.refresh(row)
    return row


@router.get(
    "/custom-metadata/schemas/",
    response_model=list[CustomMetadataSchemaOutput],
)
async def list_schemas(
    namespace: str | None = None,
    node_type: str | None = None,
    *,
    session: AsyncSession = Depends(get_session),
) -> list[CustomMetadataSchema]:
    """List active schema registrations, optionally filtered by namespace and node_type."""
    stmt = select(CustomMetadataSchema).where(
        CustomMetadataSchema.deactivated_at.is_(None),
    )
    if namespace is not None:
        stmt = stmt.where(CustomMetadataSchema.namespace == namespace)
    if node_type is not None:
        stmt = stmt.where(CustomMetadataSchema.node_type == node_type)
    return list((await session.execute(stmt)).scalars().all())


@router.delete("/custom-metadata/schemas/{schema_id}", status_code=200)
async def delete_schema(
    schema_id: int,
    *,
    session: AsyncSession = Depends(get_session),
) -> dict:
    """Soft-delete a schema registration by setting deactivated_at."""
    row = (
        await session.execute(
            select(CustomMetadataSchema).where(
                CustomMetadataSchema.id == schema_id,
                CustomMetadataSchema.deactivated_at.is_(None),
            ),
        )
    ).scalar_one_or_none()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Schema {schema_id} not found or already deleted",
        )
    row.deactivated_at = datetime.datetime.now(datetime.timezone.utc)
    await session.commit()
    return {"id": schema_id, "deactivated": True}


@router.get("/custom-metadata/facets/", response_model=list[CustomMetadataSchemaOutput])
async def list_facets(
    *,
    session: AsyncSession = Depends(get_session),
) -> list[CustomMetadataSchema]:
    """Return the curated filterable catalog (schemas with filterable=True)."""
    stmt = select(CustomMetadataSchema).where(
        CustomMetadataSchema.deactivated_at.is_(None),
        CustomMetadataSchema.filterable.is_(True),
    )
    return list((await session.execute(stmt)).scalars().all())


@router.get("/custom-metadata/violations/", response_model=ViolationReport)
async def list_violations(
    schema_id: int,
    *,
    session: AsyncSession = Depends(get_session),
) -> ViolationReport:
    """
    Advisory read-only report: find current-revision nodes whose custom_metadata
    violates the registered schema identified by schema_id.

    Never mutates. Returns a count and a sample of violating nodes.
    """
    schema_row = (
        await session.execute(
            select(CustomMetadataSchema).where(
                CustomMetadataSchema.id == schema_id,
            ),
        )
    ).scalar_one_or_none()
    if schema_row is None:
        raise HTTPException(status_code=404, detail=f"Schema {schema_id} not found")

    # Fetch all current-revision node revisions that have custom_metadata set
    stmt = (
        select(NodeRevision)
        .join(Node, Node.id == NodeRevision.node_id)
        .where(
            Node.current_version == NodeRevision.version,
            Node.deactivated_at.is_(None),
            NodeRevision.custom_metadata.isnot(None),
        )
    )
    revisions = (await session.execute(stmt)).scalars().all()

    validator = jsonschema.Draft202012Validator(schema_row.json_schema)
    key = schema_row.key
    samples: list[ViolationSample] = []

    for rev in revisions:
        cm = rev.custom_metadata or {}
        if key not in cm:
            continue
        errors = [err.message for err in validator.iter_errors(cm[key])]
        if errors:
            samples.append(ViolationSample(node_name=rev.name, errors=errors))

    return ViolationReport(
        schema_id=schema_id,
        violation_count=len(samples),
        samples=samples[:20],  # cap sample size
    )
