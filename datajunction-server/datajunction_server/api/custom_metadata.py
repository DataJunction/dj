"""REST API for the custom_metadata schema registry."""

import datetime

import jsonschema
from fastapi import Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.custom_metadata_schema import CustomMetadataSchema
from datajunction_server.api.helpers import check_namespace_not_git_only
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.custom_metadata import (
    assert_not_reserved_globally,
    check_json_schema,
    ensure_expression_index,
    upsert_schema_row,
)
from datajunction_server.models.custom_metadata import (
    CustomMetadataSchemaCreate,
    CustomMetadataSchemaOutput,
    ViolationReport,
    ViolationSample,
)
from datajunction_server.database.user import User
from datajunction_server.errors import DJAuthorizationException
from datajunction_server.internal.access.authorization import (
    AccessChecker,
    AccessDenialMode,
    get_access_checker,
)
from datajunction_server.models.access import ResourceAction
from datajunction_server.utils import get_current_user, get_session

router = SecureAPIRouter(tags=["metadata-schemas"])


@router.post("/metadata-schemas/", response_model=CustomMetadataSchemaOutput)
async def register_schema(
    data: CustomMetadataSchemaCreate,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> CustomMetadataSchema:
    """Create or upsert a custom_metadata schema registration."""
    check_json_schema(data.key, data.json_schema)

    # Two-tier authorization: a global key or a reserved one is an admin's to
    # register; a namespace-scoped one needs write access to that namespace.
    if data.namespace is None and not current_user.is_admin:
        raise DJAuthorizationException(
            message="Only administrators may register global schema keys.",
        )
    if data.reserved and not current_user.is_admin:
        raise DJAuthorizationException(
            message="Only administrators may register reserved schema keys.",
        )
    if data.namespace is not None:
        access_checker.add_namespace(data.namespace, ResourceAction.WRITE)
        await access_checker.check(on_denied=AccessDenialMode.RAISE)
        # A repo-managed namespace has one writer, and it is not this endpoint.
        # Registering here would also be undone: a deployment reconciles the
        # namespace to exactly what its manifest declares.
        await check_namespace_not_git_only(
            session,
            data.namespace,
            remedy=(
                "Declare it under `custom_metadata_schemas:` in the repo for this "
                "namespace and deploy it."
            ),
        )

    await assert_not_reserved_globally(session, data.key, data.namespace)

    row = await upsert_schema_row(
        session,
        key=data.key,
        namespace=data.namespace,
        node_type=data.node_type.value if data.node_type else None,
        json_schema=data.json_schema,
        filterable=data.filterable,
        description=data.description,
        owner=data.owner,
        reserved=data.reserved,
        current_user_id=current_user.id,
    )
    await session.commit()
    if row.filterable:
        await ensure_expression_index(session, row.key, row.value_kind)
        await session.commit()
    await session.refresh(row)
    return row


@router.get(
    "/metadata-schemas/",
    response_model=list[CustomMetadataSchemaOutput],
)
async def list_schemas(
    key: str | None = None,
    namespace: str | None = None,
    node_type: str | None = None,
    *,
    session: AsyncSession = Depends(get_session),
) -> list[CustomMetadataSchema]:
    """List active registrations, optionally filtered by key, namespace and node_type.

    The filters match the stored scope exactly. They answer "what is registered
    here", not "what governs a node here" -- a schema on `shared` applies to
    `shared.finance` without being registered at it. Resolution answers the
    second question, and exposing it as a filter can follow if callers need it.
    """
    stmt = select(CustomMetadataSchema).where(
        CustomMetadataSchema.deactivated_at.is_(None),
    )
    if key is not None:
        stmt = stmt.where(CustomMetadataSchema.key == key)
    if namespace is not None:
        stmt = stmt.where(CustomMetadataSchema.namespace == namespace)
    if node_type is not None:
        stmt = stmt.where(CustomMetadataSchema.node_type == node_type)
    return list((await session.execute(stmt)).scalars().all())


@router.get(
    "/metadata-schemas/{schema_id}",
    response_model=CustomMetadataSchemaOutput,
)
async def get_schema(
    schema_id: int,
    *,
    session: AsyncSession = Depends(get_session),
) -> CustomMetadataSchema:
    """Fetch one registration by id."""
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
            detail=f"Schema {schema_id} not found or deleted",
        )
    return row


@router.delete("/metadata-schemas/{schema_id}", status_code=200)
async def delete_schema(
    schema_id: int,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    access_checker: AccessChecker = Depends(get_access_checker),
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
    # Authorization
    if row.namespace is None:
        if not current_user.is_admin:
            raise DJAuthorizationException(
                message="Only administrators may delete global schema keys.",
            )
    else:
        access_checker.add_namespace(row.namespace, ResourceAction.WRITE)
        await access_checker.check(on_denied=AccessDenialMode.RAISE)
        # Same reasoning as registering: a deployment would put it straight back.
        await check_namespace_not_git_only(
            session,
            row.namespace,
            remedy=(
                "Remove it from `custom_metadata_schemas:` in the repo for this "
                "namespace and deploy it."
            ),
        )
    row.deactivated_at = datetime.datetime.now(datetime.UTC)
    await session.commit()
    return {"id": schema_id, "deactivated": True}


@router.get(
    "/metadata-schemas/{schema_id}/violations",
    response_model=ViolationReport,
)
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
