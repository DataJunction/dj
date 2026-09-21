"""Recording and enforcement of tag type claims."""

from http import HTTPStatus

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.tag_type_claim import TagTypeClaim
from datajunction_server.errors import (
    DJActionNotAllowedException,
    DJAlreadyExistsException,
)
from datajunction_server.internal.namespaces import get_parent_namespaces
from datajunction_server.models.deployment import TagTypeClaimSpec


def is_within(namespace: str, scope: str) -> bool:
    """Whether *namespace* is *scope* or beneath it."""
    return namespace == scope or namespace.startswith(f"{scope}.")


async def resolve_claim_namespace(session: AsyncSession, namespace: str) -> str:
    """
    The namespace to record a claim under when deploying *namespace*.

    Deploying `taxonomy.mybranch` records the claim under `taxonomy`, so the claim
    outlives the branch. Follows `parent_namespace` upward, stopping at the first
    governed namespace or at one with no parent.
    """
    seen: set[str] = set()
    current = namespace
    while current not in seen:
        seen.add(current)
        parent = None
        for candidate in [current, *reversed(get_parent_namespaces(current))]:
            row = await NodeNamespace.get(
                session,
                candidate,
                raise_if_not_exists=False,
            )
            if row is None:
                continue
            if row.is_governed_boundary:
                break
            if row.parent_namespace:
                parent = row.parent_namespace
                break
        if parent is None:
            break
        current = parent
    return current


async def upsert_tag_type_claims(
    session: AsyncSession,
    namespace: str,
    specs: list[TagTypeClaimSpec],
    *,
    current_user_id: int,
) -> None:
    """Reconcile *namespace*'s claims to *specs*, releasing the ones it drops."""
    deploy_scope = await resolve_claim_namespace(session, namespace)
    declared: dict[str, str] = {}
    for spec in specs:
        declared[spec.tag_type] = await resolve_claim_namespace(
            session,
            spec.namespace or namespace,
        )

    # The claims it holds, which it may release, plus any claim on a type it
    # declares, which may be a conflict.
    clauses = [
        TagTypeClaim.namespace == deploy_scope,
        TagTypeClaim.namespace.startswith(f"{deploy_scope}.", autoescape=True),
    ]
    if declared:
        clauses.append(TagTypeClaim.tag_type.in_(declared))
    existing = (
        (await session.execute(select(TagTypeClaim).where(or_(*clauses))))
        .scalars()
        .all()
    )
    by_type = {row.tag_type: row for row in existing}

    for tag_type, scope in declared.items():
        row = by_type.get(tag_type)
        if row is not None and not is_within(row.namespace, deploy_scope):
            raise DJAlreadyExistsException(
                message=(
                    f"Tag type `{tag_type}` is already claimed by namespace "
                    f"`{row.namespace}`. A tag type can be claimed by only one "
                    "namespace; release it there before claiming it here."
                ),
            )
        if row is None:
            row = TagTypeClaim(tag_type=tag_type, created_by_id=current_user_id)
            session.add(row)
        row.namespace = scope
        row.updated_by_id = current_user_id

    released = [
        row
        for row in existing
        if row.tag_type not in declared and is_within(row.namespace, deploy_scope)
    ]
    for row in released:
        await session.delete(row)


async def claim_owner(session: AsyncSession, tag_type: str) -> str | None:
    """The namespace claiming *tag_type*, if any."""
    return (await TagTypeClaim.get_owners(session, [tag_type])).get(tag_type)


async def assert_tag_type_unclaimed(session: AsyncSession, tag_type: str) -> None:
    """Refuse creation of a tag whose type a namespace has claimed."""
    owner = await claim_owner(session, tag_type)
    if owner is not None:
        raise DJActionNotAllowedException(
            message=(
                f"Tag type `{tag_type}` is claimed by namespace `{owner}`. Tags of "
                "this type can only be created by a deployment of that namespace."
            ),
            http_status_code=HTTPStatus.CONFLICT,
        )
