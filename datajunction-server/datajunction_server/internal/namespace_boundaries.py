"""Provisioning and lifecycle rules for governed namespace boundaries."""

import hashlib
import json
from datetime import UTC, datetime

from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload, selectinload

from datajunction_server.database.history import History
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.namespace_boundary import NamespaceBoundary
from datajunction_server.database.node import Node
from datajunction_server.database.rbac import Role, RoleAssignment, RoleScope
from datajunction_server.database.user import PrincipalKind, User
from datajunction_server.enum import StrEnum
from datajunction_server.errors import (
    DJActionNotAllowedException,
    DJAlreadyExistsException,
    DJInternalErrorException,
    DJInvalidInputException,
)
from datajunction_server.internal.history import ActivityType, EntityType
from datajunction_server.internal.namespace_locks import (
    lock_namespace_boundary_lifecycle,
)
from datajunction_server.internal.namespaces import (
    get_parent_namespaces,
    validate_namespace,
)
from datajunction_server.models.access import ResourceAction, ResourceType
from datajunction_server.models.namespace import NamespaceProvisionResponse

ROLE_NAME_MAX_LENGTH = 255


class BoundaryRoleKind(StrEnum):
    """The fixed role types generated for a governed namespace boundary."""

    OWNER = "owner"
    DEPLOYER = "deployer"


def _boundary_role_names(namespace: str) -> tuple[str, str]:
    owner_role_name = f"namespace:{namespace}:owners"
    deployer_role_name = f"namespace:{namespace}:deployers"
    if len(deployer_role_name) > ROLE_NAME_MAX_LENGTH:
        raise DJInvalidInputException(
            message=(
                f"Namespace `{namespace}` is too long to provision. Generated role "
                f"names cannot exceed {ROLE_NAME_MAX_LENGTH} characters."
            ),
        )
    return owner_role_name, deployer_role_name


def _request_fingerprint(
    owner_principal_id: int,
    deployer_principal_ids: list[int],
) -> str:
    request = {
        "owner_principal_id": owner_principal_id,
        "deployer_principal_ids": sorted(deployer_principal_ids),
    }
    encoded = json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def namespace_boundary_scope_targets(
    namespace: str,
) -> list[tuple[ResourceType, str]]:
    """Return every authorization scope controlled by a namespace boundary."""
    return [
        (ResourceType.NAMESPACE, namespace),
        (ResourceType.NAMESPACE, f"{namespace}.*"),
        (ResourceType.NODE, f"{namespace}.*"),
    ]


def _boundary_scopes(
    namespace: str,
    action: ResourceAction,
) -> list[RoleScope]:
    return [
        RoleScope(action=action, scope_type=scope_type, scope_value=scope_value)
        for scope_type, scope_value in namespace_boundary_scope_targets(namespace)
    ]


async def get_boundary_role_kind(
    session: AsyncSession,
    role_id: int,
    *,
    for_update: bool = False,
) -> tuple[NamespaceBoundary, BoundaryRoleKind] | None:
    """Return the boundary and generated role kind for a role, if managed."""
    statement = select(NamespaceBoundary).where(
        or_(
            NamespaceBoundary.owner_role_id == role_id,
            NamespaceBoundary.deployer_role_id == role_id,
        ),
    )
    if for_update:
        statement = statement.with_for_update()
    boundary = (await session.execute(statement)).scalar_one_or_none()
    if boundary is None:
        return None
    kind = (
        BoundaryRoleKind.OWNER
        if boundary.owner_role_id == role_id
        else BoundaryRoleKind.DEPLOYER
    )
    return boundary, kind


async def reject_boundary_role_definition_change(
    session: AsyncSession,
    role: Role,
) -> None:
    """Keep generated role names and scopes tied to their namespace boundary."""
    managed_role = await get_boundary_role_kind(session, role.id)
    if managed_role:
        boundary, kind = managed_role
        raise DJActionNotAllowedException(
            message=(
                f"Role `{role.name}` is the managed {kind.value} role for namespace "
                f"boundary `{boundary.namespace}`. Its name, scopes, and lifecycle "
                "cannot be changed through the role API."
            ),
            http_status_code=409,
        )


async def validate_boundary_role_assignment(
    session: AsyncSession,
    role: Role,
    principal: User,
    expires_at: datetime | None,
) -> None:
    """Enforce principal types and owner permanence for generated roles."""
    managed_role = await get_boundary_role_kind(session, role.id)
    if managed_role is None:
        return

    boundary, kind = managed_role
    expected_kind = (
        PrincipalKind.GROUP
        if kind == BoundaryRoleKind.OWNER
        else PrincipalKind.SERVICE_ACCOUNT
    )
    if principal.kind != expected_kind:
        raise DJInvalidInputException(
            message=(
                f"The managed {kind.value} role for namespace boundary "
                f"`{boundary.namespace}` can only be assigned to "
                f"{expected_kind.value} principals."
            ),
        )
    if kind == BoundaryRoleKind.OWNER and expires_at is not None:
        raise DJInvalidInputException(
            message="Managed namespace owner assignments cannot expire.",
        )


async def validate_boundary_role_revocation(
    session: AsyncSession,
    role: Role,
    principal_id: int,
) -> None:
    """Prevent revocation of the last effective namespace owner."""
    managed_role = await get_boundary_role_kind(
        session,
        role.id,
        for_update=True,
    )
    if managed_role is None or managed_role[1] != BoundaryRoleKind.OWNER:
        return

    remaining_owner_count = await session.scalar(
        select(func.count())
        .select_from(RoleAssignment)
        .where(
            RoleAssignment.role_id == role.id,
            RoleAssignment.principal_id != principal_id,
            or_(
                RoleAssignment.expires_at.is_(None),
                RoleAssignment.expires_at > datetime.now(UTC),
            ),
        ),
    )
    if not remaining_owner_count:
        raise DJActionNotAllowedException(
            message=(
                f"Cannot revoke the last owner of namespace boundary "
                f"`{managed_role[0].namespace}`. Assign another owner group first."
            ),
            http_status_code=409,
        )


async def _overlapping_boundaries(
    session: AsyncSession,
    namespace: str,
) -> list[str]:
    parents = get_parent_namespaces(namespace)
    conditions = [
        NamespaceBoundary.namespace.startswith(f"{namespace}.", autoescape=True),
    ]
    if parents:
        conditions.append(NamespaceBoundary.namespace.in_(parents))
    return list(
        (
            await session.execute(
                select(NamespaceBoundary.namespace)
                .where(or_(*conditions))
                .order_by(NamespaceBoundary.namespace),
            )
        )
        .scalars()
        .all(),
    )


async def _existing_ungoverned_content(
    session: AsyncSession,
    namespace: str,
) -> str | None:
    namespace_name = await session.scalar(
        select(NodeNamespace.namespace)
        .where(
            or_(
                NodeNamespace.namespace == namespace,
                NodeNamespace.namespace.startswith(
                    f"{namespace}.",
                    autoescape=True,
                ),
            ),
        )
        .order_by(NodeNamespace.namespace),
    )
    if namespace_name:
        return f"namespace `{namespace_name}`"

    node_name = await session.scalar(
        select(Node.name)
        .where(
            or_(
                Node.namespace == namespace,
                Node.namespace.startswith(f"{namespace}.", autoescape=True),
            ),
        )
        .order_by(Node.name),
    )
    return f"node `{node_name}`" if node_name else None


async def governed_boundary_delete_targets(
    session: AsyncSession,
    namespace: str,
) -> list[str]:
    """Include governed descendants in destructive namespace authorization."""
    await lock_namespace_boundary_lifecycle(session)
    boundaries = list(
        (
            await session.execute(
                select(NamespaceBoundary.namespace)
                .where(
                    or_(
                        NamespaceBoundary.namespace == namespace,
                        NamespaceBoundary.namespace.startswith(
                            f"{namespace}.",
                            autoescape=True,
                        ),
                    ),
                )
                .order_by(NamespaceBoundary.namespace),
            )
        )
        .scalars()
        .all(),
    )
    return list(dict.fromkeys([namespace, *boundaries]))


async def _boundary_response(
    session: AsyncSession,
    boundary: NamespaceBoundary,
) -> NamespaceProvisionResponse:
    roles = list(
        (
            await session.execute(
                select(Role)
                .options(selectinload(Role.scopes))
                .where(
                    Role.id.in_(
                        [boundary.owner_role_id, boundary.deployer_role_id],
                    ),
                ),
            )
        )
        .scalars()
        .all(),
    )
    roles_by_id = {role.id: role for role in roles}
    owner_role = roles_by_id.get(boundary.owner_role_id)
    deployer_role = roles_by_id.get(boundary.deployer_role_id)
    if (
        owner_role is None
        or deployer_role is None
        or owner_role.deleted_at is not None
        or deployer_role.deleted_at is not None
    ):
        raise DJInternalErrorException(
            message=(
                f"Namespace boundary `{boundary.namespace}` references missing or "
                "deleted managed roles."
            ),
        )
    expected_owner_name, expected_deployer_name = _boundary_role_names(
        boundary.namespace,
    )
    expected_owner_scopes = {
        (ResourceAction.MANAGE, scope_type, scope_value)
        for scope_type, scope_value in namespace_boundary_scope_targets(
            boundary.namespace,
        )
    }
    expected_deployer_scopes = {
        (ResourceAction.DELETE, scope_type, scope_value)
        for scope_type, scope_value in namespace_boundary_scope_targets(
            boundary.namespace,
        )
    }
    owner_scopes = {
        (scope.action, scope.scope_type, scope.scope_value)
        for scope in owner_role.scopes
    }
    deployer_scopes = {
        (scope.action, scope.scope_type, scope.scope_value)
        for scope in deployer_role.scopes
    }
    if (
        owner_role.name != expected_owner_name
        or deployer_role.name != expected_deployer_name
        or owner_scopes != expected_owner_scopes
        or deployer_scopes != expected_deployer_scopes
    ):
        raise DJInternalErrorException(
            message=(
                f"Namespace boundary `{boundary.namespace}` has drifted from its "
                "managed role policy."
            ),
        )

    owner_count = await session.scalar(
        select(func.count())
        .select_from(RoleAssignment)
        .join(User, User.id == RoleAssignment.principal_id)
        .where(
            RoleAssignment.role_id == owner_role.id,
            User.kind == PrincipalKind.GROUP,
            or_(
                RoleAssignment.expires_at.is_(None),
                RoleAssignment.expires_at > datetime.now(UTC),
            ),
        ),
    )
    if not owner_count:
        raise DJInternalErrorException(
            message=(
                f"Namespace boundary `{boundary.namespace}` has no effective owner "
                "group assignment."
            ),
        )
    return NamespaceProvisionResponse(
        namespace=boundary.namespace,
        owner_role=owner_role.name,
        deployer_role=deployer_role.name,
    )


def _role_history(role: Role, username: str) -> History:
    return History(
        entity_type=EntityType.ROLE,
        entity_name=role.name,
        activity_type=ActivityType.CREATE,
        user=username,
        post={
            "id": role.id,
            "name": role.name,
            "description": role.description,
            "scopes": [
                {
                    "action": scope.action.value,
                    "scope_type": scope.scope_type.value,
                    "scope_value": scope.scope_value,
                }
                for scope in role.scopes
            ],
        },
    )


def _assignment_history(
    assignment: RoleAssignment,
    principal: User,
    role: Role,
    username: str,
) -> History:
    return History(
        entity_type=EntityType.ROLE_ASSIGNMENT,
        entity_name=f"{principal.username}:{role.name}",
        activity_type=ActivityType.CREATE,
        user=username,
        post={
            "principal_id": principal.id,
            "principal_username": principal.username,
            "role_id": role.id,
            "role_name": role.name,
            "granted_by_id": assignment.granted_by_id,
            "expires_at": None,
        },
    )


async def _restore_or_recreate_namespace(
    session: AsyncSession,
    boundary: NamespaceBoundary,
    current_user: User,
) -> None:
    node_namespace = await NodeNamespace.get(
        session,
        boundary.namespace,
        raise_if_not_exists=False,
    )
    if node_namespace is None:
        session.add(NodeNamespace(namespace=boundary.namespace))
        session.add(
            History(
                entity_type=EntityType.NAMESPACE,
                entity_name=boundary.namespace,
                activity_type=ActivityType.CREATE,
                user=current_user.username,
                details={"reused_governed_boundary": True},
            ),
        )
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            if await NodeNamespace.get(
                session,
                boundary.namespace,
                raise_if_not_exists=False,
            ):
                return
            raise
    elif node_namespace.deactivated_at is not None:
        node_namespace.deactivated_at = None  # type: ignore
        session.add(
            History(
                entity_type=EntityType.NAMESPACE,
                entity_name=boundary.namespace,
                activity_type=ActivityType.RESTORE,
                user=current_user.username,
                details={"reused_governed_boundary": True},
            ),
        )
        await session.commit()


async def provision_namespace_boundary(
    *,
    session: AsyncSession,
    namespace: str,
    current_user: User,
    owner_group: str,
    deployer_service_accounts: list[str],
) -> NamespaceProvisionResponse:
    """Create a namespace and its durable owner/deployer policy atomically."""
    validate_namespace(namespace)
    if len({owner_group, *deployer_service_accounts}) != (
        1 + len(deployer_service_accounts)
    ):
        raise DJInvalidInputException(
            message="Owner and deployer principals must be unique.",
        )

    owner_role_name, deployer_role_name = _boundary_role_names(namespace)
    sorted_deployer_names = sorted(deployer_service_accounts)
    await lock_namespace_boundary_lifecycle(session)
    principals = await User.get_by_usernames(
        session,
        [owner_group, *sorted_deployer_names],
        options=[noload("*")],
    )
    owner = principals[0]
    deployers = principals[1:]
    if owner.kind != PrincipalKind.GROUP:
        raise DJInvalidInputException(
            message=f"Owner principal `{owner_group}` must be a group.",
        )
    invalid_deployers = [
        principal.username
        for principal in deployers
        if principal.kind != PrincipalKind.SERVICE_ACCOUNT
    ]
    if invalid_deployers:
        raise DJInvalidInputException(
            message=(
                "Deployer principals must be service accounts: "
                + ", ".join(invalid_deployers)
            ),
        )
    fingerprint = _request_fingerprint(
        owner.id,
        [deployer.id for deployer in deployers],
    )
    existing_boundary = await session.get(NamespaceBoundary, namespace)
    if existing_boundary:
        if existing_boundary.request_fingerprint != fingerprint:
            raise DJAlreadyExistsException(
                message=(
                    f"Namespace boundary `{namespace}` was already provisioned with "
                    "different principals."
                ),
            )
        await _restore_or_recreate_namespace(session, existing_boundary, current_user)
        return await _boundary_response(session, existing_boundary)

    overlapping = await _overlapping_boundaries(session, namespace)
    if overlapping:
        raise DJAlreadyExistsException(
            message=(
                f"Cannot provision namespace boundary `{namespace}` because it "
                f"overlaps governed boundary `{overlapping[0]}`. Nested governed "
                "boundaries are not supported."
            ),
        )
    existing_content = await _existing_ungoverned_content(session, namespace)
    if existing_content:
        raise DJAlreadyExistsException(
            message=(
                f"Cannot provision namespace boundary `{namespace}` because "
                f"{existing_content} already exists in its hierarchy without a "
                "governed boundary. Use the ownership backfill workflow for existing "
                "metadata."
            ),
        )

    conflicting_role_names = list(
        (
            await session.execute(
                select(Role.name).where(
                    Role.name.in_([owner_role_name, deployer_role_name]),
                ),
            )
        )
        .scalars()
        .all(),
    )
    if conflicting_role_names:
        raise DJAlreadyExistsException(
            message=f"Role `{min(conflicting_role_names)}` already exists.",
        )

    owner_role = Role(
        name=owner_role_name,
        description=f"Owner groups for namespace boundary {namespace}",
        created_by_id=current_user.id,
        scopes=_boundary_scopes(namespace, ResourceAction.MANAGE),
    )
    deployer_role = Role(
        name=deployer_role_name,
        description=f"Deployment access for namespace boundary {namespace}",
        created_by_id=current_user.id,
        scopes=_boundary_scopes(namespace, ResourceAction.DELETE),
    )
    try:
        session.add_all(
            [
                NodeNamespace(namespace=namespace),
                owner_role,
                deployer_role,
            ],
        )
        await session.flush()

        boundary = NamespaceBoundary(
            namespace=namespace,
            owner_role_id=owner_role.id,
            deployer_role_id=deployer_role.id,
            request_fingerprint=fingerprint,
            created_by_id=current_user.id,
        )
        owner_assignment = RoleAssignment(
            principal_id=owner.id,
            role_id=owner_role.id,
            granted_by_id=current_user.id,
        )
        deployer_assignments = [
            RoleAssignment(
                principal_id=deployer.id,
                role_id=deployer_role.id,
                granted_by_id=current_user.id,
            )
            for deployer in deployers
        ]
        session.add_all([boundary, owner_assignment, *deployer_assignments])
        await session.flush()

        session.add(
            History(
                entity_type=EntityType.NAMESPACE,
                entity_name=namespace,
                activity_type=ActivityType.CREATE,
                user=current_user.username,
                post={
                    "governed_boundary": True,
                    "owner_group": owner_group,
                    "deployer_service_accounts": sorted_deployer_names,
                    "owner_role": owner_role_name,
                    "deployer_role": deployer_role_name,
                },
            ),
        )
        session.add_all(
            [
                _role_history(owner_role, current_user.username),
                _role_history(deployer_role, current_user.username),
                _assignment_history(
                    owner_assignment,
                    owner,
                    owner_role,
                    current_user.username,
                ),
                *[
                    _assignment_history(
                        assignment,
                        deployer,
                        deployer_role,
                        current_user.username,
                    )
                    for assignment, deployer in zip(
                        deployer_assignments,
                        deployers,
                        strict=True,
                    )
                ],
            ],
        )
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        await lock_namespace_boundary_lifecycle(session)
        concurrent_boundary = await session.get(NamespaceBoundary, namespace)
        if concurrent_boundary:
            if concurrent_boundary.request_fingerprint != fingerprint:
                raise DJAlreadyExistsException(
                    message=(
                        f"Namespace boundary `{namespace}` was concurrently provisioned "
                        "with different principals."
                    ),
                ) from error
            await _restore_or_recreate_namespace(
                session,
                concurrent_boundary,
                current_user,
            )
            return await _boundary_response(session, concurrent_boundary)
        if await NodeNamespace.get(session, namespace, raise_if_not_exists=False):
            raise DJAlreadyExistsException(
                message=(
                    f"Node namespace `{namespace}` was concurrently created without a "
                    "governed boundary."
                ),
            ) from error
        concurrent_role_names = list(
            (
                await session.execute(
                    select(Role.name).where(
                        Role.name.in_([owner_role_name, deployer_role_name]),
                    ),
                )
            )
            .scalars()
            .all(),
        )
        if concurrent_role_names:
            raise DJAlreadyExistsException(
                message=f"Role `{min(concurrent_role_names)}` already exists.",
            ) from error
        raise
    return await _boundary_response(session, boundary)
