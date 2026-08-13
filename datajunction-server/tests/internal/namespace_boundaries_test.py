"""Tests for governed namespace boundary provisioning."""

import asyncio
from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from datajunction_server.database.history import History
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.namespace_boundary import NamespaceBoundary
from datajunction_server.database.node import Node
from datajunction_server.database.rbac import Role, RoleAssignment, RoleScope
from datajunction_server.database.user import OAuthProvider, PrincipalKind, User
from datajunction_server.errors import (
    DJActionNotAllowedException,
    DJAlreadyExistsException,
    DJInternalErrorException,
    DJInvalidInputException,
)
from datajunction_server.internal.history import ActivityType, EntityType
from datajunction_server.internal.namespace_boundaries import (
    BoundaryRoleKind,
    _boundary_response,
    _request_fingerprint,
    _restore_or_recreate_namespace,
    get_boundary_role_kind,
    governed_boundary_delete_targets,
    namespace_boundary_scope_targets,
    provision_namespace_boundary,
    reject_boundary_role_definition_change,
    validate_boundary_role_assignment,
    validate_boundary_role_revocation,
)
from datajunction_server.internal.namespace_locks import (
    lock_namespace_boundary_lifecycle,
)
from datajunction_server.internal.namespaces import hard_delete_namespace
from datajunction_server.models.access import ResourceAction, ResourceType
from datajunction_server.models.node_type import NodeType


def _principal(username: str, kind: PrincipalKind) -> User:
    return User(
        username=username,
        password=None,
        email=None,
        name=username,
        oauth_provider=OAuthProvider.BASIC,
        kind=kind,
    )


async def _provision(
    session: AsyncSession,
    current_user: User,
    namespace: str,
    owner: User,
    deployers: list[User],
):
    return await provision_namespace_boundary(
        session=session,
        namespace=namespace,
        current_user=current_user,
        owner_group=owner.username,
        deployer_service_accounts=[deployer.username for deployer in deployers],
    )


async def test_provision_creates_durable_roles_assignments_and_history(
    session: AsyncSession,
    current_user: User,
):
    owner = _principal("analytics-owners", PrincipalKind.GROUP)
    deployer = _principal("analytics-deployer", PrincipalKind.SERVICE_ACCOUNT)
    session.add_all([owner, deployer])
    await session.commit()

    response = await _provision(
        session,
        current_user,
        "analytics",
        owner,
        [deployer],
    )

    boundary = await session.get(NamespaceBoundary, "analytics")
    owner_role = await Role.get_by_name(session, response.owner_role)
    deployer_role = await Role.get_by_name(session, response.deployer_role)
    assert boundary is not None
    assert owner_role is not None
    assert deployer_role is not None
    assert boundary.owner_role_id == owner_role.id
    assert boundary.deployer_role_id == deployer_role.id
    assert await NodeNamespace.get(session, "analytics") is not None
    assert {
        (scope.action, scope.scope_type, scope.scope_value)
        for scope in owner_role.scopes
    } == {
        (ResourceAction.MANAGE, ResourceType.NAMESPACE, "analytics"),
        (ResourceAction.MANAGE, ResourceType.NAMESPACE, "analytics.*"),
        (ResourceAction.MANAGE, ResourceType.NODE, "analytics.*"),
    }
    assert {
        (scope.action, scope.scope_type, scope.scope_value)
        for scope in deployer_role.scopes
    } == {
        (ResourceAction.DELETE, ResourceType.NAMESPACE, "analytics"),
        (ResourceAction.DELETE, ResourceType.NAMESPACE, "analytics.*"),
        (ResourceAction.DELETE, ResourceType.NODE, "analytics.*"),
    }
    assert await RoleAssignment.find(
        session,
        principal_id=owner.id,
        role_id=owner_role.id,
    )
    assert await RoleAssignment.find(
        session,
        principal_id=deployer.id,
        role_id=deployer_role.id,
    )

    history = list(
        (
            await session.execute(
                select(History).where(
                    History.entity_name.in_(
                        [
                            "analytics",
                            response.owner_role,
                            response.deployer_role,
                            f"{owner.username}:{response.owner_role}",
                            f"{deployer.username}:{response.deployer_role}",
                        ],
                    ),
                ),
            )
        )
        .scalars()
        .all(),
    )
    assert {(event.entity_type, event.activity_type) for event in history} == {
        (EntityType.NAMESPACE, ActivityType.CREATE),
        (EntityType.ROLE, ActivityType.CREATE),
        (EntityType.ROLE_ASSIGNMENT, ActivityType.CREATE),
    }
    assert len(history) == 5
    namespace_history = next(
        event for event in history if event.entity_type == EntityType.NAMESPACE
    )
    assert namespace_history.post["owner_group"] == owner.username
    assert namespace_history.post["deployer_service_accounts"] == [deployer.username]


async def test_provision_is_idempotent_and_survives_namespace_hard_delete(
    session: AsyncSession,
    current_user: User,
):
    owner = _principal("durable-owners", PrincipalKind.GROUP)
    deployers = [
        _principal("durable-deployer-b", PrincipalKind.SERVICE_ACCOUNT),
        _principal("durable-deployer-a", PrincipalKind.SERVICE_ACCOUNT),
    ]
    session.add_all([owner, *deployers])
    await session.commit()

    first = await _provision(session, current_user, "durable", owner, deployers)
    boundary = await session.get(NamespaceBoundary, "durable")
    assert boundary is not None
    role_ids = (boundary.owner_role_id, boundary.deployer_role_id)

    second = await _provision(
        session,
        current_user,
        "durable",
        owner,
        list(reversed(deployers)),
    )
    assert second == first
    assert (
        await session.scalar(
            select(func.count()).select_from(Role).where(Role.id.in_(role_ids)),
        )
        == 2
    )
    assert (
        await session.scalar(
            select(func.count())
            .select_from(RoleAssignment)
            .where(RoleAssignment.role_id.in_(role_ids)),
        )
        == 3
    )

    async def save_history(*args, **kwargs):
        return None

    await hard_delete_namespace(
        session,
        "durable",
        current_user,
        save_history,
        cascade=True,
    )
    assert (
        await NodeNamespace.get(
            session,
            "durable",
            raise_if_not_exists=False,
        )
        is None
    )
    assert await session.get(NamespaceBoundary, "durable") is not None

    recreated = await _provision(session, current_user, "durable", owner, deployers)
    assert recreated == first
    recreated_boundary = await session.get(NamespaceBoundary, "durable")
    assert recreated_boundary is not None
    assert (
        recreated_boundary.owner_role_id,
        recreated_boundary.deployer_role_id,
    ) == role_ids
    assert await NodeNamespace.get(session, "durable") is not None
    lifecycle_history = list(
        (
            await session.execute(
                select(History)
                .where(
                    History.entity_type == EntityType.NAMESPACE,
                    History.entity_name == "durable",
                )
                .order_by(History.id),
            )
        )
        .scalars()
        .all(),
    )
    assert [event.activity_type for event in lifecycle_history] == [
        ActivityType.CREATE,
        ActivityType.DELETE,
        ActivityType.CREATE,
    ]
    assert lifecycle_history[1].details == {
        "hard_delete": True,
        "cascade_root": "durable",
    }


async def test_hard_delete_treats_underscore_as_a_literal(
    session: AsyncSession,
    current_user: User,
):
    other_node = Node(
        name="teamXa.child.table",
        type=NodeType.SOURCE,
        created_by_id=current_user.id,
        namespace="teamXa.child",
    )
    session.add_all(
        [
            NodeNamespace(namespace="team_a"),
            NodeNamespace(namespace="teamXa.child"),
            other_node,
        ],
    )
    await session.commit()

    async def save_history(*args, **kwargs):
        return None

    await hard_delete_namespace(
        session,
        "team_a",
        current_user,
        save_history,
        cascade=True,
    )

    assert (
        await NodeNamespace.get(
            session,
            "team_a",
            raise_if_not_exists=False,
        )
        is None
    )
    assert await NodeNamespace.get(session, "teamXa.child") is not None
    assert await session.get(Node, other_node.id) is not None


@pytest.mark.parametrize(
    ("first_namespace", "overlapping_namespace"),
    [
        ("governed", "governed.child"),
        ("governed.child", "governed"),
    ],
)
async def test_provision_rejects_overlapping_boundaries(
    session: AsyncSession,
    current_user: User,
    first_namespace: str,
    overlapping_namespace: str,
):
    owner = _principal("overlap-owners", PrincipalKind.GROUP)
    session.add(owner)
    await session.commit()
    await _provision(session, current_user, first_namespace, owner, [])

    with pytest.raises(DJAlreadyExistsException, match="Nested governed boundaries"):
        await _provision(
            session,
            current_user,
            overlapping_namespace,
            owner,
            [],
        )

    assert await session.get(NamespaceBoundary, overlapping_namespace) is None


async def test_provision_rejects_changed_idempotency_request(
    session: AsyncSession,
    current_user: User,
):
    first_owner = _principal("first-owners", PrincipalKind.GROUP)
    second_owner = _principal("second-owners", PrincipalKind.GROUP)
    session.add_all([first_owner, second_owner])
    await session.commit()
    await _provision(session, current_user, "stable", first_owner, [])

    with pytest.raises(DJAlreadyExistsException, match="different principals"):
        await _provision(session, current_user, "stable", second_owner, [])


async def test_idempotent_request_tracks_principal_identity_across_rename(
    session: AsyncSession,
    current_user: User,
):
    owner = _principal("rename-owners-before", PrincipalKind.GROUP)
    session.add(owner)
    await session.commit()
    first = await _provision(session, current_user, "rename_stable", owner, [])
    owner.username = "rename-owners-after"
    await session.commit()

    second = await _provision(session, current_user, "rename_stable", owner, [])

    assert second == first


async def test_provision_rejects_existing_ungoverned_namespace(
    session: AsyncSession,
    current_user: User,
):
    owner = _principal("backfill-owners", PrincipalKind.GROUP)
    session.add_all([owner, NodeNamespace(namespace="existing_ungoverned")])
    await session.commit()

    with pytest.raises(DJAlreadyExistsException, match="ownership backfill"):
        await _provision(
            session,
            current_user,
            "existing_ungoverned",
            owner,
            [],
        )

    assert await session.get(NamespaceBoundary, "existing_ungoverned") is None
    assert (
        await Role.get_by_name(
            session,
            "namespace:existing_ungoverned:owners",
        )
        is None
    )


@pytest.mark.parametrize("existing_content", ["namespace", "node"])
async def test_provision_rejects_existing_ungoverned_descendants(
    session: AsyncSession,
    current_user: User,
    existing_content: str,
):
    owner = _principal(f"descendant-{existing_content}-owners", PrincipalKind.GROUP)
    session.add(owner)
    if existing_content == "namespace":
        session.add(NodeNamespace(namespace="existing_tree.child"))
    else:
        session.add(
            Node(
                name="existing_tree.child.table",
                type=NodeType.SOURCE,
                created_by_id=current_user.id,
                namespace="existing_tree.child",
            ),
        )
    await session.commit()

    with pytest.raises(DJAlreadyExistsException, match="ownership backfill"):
        await _provision(session, current_user, "existing_tree", owner, [])

    assert await session.get(NamespaceBoundary, "existing_tree") is None


async def test_concurrent_overlapping_provisioning_is_serialized(
    session: AsyncSession,
    session_factory,
    current_user: User,
):
    owner = _principal("race-owners", PrincipalKind.GROUP)
    session.add(owner)
    await session.commit()

    async def provision(namespace: str):
        worker_session = await session_factory()
        try:
            worker_user = await User.get_by_username(
                worker_session,
                current_user.username,
                options=[noload("*")],
            )
            worker_owner = await User.get_by_username(
                worker_session,
                owner.username,
                options=[noload("*")],
            )
            assert worker_user is not None
            assert worker_owner is not None
            return await _provision(
                worker_session,
                worker_user,
                namespace,
                worker_owner,
                [],
            )
        finally:
            await worker_session.close()

    outcomes = await asyncio.gather(
        provision("race_boundary"),
        provision("race_boundary.child"),
        return_exceptions=True,
    )

    assert (
        len(
            [
                outcome
                for outcome in outcomes
                if isinstance(outcome, DJAlreadyExistsException)
            ],
        )
        == 1
    )
    assert (
        len([outcome for outcome in outcomes if not isinstance(outcome, Exception)])
        == 1
    )
    assert (
        await session.scalar(
            select(func.count())
            .select_from(NamespaceBoundary)
            .where(NamespaceBoundary.namespace.startswith("race_boundary")),
        )
        == 1
    )


async def test_provision_rejects_non_service_deployer_without_partial_state(
    session: AsyncSession,
    current_user: User,
):
    owner = _principal("invalid-owners", PrincipalKind.GROUP)
    session.add(owner)
    await session.commit()

    with pytest.raises(DJInvalidInputException, match="service accounts"):
        await provision_namespace_boundary(
            session=session,
            namespace="invalid_boundary",
            current_user=current_user,
            owner_group=owner.username,
            deployer_service_accounts=[current_user.username],
        )

    assert (
        await NodeNamespace.get(
            session,
            "invalid_boundary",
            raise_if_not_exists=False,
        )
        is None
    )
    assert await session.get(NamespaceBoundary, "invalid_boundary") is None
    assert (
        await Role.get_by_name(
            session,
            "namespace:invalid_boundary:owners",
        )
        is None
    )


async def test_provision_rejects_non_group_owner(
    session: AsyncSession,
    current_user: User,
):
    with pytest.raises(DJInvalidInputException, match="must be a group"):
        await provision_namespace_boundary(
            session=session,
            namespace="invalid_owner",
            current_user=current_user,
            owner_group=current_user.username,
            deployer_service_accounts=[],
        )

    assert await session.get(NamespaceBoundary, "invalid_owner") is None


async def test_provision_rejects_duplicate_principals(
    session: AsyncSession,
    current_user: User,
):
    principal = _principal("duplicate-principal", PrincipalKind.GROUP)
    session.add(principal)
    await session.commit()

    with pytest.raises(DJInvalidInputException, match="must be unique"):
        await provision_namespace_boundary(
            session=session,
            namespace="duplicate_principals",
            current_user=current_user,
            owner_group=principal.username,
            deployer_service_accounts=[principal.username],
        )


async def test_provision_rejects_generated_role_name_that_is_too_long(
    session: AsyncSession,
    current_user: User,
):
    owner = _principal("long-name-owners", PrincipalKind.GROUP)
    session.add(owner)
    await session.commit()

    namespace = "a" * 240
    with pytest.raises(DJInvalidInputException, match="role names cannot exceed"):
        await _provision(session, current_user, namespace, owner, [])


async def test_provision_rejects_existing_generated_role(
    session: AsyncSession,
    current_user: User,
):
    owner = _principal("conflicting-role-owners", PrincipalKind.GROUP)
    session.add_all(
        [
            owner,
            Role(
                name="namespace:role_conflict:owners",
                created_by_id=current_user.id,
            ),
        ],
    )
    await session.commit()

    with pytest.raises(DJAlreadyExistsException, match="already exists"):
        await _provision(session, current_user, "role_conflict", owner, [])

    assert await session.get(NamespaceBoundary, "role_conflict") is None


async def test_idempotent_provision_restores_deactivated_namespace(
    session: AsyncSession,
    current_user: User,
):
    owner = _principal("restore-owners", PrincipalKind.GROUP)
    session.add(owner)
    await session.commit()
    expected = await _provision(session, current_user, "restore_boundary", owner, [])
    namespace = await NodeNamespace.get(session, "restore_boundary")
    assert namespace is not None
    namespace.deactivated_at = datetime.now(UTC)
    await session.commit()

    response = await _provision(
        session,
        current_user,
        "restore_boundary",
        owner,
        [],
    )

    assert response == expected
    assert namespace.deactivated_at is None
    restore_history = (
        await session.execute(
            select(History).where(
                History.entity_type == EntityType.NAMESPACE,
                History.entity_name == "restore_boundary",
                History.activity_type == ActivityType.RESTORE,
            ),
        )
    ).scalar_one()
    assert restore_history.details == {"reused_governed_boundary": True}


async def test_idempotent_provision_rejects_deleted_managed_role(
    session: AsyncSession,
    current_user: User,
):
    owner = _principal("deleted-role-owners", PrincipalKind.GROUP)
    session.add(owner)
    await session.commit()
    provisioned = await _provision(
        session,
        current_user,
        "deleted_role_boundary",
        owner,
        [],
    )
    owner_role = await Role.get_by_name(session, provisioned.owner_role)
    assert owner_role is not None
    owner_role.deleted_at = datetime.now(UTC)
    await session.commit()

    with pytest.raises(DJInternalErrorException, match="deleted managed roles"):
        await _provision(
            session,
            current_user,
            "deleted_role_boundary",
            owner,
            [],
        )


@pytest.mark.parametrize(
    "drift",
    ["owner_name", "deployer_name", "owner_scope", "deployer_scope"],
)
async def test_idempotent_provision_detects_managed_role_drift(
    session: AsyncSession,
    current_user: User,
    drift: str,
):
    owner = _principal(f"drifted-role-owners-{drift}", PrincipalKind.GROUP)
    session.add(owner)
    await session.commit()
    provisioned = await _provision(
        session,
        current_user,
        f"drifted_role_boundary_{drift}",
        owner,
        [],
    )
    owner_role = await Role.get_by_name(session, provisioned.owner_role)
    deployer_role = await Role.get_by_name(session, provisioned.deployer_role)
    assert owner_role is not None
    assert deployer_role is not None
    if drift == "owner_name":
        owner_role.name = f"{owner_role.name}:drifted"
    elif drift == "deployer_name":
        deployer_role.name = f"{deployer_role.name}:drifted"
    elif drift == "owner_scope":
        owner_role.scopes[0].action = ResourceAction.READ
    else:
        deployer_role.scopes[0].action = ResourceAction.READ
    await session.commit()

    with pytest.raises(DJInternalErrorException, match="drifted"):
        await _provision(
            session,
            current_user,
            f"drifted_role_boundary_{drift}",
            owner,
            [],
        )


async def test_idempotent_provision_detects_missing_owner_assignment(
    session: AsyncSession,
    current_user: User,
):
    owner = _principal("missing-assignment-owners", PrincipalKind.GROUP)
    session.add(owner)
    await session.commit()
    provisioned = await _provision(
        session,
        current_user,
        "missing_assignment_boundary",
        owner,
        [],
    )
    owner_role = await Role.get_by_name(session, provisioned.owner_role)
    assert owner_role is not None
    assignments = await RoleAssignment.find(session, role_id=owner_role.id)
    await session.delete(assignments[0])
    await session.commit()

    with pytest.raises(DJInternalErrorException, match="no effective owner"):
        await _provision(
            session,
            current_user,
            "missing_assignment_boundary",
            owner,
            [],
        )


@pytest.mark.parametrize("namespace_exists", [True, False])
async def test_namespace_recreation_classifies_commit_race(
    session: AsyncSession,
    current_user: User,
    mocker,
    namespace_exists: bool,
):
    boundary = NamespaceBoundary(
        namespace="recreation_race",
        owner_role_id=1,
        deployer_role_id=2,
        request_fingerprint="fingerprint",
        created_by_id=current_user.id,
    )
    concurrent_namespace = (
        NodeNamespace(namespace=boundary.namespace) if namespace_exists else None
    )
    mocker.patch.object(
        NodeNamespace,
        "get",
        side_effect=[None, concurrent_namespace],
    )
    mocker.patch.object(
        session,
        "commit",
        side_effect=IntegrityError("insert", {}, Exception("race")),
    )

    if namespace_exists:
        await _restore_or_recreate_namespace(session, boundary, current_user)
    else:
        with pytest.raises(IntegrityError):
            await _restore_or_recreate_namespace(session, boundary, current_user)


async def _inject_concurrent_rows_on_flush(
    *,
    session: AsyncSession,
    session_factory,
    mocker,
    rows,
) -> None:
    original_flush = session.flush
    injected = False

    async def racing_flush(*args, **kwargs):
        nonlocal injected
        if not injected:
            injected = True
            worker_session = await session_factory()
            try:
                worker_session.add_all(rows(worker_session))
                await worker_session.commit()
            finally:
                await worker_session.close()
        return await original_flush(*args, **kwargs)

    mocker.patch.object(session, "flush", side_effect=racing_flush)


@pytest.mark.parametrize("same_request", [True, False])
async def test_provision_classifies_concurrent_boundary(
    session: AsyncSession,
    session_factory,
    current_user: User,
    mocker,
    same_request: bool,
):
    owner = _principal("concurrent-boundary-owners", PrincipalKind.GROUP)
    session.add(owner)
    await session.commit()
    namespace = f"concurrent_boundary_{same_request}"
    request_fingerprint = _request_fingerprint(owner.id, [])

    def concurrent_rows(worker_session):
        owner_role = Role(
            name=f"namespace:{namespace}:owners",
            created_by_id=current_user.id,
            scopes=[
                RoleScope(
                    action=ResourceAction.MANAGE,
                    scope_type=scope_type,
                    scope_value=scope_value,
                )
                for scope_type, scope_value in namespace_boundary_scope_targets(
                    namespace,
                )
            ],
        )
        deployer_role = Role(
            name=f"namespace:{namespace}:deployers",
            created_by_id=current_user.id,
            scopes=[
                RoleScope(
                    action=ResourceAction.DELETE,
                    scope_type=scope_type,
                    scope_value=scope_value,
                )
                for scope_type, scope_value in namespace_boundary_scope_targets(
                    namespace,
                )
            ],
        )
        worker_session.add_all([owner_role, deployer_role])

        async def add_boundary_after_role_flush():
            await worker_session.flush()
            return [
                NodeNamespace(namespace=namespace),
                NamespaceBoundary(
                    namespace=namespace,
                    owner_role_id=owner_role.id,
                    deployer_role_id=deployer_role.id,
                    request_fingerprint=(
                        request_fingerprint
                        if same_request
                        else _request_fingerprint(owner.id + 100_000, [])
                    ),
                    created_by_id=current_user.id,
                ),
                RoleAssignment(
                    principal_id=owner.id,
                    role_id=owner_role.id,
                    granted_by_id=current_user.id,
                ),
            ]

        return add_boundary_after_role_flush

    original_flush = session.flush
    injected = False

    async def racing_flush(*args, **kwargs):
        nonlocal injected
        if not injected:
            injected = True
            worker_session = await session_factory()
            try:
                row_factory = concurrent_rows(worker_session)
                worker_session.add_all(await row_factory())
                await worker_session.commit()
            finally:
                await worker_session.close()
        return await original_flush(*args, **kwargs)

    mocker.patch.object(session, "flush", side_effect=racing_flush)

    if same_request:
        response = await _provision(
            session,
            current_user,
            namespace,
            owner,
            [],
        )
        assert response.namespace == namespace
    else:
        with pytest.raises(DJAlreadyExistsException, match="different principals"):
            await _provision(
                session,
                current_user,
                namespace,
                owner,
                [],
            )


async def test_provision_classifies_concurrent_namespace(
    session: AsyncSession,
    session_factory,
    current_user: User,
    mocker,
):
    owner = _principal("concurrent-namespace-owners", PrincipalKind.GROUP)
    session.add(owner)
    await session.commit()
    namespace = "concurrent_namespace"
    await _inject_concurrent_rows_on_flush(
        session=session,
        session_factory=session_factory,
        mocker=mocker,
        rows=lambda _worker_session: [NodeNamespace(namespace=namespace)],
    )

    with pytest.raises(DJAlreadyExistsException, match="concurrently created"):
        await _provision(session, current_user, namespace, owner, [])


async def test_provision_classifies_concurrent_role(
    session: AsyncSession,
    session_factory,
    current_user: User,
    mocker,
):
    owner = _principal("concurrent-role-owners", PrincipalKind.GROUP)
    session.add(owner)
    await session.commit()
    namespace = "concurrent_role"
    await _inject_concurrent_rows_on_flush(
        session=session,
        session_factory=session_factory,
        mocker=mocker,
        rows=lambda _worker_session: [
            Role(
                name=f"namespace:{namespace}:owners",
                created_by_id=current_user.id,
            ),
        ],
    )

    with pytest.raises(DJAlreadyExistsException, match="already exists"):
        await _provision(session, current_user, namespace, owner, [])


async def test_provision_reraises_unclassified_integrity_error(
    session: AsyncSession,
    current_user: User,
    mocker,
):
    owner = _principal("unclassified-race-owners", PrincipalKind.GROUP)
    session.add(owner)
    await session.commit()
    mocker.patch.object(
        session,
        "flush",
        side_effect=IntegrityError("insert", {}, Exception("unknown")),
    )

    with pytest.raises(IntegrityError):
        await _provision(
            session,
            current_user,
            "unclassified_race",
            owner,
            [],
        )


async def test_managed_role_helpers_cover_owner_and_deployer_rules(
    session: AsyncSession,
    current_user: User,
):
    owner = _principal("helper-primary-owner", PrincipalKind.GROUP)
    replacement_owner = _principal("helper-replacement-owner", PrincipalKind.GROUP)
    deployer = _principal("helper-deployer", PrincipalKind.SERVICE_ACCOUNT)
    session.add_all([owner, replacement_owner, deployer])
    await session.commit()
    provisioned = await _provision(
        session,
        current_user,
        "helper_boundary",
        owner,
        [deployer],
    )
    owner_role = await Role.get_by_name(session, provisioned.owner_role)
    deployer_role = await Role.get_by_name(session, provisioned.deployer_role)
    unmanaged_role = Role(
        name="helper-unmanaged",
        created_by_id=current_user.id,
    )
    session.add(unmanaged_role)
    await session.commit()
    assert owner_role is not None
    assert deployer_role is not None

    managed_owner = await get_boundary_role_kind(
        session,
        owner_role.id,
        for_update=True,
    )
    managed_deployer = await get_boundary_role_kind(session, deployer_role.id)
    assert managed_owner is not None
    assert managed_owner[1] == BoundaryRoleKind.OWNER
    assert managed_deployer is not None
    assert managed_deployer[1] == BoundaryRoleKind.DEPLOYER
    assert await get_boundary_role_kind(session, unmanaged_role.id) is None

    await reject_boundary_role_definition_change(session, unmanaged_role)
    with pytest.raises(DJActionNotAllowedException, match="managed owner role"):
        await reject_boundary_role_definition_change(session, owner_role)

    await validate_boundary_role_assignment(session, unmanaged_role, current_user, None)
    await validate_boundary_role_assignment(
        session,
        owner_role,
        replacement_owner,
        None,
    )
    await validate_boundary_role_assignment(session, deployer_role, deployer, None)
    with pytest.raises(DJInvalidInputException, match="group principals"):
        await validate_boundary_role_assignment(
            session,
            owner_role,
            current_user,
            None,
        )
    with pytest.raises(DJInvalidInputException, match="cannot expire"):
        await validate_boundary_role_assignment(
            session,
            owner_role,
            replacement_owner,
            datetime.now(UTC),
        )
    with pytest.raises(DJInvalidInputException, match="service_account principals"):
        await validate_boundary_role_assignment(
            session,
            deployer_role,
            replacement_owner,
            None,
        )

    await validate_boundary_role_revocation(
        session,
        unmanaged_role,
        current_user.id,
    )
    await validate_boundary_role_revocation(session, deployer_role, deployer.id)
    with pytest.raises(DJActionNotAllowedException, match="last owner"):
        await validate_boundary_role_revocation(session, owner_role, owner.id)

    session.add(
        RoleAssignment(
            principal_id=replacement_owner.id,
            role_id=owner_role.id,
            granted_by_id=current_user.id,
        ),
    )
    await session.commit()
    await validate_boundary_role_revocation(session, owner_role, owner.id)


@pytest.mark.parametrize(
    "broken_role",
    [
        "missing_owner",
        "missing_deployer",
        "deleted_owner",
        "deleted_deployer",
    ],
)
async def test_boundary_response_rejects_broken_managed_roles(
    session: AsyncSession,
    current_user: User,
    broken_role: str,
):
    owner_role = Role(
        name=f"broken:{broken_role}:owners",
        created_by_id=current_user.id,
    )
    deployer_role = Role(
        name=f"broken:{broken_role}:deployers",
        created_by_id=current_user.id,
    )
    session.add_all([owner_role, deployer_role])
    await session.commit()
    missing_role_id = max(owner_role.id, deployer_role.id) + 100_000
    if broken_role == "deleted_owner":
        owner_role.deleted_at = datetime.now(UTC)
    elif broken_role == "deleted_deployer":
        deployer_role.deleted_at = datetime.now(UTC)
    await session.commit()

    boundary = NamespaceBoundary(
        namespace=f"broken_{broken_role}",
        owner_role_id=(
            missing_role_id if broken_role == "missing_owner" else owner_role.id
        ),
        deployer_role_id=(
            missing_role_id if broken_role == "missing_deployer" else deployer_role.id
        ),
        request_fingerprint="fingerprint",
        created_by_id=current_user.id,
    )
    with pytest.raises(DJInternalErrorException, match="missing or deleted"):
        await _boundary_response(session, boundary)


async def test_boundary_requires_distinct_managed_roles(
    session: AsyncSession,
    current_user: User,
):
    role = Role(name="same-boundary-role", created_by_id=current_user.id)
    session.add(role)
    await session.commit()
    session.add(
        NamespaceBoundary(
            namespace="same_role_boundary",
            owner_role_id=role.id,
            deployer_role_id=role.id,
            request_fingerprint="fingerprint",
            created_by_id=current_user.id,
        ),
    )

    with pytest.raises(IntegrityError):
        await session.commit()
    await session.rollback()


async def test_namespace_lifecycle_lock_is_a_noop_outside_postgres(mocker):
    session = mocker.MagicMock()
    session.get_bind.return_value.dialect.name = "sqlite"
    session.execute = mocker.AsyncMock()

    await lock_namespace_boundary_lifecycle(session)

    session.execute.assert_not_awaited()


async def test_namespace_lifecycle_lock_uses_postgres_advisory_lock(mocker):
    session = mocker.MagicMock()
    session.get_bind.return_value.dialect.name = "postgresql"
    session.execute = mocker.AsyncMock()

    await lock_namespace_boundary_lifecycle(session)

    statement, parameters = session.execute.await_args.args
    assert str(statement) == "SELECT pg_advisory_xact_lock(:lock_id)"
    assert parameters == {"lock_id": 0x444A4E53424E4459}


async def test_destructive_namespace_targets_include_governed_descendants(
    session: AsyncSession,
    current_user: User,
):
    owner = _principal("delete-target-owners", PrincipalKind.GROUP)
    session.add(owner)
    await session.commit()
    await _provision(session, current_user, "delete_target.child", owner, [])

    assert await governed_boundary_delete_targets(session, "delete_target") == [
        "delete_target",
        "delete_target.child",
    ]
    assert await governed_boundary_delete_targets(
        session,
        "delete_target.child",
    ) == ["delete_target.child"]
