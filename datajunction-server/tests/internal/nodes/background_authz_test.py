"""
Authorization on the background mutation path (#2234 step 0).

``derive_frozen_measures`` and ``save_column_level_lineage`` run after the HTTP
response has returned, in their own session, and used to take only a revision id
-- so the mutation carried no authorization context of its own and relied
entirely on whichever caller scheduled it having checked first.

They now re-authorize the resource their scheduling endpoint governed, which is
not always the node: creation is governed on the target namespace, updates and
revalidation on the node. The tests below pin the actor and that exact resource,
not merely that "some WRITE" was denied.

(Downstream revalidation is the deliberate exception -- see
``_propagate_update_downstream`` and tests/api/background_propagation_test.py.)
"""

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from datajunction_server.database.node import NodeRevision
from datajunction_server.database.rbac import Role, RoleAssignment, RoleScope
from datajunction_server.internal.access.authorization import (
    AuthorizationService,
    RBACAuthorizationService,
)
from datajunction_server.internal.nodes import (
    _background_write_allowed,
    derive_frozen_measures,
    save_column_level_lineage,
)
from datajunction_server.models import access

VALIDATOR_AUTH_SERVICE = (
    "datajunction_server.internal.access.authorization."
    "validator.get_authorization_service"
)

NAMESPACE = "bgwrite"
METRIC = f"{NAMESPACE}.total_events"


class RecordingAuthorizationService(AuthorizationService):
    """
    Records what it was asked to authorize, and approves or denies wholesale.

    Recording the requests (and the acting principal) is what lets a test assert
    the implementation authorized the *right* resource for the *right* user,
    rather than only that it consulted authorization at all.
    """

    name = "test_recording_background"

    def __init__(self, approve: bool):
        self.approve = approve
        self.requests: list[access.ResourceRequest] = []
        self.usernames: list[str] = []

    def authorize(self, auth_context, requests):
        self.requests.extend(requests)
        self.usernames.append(auth_context.username)
        return [
            access.AccessDecision(request=request, approved=self.approve)
            for request in requests
        ]


def assert_authorized_namespace_write(
    recorder: RecordingAuthorizationService,
    username: str,
) -> None:
    """The task must request WRITE on the namespace create was governed on."""
    assert recorder.usernames == [username], (
        f"built authorization context for {recorder.usernames}, expected [{username}]"
    )
    assert [
        (request.verb, request.access_object.resource_type, request.access_object.name)
        for request in recorder.requests
    ] == [
        (
            access.ResourceAction.WRITE,
            access.ResourceType.NAMESPACE,
            NAMESPACE,
        ),
    ]


async def _create_metric(client: AsyncClient) -> None:
    response = await client.post("/catalogs/", json={"name": "warehouse"})
    assert response.status_code in (200, 201, 409)
    response = await client.post(f"/namespaces/{NAMESPACE}/")
    assert response.status_code in (200, 201, 409)
    response = await client.post(
        "/nodes/source/",
        json={
            "name": f"{NAMESPACE}.events",
            "description": "Raw events",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "country", "type": "string"},
            ],
            "mode": "published",
            "catalog": "warehouse",
            "schema_": "db",
            "table": "events",
        },
    )
    assert response.status_code in (200, 201), response.text
    response = await client.post(
        "/nodes/metric/",
        json={
            "name": METRIC,
            "description": "Total events",
            "query": f"SELECT COUNT(DISTINCT id) FROM {NAMESPACE}.events",
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201), response.text


async def _current_revision(session, name: str) -> NodeRevision:
    revision = (
        (
            await session.execute(
                select(NodeRevision)
                .where(NodeRevision.name == name)
                .order_by(NodeRevision.id.desc()),
            )
        )
        .scalars()
        .first()
    )
    assert revision is not None
    return revision


@pytest.mark.asyncio
async def test_create_schedules_background_work_against_the_namespace(
    client: AsyncClient,
    mocker,
):
    """
    Node creation must hand its background tasks the namespace it authorized.

    Pins the wiring, not just the helper: creation is governed on
    ``data.namespace``, so scheduling a node target would deny the work under a
    namespace-scoped grant (see the RBAC test above).
    """
    derive = mocker.patch(
        "datajunction_server.internal.nodes.derive_frozen_measures",
    )
    lineage = mocker.patch(
        "datajunction_server.internal.nodes.save_column_level_lineage",
    )

    await _create_metric(client)

    expected = access.Resource.from_namespace(NAMESPACE)
    assert derive.call_args.kwargs["access_target"] == expected
    assert lineage.call_args.kwargs["access_target"] == expected


@pytest.mark.asyncio
async def test_exact_namespace_grant_authorizes_create_background_work(
    session,
    current_user,
    mocker,
):
    """
    An exact namespace WRITE grant -- enough to create the node -- must also
    satisfy the background work scheduled by that create.

    Run against the real RBAC matcher under a restrictive policy, because the
    resource type is what decides this: a NAMESPACE scope only covers a NODE
    request through pattern matching, and `bgwrite` does not match the node name
    `bgwrite.total_events`. Authorizing the node here would therefore leave a
    successfully created node without its derived metadata.
    """
    settings = mocker.patch(
        "datajunction_server.internal.access.authorization.service.settings",
    )
    settings.authorization_provider = "rbac"
    settings.default_access_policy = "restrictive"

    role = Role(name="bgwrite-namespace-writer", created_by_id=current_user.id)
    session.add(role)
    await session.flush()
    session.add(
        RoleScope(
            role_id=role.id,
            action=access.ResourceAction.WRITE,
            scope_type=access.ResourceType.NAMESPACE,
            scope_value=NAMESPACE,
        ),
    )
    session.add(
        RoleAssignment(
            principal_id=current_user.id,
            role_id=role.id,
            granted_by_id=current_user.id,
        ),
    )
    await session.commit()
    mocker.patch(VALIDATOR_AUTH_SERVICE, lambda: RBACAuthorizationService())

    assert (
        await _background_write_allowed(
            session,
            access.Resource.from_namespace(NAMESPACE),
            current_user,
            "test",
        )
        is True
    ), "the grant that allowed the create did not allow its background work"

    # The node target the create path must NOT use, shown denied by the same grant.
    assert (
        await _background_write_allowed(
            session,
            access.Resource(
                name=METRIC,
                resource_type=access.ResourceType.NODE,
            ),
            current_user,
            "test",
        )
        is False
    )


@pytest.mark.asyncio
async def test_save_column_level_lineage_authorizes_create_target(
    client: AsyncClient,
    session,
    current_user,
    mocker,
):
    """Lineage is written only when the caller may write the create target."""
    await _create_metric(client)
    revision = await _current_revision(session, METRIC)
    revision.lineage = None
    await session.commit()
    access_target = access.Resource.from_namespace(NAMESPACE)

    denied = RecordingAuthorizationService(approve=False)
    mocker.patch(VALIDATOR_AUTH_SERVICE, lambda: denied)
    await save_column_level_lineage(
        node_revision_id=revision.id,
        current_user=current_user,
        access_target=access_target,
    )
    assert_authorized_namespace_write(denied, current_user.username)

    session.expire_all()
    revision = await _current_revision(session, METRIC)
    assert not revision.lineage, "lineage was written despite WRITE being denied"

    # Allow-path control: the same call writes lineage once WRITE is granted.
    allowed = RecordingAuthorizationService(approve=True)
    mocker.patch(VALIDATOR_AUTH_SERVICE, lambda: allowed)
    await save_column_level_lineage(
        node_revision_id=revision.id,
        current_user=current_user,
        access_target=access_target,
    )
    assert_authorized_namespace_write(allowed, current_user.username)

    session.expire_all()
    revision = await _current_revision(session, METRIC)
    assert revision.lineage, "lineage was not written when WRITE was granted"


@pytest.mark.asyncio
async def test_derive_frozen_measures_authorizes_create_target(
    client: AsyncClient,
    session,
    current_user,
    mocker,
):
    """
    Derivation runs only when the caller may write the create target.

    Asserts the derivation body is or is not reached rather than inspecting the
    return value, which is empty for several unrelated reasons.
    """
    await _create_metric(client)
    revision = await _current_revision(session, METRIC)
    access_target = access.Resource.from_namespace(NAMESPACE)
    derive = mocker.patch(
        "datajunction_server.internal.nodes._derive_frozen_measures_impl",
        return_value=[],
    )

    denied = RecordingAuthorizationService(approve=False)
    mocker.patch(VALIDATOR_AUTH_SERVICE, lambda: denied)
    await derive_frozen_measures(
        node_revision_id=revision.id,
        current_user=current_user,
        access_target=access_target,
    )
    assert_authorized_namespace_write(denied, current_user.username)
    derive.assert_not_called()

    # Allow-path control: the same call proceeds once WRITE is granted.
    allowed = RecordingAuthorizationService(approve=True)
    mocker.patch(VALIDATOR_AUTH_SERVICE, lambda: allowed)
    await derive_frozen_measures(
        node_revision_id=revision.id,
        current_user=current_user,
        access_target=access_target,
    )
    assert_authorized_namespace_write(allowed, current_user.username)
    derive.assert_called_once()
