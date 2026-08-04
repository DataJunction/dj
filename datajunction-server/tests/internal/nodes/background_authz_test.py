"""
Authorization on the background mutation path (#2234 step 0).

These tasks run after the HTTP response has returned, in their own session, so
they sit outside the request's AccessChecker. Two contracts follow from that:

* ``derive_frozen_measures`` and ``save_column_level_lineage`` re-authorize the
  resource their scheduling endpoint governed -- the target namespace for
  creation, the node for updates -- rather than trusting that caller to have
  checked. The resource matters: a namespace scope reaches a node request only
  through pattern matching, so authorizing the node would deny work scheduled by
  a create that a namespace-scoped grant allowed.

* ``propagate_update_downstream`` deliberately does *not* authorize, because a
  node is only downstream once its own owner points it at the upstream. Gating it
  would either block owners whose nodes have dependents, or skip the denied ones
  and leave the graph asserting VALID for nodes that are now broken -- silently,
  since propagation swallows exceptions.
"""

from functools import partial

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
from datajunction_server.models.node import NodeStatus

# Patch target: the name as imported into the validator module, where
# AccessChecker.check() looks the authorization service up.
VALIDATOR_AUTH_SERVICE = (
    "datajunction_server.internal.access.authorization."
    "validator.get_authorization_service"
)

NAMESPACE = "bgauthz"
SOURCE = f"{NAMESPACE}.events"
TRANSFORM = f"{NAMESPACE}.users_per_country"
METRIC = f"{NAMESPACE}.total_users"
# Node creation is governed on the target namespace, so that is the resource its
# background work must re-authorize.
CREATE_TARGET = access.Resource.from_namespace(NAMESPACE)


def deny(_request: access.ResourceRequest) -> bool:
    return False


def allow(_request: access.ResourceRequest) -> bool:
    return True


class RecordingAuthorizationService(AuthorizationService):
    """
    Decides each request by ``approves``, recording what it was asked.

    Recording the requests and the acting principal is what lets a test assert
    the implementation authorized the *right* resource for the *right* user,
    rather than only that it consulted authorization at all.
    """

    name = "test_recording_background"

    def __init__(self, approves=allow):
        self.approves = approves
        self.requests: list[access.ResourceRequest] = []
        self.usernames: list[str] = []

    def authorize(self, auth_context, requests):
        self.requests.extend(requests)
        self.usernames.append(auth_context.username)
        return [
            access.AccessDecision(request=request, approved=self.approves(request))
            for request in requests
        ]


async def _post(client: AsyncClient, url: str, json=None) -> None:
    response = await client.post(url, json=json)
    assert response.status_code in (200, 201, 409), response.text


async def _create_metric(client: AsyncClient, name: str) -> None:
    await _post(
        client,
        "/nodes/metric/",
        {
            "name": name,
            "description": "Total users",
            "query": f"SELECT SUM(num_users) FROM {TRANSFORM}",
            "mode": "published",
        },
    )


@pytest.fixture
async def metric_graph(client: AsyncClient) -> None:
    """A source, a transform over it, and a metric over the transform."""
    await _post(client, "/catalogs/", {"name": "warehouse"})
    await _post(client, f"/namespaces/{NAMESPACE}/")
    await _post(
        client,
        "/nodes/source/",
        {
            "name": SOURCE,
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
    await _post(
        client,
        "/nodes/transform/",
        {
            "name": TRANSFORM,
            "description": "Users per country",
            "query": (
                f"SELECT country, COUNT(DISTINCT id) AS num_users "
                f"FROM {SOURCE} GROUP BY 1"
            ),
            "mode": "published",
        },
    )
    await _create_metric(client, METRIC)


async def _revision(session, name: str) -> NodeRevision:
    """Latest revision of ``name``, read fresh -- the tasks commit elsewhere."""
    session.expire_all()
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


async def _run_task(mocker, task, approves) -> RecordingAuthorizationService:
    """Run ``task`` under a recording service and hand it back for assertions."""
    recorder = RecordingAuthorizationService(approves)
    mocker.patch(VALIDATOR_AUTH_SERVICE, lambda: recorder)
    await task()
    return recorder


def assert_write_authorized(
    recorder: RecordingAuthorizationService,
    username: str,
    target: access.Resource = CREATE_TARGET,
) -> None:
    """The task must have asked for WRITE on ``target``, acting as ``username``."""
    assert recorder.usernames == [username]
    assert [(request.verb, request.access_object) for request in recorder.requests] == [
        (access.ResourceAction.WRITE, target),
    ]


@pytest.mark.asyncio
async def test_create_schedules_background_work_against_the_namespace(
    client: AsyncClient,
    metric_graph,
    mocker,
):
    """
    Creation must hand its background tasks the namespace it authorized.

    Pins the wiring rather than the helper: scheduling a node target would deny
    the work under the namespace-scoped grant that allowed the create.
    """
    derive = mocker.patch("datajunction_server.internal.nodes.derive_frozen_measures")
    lineage = mocker.patch(
        "datajunction_server.internal.nodes.save_column_level_lineage",
    )

    await _create_metric(client, f"{METRIC}_again")

    assert derive.call_args.kwargs["access_target"] == CREATE_TARGET
    assert lineage.call_args.kwargs["access_target"] == CREATE_TARGET


@pytest.mark.asyncio
async def test_exact_namespace_grant_authorizes_create_background_work(
    session,
    current_user,
    mocker,
):
    """
    An exact namespace WRITE grant -- enough to create the node -- must also
    satisfy the background work that create schedules.

    Runs the real RBAC matcher under a restrictive policy, since resource type is
    what decides it: `bgauthz` does not match the node name `bgauthz.total_users`,
    so authorizing the node would leave a created node without derived metadata.
    """
    settings = mocker.patch(
        "datajunction_server.internal.access.authorization.service.settings",
    )
    settings.authorization_provider = "rbac"
    settings.default_access_policy = "restrictive"

    role = Role(name="bgauthz-namespace-writer", created_by_id=current_user.id)
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

    allowed = partial(_background_write_allowed, session, current_user=current_user)
    assert await allowed(CREATE_TARGET, action_description="test") is True, (
        "the grant that allowed the create did not allow its background work"
    )
    # The node target the create path must not use, denied by that same grant.
    node_target = access.Resource(name=METRIC, resource_type=access.ResourceType.NODE)
    assert await allowed(node_target, action_description="test") is False


@pytest.mark.asyncio
async def test_lineage_authorizes_the_create_target(
    metric_graph,
    session,
    current_user,
    mocker,
):
    """Lineage is written only when the caller may write the create target."""
    revision = await _revision(session, METRIC)
    revision.lineage = None
    await session.commit()
    task = partial(
        save_column_level_lineage,
        node_revision_id=revision.id,
        current_user=current_user,
        access_target=CREATE_TARGET,
    )

    denied = await _run_task(mocker, task, deny)
    assert_write_authorized(denied, current_user.username)
    assert not (await _revision(session, METRIC)).lineage, (
        "lineage was written despite WRITE being denied"
    )

    granted = await _run_task(mocker, task, allow)
    assert_write_authorized(granted, current_user.username)
    assert (await _revision(session, METRIC)).lineage, (
        "lineage was not written when WRITE was granted"
    )


@pytest.mark.asyncio
async def test_frozen_measures_authorize_the_create_target(
    metric_graph,
    session,
    current_user,
    mocker,
):
    """
    Derivation runs only when the caller may write the create target.

    Asserts whether the derivation body is reached rather than inspecting the
    return value, which is empty for several unrelated reasons.
    """
    revision = await _revision(session, METRIC)
    derive = mocker.patch(
        "datajunction_server.internal.nodes._derive_frozen_measures_impl",
        return_value=[],
    )
    task = partial(
        derive_frozen_measures,
        node_revision_id=revision.id,
        current_user=current_user,
        access_target=CREATE_TARGET,
    )

    denied = await _run_task(mocker, task, deny)
    assert_write_authorized(denied, current_user.username)
    derive.assert_not_called()

    granted = await _run_task(mocker, task, allow)
    assert_write_authorized(granted, current_user.username)
    derive.assert_called_once()


@pytest.mark.asyncio
async def test_downstream_revalidation_not_gated_on_caller_write(
    client: AsyncClient,
    metric_graph,
    session,
    mocker,
):
    """
    A caller who may write only the upstream still gets downstream revalidation.

    Fails if a WRITE check is ever added to the propagation path -- which is the
    point: such a check would break cross-namespace graphs.
    """
    assert (await client.get(f"/nodes/{METRIC}/")).json()["status"] == NodeStatus.VALID

    # WRITE only on the upstream; reads stay allowed so the assertions below work.
    mocker.patch(
        VALIDATOR_AUTH_SERVICE,
        lambda: RecordingAuthorizationService(
            lambda request: (
                request.verb != access.ResourceAction.WRITE
                or request.access_object.name == TRANSFORM
            ),
        ),
    )

    # Rename the column the downstream metric selects, invalidating it.
    response = await client.patch(
        f"/nodes/{TRANSFORM}/",
        json={
            "query": (
                f"SELECT country, COUNT(DISTINCT id) AS user_count "
                f"FROM {SOURCE} GROUP BY 1"
            ),
        },
    )
    assert response.status_code == 200, response.text

    session.expire_all()
    assert (await client.get(f"/nodes/{METRIC}/")).json()[
        "status"
    ] == NodeStatus.INVALID, (
        "downstream was not revalidated; propagation appears to be gated on the "
        "caller's WRITE, which would break cross-namespace graphs"
    )
