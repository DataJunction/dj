"""
Authorization contract for downstream revalidation (#2234 step 0).

Updating a node schedules ``propagate_update_downstream``, which revalidates
every node downstream in the DAG -- updating status, parents, and possibly
bumping revisions on nodes the updater does not own.

That is deliberately *not* gated on the updater's WRITE. A node is only
downstream because its own owner pointed it at the upstream, so the blast radius
is opt-in rather than caller-controlled, and the propagation only makes stored
status reflect reality. Requiring WRITE on downstreams would either block owners
from updating their own nodes whenever another team depends on them, or skip the
denied ones and leave the graph asserting VALID for nodes that are now broken.
Propagation also swallows exceptions, so denials would be silent.

The test below pins that contract: propagation must still run for downstreams the
caller cannot write.
"""

import pytest
from httpx import AsyncClient

from datajunction_server.internal.access.authorization import AuthorizationService
from datajunction_server.models import access
from datajunction_server.models.node import NodeStatus

# Patch target: the name as imported into the validator module, where
# AccessChecker.check() looks the authorization service up.
VALIDATOR_AUTH_SERVICE = (
    "datajunction_server.internal.access.authorization."
    "validator.get_authorization_service"
)

UPSTREAM = "bgauthz.agg"
DOWNSTREAM = "bgauthz.total_users"


class WriteOnlyOnAuthorizationService(AuthorizationService):
    """Approves WRITE only for ``allowed`` names; approves every other action."""

    name = "test_write_only_on"

    def __init__(self, allowed: set[str]):
        self.allowed = allowed

    def authorize(self, auth_context, requests):
        return [
            access.AccessDecision(
                request=request,
                approved=(
                    request.verb != access.ResourceAction.WRITE
                    or request.access_object.name in self.allowed
                ),
            )
            for request in requests
        ]


@pytest.mark.asyncio
async def test_downstream_revalidation_not_gated_on_caller_write(
    client: AsyncClient,
    session,
    mocker,
):
    """
    A caller with WRITE on only the upstream still gets downstream revalidation.

    If someone later adds a WRITE check to the propagation path, this fails --
    which is the point: that check would break cross-namespace graphs.
    """
    response = await client.post("/catalogs/", json={"name": "warehouse"})
    assert response.status_code in (200, 201, 409)
    response = await client.post("/namespaces/bgauthz/")
    assert response.status_code in (200, 201, 409)

    response = await client.post(
        "/nodes/source/",
        json={
            "name": "bgauthz.events",
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
        "/nodes/transform/",
        json={
            "name": UPSTREAM,
            "description": "Users per country",
            "query": (
                "SELECT country, COUNT(DISTINCT id) AS num_users "
                "FROM bgauthz.events GROUP BY 1"
            ),
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201), response.text

    response = await client.post(
        "/nodes/metric/",
        json={
            "name": DOWNSTREAM,
            "description": "Total users",
            "query": f"SELECT SUM(num_users) FROM {UPSTREAM}",
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201), response.text
    assert (await client.get(f"/nodes/{DOWNSTREAM}/")).json()[
        "status"
    ] == NodeStatus.VALID

    # From here the caller may write the upstream but not the downstream.
    mocker.patch(
        VALIDATOR_AUTH_SERVICE,
        lambda: WriteOnlyOnAuthorizationService({UPSTREAM}),
    )

    # Rename the column the downstream metric selects, invalidating it.
    response = await client.patch(
        f"/nodes/{UPSTREAM}/",
        json={
            "query": (
                "SELECT country, COUNT(DISTINCT id) AS user_count "
                "FROM bgauthz.events GROUP BY 1"
            ),
        },
    )
    assert response.status_code == 200, response.text

    # Propagation commits in its own session, so expire the shared test session
    # before reading or the assertion can see the pre-propagation revision.
    session.expire_all()
    assert (await client.get(f"/nodes/{DOWNSTREAM}/")).json()[
        "status"
    ] == NodeStatus.INVALID, (
        "downstream was not revalidated; propagation appears to be gated on the "
        "caller's WRITE, which would break cross-namespace graphs"
    )
