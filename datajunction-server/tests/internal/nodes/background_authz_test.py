"""
Authorization on the background mutation path (#2234 step 0).

``derive_frozen_measures`` and ``save_column_level_lineage`` run after the HTTP
response has returned, in their own session, and used to take only a revision id
-- so the mutation carried no authorization context of its own and relied
entirely on whichever caller scheduled it having checked first.

Every current scheduling site is a user-initiated create/update/revalidate that
already required WRITE on that node, so requiring it again here changes nothing
for legitimate flows; it makes the internal path independently fail-closed.

(Downstream revalidation is the deliberate exception -- see
``_propagate_update_downstream`` and tests/api/background_propagation_test.py.)
"""

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from datajunction_server.database.node import NodeRevision
from datajunction_server.internal.access.authorization import AuthorizationService
from datajunction_server.internal.nodes import (
    derive_frozen_measures,
    save_column_level_lineage,
)
from datajunction_server.models import access

VALIDATOR_AUTH_SERVICE = (
    "datajunction_server.internal.access.authorization."
    "validator.get_authorization_service"
)

METRIC = "bgwrite.total_events"


class DenyWriteAuthorizationService(AuthorizationService):
    """Approves everything except WRITE -- a caller without write access."""

    name = "test_deny_write_background"

    def authorize(self, auth_context, requests):
        return [
            access.AccessDecision(
                request=request,
                approved=request.verb != access.ResourceAction.WRITE,
            )
            for request in requests
        ]


class AllowAllAuthorizationService(AuthorizationService):
    """Approves everything -- the control for the denial cases."""

    name = "test_allow_all_background"

    def authorize(self, auth_context, requests):
        return [
            access.AccessDecision(request=request, approved=True)
            for request in requests
        ]


async def _create_metric(client: AsyncClient) -> None:
    response = await client.post("/catalogs/", json={"name": "warehouse"})
    assert response.status_code in (200, 201, 409)
    response = await client.post("/namespaces/bgwrite/")
    assert response.status_code in (200, 201, 409)
    response = await client.post(
        "/nodes/source/",
        json={
            "name": "bgwrite.events",
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
            "query": "SELECT COUNT(DISTINCT id) FROM bgwrite.events",
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
async def test_save_column_level_lineage_denied_without_write(
    client: AsyncClient,
    session,
    current_user,
    mocker,
):
    """Lineage is not written for a caller who cannot write the node."""
    await _create_metric(client)
    revision = await _current_revision(session, METRIC)
    revision.lineage = None
    await session.commit()

    mocker.patch(VALIDATOR_AUTH_SERVICE, lambda: DenyWriteAuthorizationService())
    await save_column_level_lineage(
        node_revision_id=revision.id,
        current_user=current_user,
    )

    session.expire_all()
    revision = await _current_revision(session, METRIC)
    assert not revision.lineage, "lineage was written despite WRITE being denied"


@pytest.mark.asyncio
async def test_derive_frozen_measures_denied_without_write(
    client: AsyncClient,
    session,
    current_user,
    mocker,
):
    """
    Derivation never runs for a caller who cannot write the node.

    Asserts the derivation body is not reached rather than inspecting the return
    value, which is empty for several unrelated reasons.
    """
    await _create_metric(client)
    revision = await _current_revision(session, METRIC)
    derive = mocker.patch(
        "datajunction_server.internal.nodes._derive_frozen_measures_impl",
        return_value=[],
    )

    mocker.patch(VALIDATOR_AUTH_SERVICE, lambda: DenyWriteAuthorizationService())
    await derive_frozen_measures(
        node_revision_id=revision.id,
        current_user=current_user,
    )
    derive.assert_not_called()

    # Control: the same call proceeds once WRITE is allowed.
    mocker.patch(VALIDATOR_AUTH_SERVICE, lambda: AllowAllAuthorizationService())
    await derive_frozen_measures(
        node_revision_id=revision.id,
        current_user=current_user,
    )
    derive.assert_called_once()
