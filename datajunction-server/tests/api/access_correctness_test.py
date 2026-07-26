"""
Action/resource correctness tests for mutating endpoints (#2234 step 0).

The route-coverage work proves each mutating endpoint *reaches*
AccessChecker.check(); these prove the check asks for the *right* thing -- the
correct action and the correct resource. A check with the wrong argument still
"reaches check()" and still denies *something*, so only a targeted test catches
the mismatch.
"""

from httpx import AsyncClient

from datajunction_server.internal.access.authorization import AuthorizationService
from datajunction_server.models import access

# Patch target: the name as imported into the validator module, where
# AccessChecker.check() looks the service up.
VALIDATOR_AUTH_SERVICE = (
    "datajunction_server.internal.access.authorization."
    "validator.get_authorization_service"
)


class DenyDeleteAuthorizationService(AuthorizationService):
    """Approves everything except DELETE -- a caller with WRITE but not DELETE."""

    name = "test_deny_delete"

    def authorize(self, auth_context, requests):
        return [
            access.AccessDecision(
                request=request,
                approved=request.verb != access.ResourceAction.DELETE,
            )
            for request in requests
        ]


class RecordingAuthorizationService(AuthorizationService):
    """Approves everything and records the requests it was asked to authorize."""

    name = "test_recording"

    def __init__(self):
        self.requests: list[access.ResourceRequest] = []

    def authorize(self, auth_context, requests):
        self.requests.extend(requests)
        return [
            access.AccessDecision(request=request, approved=True)
            for request in requests
        ]


async def test_deactivate_namespace_requires_delete(
    client_with_roads: AsyncClient,
    mocker,
):
    """
    Deactivating a namespace is a delete-class operation and must require
    DELETE, matching node deactivation and namespace hard-delete. A caller
    holding only WRITE (not DELETE) must be denied.

    Before the fix the endpoint requested WRITE, so a WRITE-but-not-DELETE
    caller was allowed through (and hit the 405 "still active nodes" path);
    now the DELETE request is denied up front with 403.
    """
    mocker.patch(VALIDATOR_AUTH_SERVICE, lambda: DenyDeleteAuthorizationService())

    response = await client_with_roads.delete("/namespaces/default/")

    assert response.status_code == 403, (
        f"deactivate namespace did not require DELETE "
        f"(got {response.status_code}: {response.text[:200]})"
    )
    assert "Access denied" in response.json()["message"]


async def test_copy_node_authorizes_parent_namespace_not_new_name(
    client_with_roads: AsyncClient,
    mocker,
):
    """
    Copying a node writes into the *namespace* of the new name. The WRITE check
    must target that parent namespace (e.g. ``default``), not the full new node
    name (``default.repair_order_copy``) -- otherwise the grant that governs the
    namespace does not match and an unrelated resource is checked instead.
    """
    recorder = RecordingAuthorizationService()
    mocker.patch(VALIDATOR_AUTH_SERVICE, lambda: recorder)

    response = await client_with_roads.post(
        "/nodes/default.repair_order/copy?new_name=default.repair_order_copy",
    )
    assert response.status_code == 200, response.text[:200]

    namespace_writes = {
        request.access_object.name
        for request in recorder.requests
        if request.access_object.resource_type == access.ResourceType.NAMESPACE
        and request.verb == access.ResourceAction.WRITE
    }
    assert "default" in namespace_writes, (
        f"copy_node did not authorize the parent namespace; saw {namespace_writes}"
    )
    assert "default.repair_order_copy" not in namespace_writes, (
        "copy_node authorized the full new node name as a namespace"
    )
