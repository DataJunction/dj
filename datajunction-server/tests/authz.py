"""Shared authorization test helpers."""

from typing import Callable

from datajunction_server.internal.access.authorization import AuthorizationService
from datajunction_server.models import access

# Patch target: the name as imported into the validator module, where
# AccessChecker.check() looks it up. Patch this to swap the authorization service
# used by AccessChecker.check() for a test.
VALIDATOR_AUTH_SERVICE = (
    "datajunction_server.internal.access.authorization."
    "validator.get_authorization_service"
)


class DenyActionAuthorizationService(AuthorizationService):
    """Approves every request except those using the denied action."""

    name = "test_deny_action"

    def __init__(self, denied: access.ResourceAction):
        self.denied = denied

    def authorize(self, auth_context, requests):
        return [
            access.AccessDecision(request=request, approved=request.verb != self.denied)
            for request in requests
        ]


def deny(action: access.ResourceAction) -> Callable[[], AuthorizationService]:
    """
    A ``get_authorization_service`` replacement that denies one action.

    Denying a single action is what isolates an endpoint's own gate: a DELETE
    grant implies WRITE, so a delete-governed endpoint is only exercised by
    ``deny(ResourceAction.DELETE)``.

    Usage: ``mocker.patch(VALIDATOR_AUTH_SERVICE, deny(ResourceAction.WRITE))``
    """
    return lambda: DenyActionAuthorizationService(action)
