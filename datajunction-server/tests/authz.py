"""Shared authorization test helpers."""

from typing import Callable

from datajunction_server.internal.access.authorization import (
    AuthorizationService,
    RBACAuthorizationService,
)
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


class ScopedActionAuthorizationService(AuthorizationService):
    """Grants one action only on resources matching a scope pattern."""

    name = "test_scoped_action"

    def __init__(self, action: access.ResourceAction, pattern: str):
        self.action = action
        self.pattern = pattern

    def authorize(self, auth_context, requests):
        return [
            access.AccessDecision(
                request=request,
                approved=request.verb != self.action
                or RBACAuthorizationService.resource_matches_pattern(
                    request.access_object.name,
                    self.pattern,
                ),
            )
            for request in requests
        ]


def scoped(
    action: access.ResourceAction,
    pattern: str,
) -> Callable[[], AuthorizationService]:
    """
    A service granting ``action`` only within ``pattern``, as a real scope would.

    Delegates to the production matcher, so wildcard semantics are exercised
    rather than restated (``finance.*`` covers ``finance.sub`` but not
    ``finance``).
    """
    return lambda: ScopedActionAuthorizationService(action, pattern)
