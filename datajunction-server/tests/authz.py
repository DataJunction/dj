"""Shared authorization test helpers."""

from datajunction_server.internal.access.authorization import AuthorizationService
from datajunction_server.models import access

# Patch target: the name as imported into the validator module, where
# AccessChecker.check() looks it up. Patch this to swap the authorization service
# used by AccessChecker.check() for a test.
VALIDATOR_AUTH_SERVICE = (
    "datajunction_server.internal.access.authorization."
    "validator.get_authorization_service"
)


class DenyWriteAuthorizationService(AuthorizationService):
    """Approves everything except WRITE -- a caller without write access."""

    name = "test_deny_write"

    def authorize(self, auth_context, requests):
        return [
            access.AccessDecision(
                request=request,
                approved=request.verb != access.ResourceAction.WRITE,
            )
            for request in requests
        ]
