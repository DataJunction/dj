"""
Authorization related functionality
"""

from typing import Iterable, List, Union

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.models.access import (
    AccessControl,
    AccessControlStore,
    ResourceRequest,
    ValidateAccessFn,
)
from datajunction_server.models.user import UserOutput


def validate_access_requests(
    validate_access: ValidateAccessFn,
    user: User,
    resource_requests: Iterable[ResourceRequest],
    raise_: bool = False,
) -> List[Union[NodeRevision, Node, ResourceRequest]]:
    """
    Validate a set of access requests. Only approved requests are returned.
    """
    if user is None:
        return list(resource_requests)  # pragma: no cover
    access_control = AccessControlStore(
        validate_access=validate_access,
        user=UserOutput(
            id=user.id,
            username=user.username,
            oauth_provider=user.oauth_provider,
        ),
    )

    for request in resource_requests:
        access_control.add_request(request)

    validation_results = access_control.validate()
    if raise_:
        access_control.raise_if_invalid_requests()  # pragma: no cover
    return [result for result in validation_results if result.approved]


def validate_access() -> ValidateAccessFn:
    """
    A placeholder validate access dependency injected function
    that returns a ValidateAccessFn that approves all requests
    """

    def _(access_control: AccessControl):
        """
        Examines all requests in the AccessControl
        and approves or denies each

        Args:
            access_control (AccessControl): The access control object
                containing the access control state and requests.

        Example:
            if access_control.state == 'direct':
                access_control.approve_all()
                return

            if access_control.user=='dj':
                request.approve_all()
                return

            request.deny_all()
        """
        access_control.approve_all()

    return _
