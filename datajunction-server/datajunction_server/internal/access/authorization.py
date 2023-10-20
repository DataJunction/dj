"""
Authorization related functionality
"""
from typing import Iterable, List, Optional, Union

from datajunction_server.models.access import (
    AccessControl,
    AccessControlStore,
    Node,
    NodeRevision,
    ResourceRequest,
    ResourceRequestVerb,
    ValidateAccessFn,
)
from datajunction_server.models.user import User


def validate_access_nodes(
    validate_access: ValidateAccessFn,  # pylint: disable=W0621
    user: Optional[User],
    resource_requests: Iterable[ResourceRequest],
    raise_: bool = False,
) -> List[Union[NodeRevision, Node, ResourceRequest]]:
    """
    Validate the access of the user to a set of nodes
    """
    if user is None:
        return list(resource_requests)  # pragma: no cover
    access_control = AccessControlStore(
        validate_access=validate_access,
        user=user,
    )

    for request in resource_requests:
        access_control.add_request(request)

    validation_results = access_control.validate()
    if raise_:
        access_control.raise_if_invalid_requests()  # pragma: no cover
    return [result for result in validation_results if result.approved]


def validate_access_namespaces(
    validate_access: "ValidateAccessFn",  # pylint: disable=W0621
    verb: ResourceRequestVerb,
    user: Optional[User],
    namespaces: Iterable[str],
    raise_: bool = False,
) -> List[str]:
    """
    Validate the access of the user to a set of namespaces
    """
    if user is None:
        if user is None:  # pragma: no cover
            return list(namespaces)
    access_control = AccessControlStore(
        validate_access=validate_access,
        user=user,
    )
    for namespace in namespaces:
        access_control.add_request_by_namespace(namespace, verb)

    validation_results = access_control.validate()
    if raise_:
        access_control.raise_if_invalid_requests()  # pragma: no cover

    return [
        namespace
        for namespace in namespaces
        if namespace
        in {
            request.access_object.name
            for request in validation_results
            if request.approved
        }
    ]


# Dummy default if not dependency injected
def validate_access() -> ValidateAccessFn:
    """
    Validate access returns a ValidateAccessFn
    """

    def _validate_access(access_control: AccessControl):
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

    return _validate_access
