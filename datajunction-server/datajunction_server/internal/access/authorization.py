"""
Authorization related functionality
"""
from typing import Iterable, List, Optional, Union

from datajunction_server.models.access import (
    AccessControl,
    AccessControlStore,
    Node,
    NodeRevision,
    ResourceRequestVerb,
    ValidateAccessFn,
)
from datajunction_server.models.user import User


def validate_access_nodes(
    validate_access: ValidateAccessFn,  # pylint: disable=W0621
    verb: ResourceRequestVerb,
    user: Optional[User],
    nodes: Iterable[Union[NodeRevision, Node]],
    raise_: bool = False,
) -> List[Union[NodeRevision, Node]]:
    """
    Validate the access of the user to a set of nodes
    """
    if user is None:
        return list(nodes)  # pragma: no cover
    access_control = AccessControlStore(
        validate_access=validate_access,
        user=user,
    )

    access_control.add_request_by_nodes(nodes, verb)

    validation_results = access_control.validate()
    if raise_:
        access_control.raise_if_invalid_requests()  # pragma: no cover
    approved_node_ids = {
        request.access_object.id for request in validation_results if request.approved
    }
    approved_node_revision_ids = {
        request.access_object.revision_id
        for request in validation_results
        if request.approved
    }
    return [
        node
        for node in nodes
        if (
            (isinstance(node, Node) and node.id in approved_node_ids)
            or (
                isinstance(node, NodeRevision) and node.id in approved_node_revision_ids
            )
        )
    ]


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
