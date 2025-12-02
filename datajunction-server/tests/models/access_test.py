"""
Tests for ``datajunction_server.models.access``.
"""

from datajunction_server.models.access import (
    Resource,
    ResourceAction,
    ResourceRequest,
    ResourceType,
)


class TestResource:
    """Tests for Resource dataclass"""

    def test_resource_hash(self) -> None:
        """Test Resource.__hash__ method (line 43)"""
        resource1 = Resource(name="test.node", resource_type=ResourceType.NODE)
        resource2 = Resource(name="test.node", resource_type=ResourceType.NODE)
        resource3 = Resource(name="other.node", resource_type=ResourceType.NODE)
        resource4 = Resource(name="test.node", resource_type=ResourceType.NAMESPACE)

        # Same name and type should have same hash
        assert hash(resource1) == hash(resource2)

        # Different name should have different hash
        assert hash(resource1) != hash(resource3)

        # Different type should have different hash
        assert hash(resource1) != hash(resource4)

        # Resources can be used in sets and dicts
        resource_set = {resource1, resource2, resource3}
        assert len(resource_set) == 2  # resource1 and resource2 are same

        resource_dict = {resource1: "value1"}
        assert resource_dict[resource2] == "value1"  # Same hash, same key


class TestResourceRequest:
    """Tests for ResourceRequest dataclass"""

    def test_resource_request_hash(self) -> None:
        """Test ResourceRequest.__hash__ method (line 71)"""
        resource = Resource(name="test.node", resource_type=ResourceType.NODE)

        request1 = ResourceRequest(verb=ResourceAction.READ, access_object=resource)
        request2 = ResourceRequest(verb=ResourceAction.READ, access_object=resource)
        request3 = ResourceRequest(verb=ResourceAction.WRITE, access_object=resource)

        other_resource = Resource(name="other.node", resource_type=ResourceType.NODE)
        request4 = ResourceRequest(
            verb=ResourceAction.READ,
            access_object=other_resource,
        )

        # Same verb and resource should have same hash
        assert hash(request1) == hash(request2)

        # Different verb should have different hash
        assert hash(request1) != hash(request3)

        # Different resource should have different hash
        assert hash(request1) != hash(request4)

        # ResourceRequests can be used in sets and dicts
        request_set = {request1, request2, request3}
        assert len(request_set) == 2  # request1 and request2 are same

    def test_resource_request_eq(self) -> None:
        """Test ResourceRequest.__eq__ method (line 74)"""
        resource = Resource(name="test.node", resource_type=ResourceType.NODE)
        other_resource = Resource(name="other.node", resource_type=ResourceType.NODE)

        request1 = ResourceRequest(verb=ResourceAction.READ, access_object=resource)
        request2 = ResourceRequest(verb=ResourceAction.READ, access_object=resource)
        request3 = ResourceRequest(verb=ResourceAction.WRITE, access_object=resource)
        request4 = ResourceRequest(
            verb=ResourceAction.READ,
            access_object=other_resource,
        )

        # Same verb and access_object
        assert request1 == request2

        # Different verb
        assert request1 != request3

        # Different access_object
        assert request1 != request4

    def test_resource_request_str(self) -> None:
        """Test ResourceRequest.__str__ method (line 77)"""
        node_resource = Resource(name="test.node", resource_type=ResourceType.NODE)
        namespace_resource = Resource(
            name="test.namespace",
            resource_type=ResourceType.NAMESPACE,
        )

        read_request = ResourceRequest(
            verb=ResourceAction.READ,
            access_object=node_resource,
        )
        assert str(read_request) == "read:node/test.node"

        write_request = ResourceRequest(
            verb=ResourceAction.WRITE,
            access_object=namespace_resource,
        )
        assert str(write_request) == "write:namespace/test.namespace"

        execute_request = ResourceRequest(
            verb=ResourceAction.EXECUTE,
            access_object=node_resource,
        )
        assert str(execute_request) == "execute:node/test.node"

        delete_request = ResourceRequest(
            verb=ResourceAction.DELETE,
            access_object=node_resource,
        )
        assert str(delete_request) == "delete:node/test.node"

        manage_request = ResourceRequest(
            verb=ResourceAction.MANAGE,
            access_object=namespace_resource,
        )
        assert str(manage_request) == "manage:namespace/test.namespace"
