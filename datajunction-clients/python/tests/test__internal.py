"""
Tests DJ client (internal) functionality.
"""

from unittest.mock import MagicMock

import pytest

from datajunction._internal import DJClient


class TestDJClient:  # pylint: disable=too-many-public-methods, protected-access
    """
    Tests for DJClient internal functionality.
    """

    @pytest.fixture
    def client(self, server):
        """
        Returns a DJ client instance
        """
        return DJClient(requests_session=server)

    def test__list_nodes_with_tag(self, client):
        """
        Check that `client._list_nodes_with_tag()` works as expected.
        """
        # success
        nodes = client._list_nodes_with_tag(
            tag_name="foo",
        )
        assert nodes == []

        # error: invalid node_type
        with pytest.raises(AttributeError) as exc_info:
            client._list_nodes_with_tag(
                tag_name="foo",
                node_type="not_a_node",
            )
        with pytest.raises(TypeError) as exc_info:
            client._list_nodes_with_tag(
                tag_name="foo",
                node_type=MagicMock(),
            )

        # error: exception during request
        client._session.get = MagicMock(side_effect=Exception("Boom!"))
        with pytest.raises(Exception) as exc_info:
            client._list_nodes_with_tag(
                tag_name="foo",
            )
        assert "Boom!" in str(exc_info.value)
