"""
Tests DJ client (internal) functionality.
"""

from unittest.mock import MagicMock, call, patch

import pytest

from datajunction._internal import DJClient
from datajunction.exceptions import DJClientException, DJTagDoesNotExist


class TestDJClient:  # pylint: disable=too-many-public-methods, protected-access
    """
    Tests for DJClient internal functionality.
    """

    @pytest.fixture
    def client(self, module__server):
        """
        Returns a DJ client instance
        """
        return DJClient(requests_session=module__server)

    def test_create_user(self, client):
        """
        Check that `client.create_user()` works as expected.
        """
        client._session.post = MagicMock(
            return_value=MagicMock(
                json=MagicMock(return_value={"text": "User already exists."}),
            ),
        )
        response = client.create_user(email="foo", username="bar", password="baz")
        assert response == {"text": "User already exists."}
        assert client._session.post.call_args == call(
            "/basic/user/",
            json={"email": "foo", "username": "bar", "password": "baz"},
        )

    def test_basic_login(self, client):
        """
        Check that `client.basic_login()` works as expected.
        """
        client._session.post = MagicMock()
        client.basic_login(username="bar", password="baz")
        assert client._session.post.call_args == call(
            "/basic/login/",
            data={"username": "bar", "password": "baz"},
        )

    def test__verify_node_exists(self, client):
        """
        Check that `client._verify_node_exists()` works as expected.
        """
        with patch("starlette.testclient.TestClient.get") as get_mock:
            get_mock.return_value = MagicMock(
                json=MagicMock(return_value={"name": "_", "type": "foo"}),
            )
            with pytest.raises(DJClientException):
                client._verify_node_exists(node_name="_", type_="bar")

    def test__list_nodes_with_tag(self, client):
        """
        Check that `client._list_nodes_with_tag()` works as expected.
        """
        # error: tag does not exist
        with pytest.raises(DJTagDoesNotExist):
            client._list_nodes_with_tag(
                tag_name="foo",
            )

        # error: invalid node_type
        with pytest.raises(AttributeError):
            client._list_nodes_with_tag(
                tag_name="foo",
                node_type="not_a_node",
            )
        with pytest.raises(TypeError):
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

    def test__set_namespace_git_config(self, client):
        """
        Check that `client._set_namespace_git_config()` sends the right PATCH payload.
        """
        client._session.patch = MagicMock(
            return_value=MagicMock(json=MagicMock(return_value={"ok": True})),
        )
        result = client._set_namespace_git_config(
            "my.namespace",
            git_branch="feature/foo",
            parent_namespace="my.main",
        )
        assert result == {"ok": True}
        client._session.patch.assert_called_once_with(
            "/namespaces/my.namespace/git",
            json={"git_branch": "feature/foo", "parent_namespace": "my.main"},
            timeout=client._timeout,
        )

    def test__set_namespace_git_config_omits_none_fields(self, client):
        """
        Fields that are None should be omitted from the PATCH payload.
        """
        client._session.patch = MagicMock(
            return_value=MagicMock(json=MagicMock(return_value={})),
        )
        client._set_namespace_git_config("my.namespace")
        client._session.patch.assert_called_once_with(
            "/namespaces/my.namespace/git",
            json={},
            timeout=client._timeout,
        )
