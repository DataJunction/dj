"""
Tests for git integration API endpoints.

Tests for:
- Namespace git configuration (GET/PATCH /namespaces/{namespace}/git)
- Branch management (POST/GET/DELETE /namespaces/{namespace}/branches)
- Git sync operations (POST /nodes/{name}/sync-to-git, POST /namespaces/{namespace}/sync-to-git)
- Pull request creation (POST /namespaces/{namespace}/pull-request)
"""

from http import HTTPStatus
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient


class TestNamespaceGitConfig:
    """Tests for namespace git configuration endpoints."""

    @pytest.mark.asyncio
    async def test_get_git_config_not_configured(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test getting git config for namespace without git configured."""
        # Create a namespace first
        await client_with_service_setup.post("/namespaces/test_git_ns")

        response = await client_with_service_setup.get("/namespaces/test_git_ns/git")
        assert response.status_code == HTTPStatus.OK

        data = response.json()
        assert data["github_repo_path"] is None
        assert data["git_branch"] is None
        assert data["git_path"] is None
        assert data["parent_namespace"] is None

    @pytest.mark.asyncio
    async def test_update_git_config(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test updating git configuration for a namespace."""
        # Create a namespace first
        await client_with_service_setup.post("/namespaces/test_git_config")

        # Update git config
        response = await client_with_service_setup.patch(
            "/namespaces/test_git_config/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
                "git_path": "definitions/test",
            },
        )
        assert response.status_code == HTTPStatus.OK

        data = response.json()
        assert data["github_repo_path"] == "myorg/myrepo"
        assert data["git_branch"] == "main"
        assert data["git_path"] == "definitions/test"

        # Verify by getting the config again
        response = await client_with_service_setup.get(
            "/namespaces/test_git_config/git",
        )
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data["github_repo_path"] == "myorg/myrepo"
        assert data["git_branch"] == "main"
        assert data["git_path"] == "definitions/test"

    @pytest.mark.asyncio
    async def test_update_git_config_partial(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test partial update of git configuration."""
        # Create a namespace and set initial config
        await client_with_service_setup.post("/namespaces/test_git_partial")
        await client_with_service_setup.patch(
            "/namespaces/test_git_partial/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        # Partial update - only change git_path
        response = await client_with_service_setup.patch(
            "/namespaces/test_git_partial/git",
            json={
                "git_path": "new/path",
            },
        )
        assert response.status_code == HTTPStatus.OK

        data = response.json()
        # Original values preserved
        assert data["github_repo_path"] == "myorg/myrepo"
        assert data["git_branch"] == "main"
        # New value applied
        assert data["git_path"] == "new/path"

    @pytest.mark.asyncio
    async def test_get_git_config_nonexistent_namespace(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test getting git config for non-existent namespace."""
        response = await client_with_service_setup.get(
            "/namespaces/nonexistent_ns_12345/git",
        )
        assert response.status_code == HTTPStatus.NOT_FOUND
        assert "does not exist" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_update_git_config_nonexistent_parent(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test setting parent_namespace to a non-existent namespace."""
        await client_with_service_setup.post("/namespaces/orphan_ns")

        response = await client_with_service_setup.patch(
            "/namespaces/orphan_ns/git",
            json={
                "parent_namespace": "nonexistent_parent_xyz",
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["message"]
            == "Parent namespace 'nonexistent_parent_xyz' does not exist."
        )

    @pytest.mark.asyncio
    async def test_update_git_config_self_parent(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test setting parent_namespace to itself (self-reference)."""
        await client_with_service_setup.post("/namespaces/self_ref_ns")

        response = await client_with_service_setup.patch(
            "/namespaces/self_ref_ns/git",
            json={
                "parent_namespace": "self_ref_ns",
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert "cannot be its own parent" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_update_git_config_repo_mismatch_with_parent(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test setting a different repo than parent namespace."""
        # Create parent with repo A
        await client_with_service_setup.post("/namespaces/parent_repo_ns")
        await client_with_service_setup.patch(
            "/namespaces/parent_repo_ns/git",
            json={
                "github_repo_path": "myorg/repo-a",
                "git_branch": "main",
            },
        )

        # Create child and try to use repo B
        await client_with_service_setup.post("/namespaces/child_repo_ns")
        response = await client_with_service_setup.patch(
            "/namespaces/child_repo_ns/git",
            json={
                "github_repo_path": "myorg/repo-b",
                "git_branch": "feature",
                "parent_namespace": "parent_repo_ns",
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["message"] == (
            "Repository mismatch: this namespace uses 'myorg/repo-b' but parent "
            "'parent_repo_ns' uses 'myorg/repo-a'. Branch namespaces must use the same "
            "repository as their parent for pull requests to work."
        )

    @pytest.mark.asyncio
    async def test_update_git_config_same_repo_as_parent_ok(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test setting same repo as parent namespace succeeds."""
        # Create parent
        await client_with_service_setup.post("/namespaces/valid_parent_ns")
        await client_with_service_setup.patch(
            "/namespaces/valid_parent_ns/git",
            json={
                "github_repo_path": "myorg/shared-repo",
                "git_branch": "main",
            },
        )

        # Create child with same repo but different branch
        await client_with_service_setup.post("/namespaces/valid_child_ns")
        response = await client_with_service_setup.patch(
            "/namespaces/valid_child_ns/git",
            json={
                "github_repo_path": "myorg/shared-repo",
                "git_branch": "feature-x",
                "parent_namespace": "valid_parent_ns",
            },
        )
        assert response.status_code == HTTPStatus.OK

    @pytest.mark.asyncio
    async def test_update_git_config_duplicate_location(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test setting git config that conflicts with another namespace."""
        # Create first namespace with git config
        await client_with_service_setup.post("/namespaces/first_ns")
        await client_with_service_setup.patch(
            "/namespaces/first_ns/git",
            json={
                "github_repo_path": "myorg/shared-repo",
                "git_branch": "main",
                "git_path": "definitions",
            },
        )

        # Create second namespace and try to use same git location
        await client_with_service_setup.post("/namespaces/second_ns")
        response = await client_with_service_setup.patch(
            "/namespaces/second_ns/git",
            json={
                "github_repo_path": "myorg/shared-repo",
                "git_branch": "main",
                "git_path": "definitions",
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["message"] == (
            "Git location conflict: namespace 'first_ns' already uses repo 'myorg/shared-repo', "
            "branch 'main', path 'definitions'. Each namespace must have a unique git location "
            "to avoid overwriting files."
        )

    @pytest.mark.asyncio
    async def test_update_git_config_same_repo_different_path_ok(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test that same repo+branch with different path is allowed."""
        # Create first namespace
        await client_with_service_setup.post("/namespaces/path_ns_a")
        await client_with_service_setup.patch(
            "/namespaces/path_ns_a/git",
            json={
                "github_repo_path": "myorg/monorepo",
                "git_branch": "main",
                "git_path": "project-a",
            },
        )

        # Create second namespace with same repo but different path
        await client_with_service_setup.post("/namespaces/path_ns_b")
        response = await client_with_service_setup.patch(
            "/namespaces/path_ns_b/git",
            json={
                "github_repo_path": "myorg/monorepo",
                "git_branch": "main",
                "git_path": "project-b",
            },
        )
        # Should succeed - different paths don't conflict
        assert response.status_code == HTTPStatus.OK

    @pytest.mark.asyncio
    async def test_update_git_config_same_repo_different_branch_ok(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test that same repo+path with different branch is allowed."""
        # Create first namespace on main branch
        await client_with_service_setup.post("/namespaces/branch_ns_main")
        await client_with_service_setup.patch(
            "/namespaces/branch_ns_main/git",
            json={
                "github_repo_path": "myorg/repo",
                "git_branch": "main",
            },
        )

        # Create second namespace on feature branch (same path)
        await client_with_service_setup.post("/namespaces/branch_ns_feature")
        response = await client_with_service_setup.patch(
            "/namespaces/branch_ns_feature/git",
            json={
                "github_repo_path": "myorg/repo",
                "git_branch": "feature",
            },
        )
        # Should succeed - different branches don't conflict
        assert response.status_code == HTTPStatus.OK


class TestBranchManagement:
    """Tests for branch management endpoints."""

    @pytest.mark.asyncio
    async def test_create_branch_without_git_config(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test creating a branch when namespace doesn't have git configured."""
        # Create namespace without git config
        await client_with_service_setup.post("/namespaces/test_no_git")

        response = await client_with_service_setup.post(
            "/namespaces/test_no_git/branches",
            json={"branch_name": "feature-x"},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["message"] == (
            "Namespace 'test_no_git' does not have git configured. Set github_repo_path first."
        )

    @pytest.mark.asyncio
    async def test_create_branch_without_git_branch(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test creating a branch when namespace has repo but no branch."""
        # Create namespace with only repo path
        await client_with_service_setup.post("/namespaces/test_no_branch")
        await client_with_service_setup.patch(
            "/namespaces/test_no_branch/git",
            json={"github_repo_path": "myorg/myrepo"},
        )

        response = await client_with_service_setup.post(
            "/namespaces/test_no_branch/branches",
            json={"branch_name": "feature-x"},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["message"] == (
            "Namespace 'test_no_branch' does not have a git branch configured. Set git_branch first."
        )

    @pytest.mark.asyncio
    async def test_create_branch_success(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test successfully creating a branch."""
        # Create namespace with full git config
        await client_with_service_setup.post("/namespaces/sales.main")
        await client_with_service_setup.patch(
            "/namespaces/sales.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
                "git_path": "definitions",
            },
        )

        # Mock the GitHubService
        with patch(
            "datajunction_server.api.branches.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.create_branch = AsyncMock(
                return_value={
                    "ref": "refs/heads/feature-x",
                    "object": {"sha": "abc123"},
                },
            )
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.post(
                "/namespaces/sales.main/branches",
                json={"branch_name": "feature-x"},
            )

            assert response.status_code == HTTPStatus.CREATED

            data = response.json()
            assert data["namespace"] == "sales.feature_x"
            assert data["git_branch"] == "feature-x"
            assert data["parent_namespace"] == "sales.main"
            assert data["github_repo_path"] == "myorg/myrepo"

            # Verify GitHub service was called
            mock_github.create_branch.assert_called_once_with(
                repo_path="myorg/myrepo",
                branch="feature-x",
                from_ref="main",
            )

    @pytest.mark.asyncio
    async def test_create_branch_duplicate(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test creating a branch when namespace already exists."""
        # Create parent namespace
        await client_with_service_setup.post("/namespaces/dup_test.main")
        await client_with_service_setup.patch(
            "/namespaces/dup_test.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        # Create the branch namespace manually
        await client_with_service_setup.post("/namespaces/dup_test.feature_x")

        # Now try to create it via branches endpoint
        with patch(
            "datajunction_server.api.branches.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.post(
                "/namespaces/dup_test.main/branches",
                json={"branch_name": "feature-x"},
            )

            assert response.status_code == HTTPStatus.CONFLICT
            assert "already exists" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_create_branch_github_error(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test handling GitHub API errors during branch creation."""
        from datajunction_server.internal.git.github_service import GitHubServiceError

        await client_with_service_setup.post("/namespaces/gh_err.main")
        await client_with_service_setup.patch(
            "/namespaces/gh_err.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        with patch(
            "datajunction_server.api.branches.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.create_branch = AsyncMock(
                side_effect=GitHubServiceError(
                    "Branch already exists",
                    http_status_code=400,
                    github_status=422,
                ),
            )
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.post(
                "/namespaces/gh_err.main/branches",
                json={"branch_name": "existing-branch"},
            )

            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
            assert (
                response.json()["message"]
                == "Failed to create git branch 'existing-branch': Branch already exists"
            )

    @pytest.mark.asyncio
    async def test_list_branches_empty(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test listing branches when none exist."""
        await client_with_service_setup.post("/namespaces/list_test.main")
        await client_with_service_setup.patch(
            "/namespaces/list_test.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        response = await client_with_service_setup.get(
            "/namespaces/list_test.main/branches",
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json() == []

    @pytest.mark.asyncio
    async def test_list_branches_with_children(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test listing branches that have child namespaces."""
        # Create parent namespace
        await client_with_service_setup.post("/namespaces/list_parent.main")
        await client_with_service_setup.patch(
            "/namespaces/list_parent.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        # Create branch namespaces with parent_namespace set via mock
        with patch(
            "datajunction_server.api.branches.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.create_branch = AsyncMock(
                return_value={"ref": "refs/heads/feat1", "object": {"sha": "abc"}},
            )
            mock_github_class.return_value = mock_github

            await client_with_service_setup.post(
                "/namespaces/list_parent.main/branches",
                json={"branch_name": "feat1"},
            )

        with patch(
            "datajunction_server.api.branches.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.create_branch = AsyncMock(
                return_value={"ref": "refs/heads/feat2", "object": {"sha": "def"}},
            )
            mock_github_class.return_value = mock_github

            await client_with_service_setup.post(
                "/namespaces/list_parent.main/branches",
                json={"branch_name": "feat2"},
            )

        response = await client_with_service_setup.get(
            "/namespaces/list_parent.main/branches",
        )
        assert response.status_code == HTTPStatus.OK

        branches = response.json()
        assert len(branches) == 2

        branch_names = {b["namespace"] for b in branches}
        assert "list_parent.feat1" in branch_names
        assert "list_parent.feat2" in branch_names

    @pytest.mark.asyncio
    async def test_delete_branch(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test deleting a branch namespace."""
        # Create parent and branch
        await client_with_service_setup.post("/namespaces/del_test.main")
        await client_with_service_setup.patch(
            "/namespaces/del_test.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        with patch(
            "datajunction_server.api.branches.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.create_branch = AsyncMock(
                return_value={"ref": "refs/heads/to-delete", "object": {"sha": "abc"}},
            )
            mock_github_class.return_value = mock_github

            await client_with_service_setup.post(
                "/namespaces/del_test.main/branches",
                json={"branch_name": "to-delete"},
            )

        # Delete the branch without deleting git branch
        response = await client_with_service_setup.delete(
            "/namespaces/del_test.main/branches/del_test.to_delete",
        )
        assert response.status_code == HTTPStatus.OK
        assert (
            response.json()["message"]
            == "Branch namespace 'del_test.to_delete' deleted"
        )

        # Verify branch namespace is unlinked (not a child anymore)
        response = await client_with_service_setup.get(
            "/namespaces/del_test.main/branches",
        )
        assert response.json() == []

    @pytest.mark.asyncio
    async def test_delete_branch_with_git_branch(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test deleting a branch namespace along with its git branch."""
        # Create parent and branch
        await client_with_service_setup.post("/namespaces/del_git.main")
        await client_with_service_setup.patch(
            "/namespaces/del_git.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        with patch(
            "datajunction_server.api.branches.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.create_branch = AsyncMock(
                return_value={"ref": "refs/heads/to-delete", "object": {"sha": "abc"}},
            )
            mock_github_class.return_value = mock_github

            await client_with_service_setup.post(
                "/namespaces/del_git.main/branches",
                json={"branch_name": "to-delete"},
            )

        # Delete the branch with git branch deletion
        with patch(
            "datajunction_server.api.branches.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.delete_branch = AsyncMock(return_value=None)
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.delete(
                "/namespaces/del_git.main/branches/del_git.to_delete"
                "?delete_git_branch=true",
            )
            assert response.status_code == HTTPStatus.OK
            assert response.json()["git_branch_deleted"] is True

            mock_github.delete_branch.assert_called_once_with(
                repo_path="myorg/myrepo",
                branch="to-delete",
            )

    @pytest.mark.asyncio
    async def test_delete_branch_not_a_child(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test deleting a namespace that isn't a child of the parent."""
        # Create two unrelated namespaces
        await client_with_service_setup.post("/namespaces/parent_a.main")
        await client_with_service_setup.post("/namespaces/other_ns")

        response = await client_with_service_setup.delete(
            "/namespaces/parent_a.main/branches/other_ns",
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["message"]
            == "Namespace 'other_ns' is not a branch of 'parent_a.main'."
        )


class TestGitSync:
    """Tests for git sync endpoints."""

    @pytest.mark.asyncio
    async def test_sync_node_no_git_config(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test syncing a node when namespace has no git config."""
        response = await client_with_roads.post(
            "/nodes/default.repair_orders/sync-to-git",
            json={},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["message"]
            == "Namespace 'default' does not have git configured."
        )

    @pytest.mark.asyncio
    async def test_sync_node_success(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test successfully syncing a node to git."""
        # Configure git for the namespace
        await client_with_roads.patch(
            "/namespaces/default/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
                "git_path": "definitions",
            },
        )

        with patch(
            "datajunction_server.api.git_sync.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            # File doesn't exist yet
            mock_github.get_file = AsyncMock(return_value=None)
            mock_github.commit_file = AsyncMock(
                return_value={
                    "commit": {
                        "sha": "abc123def456",
                        "html_url": "https://github.com/myorg/myrepo/commit/abc123",
                    },
                    "content": {"sha": "file-sha-123"},
                },
            )
            mock_github_class.return_value = mock_github

            response = await client_with_roads.post(
                "/nodes/default.repair_orders/sync-to-git",
                json={"commit_message": "Update repair_orders"},
            )

            assert response.status_code == HTTPStatus.OK

            data = response.json()
            assert data["node_name"] == "default.repair_orders"
            assert data["commit_sha"] == "abc123def456"
            assert "repair_orders" in data["file_path"]
            assert data["created"] is True

    @pytest.mark.asyncio
    async def test_sync_node_update_existing(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test syncing a node that already exists in git."""
        await client_with_roads.patch(
            "/namespaces/default/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        with patch(
            "datajunction_server.api.git_sync.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            # File exists
            mock_github.get_file = AsyncMock(
                return_value={
                    "sha": "existing-sha-123",
                    "content": "b2xkIGNvbnRlbnQ=",  # base64 "old content"
                },
            )
            mock_github.commit_file = AsyncMock(
                return_value={
                    "commit": {
                        "sha": "newcommit123",
                        "html_url": "https://github.com/myorg/myrepo/commit/new",
                    },
                },
            )
            mock_github_class.return_value = mock_github

            response = await client_with_roads.post(
                "/nodes/default.repair_orders/sync-to-git",
                json={},
            )

            assert response.status_code == HTTPStatus.OK
            data = response.json()
            assert data["created"] is False

            # Verify SHA was passed for update
            call_kwargs = mock_github.commit_file.call_args.kwargs
            assert call_kwargs["sha"] == "existing-sha-123"

    @pytest.mark.asyncio
    async def test_sync_node_nonexistent(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test syncing a non-existent node."""
        response = await client_with_roads.post(
            "/nodes/default.nonexistent_node_xyz/sync-to-git",
            json={},
        )
        assert response.status_code == HTTPStatus.NOT_FOUND
        assert "does not exist" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_sync_namespace_success(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test syncing an entire namespace to git."""
        # Configure git for the namespace
        await client_with_roads.patch(
            "/namespaces/default/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
                "git_path": "defs",
            },
        )

        with patch(
            "datajunction_server.api.git_sync.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            # Namespace sync uses commit_files (batch) not commit_file (single)
            mock_github.commit_files = AsyncMock(
                return_value={
                    "sha": "abc123",
                    "html_url": "https://github.com/myorg/myrepo/commit/abc",
                },
            )
            mock_github_class.return_value = mock_github

            response = await client_with_roads.post(
                "/namespaces/default/sync-to-git",
                json={"commit_message": "Sync all default nodes"},
            )

            assert response.status_code == HTTPStatus.OK

            data = response.json()
            assert data["namespace"] == "default"
            assert data["files_synced"] > 0
            assert len(data["results"]) > 0

    @pytest.mark.asyncio
    async def test_sync_namespace_empty(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test syncing an empty namespace."""
        await client_with_service_setup.post("/namespaces/empty_sync_ns")
        await client_with_service_setup.patch(
            "/namespaces/empty_sync_ns/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        response = await client_with_service_setup.post(
            "/namespaces/empty_sync_ns/sync-to-git",
            json={},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["message"]
            == "Namespace 'empty_sync_ns' has no nodes to sync."
        )


class TestPullRequest:
    """Tests for pull request creation endpoint."""

    @pytest.mark.asyncio
    async def test_create_pr_not_branch_namespace(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test creating PR from non-branch namespace."""
        await client_with_service_setup.post("/namespaces/not_branch_ns")
        await client_with_service_setup.patch(
            "/namespaces/not_branch_ns/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        response = await client_with_service_setup.post(
            "/namespaces/not_branch_ns/pull-request",
            json={"title": "My PR"},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["message"] == (
            "Namespace 'not_branch_ns' is not a branch namespace. Only branch namespaces "
            "(with parent_namespace) can create PRs."
        )

    @pytest.mark.asyncio
    async def test_create_pr_success(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test successfully creating a pull request."""
        # Create parent namespace with git config
        await client_with_service_setup.post("/namespaces/pr_parent.main")
        await client_with_service_setup.patch(
            "/namespaces/pr_parent.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        # Create branch namespace
        with patch(
            "datajunction_server.api.branches.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.create_branch = AsyncMock(
                return_value={"ref": "refs/heads/feature-pr", "object": {"sha": "abc"}},
            )
            mock_github_class.return_value = mock_github

            await client_with_service_setup.post(
                "/namespaces/pr_parent.main/branches",
                json={"branch_name": "feature-pr"},
            )

        # Create PR from branch
        with patch(
            "datajunction_server.api.git_sync.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.get_pull_request = AsyncMock(return_value=None)
            mock_github.create_pull_request = AsyncMock(
                return_value={
                    "number": 42,
                    "html_url": "https://github.com/myorg/myrepo/pull/42",
                    "title": "My Feature",
                },
            )
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.post(
                "/namespaces/pr_parent.feature_pr/pull-request",
                json={
                    "title": "My Feature",
                    "body": "This PR adds my feature",
                },
            )

            assert response.status_code == HTTPStatus.OK

            data = response.json()
            assert data["pr_number"] == 42
            assert data["pr_url"] == "https://github.com/myorg/myrepo/pull/42"
            assert data["head_branch"] == "feature-pr"
            assert data["base_branch"] == "main"

    @pytest.mark.asyncio
    async def test_create_pr_already_exists(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test creating PR when one already exists."""
        # Create parent and branch namespaces
        await client_with_service_setup.post("/namespaces/pr_exists.main")
        await client_with_service_setup.patch(
            "/namespaces/pr_exists.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        with patch(
            "datajunction_server.api.branches.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.create_branch = AsyncMock(
                return_value={
                    "ref": "refs/heads/existing-pr",
                    "object": {"sha": "abc"},
                },
            )
            mock_github_class.return_value = mock_github

            await client_with_service_setup.post(
                "/namespaces/pr_exists.main/branches",
                json={"branch_name": "existing-pr"},
            )

        # PR already exists
        with patch(
            "datajunction_server.api.git_sync.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.get_pull_request = AsyncMock(
                return_value={
                    "number": 99,
                    "html_url": "https://github.com/myorg/myrepo/pull/99",
                },
            )
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.post(
                "/namespaces/pr_exists.existing_pr/pull-request",
                json={"title": "Should return existing PR"},
            )

            assert response.status_code == HTTPStatus.OK

            data = response.json()
            # Should return the existing PR
            assert data["pr_number"] == 99

            # create_pull_request should NOT have been called
            mock_github.create_pull_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_pr_parent_no_git_branch(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test creating PR when parent namespace has no git branch."""
        # Create parent without git_branch but with repo
        await client_with_service_setup.post("/namespaces/pr_nobranch.main")
        await client_with_service_setup.patch(
            "/namespaces/pr_nobranch.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
            },
        )

        # Create branch namespace manually with parent_namespace set
        await client_with_service_setup.post("/namespaces/pr_nobranch.feature")
        await client_with_service_setup.patch(
            "/namespaces/pr_nobranch.feature/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "feature",
                "parent_namespace": "pr_nobranch.main",
            },
        )

        response = await client_with_service_setup.post(
            "/namespaces/pr_nobranch.feature/pull-request",
            json={"title": "My PR"},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["message"] == (
            "Parent namespace 'pr_nobranch.main' does not have a git branch configured."
        )
