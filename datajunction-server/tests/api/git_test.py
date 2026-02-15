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
import base64
import io
import tarfile
import pytest
from httpx import AsyncClient


def create_mock_tarball(files: dict[str, str]) -> bytes:
    """Create a mock tarball with the given files.

    Args:
        files: Dict mapping file paths to file contents

    Returns:
        Tarball bytes
    """
    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w:gz") as tar:
        # GitHub archives have a root directory like "owner-repo-sha"
        root_dir = "myorg-myrepo-abc123"

        # Always add the root directory
        root_info = tarfile.TarInfo(name=root_dir)
        root_info.type = tarfile.DIRTYPE
        root_info.mode = 0o755
        tar.addfile(root_info)

        for file_path, content in files.items():
            # Add file to tarball
            file_data = content.encode("utf-8")
            tarinfo = tarfile.TarInfo(name=f"{root_dir}/{file_path}")
            tarinfo.size = len(file_data)
            tar.addfile(tarinfo, io.BytesIO(file_data))

    tar_buffer.seek(0)
    return tar_buffer.read()


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
        assert data["default_branch"] is None

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
        """Test that branch namespaces cannot set github_repo_path (must inherit from parent)."""
        # Create parent with repo
        await client_with_service_setup.post("/namespaces/parent_repo_ns")
        await client_with_service_setup.patch(
            "/namespaces/parent_repo_ns/git",
            json={
                "github_repo_path": "myorg/repo-a",
            },
        )

        # Create child and try to set a repo (should fail - must inherit from parent)
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
        assert (
            "Cannot set github_repo_path on a branch namespace"
            in response.json()["message"]
        )

    @pytest.mark.asyncio
    async def test_update_git_config_branch_namespace_inherits_repo(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test that branch namespaces only set branch and parent, inheriting repo."""
        # Create parent with repo
        await client_with_service_setup.post("/namespaces/valid_parent_ns")
        await client_with_service_setup.patch(
            "/namespaces/valid_parent_ns/git",
            json={
                "github_repo_path": "myorg/shared-repo",
            },
        )

        # Create child with only branch and parent (no repo - it's inherited)
        await client_with_service_setup.post("/namespaces/valid_child_ns")
        response = await client_with_service_setup.patch(
            "/namespaces/valid_child_ns/git",
            json={
                "git_branch": "feature-x",
                "parent_namespace": "valid_parent_ns",
            },
        )
        assert response.status_code == HTTPStatus.OK
        # Verify the response includes the inherited repo
        data = response.json()
        assert data["github_repo_path"] == "myorg/shared-repo"
        assert data["git_branch"] == "feature-x"
        assert data["parent_namespace"] == "valid_parent_ns"

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
        assert response.json() == {
            "git_branch": "main",
            "git_only": False,
            "git_path": "project-b",
            "github_repo_path": "myorg/monorepo",
            "parent_namespace": None,
            "default_branch": None,
        }

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
        assert response.json() == {
            "git_branch": "feature",
            "git_only": False,
            "git_path": None,
            "github_repo_path": "myorg/repo",
            "parent_namespace": None,
            "default_branch": None,
        }


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
            "Namespace 'test_no_git' does not have git configured. Set github_repo_path"
            " on this namespace or a parent namespace."
        )

    @pytest.mark.asyncio
    async def test_create_branch_without_default_branch(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test creating a branch from git root without default_branch fails."""
        # Create git root namespace with only repo path (no default_branch)
        await client_with_service_setup.post("/namespaces/test_no_default")
        await client_with_service_setup.patch(
            "/namespaces/test_no_default/git",
            json={"github_repo_path": "myorg/myrepo"},
        )

        response = await client_with_service_setup.post(
            "/namespaces/test_no_default/branches",
            json={"branch_name": "feature-x"},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert "default_branch" in response.json()["message"]

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
            assert data["branch"]["namespace"] == "sales.feature_x"
            assert data["branch"]["git_branch"] == "feature-x"
            assert data["branch"]["parent_namespace"] == "sales.main"
            assert data["branch"]["github_repo_path"] == "myorg/myrepo"
            assert data["deployment_results"] == []

            # Verify GitHub service was called
            mock_github.create_branch.assert_called_once_with(
                repo_path="myorg/myrepo",
                branch="feature-x",
                from_ref="main",
            )

    @pytest.mark.asyncio
    async def test_create_branch_from_git_root(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test successfully creating a branch from a git root namespace."""
        # Create git root namespace
        await client_with_service_setup.post("/namespaces/company.metrics")
        await client_with_service_setup.patch(
            "/namespaces/company.metrics/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_path": "definitions",
                "default_branch": "main",
            },
        )

        # Create the main branch namespace (that we'll copy from)
        await client_with_service_setup.post("/namespaces/company.metrics.main")
        await client_with_service_setup.patch(
            "/namespaces/company.metrics.main/git",
            json={
                "parent_namespace": "company.metrics",
                "git_branch": "main",
            },
        )

        # Mock the GitHubService
        with patch(
            "datajunction_server.api.branches.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.create_branch = AsyncMock(
                return_value={
                    "ref": "refs/heads/feature-y",
                    "object": {"sha": "def456"},
                },
            )
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.post(
                "/namespaces/company.metrics/branches",
                json={"branch_name": "feature-y"},
            )

            assert response.status_code == HTTPStatus.CREATED

            data = response.json()
            # New namespace should be child of git root
            assert data["branch"]["namespace"] == "company.metrics.feature_y"
            assert data["branch"]["git_branch"] == "feature-y"
            assert data["branch"]["parent_namespace"] == "company.metrics"
            assert data["branch"]["github_repo_path"] == "myorg/myrepo"

            # Verify GitHub service was called with default_branch as source
            mock_github.create_branch.assert_called_once_with(
                repo_path="myorg/myrepo",
                branch="feature-y",
                from_ref="main",  # Uses default_branch from git root
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
        # Create parent namespace with git config
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

            response = await client_with_service_setup.post(
                "/namespaces/del_git.main/branches",
                json={"branch_name": "to-delete"},
            )
            assert response.status_code == HTTPStatus.CREATED
            assert response.json()["branch"]["namespace"] == "del_git.to_delete"

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
            print("response!!", response.json())
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

    @pytest.mark.asyncio
    async def test_create_branch_empty_name(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test creating a branch with empty or whitespace-only name."""
        await client_with_service_setup.post("/namespaces/empty_name.main")
        await client_with_service_setup.patch(
            "/namespaces/empty_name.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        # Test with empty string
        response = await client_with_service_setup.post(
            "/namespaces/empty_name.main/branches",
            json={"branch_name": ""},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["message"] == "Branch name cannot be empty."

        # Test with whitespace only
        response = await client_with_service_setup.post(
            "/namespaces/empty_name.main/branches",
            json={"branch_name": "   "},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["message"] == "Branch name cannot be empty."

    @pytest.mark.asyncio
    async def test_create_branch_single_part_namespace(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test creating a branch from a single-part namespace (no dot)."""
        # Create a namespace without a dot in the name
        await client_with_service_setup.post("/namespaces/singlepart.main")
        await client_with_service_setup.patch(
            "/namespaces/singlepart.main/git",
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
                    "ref": "refs/heads/feature-branch",
                    "object": {"sha": "abc123"},
                },
            )
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.post(
                "/namespaces/singlepart.main/branches",
                json={"branch_name": "feature-branch"},
            )
            print("data!!", response.json())
            assert response.status_code == HTTPStatus.CREATED
            data = response.json()
            # For single-part namespace "singlepart", branch becomes "singlepart.feature_branch"
            assert data["branch"]["namespace"] == "singlepart.feature_branch"
            assert data["branch"]["git_branch"] == "feature-branch"
            assert data["branch"]["parent_namespace"] == "singlepart.main"
            assert data["branch"]["github_repo_path"] == "myorg/myrepo"
            assert data["deployment_results"] == []

    @pytest.mark.asyncio
    async def test_delete_branch_skip_git_deletion(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test deleting a branch namespace without deleting the git branch."""
        # Create parent and branch
        await client_with_service_setup.post("/namespaces/skip_git.main")
        await client_with_service_setup.patch(
            "/namespaces/skip_git.main/git",
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
                    "ref": "refs/heads/keep-branch",
                    "object": {"sha": "abc"},
                },
            )
            mock_github_class.return_value = mock_github

            await client_with_service_setup.post(
                "/namespaces/skip_git.main/branches",
                json={"branch_name": "keep-branch"},
            )

        # Delete the branch with delete_git_branch=false
        response = await client_with_service_setup.delete(
            "/namespaces/skip_git.main/branches/skip_git.keep_branch"
            "?delete_git_branch=false",
        )
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data["message"] == "Branch namespace 'skip_git.keep_branch' deleted"
        # Git branch should NOT have been deleted
        assert data["git_branch_deleted"] is False

    @pytest.mark.asyncio
    async def test_delete_branch_with_nodes(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test deleting a branch namespace that contains nodes."""
        # Create parent namespace and branch
        await client_with_service_setup.post("/namespaces/with_nodes.main")
        await client_with_service_setup.patch(
            "/namespaces/with_nodes.main/git",
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
                return_value={"ref": "refs/heads/has-nodes", "object": {"sha": "abc"}},
            )
            mock_github_class.return_value = mock_github

            await client_with_service_setup.post(
                "/namespaces/with_nodes.main/branches",
                json={"branch_name": "has-nodes"},
            )

        # Create some nodes in the branch namespace
        await client_with_service_setup.post(
            "/nodes/source/",
            json={
                "name": "with_nodes.has_nodes.test_source",
                "description": "Test source in branch",
                "catalog": "default",
                "schema_": "test",
                "table": "test_table",
                "columns": [{"name": "id", "type": "int"}],
            },
        )
        await client_with_service_setup.post(
            "/nodes/transform/",
            json={
                "name": "with_nodes.has_nodes.test_transform",
                "description": "Test transform in branch",
                "query": "SELECT id FROM with_nodes.has_nodes.test_source",
            },
        )

        # Delete the branch (without git deletion to simplify)
        response = await client_with_service_setup.delete(
            "/namespaces/with_nodes.main/branches/with_nodes.has_nodes"
            "?delete_git_branch=false",
        )
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data["nodes_deleted"] == 2

        # Verify nodes are gone
        response = await client_with_service_setup.get(
            "/nodes/with_nodes.has_nodes.test_source/",
        )
        assert response.status_code == HTTPStatus.NOT_FOUND

    @pytest.mark.asyncio
    async def test_delete_branch_with_child_namespaces(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test deleting a branch namespace that has nested child namespaces."""
        # Create parent namespace and branch
        await client_with_service_setup.post("/namespaces/nested.main")
        await client_with_service_setup.patch(
            "/namespaces/nested.main/git",
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
                    "ref": "refs/heads/parent-branch",
                    "object": {"sha": "abc"},
                },
            )
            mock_github_class.return_value = mock_github

            await client_with_service_setup.post(
                "/namespaces/nested.main/branches",
                json={"branch_name": "parent-branch"},
            )

        # Create nested child namespaces under the branch
        await client_with_service_setup.post(
            "/namespaces/nested.parent_branch.child1",
        )
        await client_with_service_setup.post(
            "/namespaces/nested.parent_branch.child2",
        )

        # Verify child namespaces exist
        response = await client_with_service_setup.get(
            "/namespaces/nested.parent_branch.child1/",
        )
        assert response.status_code == HTTPStatus.OK

        # Delete the branch (without git deletion to simplify)
        response = await client_with_service_setup.delete(
            "/namespaces/nested.main/branches/nested.parent_branch"
            "?delete_git_branch=false",
        )
        assert response.status_code == HTTPStatus.OK

        # Verify nested child namespaces are also deleted
        response = await client_with_service_setup.get(
            "/namespaces/nested.parent_branch.child1/",
        )
        assert response.status_code == HTTPStatus.NOT_FOUND

        response = await client_with_service_setup.get(
            "/namespaces/nested.parent_branch.child2/",
        )
        assert response.status_code == HTTPStatus.NOT_FOUND

    @pytest.mark.asyncio
    async def test_create_branch_git_fails_namespace_succeeds(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test that namespace gets cleaned up when git branch creation fails."""
        from datajunction_server.internal.git.github_service import GitHubServiceError

        await client_with_service_setup.post("/namespaces/cleanup_test.main")
        await client_with_service_setup.patch(
            "/namespaces/cleanup_test.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        with patch(
            "datajunction_server.api.branches.GitHubService",
        ) as mock_github_class:
            # Mock git branch creation to fail
            mock_github = MagicMock()
            mock_github.create_branch = AsyncMock(
                side_effect=GitHubServiceError(
                    "Git API error",
                    http_status_code=500,
                    github_status=500,
                ),
            )
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.post(
                "/namespaces/cleanup_test.main/branches",
                json={"branch_name": "test-branch"},
            )

            # Should fail
            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
            assert (
                response.json()["message"]
                == "Failed to create git branch 'test-branch': Git API error"
            )

        # Verify namespace was cleaned up (should not exist)
        response = await client_with_service_setup.get(
            "/namespaces/cleanup_test.test_branch/",
        )
        assert response.status_code == HTTPStatus.NOT_FOUND

    @pytest.mark.asyncio
    async def test_create_branch_namespace_fails_git_succeeds(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test that git branch gets cleaned up when namespace creation fails."""
        await client_with_service_setup.post("/namespaces/db_fail.main")
        await client_with_service_setup.patch(
            "/namespaces/db_fail.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        with (
            patch(
                "datajunction_server.api.branches.GitHubService",
            ) as mock_github_class,
            patch(
                "datajunction_server.api.branches.copy_nodes_to_namespace",
            ) as mock_copy,
        ):
            # Mock git branch creation to succeed
            mock_github = MagicMock()
            mock_github.create_branch = AsyncMock(
                return_value={
                    "ref": "refs/heads/fail-branch",
                    "object": {"sha": "abc123"},
                },
            )
            mock_github.delete_branch = AsyncMock()
            mock_github_class.return_value = mock_github

            # Mock node copy to fail
            mock_copy.side_effect = Exception("Database error during copy")

            response = await client_with_service_setup.post(
                "/namespaces/db_fail.main/branches",
                json={"branch_name": "fail-branch"},
            )

            # Should fail
            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
            assert response.json()["message"] == (
                "Failed to create namespace 'db_fail.fail_branch': Database error during copy"
            )

            # Verify git cleanup was called
            mock_github.delete_branch.assert_called_once_with(
                repo_path="myorg/myrepo",
                branch="fail-branch",
            )

    @pytest.mark.asyncio
    async def test_create_branch_both_operations_fail(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test handling when both git and namespace operations fail."""
        from datajunction_server.internal.git.github_service import GitHubServiceError

        await client_with_service_setup.post("/namespaces/both_fail.main")
        await client_with_service_setup.patch(
            "/namespaces/both_fail.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        with (
            patch(
                "datajunction_server.api.branches.GitHubService",
            ) as mock_github_class,
            patch(
                "datajunction_server.api.branches.copy_nodes_to_namespace",
            ) as mock_copy,
        ):
            # Mock git to fail
            mock_github = MagicMock()
            mock_github.create_branch = AsyncMock(
                side_effect=GitHubServiceError(
                    "Git error",
                    http_status_code=500,
                    github_status=500,
                ),
            )
            mock_github_class.return_value = mock_github

            # Mock namespace to fail
            mock_copy.side_effect = Exception("DB error")

            response = await client_with_service_setup.post(
                "/namespaces/both_fail.main/branches",
                json={"branch_name": "fail-both"},
            )

            # Should fail with error mentioning both
            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
            message = response.json()["message"]
            assert (
                message
                == "Failed to create branch: Git error: Git error, Namespace error: DB error"
            )

    @pytest.mark.asyncio
    async def test_create_branch_cleanup_git_fails_gracefully(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test that git cleanup failure is logged but doesn't crash."""
        await client_with_service_setup.post("/namespaces/git_cleanup.main")
        await client_with_service_setup.patch(
            "/namespaces/git_cleanup.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        with (
            patch(
                "datajunction_server.api.branches.GitHubService",
            ) as mock_github_class,
            patch(
                "datajunction_server.api.branches.copy_nodes_to_namespace",
            ) as mock_copy,
        ):
            # Mock git to succeed
            mock_github = MagicMock()
            mock_github.create_branch = AsyncMock(
                return_value={
                    "ref": "refs/heads/test",
                    "object": {"sha": "abc"},
                },
            )
            # Mock git delete to fail during cleanup
            mock_github.delete_branch = AsyncMock(
                side_effect=Exception("Delete failed"),
            )
            mock_github_class.return_value = mock_github

            # Mock namespace copy to fail
            mock_copy.side_effect = Exception("Copy failed")

            response = await client_with_service_setup.post(
                "/namespaces/git_cleanup.main/branches",
                json={"branch_name": "test"},
            )

            # Should still return error, but cleanup failure is logged
            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
            # Verify cleanup was attempted
            mock_github.delete_branch.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_branch_cleanup_namespace_fails_gracefully(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test that namespace cleanup failure is logged but doesn't crash."""
        from datajunction_server.internal.git.github_service import GitHubServiceError

        await client_with_service_setup.post("/namespaces/ns_cleanup.main")
        await client_with_service_setup.patch(
            "/namespaces/ns_cleanup.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        # Create some nodes in parent namespace that will be copied
        await client_with_service_setup.post(
            "/nodes/source/",
            json={
                "name": "ns_cleanup.main.source1",
                "catalog": "public",
                "schema_": "test",
                "table": "foo",
                "columns": [{"name": "id", "type": "int"}],
            },
        )
        await client_with_service_setup.post(
            "/nodes/source/",
            json={
                "name": "ns_cleanup.main.source2",
                "catalog": "public",
                "schema_": "test",
                "table": "bar",
                "columns": [{"name": "id", "type": "int"}],
            },
        )

        with (
            patch(
                "datajunction_server.api.branches.GitHubService",
            ) as mock_github_class,
            # patch(
            #     "datajunction_server.api.branches._cleanup_namespace_and_nodes",
            # ) as mock_cleanup,
        ):
            # Mock git to fail
            mock_github = MagicMock()
            mock_github.create_branch = AsyncMock(
                side_effect=GitHubServiceError(
                    "Git failed",
                    http_status_code=500,
                    github_status=500,
                ),
            )
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.post(
                "/namespaces/ns_cleanup.main/branches",
                json={"branch_name": "test"},
            )

            # Should still return original error (git failure), not cleanup error
            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
            assert (
                response.json()["message"]
                == "Failed to create git branch 'test': Git failed"
            )

            # Verify cleanup was attempted
            # mock_cleanup.assert_called_once()


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
            == "Namespace 'default' does not have git configured. Set github_repo_path"
            " on this namespace or a parent namespace."
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
    async def test_sync_node_preserves_comments_from_existing(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test that syncing decodes base64 content and passes it for comment preservation."""
        await client_with_roads.patch(
            "/namespaces/default/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        # Existing YAML with a comment
        existing_yaml_with_comment = (
            "# Important comment\nname: old_name\nquery: SELECT 1"
        )

        encoded_content = base64.b64encode(
            existing_yaml_with_comment.encode("utf-8"),
        ).decode("utf-8")

        with (
            patch(
                "datajunction_server.api.git_sync.GitHubService",
            ) as mock_github_class,
            patch(
                "datajunction_server.api.git_sync.node_spec_to_yaml",
            ) as mock_node_spec_to_yaml,
        ):
            mock_github = MagicMock()
            mock_github.get_file = AsyncMock(
                return_value={
                    "sha": "existing-sha-456",
                    "content": encoded_content,
                },
            )
            mock_github.commit_file = AsyncMock(
                return_value={
                    "commit": {
                        "sha": "newcommit456",
                        "html_url": "https://github.com/myorg/myrepo/commit/new2",
                    },
                },
            )
            mock_github_class.return_value = mock_github
            mock_node_spec_to_yaml.return_value = "updated: yaml"

            response = await client_with_roads.post(
                "/nodes/default.repair_orders/sync-to-git",
                json={},
            )

            assert response.status_code == HTTPStatus.OK

            # Verify node_spec_to_yaml was called with the decoded existing YAML
            assert mock_node_spec_to_yaml.called
            call_args = mock_node_spec_to_yaml.call_args
            # Second argument should be existing_yaml
            assert call_args.kwargs["existing_yaml"] == existing_yaml_with_comment

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
    async def test_sync_node_no_git_branch(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test syncing a node when namespace has repo but no git_branch."""
        # Configure git with repo but no branch
        await client_with_roads.patch(
            "/namespaces/default/git",
            json={
                "github_repo_path": "myorg/myrepo",
                # No git_branch
            },
        )

        response = await client_with_roads.post(
            "/nodes/default.repair_orders/sync-to-git",
            json={},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["message"]
            == "Namespace 'default' does not have a git branch configured."
        )

    @pytest.mark.asyncio
    async def test_sync_node_with_query(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test syncing a transform node (which has a query) to git."""
        # Configure git for the namespace
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
            mock_github.get_file = AsyncMock(return_value=None)
            mock_github.commit_file = AsyncMock(
                return_value={
                    "commit": {
                        "sha": "abc123def456",
                        "html_url": "https://github.com/myorg/myrepo/commit/abc123",
                    },
                },
            )
            mock_github_class.return_value = mock_github

            # Sync a transform node (has query) - uses repair_orders_fact from roads example
            response = await client_with_roads.post(
                "/nodes/default.repair_orders_fact/sync-to-git",
                json={},
            )

            assert response.status_code == HTTPStatus.OK
            data = response.json()
            assert data["node_name"] == "default.repair_orders_fact"

    @pytest.mark.asyncio
    async def test_sync_node_github_error(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test handling GitHubServiceError when syncing a node."""
        from datajunction_server.internal.git.github_service import GitHubServiceError

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
            mock_github.get_file = AsyncMock(return_value=None)
            mock_github.commit_file = AsyncMock(
                side_effect=GitHubServiceError(
                    "Rate limit exceeded",
                    http_status_code=500,
                    github_status=403,
                ),
            )
            mock_github_class.return_value = mock_github

            response = await client_with_roads.post(
                "/nodes/default.repair_orders/sync-to-git",
                json={},
            )

            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
            assert (
                "Failed to sync to git: Rate limit exceeded"
                in response.json()["message"]
            )

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
            # Mock download_archive to return empty tarball
            mock_github.download_archive = AsyncMock(
                return_value=create_mock_tarball({}),
            )
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
    async def test_sync_namespace_preserves_comments_from_existing(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test that namespace sync reads existing YAML from archive for comment preservation."""
        await client_with_roads.patch(
            "/namespaces/default/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        # Existing YAML with comments - we'll put some files in the mock tarball
        # The namespace "default" should have nodes like "default.repair_orders"
        existing_yaml = "# File comment\nname: old_value\nquery: SELECT 1"

        with (
            patch(
                "datajunction_server.api.git_sync.GitHubService",
            ) as mock_github_class,
            patch(
                "datajunction_server.api.git_sync.node_spec_to_yaml",
            ) as mock_node_spec_to_yaml,
        ):
            mock_github = MagicMock()
            # Mock download_archive to return tarball with existing YAML files
            # Create tarball with some existing files
            mock_github.download_archive = AsyncMock(
                return_value=create_mock_tarball(
                    {
                        "repair_orders.yaml": existing_yaml,
                    },
                ),
            )
            mock_github.commit_files = AsyncMock(
                return_value={
                    "sha": "batch-commit-789",
                    "html_url": "https://github.com/myorg/myrepo/commit/batch",
                },
            )
            mock_github_class.return_value = mock_github
            mock_node_spec_to_yaml.return_value = "updated: yaml content"

            response = await client_with_roads.post(
                "/namespaces/default/sync-to-git",
                json={},
            )

            assert response.status_code == HTTPStatus.OK

            # Verify node_spec_to_yaml was called with decoded existing_yaml
            assert mock_node_spec_to_yaml.called
            # Check that at least one call had existing_yaml parameter
            calls_with_existing = [
                call
                for call in mock_node_spec_to_yaml.call_args_list
                if call.kwargs.get("existing_yaml") == existing_yaml
            ]
            assert len(calls_with_existing) > 0, (
                "Expected at least one call with decoded existing_yaml"
            )

    @pytest.mark.asyncio
    async def test_sync_node_file_doesnt_exist_yet(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test syncing a node when the YAML file doesn't exist in Git yet (covers exception handler)."""
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
            # Simulate file not existing by raising exception
            mock_github.get_file = AsyncMock(side_effect=Exception("File not found"))
            mock_github.commit_file = AsyncMock(
                return_value={
                    "commit": {
                        "sha": "newcommit789",
                        "html_url": "https://github.com/myorg/myrepo/commit/new3",
                    },
                },
            )
            mock_github_class.return_value = mock_github

            response = await client_with_roads.post(
                "/nodes/default.repair_orders/sync-to-git",
                json={},
            )

            assert response.status_code == HTTPStatus.OK
            # Verify commit_file was called (file created)
            assert mock_github.commit_file.called
            # Verify sha=None was passed (new file)
            call_kwargs = mock_github.commit_file.call_args.kwargs
            assert call_kwargs.get("sha") is None

    @pytest.mark.asyncio
    async def test_sync_node_with_existing_file_real_decode(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test syncing with real base64 decoding (no mocking of node_spec_to_yaml)."""
        await client_with_roads.patch(
            "/namespaces/default/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        # Create realistic existing YAML for a transform node
        existing_yaml = """name: default.repair_orders
query: SELECT 1
node_type: transform
mode: published
"""
        encoded_content = base64.b64encode(existing_yaml.encode("utf-8")).decode(
            "utf-8",
        )

        with patch(
            "datajunction_server.api.git_sync.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            # Return existing file with base64 content
            mock_github.get_file = AsyncMock(
                return_value={
                    "sha": "existing-file-sha",
                    "content": encoded_content,
                },
            )
            mock_github.commit_file = AsyncMock(
                return_value={
                    "commit": {
                        "sha": "updated-commit",
                        "html_url": "https://github.com/myorg/myrepo/commit/upd",
                    },
                },
            )
            mock_github_class.return_value = mock_github

            # Don't mock node_spec_to_yaml - let it run for real
            response = await client_with_roads.post(
                "/nodes/default.repair_orders/sync-to-git",
                json={},
            )

            assert response.status_code == HTTPStatus.OK
            # Verify the file was updated (sha was passed)
            assert mock_github.commit_file.called
            call_kwargs = mock_github.commit_file.call_args.kwargs
            assert call_kwargs["sha"] == "existing-file-sha"

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

    @pytest.mark.asyncio
    async def test_sync_namespace_no_git_config(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test syncing namespace when no git config is set."""
        await client_with_service_setup.post("/namespaces/no_git_ns")

        response = await client_with_service_setup.post(
            "/namespaces/no_git_ns/sync-to-git",
            json={},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["message"]
            == "Namespace 'no_git_ns' does not have git configured. "
            "Set github_repo_path on this namespace or a parent namespace."
        )

    @pytest.mark.asyncio
    async def test_sync_namespace_no_git_branch(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test syncing namespace when repo is set but branch is not."""
        await client_with_service_setup.post("/namespaces/no_branch_ns")
        await client_with_service_setup.patch(
            "/namespaces/no_branch_ns/git",
            json={
                "github_repo_path": "myorg/myrepo",
                # No git_branch
            },
        )

        response = await client_with_service_setup.post(
            "/namespaces/no_branch_ns/sync-to-git",
            json={},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["message"]
            == "Namespace 'no_branch_ns' does not have a git branch configured."
        )

    @pytest.mark.asyncio
    async def test_sync_namespace_github_error(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test handling GitHubServiceError when syncing namespace."""
        from datajunction_server.internal.git.github_service import GitHubServiceError

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
            # Mock download_archive to return empty tarball
            mock_github.download_archive = AsyncMock(
                return_value=create_mock_tarball({}),
            )
            mock_github.commit_files = AsyncMock(
                side_effect=GitHubServiceError(
                    "Repository not found",
                    http_status_code=500,
                    github_status=404,
                ),
            )
            mock_github_class.return_value = mock_github

            response = await client_with_roads.post(
                "/namespaces/default/sync-to-git",
                json={},
            )

            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
            assert (
                "Failed to sync to git: Repository not found"
                in response.json()["message"]
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
    async def test_create_pr_parent_is_git_root(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test creating PR when parent is a git root with default_branch."""
        # Create git root with default_branch
        await client_with_service_setup.post("/namespaces/pr_gitroot")
        await client_with_service_setup.patch(
            "/namespaces/pr_gitroot/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "default_branch": "main",
            },
        )

        # Create main branch namespace
        await client_with_service_setup.post("/namespaces/pr_gitroot.main")
        await client_with_service_setup.patch(
            "/namespaces/pr_gitroot.main/git",
            json={
                "parent_namespace": "pr_gitroot",
                "git_branch": "main",
            },
        )

        # Create feature branch namespace with git root as parent
        await client_with_service_setup.post("/namespaces/pr_gitroot.feature")
        await client_with_service_setup.patch(
            "/namespaces/pr_gitroot.feature/git",
            json={
                "parent_namespace": "pr_gitroot",
                "git_branch": "feature",
            },
        )

        # Mock GitHub service
        with patch(
            "datajunction_server.api.git_sync.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.get_pull_request = AsyncMock(return_value=None)
            mock_github.create_pull_request = AsyncMock(
                return_value={
                    "number": 42,
                    "html_url": "https://github.com/myorg/myrepo/pull/42",
                },
            )
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.post(
                "/namespaces/pr_gitroot.feature/pull-request",
                json={"title": "My PR", "body": "Test PR"},
            )

            assert response.status_code == HTTPStatus.OK
            data = response.json()
            assert data["pr_number"] == 42
            assert data["head_branch"] == "feature"
            assert data["base_branch"] == "main"  # Targets default_branch

            # Verify PR was created targeting main branch
            mock_github.create_pull_request.assert_called_once()
            call_args = mock_github.create_pull_request.call_args[1]
            assert call_args["base"] == "main"
            assert call_args["head"] == "feature"

    @pytest.mark.asyncio
    async def test_create_pr_no_git_config(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test creating PR when branch namespace has no git configured."""
        # Create parent namespace
        await client_with_service_setup.post("/namespaces/pr_noconfig.main")
        await client_with_service_setup.patch(
            "/namespaces/pr_noconfig.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        # Create branch namespace with parent but NO git config (no repo/branch)
        await client_with_service_setup.post("/namespaces/pr_noconfig.feature")
        await client_with_service_setup.patch(
            "/namespaces/pr_noconfig.feature/git",
            json={
                "parent_namespace": "pr_noconfig.main",
                # No github_repo_path or git_branch
            },
        )

        response = await client_with_service_setup.post(
            "/namespaces/pr_noconfig.feature/pull-request",
            json={"title": "My PR"},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["message"]
            == "Namespace 'pr_noconfig.feature' does not have git configured. Set "
            "github_repo_path and git_branch on this namespace or a parent."
        )

    @pytest.mark.asyncio
    async def test_create_pr_github_error(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test handling GitHubServiceError when creating PR."""
        from datajunction_server.internal.git.github_service import GitHubServiceError

        # Create parent namespace
        await client_with_service_setup.post("/namespaces/pr_error.main")
        await client_with_service_setup.patch(
            "/namespaces/pr_error.main/git",
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
                return_value={"ref": "refs/heads/error-pr", "object": {"sha": "abc"}},
            )
            mock_github_class.return_value = mock_github

            await client_with_service_setup.post(
                "/namespaces/pr_error.main/branches",
                json={"branch_name": "error-pr"},
            )

        # Create PR with GitHub error
        with patch(
            "datajunction_server.api.git_sync.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.get_pull_request = AsyncMock(return_value=None)
            mock_github.create_pull_request = AsyncMock(
                side_effect=GitHubServiceError(
                    "Validation failed: head and base must be different",
                    http_status_code=500,
                    github_status=422,
                ),
            )
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.post(
                "/namespaces/pr_error.error_pr/pull-request",
                json={"title": "My PR"},
            )

            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
            assert (
                response.json()["message"]
                == "Validation failed: head and base must be different"
            )

    @pytest.mark.asyncio
    async def test_find_nodes_with_git_info_graphql(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test finding nodes with git_info via GraphQL."""
        # Create namespace with git config
        await client_with_service_setup.post("/namespaces/gql_test.main")
        await client_with_service_setup.patch(
            "/namespaces/gql_test.main/git",
            json={
                "github_repo_path": "myorg/metrics-repo",
                "git_branch": "main",
                "default_branch": "main",
                "git_path": "definitions/",
                "git_only": False,
            },
        )

        # Create a transform node in this namespace
        await client_with_service_setup.post(
            "/nodes/transform/",
            json={
                "name": "gql_test.main.revenue",
                "description": "Revenue metric",
                "query": "SELECT SUM(amount) as revenue FROM sales",
                "mode": "draft",
            },
        )

        # Query with GraphQL
        query = """
        {
            findNodes(names: ["gql_test.main.revenue"]) {
                name
                type
                gitInfo {
                    repo
                    branch
                    defaultBranch
                    isDefaultBranch
                    path
                    parentNamespace
                    gitOnly
                }
            }
        }
        """

        response = await client_with_service_setup.post(
            "/graphql",
            json={"query": query},
        )
        assert response.status_code == 200
        data = response.json()

        # Validate git_info is present and correct
        nodes = data["data"]["findNodes"]
        assert len(nodes) == 1
        node = nodes[0]
        assert node["name"] == "gql_test.main.revenue"
        assert node["type"] == "TRANSFORM"

        git_info = node["gitInfo"]
        assert git_info is not None
        assert git_info["repo"] == "myorg/metrics-repo"
        assert git_info["branch"] == "main"
        assert git_info["defaultBranch"] == "main"
        assert git_info["isDefaultBranch"] is True
        assert git_info["path"] == "definitions/"
        assert git_info["parentNamespace"] is None
        assert git_info["gitOnly"] is False

    @pytest.mark.asyncio
    async def test_find_nodes_with_git_info_feature_branch_graphql(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test finding nodes in feature branch with git_info via GraphQL."""
        # Create parent namespace with git config
        await client_with_service_setup.post("/namespaces/gql_branch_test.main")
        await client_with_service_setup.patch(
            "/namespaces/gql_branch_test.main/git",
            json={
                "github_repo_path": "myorg/metrics-repo",
                "git_branch": "main",
                "default_branch": "main",
                "git_path": "definitions/",
            },
        )

        # Create a child namespace (feature branch)
        # Mock GitHub service for branch creation
        with patch(
            "datajunction_server.api.branches.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.create_branch = AsyncMock(
                return_value={
                    "ref": "refs/heads/feature-new-metric",
                    "object": {"sha": "def456"},
                },
            )
            mock_github_class.return_value = mock_github

            await client_with_service_setup.post(
                "/namespaces/gql_branch_test.main/branches",
                json={"branch_name": "feature-new-metric"},
            )

        # Create a node in the feature branch namespace
        await client_with_service_setup.post(
            "/nodes/transform/",
            json={
                "name": "gql_branch_test.feature_new_metric.revenue",
                "description": "Revenue metric in feature branch",
                "query": "SELECT SUM(amount) as revenue FROM sales",
                "mode": "draft",
            },
        )

        # Query with GraphQL
        query = """
        {
            findNodes(names: ["gql_branch_test.feature_new_metric.revenue"]) {
                name
                type
                gitInfo {
                    repo
                    branch
                    defaultBranch
                    isDefaultBranch
                    path
                    parentNamespace
                    gitOnly
                }
            }
        }
        """

        response = await client_with_service_setup.post(
            "/graphql",
            json={"query": query},
        )
        assert response.status_code == 200
        data = response.json()

        # Validate git_info is present and correct
        nodes = data["data"]["findNodes"]
        assert len(nodes) == 1
        node = nodes[0]
        assert node["name"] == "gql_branch_test.feature_new_metric.revenue"

        git_info = node["gitInfo"]
        assert git_info is not None
        # Repo info comes from parent namespace
        assert git_info["repo"] == "myorg/metrics-repo"
        # Branch comes from child namespace
        assert git_info["branch"] == "feature-new-metric"
        # Default branch comes from parent
        assert git_info["defaultBranch"] == "main"
        # Not on default branch
        assert git_info["isDefaultBranch"] is False
        # Path comes from parent
        assert git_info["path"] == "definitions/"
        # Parent namespace is set
        assert git_info["parentNamespace"] == "gql_branch_test"


class TestGetPullRequest:
    """Tests for GET /namespaces/{namespace}/pull-request endpoint."""

    @pytest.mark.asyncio
    async def test_get_pr_non_branch_namespace(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test getting PR for a non-branch namespace (no parent_namespace)."""
        await client_with_service_setup.post("/namespaces/get_pr_no_parent")
        await client_with_service_setup.patch(
            "/namespaces/get_pr_no_parent/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        response = await client_with_service_setup.get(
            "/namespaces/get_pr_no_parent/pull-request",
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json() is None

    @pytest.mark.asyncio
    async def test_get_pr_no_git_config(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test getting PR for branch namespace without git config."""
        # Create parent namespace
        await client_with_service_setup.post("/namespaces/get_pr_parent.main")
        await client_with_service_setup.patch(
            "/namespaces/get_pr_parent.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        # Create child namespace with parent_namespace but no github_repo_path
        await client_with_service_setup.post("/namespaces/get_pr_parent.child")
        await client_with_service_setup.patch(
            "/namespaces/get_pr_parent.child/git",
            json={
                "parent_namespace": "get_pr_parent.main",
                # No github_repo_path or git_branch
            },
        )

        response = await client_with_service_setup.get(
            "/namespaces/get_pr_parent.child/pull-request",
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json() is None

    @pytest.mark.asyncio
    async def test_get_pr_parent_is_git_root(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test getting PR when parent is a git root with default_branch."""
        # Create git root with default_branch
        await client_with_service_setup.post("/namespaces/get_pr_gitroot")
        await client_with_service_setup.patch(
            "/namespaces/get_pr_gitroot/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "default_branch": "main",
            },
        )

        # Create main branch namespace
        await client_with_service_setup.post("/namespaces/get_pr_gitroot.main")
        await client_with_service_setup.patch(
            "/namespaces/get_pr_gitroot.main/git",
            json={
                "parent_namespace": "get_pr_gitroot",
                "git_branch": "main",
            },
        )

        # Create feature branch with git root as parent
        await client_with_service_setup.post("/namespaces/get_pr_gitroot.feature")
        await client_with_service_setup.patch(
            "/namespaces/get_pr_gitroot.feature/git",
            json={
                "parent_namespace": "get_pr_gitroot",
                "git_branch": "feature",
            },
        )

        # Mock GitHub service
        with patch(
            "datajunction_server.api.git_sync.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.get_pull_request = AsyncMock(
                return_value={
                    "number": 55,
                    "html_url": "https://github.com/myorg/myrepo/pull/55",
                },
            )
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.get(
                "/namespaces/get_pr_gitroot.feature/pull-request",
            )

            assert response.status_code == HTTPStatus.OK
            data = response.json()
            assert data["pr_number"] == 55
            assert data["head_branch"] == "feature"
            assert data["base_branch"] == "main"  # Targets default_branch

            # Verify get_pull_request was called with correct branches
            mock_github.get_pull_request.assert_called_once()
            call_args = mock_github.get_pull_request.call_args[1]
            assert call_args["base"] == "main"
            assert call_args["head"] == "feature"

    @pytest.mark.asyncio
    async def test_get_pr_exists(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test getting PR when one exists."""
        # Create parent namespace
        await client_with_service_setup.post("/namespaces/get_pr_exists.main")
        await client_with_service_setup.patch(
            "/namespaces/get_pr_exists.main/git",
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
                return_value={"ref": "refs/heads/has-pr", "object": {"sha": "abc"}},
            )
            mock_github_class.return_value = mock_github

            await client_with_service_setup.post(
                "/namespaces/get_pr_exists.main/branches",
                json={"branch_name": "has-pr"},
            )

        # Mock get_pull_request to return existing PR
        with patch(
            "datajunction_server.api.git_sync.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.get_pull_request = AsyncMock(
                return_value={
                    "number": 123,
                    "html_url": "https://github.com/myorg/myrepo/pull/123",
                },
            )
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.get(
                "/namespaces/get_pr_exists.has_pr/pull-request",
            )

            assert response.status_code == HTTPStatus.OK
            data = response.json()
            assert data["pr_number"] == 123
            assert data["pr_url"] == "https://github.com/myorg/myrepo/pull/123"
            assert data["head_branch"] == "has-pr"
            assert data["base_branch"] == "main"

            mock_github.get_pull_request.assert_called_once_with(
                repo_path="myorg/myrepo",
                head="has-pr",
                base="main",
            )

    @pytest.mark.asyncio
    async def test_get_pr_not_exists(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test getting PR when none exists."""
        # Create parent namespace
        await client_with_service_setup.post("/namespaces/get_pr_none.main")
        await client_with_service_setup.patch(
            "/namespaces/get_pr_none.main/git",
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
                return_value={"ref": "refs/heads/no-pr", "object": {"sha": "abc"}},
            )
            mock_github_class.return_value = mock_github

            await client_with_service_setup.post(
                "/namespaces/get_pr_none.main/branches",
                json={"branch_name": "no-pr"},
            )

        # Mock get_pull_request to return None
        with patch(
            "datajunction_server.api.git_sync.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.get_pull_request = AsyncMock(return_value=None)
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.get(
                "/namespaces/get_pr_none.no_pr/pull-request",
            )

            assert response.status_code == HTTPStatus.OK
            assert response.json() is None

    @pytest.mark.asyncio
    async def test_get_pr_github_error(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test getting PR when GitHub API fails."""
        from datajunction_server.internal.git.github_service import GitHubServiceError

        # Create parent namespace
        await client_with_service_setup.post("/namespaces/get_pr_error.main")
        await client_with_service_setup.patch(
            "/namespaces/get_pr_error.main/git",
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
                return_value={
                    "ref": "refs/heads/error-branch",
                    "object": {"sha": "abc"},
                },
            )
            mock_github_class.return_value = mock_github

            await client_with_service_setup.post(
                "/namespaces/get_pr_error.main/branches",
                json={"branch_name": "error-branch"},
            )

        # Mock get_pull_request to raise GitHubServiceError
        with patch(
            "datajunction_server.api.git_sync.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.get_pull_request = AsyncMock(
                side_effect=GitHubServiceError(
                    "API rate limit exceeded",
                    http_status_code=500,
                    github_status=403,
                ),
            )
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.get(
                "/namespaces/get_pr_error.error_branch/pull-request",
            )

            # Should return None on error, not raise
            assert response.status_code == HTTPStatus.OK
            assert response.json() is None


class TestGitOnlyNamespaceProtection:
    """Tests for git_only namespace protection against direct node mutations."""

    @pytest.mark.asyncio
    async def test_create_node_in_git_only_namespace_rejected(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test that creating a node in a git_only namespace is rejected."""
        root_namespace = "git_only_root"
        branch_namespace = "git_only_protected"

        # Create git root namespace with only github_repo_path
        await client_with_service_setup.post(f"/namespaces/{root_namespace}")
        response = await client_with_service_setup.patch(
            f"/namespaces/{root_namespace}/git",
            json={
                "github_repo_path": "myorg/myrepo",
            },
        )
        assert response.status_code == HTTPStatus.OK

        # Create branch namespace and set git_only=True
        await client_with_service_setup.post(f"/namespaces/{branch_namespace}")
        response = await client_with_service_setup.patch(
            f"/namespaces/{branch_namespace}/git",
            json={
                "parent_namespace": root_namespace,
                "git_branch": "main",
                "git_only": True,
            },
        )
        assert response.status_code == HTTPStatus.OK
        print("response", response.json())

        # Try to create a source node directly
        print("Creating node in ", branch_namespace, " namespace...")
        response = await client_with_service_setup.post(
            "/nodes/source/",
            json={
                "name": f"{branch_namespace}.test_source",
                "description": "Test source",
                "catalog": "default",
                "schema_": "test",
                "table": "test_table",
                "columns": [{"name": "id", "type": "int"}],
            },
        )

        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert "git-only" in response.json()["message"]
        assert "must be deployed from git" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_update_git_only_field(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test updating git_only field via PATCH endpoint."""
        root_namespace = "update_git_only_root"
        branch_namespace = "update_git_only"

        # Create git root namespace
        await client_with_service_setup.post(f"/namespaces/{root_namespace}")
        response = await client_with_service_setup.patch(
            f"/namespaces/{root_namespace}/git",
            json={
                "github_repo_path": "myorg/myrepo",
            },
        )
        assert response.status_code == HTTPStatus.OK

        # Create branch namespace
        await client_with_service_setup.post(f"/namespaces/{branch_namespace}")

        # Set git_only=True on branch namespace
        response = await client_with_service_setup.patch(
            f"/namespaces/{branch_namespace}/git",
            json={
                "parent_namespace": root_namespace,
                "git_branch": "main",
                "git_only": True,
            },
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["git_only"] is True

        # Update git_only back to False
        response = await client_with_service_setup.patch(
            f"/namespaces/{branch_namespace}/git",
            json={
                "git_only": False,
            },
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["git_only"] is False

    @pytest.mark.asyncio
    async def test_set_parent_without_repo_skips_mismatch_check(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test setting parent_namespace when parent has no github_repo_path."""
        # Create parent namespace WITHOUT github_repo_path
        await client_with_service_setup.post("/namespaces/parent_no_repo")
        # Don't set github_repo_path on parent

        # Create child namespace
        await client_with_service_setup.post("/namespaces/child_with_parent")

        # Set parent_namespace - should succeed since there's no repo to mismatch
        response = await client_with_service_setup.patch(
            "/namespaces/child_with_parent/git",
            json={
                "parent_namespace": "parent_no_repo",
                # No github_repo_path set on child either
            },
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["parent_namespace"] == "parent_no_repo"

    @pytest.mark.asyncio
    async def test_set_parent_child_has_no_repo(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test setting parent_namespace when child has no github_repo_path."""
        # Create parent with github_repo_path
        await client_with_service_setup.post("/namespaces/parent_has_repo")
        await client_with_service_setup.patch(
            "/namespaces/parent_has_repo/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        # Create child namespace
        await client_with_service_setup.post("/namespaces/child_no_repo")

        # Set parent_namespace without setting repo - should succeed
        # (repo mismatch check skipped because child has no repo)
        response = await client_with_service_setup.patch(
            "/namespaces/child_no_repo/git",
            json={
                "parent_namespace": "parent_has_repo",
                # No github_repo_path set
            },
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["parent_namespace"] == "parent_has_repo"


class TestCopyNodesToNamespace:
    """Tests for copy_nodes_to_namespace function (used during branch creation)."""

    @pytest.mark.asyncio
    async def test_branch_creation_copies_nodes(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test that creating a branch copies nodes from parent namespace."""
        # Create parent namespace with git config
        await client_with_service_setup.post("/namespaces/copy_test.main")
        await client_with_service_setup.patch(
            "/namespaces/copy_test.main/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        # Create some nodes in the parent namespace
        response = await client_with_service_setup.post(
            "/nodes/source/",
            json={
                "name": "copy_test.main.source_table",
                "description": "Source table to copy",
                "catalog": "default",
                "schema_": "test",
                "table": "source_table",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "value", "type": "string"},
                ],
            },
        )
        assert response.status_code <= HTTPStatus.CREATED

        response = await client_with_service_setup.post(
            "/nodes/transform/",
            json={
                "name": "copy_test.main.transform_node",
                "description": "Transform to copy",
                "query": "SELECT id, value FROM copy_test.main.source_table",
            },
        )
        assert response.status_code <= HTTPStatus.CREATED

        # Create a branch - this should trigger copy_nodes_to_namespace
        with patch(
            "datajunction_server.api.branches.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.create_branch = AsyncMock(
                return_value={
                    "ref": "refs/heads/feature-copy",
                    "object": {"sha": "abc123"},
                },
            )
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.post(
                "/namespaces/copy_test.main/branches",
                json={"branch_name": "feature-copy"},
            )

            assert response.status_code == HTTPStatus.CREATED
            data = response.json()
            assert data["branch"]["namespace"] == "copy_test.feature_copy"
            assert data["deployment_results"] == [
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": "copy_test.feature_copy.source_table",
                    "operation": "create",
                    "status": "success",
                },
                {
                    "deploy_type": "node",
                    "message": "Created transform (v1.0)",
                    "name": "copy_test.feature_copy.transform_node",
                    "operation": "create",
                    "status": "success",
                },
            ]

        # Verify nodes exist in the branch namespace
        response = await client_with_service_setup.get(
            "/nodes/copy_test.feature_copy.source_table/",
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["name"] == "copy_test.feature_copy.source_table"

        response = await client_with_service_setup.get(
            "/nodes/copy_test.feature_copy.transform_node/",
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["name"] == "copy_test.feature_copy.transform_node"
        # The query should reference the branch namespace
        assert "copy_test.feature_copy.source_table" in response.json()["query"]


class TestGitHubServiceErrorHandling:
    """Tests for GitHub service error handling edge cases."""

    @pytest.mark.asyncio
    async def test_github_error_with_detailed_errors_array(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test handling GitHub errors that include a detailed errors array."""
        from datajunction_server.internal.git.github_service import GitHubServiceError

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
            mock_github.get_file = AsyncMock(return_value=None)
            # Simulate a GitHub error with detailed errors array
            mock_github.commit_file = AsyncMock(
                side_effect=GitHubServiceError(
                    "Validation Failed\n- Resource not accessible",
                    http_status_code=502,
                    github_status=422,
                ),
            )
            mock_github_class.return_value = mock_github

            response = await client_with_roads.post(
                "/nodes/default.repair_orders/sync-to-git",
                json={},
            )

            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
            # Error message should include the detailed error
            assert "Validation Failed" in response.json()["message"]


class TestGitSyncEdgeCases:
    """Tests for git sync edge cases like non-prefixed node names."""

    @pytest.mark.asyncio
    async def test_sync_node_without_namespace_prefix(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """Test syncing a node whose name doesn't start with namespace prefix."""
        # Create a namespace
        await client_with_service_setup.post("/namespaces/edge_test")
        await client_with_service_setup.patch(
            "/namespaces/edge_test/git",
            json={
                "github_repo_path": "myorg/myrepo",
                "git_branch": "main",
            },
        )

        # Create a source node that has a short name matching namespace
        # (edge case where node_name doesn't start with "namespace.")
        response = await client_with_service_setup.post(
            "/nodes/source/",
            json={
                "name": "edge_test.test_node",
                "description": "Test node",
                "catalog": "default",
                "schema_": "test",
                "table": "test_table",
                "columns": [{"name": "id", "type": "int"}],
            },
        )
        assert response.status_code <= HTTPStatus.CREATED

        with patch(
            "datajunction_server.api.git_sync.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.get_file = AsyncMock(return_value=None)
            mock_github.commit_file = AsyncMock(
                return_value={
                    "commit": {
                        "sha": "abc123",
                        "html_url": "https://github.com/myorg/myrepo/commit/abc123",
                    },
                },
            )
            mock_github_class.return_value = mock_github

            response = await client_with_service_setup.post(
                "/nodes/edge_test.test_node/sync-to-git",
                json={},
            )

            assert response.status_code == HTTPStatus.OK
            # File path should be just the short name
            assert response.json()["file_path"] == "test_node.yaml"
