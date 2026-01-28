"""Tests for GitHubService."""

import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datajunction_server.internal.git.github_service import (
    GitHubService,
    GitHubServiceError,
)


@pytest.fixture
def mock_settings():
    """Mock settings with GitHub PAT config."""
    settings = MagicMock()
    settings.github_service_token = "test-token"
    settings.github_api_url = "https://api.github.com"
    # GitHub App settings not configured (PAT mode)
    settings.github_app_id = None
    settings.github_app_private_key = None
    settings.github_app_installation_id = None
    return settings


@pytest.fixture
def github_service(mock_settings):
    """Create GitHubService with mocked settings."""
    with patch(
        "datajunction_server.internal.git.github_service.Settings",
        return_value=mock_settings,
    ):
        with patch(
            "datajunction_server.internal.git.github_service.load_dotenv",
        ):
            return GitHubService()


class TestGitHubServiceInit:
    """Tests for GitHubService initialization."""

    def test_init_without_any_auth(self):
        """Should raise error when no auth configured."""
        mock_settings = MagicMock()
        mock_settings.github_service_token = None
        mock_settings.github_api_url = "https://api.github.com"
        mock_settings.github_app_id = None
        mock_settings.github_app_private_key = None
        mock_settings.github_app_installation_id = None

        with patch(
            "datajunction_server.internal.git.github_service.Settings",
            return_value=mock_settings,
        ):
            with patch(
                "datajunction_server.internal.git.github_service.load_dotenv",
            ):
                with pytest.raises(GitHubServiceError) as exc_info:
                    GitHubService()

                assert "GitHub authentication not configured" in str(exc_info.value)
                assert exc_info.value.http_status_code == 503

    def test_init_with_pat_token(self, github_service):
        """Should initialize correctly with PAT token."""
        assert github_service.token == "test-token"
        assert github_service.base_url == "https://api.github.com"
        assert "Authorization" in github_service.headers
        assert github_service.headers["Authorization"] == "Bearer test-token"

    def test_init_with_github_app(self):
        """Should initialize with GitHub App credentials."""
        mock_settings = MagicMock()
        mock_settings.github_service_token = None  # No PAT
        mock_settings.github_api_url = "https://api.github.com"
        mock_settings.github_app_id = "12345"
        mock_settings.github_app_private_key = (
            "-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----"
        )
        mock_settings.github_app_installation_id = "67890"

        # Mock the installation token response
        mock_token_response = MagicMock()
        mock_token_response.is_success = True
        mock_token_response.json.return_value = {"token": "installation-token-abc"}

        with patch(
            "datajunction_server.internal.git.github_service.Settings",
            return_value=mock_settings,
        ):
            with patch(
                "datajunction_server.internal.git.github_service.load_dotenv",
            ):
                with patch("httpx.Client") as mock_client:
                    mock_client.return_value.__enter__.return_value.post.return_value = mock_token_response
                    with patch("jwt.encode", return_value="mock-jwt"):
                        service = GitHubService()

                        assert service.token == "installation-token-abc"
                        assert (
                            service.headers["Authorization"]
                            == "Bearer installation-token-abc"
                        )

    def test_init_github_app_prefers_over_pat(self):
        """Should prefer GitHub App auth when both are configured."""
        mock_settings = MagicMock()
        mock_settings.github_service_token = "pat-token"  # PAT also configured
        mock_settings.github_api_url = "https://api.github.com"
        mock_settings.github_app_id = "12345"
        mock_settings.github_app_private_key = (
            "-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----"
        )
        mock_settings.github_app_installation_id = "67890"

        mock_token_response = MagicMock()
        mock_token_response.is_success = True
        mock_token_response.json.return_value = {"token": "installation-token"}

        with patch(
            "datajunction_server.internal.git.github_service.Settings",
            return_value=mock_settings,
        ):
            with patch(
                "datajunction_server.internal.git.github_service.load_dotenv",
            ):
                with patch("httpx.Client") as mock_client:
                    mock_client.return_value.__enter__.return_value.post.return_value = mock_token_response
                    with patch("jwt.encode", return_value="mock-jwt"):
                        service = GitHubService()

                        # Should use App token, not PAT
                        assert service.token == "installation-token"

    def test_init_github_app_partial_config_falls_back_to_pat(self):
        """Should fall back to PAT when GitHub App config is incomplete."""
        mock_settings = MagicMock()
        mock_settings.github_service_token = "pat-token"
        mock_settings.github_api_url = "https://api.github.com"
        # Only app_id set, missing private_key and installation_id
        mock_settings.github_app_id = "12345"
        mock_settings.github_app_private_key = None
        mock_settings.github_app_installation_id = None

        with patch(
            "datajunction_server.internal.git.github_service.Settings",
            return_value=mock_settings,
        ):
            with patch(
                "datajunction_server.internal.git.github_service.load_dotenv",
            ):
                service = GitHubService()

                # Should fall back to PAT
                assert service.token == "pat-token"

    def test_init_github_app_token_fetch_failure(self):
        """Should raise error when GitHub App token fetch fails."""
        mock_settings = MagicMock()
        mock_settings.github_service_token = None
        mock_settings.github_api_url = "https://api.github.com"
        mock_settings.github_app_id = "12345"
        mock_settings.github_app_private_key = (
            "-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----"
        )
        mock_settings.github_app_installation_id = "67890"

        mock_token_response = MagicMock()
        mock_token_response.is_success = False
        mock_token_response.status_code = 401
        mock_token_response.json.return_value = {"message": "Bad credentials"}

        with patch(
            "datajunction_server.internal.git.github_service.Settings",
            return_value=mock_settings,
        ):
            with patch(
                "datajunction_server.internal.git.github_service.load_dotenv",
            ):
                with patch("httpx.Client") as mock_client:
                    mock_client.return_value.__enter__.return_value.post.return_value = mock_token_response
                    with patch("jwt.encode", return_value="mock-jwt"):
                        with pytest.raises(GitHubServiceError) as exc_info:
                            GitHubService()

                        assert "Failed to get GitHub App installation token" in str(
                            exc_info.value,
                        )
                        assert exc_info.value.github_status == 401


class TestListBranches:
    """Tests for list_branches method."""

    @pytest.mark.asyncio
    async def test_list_branches_success(self, github_service):
        """Should return list of branches."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"name": "main", "commit": {"sha": "abc123"}},
            {"name": "feature-x", "commit": {"sha": "def456"}},
        ]

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response,
            )

            branches = await github_service.list_branches("owner/repo")

            assert len(branches) == 2
            assert branches[0]["name"] == "main"

    @pytest.mark.asyncio
    async def test_list_branches_error(self, github_service):
        """Should raise error on API failure."""
        mock_response = MagicMock()
        mock_response.is_success = False
        mock_response.status_code = 404
        mock_response.json.return_value = {"message": "Not Found"}

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response,
            )

            with pytest.raises(GitHubServiceError) as exc_info:
                await github_service.list_branches("owner/repo")

            assert "list branches failed" in str(exc_info.value)
            assert exc_info.value.github_status == 404


class TestGetBranch:
    """Tests for get_branch method."""

    @pytest.mark.asyncio
    async def test_get_branch_exists(self, github_service):
        """Should return branch when found."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.is_success = True
        mock_response.json.return_value = {
            "name": "main",
            "commit": {"sha": "abc123"},
        }

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response,
            )

            branch = await github_service.get_branch("owner/repo", "main")

            assert branch["name"] == "main"

    @pytest.mark.asyncio
    async def test_get_branch_not_found(self, github_service):
        """Should return None when branch not found."""
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response,
            )

            branch = await github_service.get_branch("owner/repo", "nonexistent")

            assert branch is None


class TestCreateBranch:
    """Tests for create_branch method."""

    @pytest.mark.asyncio
    async def test_create_branch_success(self, github_service):
        """Should create branch from ref."""
        # Mock get ref response
        ref_response = MagicMock()
        ref_response.is_success = True
        ref_response.json.return_value = {"object": {"sha": "abc123"}}

        # Mock create ref response
        create_response = MagicMock()
        create_response.is_success = True
        create_response.json.return_value = {
            "ref": "refs/heads/feature-x",
            "object": {"sha": "abc123"},
        }

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = mock_client.return_value.__aenter__.return_value
            mock_instance.get = AsyncMock(return_value=ref_response)
            mock_instance.post = AsyncMock(return_value=create_response)

            result = await github_service.create_branch(
                "owner/repo",
                "feature-x",
                "main",
            )

            assert result["ref"] == "refs/heads/feature-x"

    @pytest.mark.asyncio
    async def test_create_branch_source_not_found(self, github_service):
        """Should raise error when source ref not found."""
        ref_response = MagicMock()
        ref_response.is_success = False
        ref_response.status_code = 404
        ref_response.json.return_value = {"message": "Not Found"}

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=ref_response,
            )

            with pytest.raises(GitHubServiceError) as exc_info:
                await github_service.create_branch("owner/repo", "feature-x", "main")

            assert "get ref main failed" in str(exc_info.value)


class TestGetFile:
    """Tests for get_file method."""

    @pytest.mark.asyncio
    async def test_get_file_exists(self, github_service):
        """Should return file content and SHA."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.is_success = True
        mock_response.json.return_value = {
            "sha": "file-sha-123",
            "content": base64.b64encode(b"file content").decode(),
            "path": "path/to/file.yaml",
        }

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response,
            )

            result = await github_service.get_file(
                "owner/repo",
                "path/to/file.yaml",
                "main",
            )

            assert result["sha"] == "file-sha-123"

    @pytest.mark.asyncio
    async def test_get_file_not_found(self, github_service):
        """Should return None when file not found."""
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response,
            )

            result = await github_service.get_file(
                "owner/repo",
                "nonexistent.yaml",
                "main",
            )

            assert result is None


class TestCommitFile:
    """Tests for commit_file method."""

    @pytest.mark.asyncio
    async def test_commit_file_create(self, github_service):
        """Should create new file without SHA."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {
            "commit": {
                "sha": "commit-sha-123",
                "html_url": "https://github.com/owner/repo/commit/abc",
            },
            "content": {"sha": "file-sha-456"},
        }

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = mock_client.return_value.__aenter__.return_value
            mock_instance.put = AsyncMock(return_value=mock_response)

            result = await github_service.commit_file(
                repo_path="owner/repo",
                path="path/to/file.yaml",
                content="file content",
                message="Add file",
                branch="main",
            )

            assert result["commit"]["sha"] == "commit-sha-123"

            # Verify the call
            call_args = mock_instance.put.call_args
            payload = call_args.kwargs["json"]
            assert payload["message"] == "Add file"
            assert payload["branch"] == "main"
            assert "sha" not in payload  # No SHA for new file

    @pytest.mark.asyncio
    async def test_commit_file_update(self, github_service):
        """Should update existing file with SHA."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {
            "commit": {"sha": "commit-sha-123", "html_url": "https://..."},
        }

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = mock_client.return_value.__aenter__.return_value
            mock_instance.put = AsyncMock(return_value=mock_response)

            await github_service.commit_file(
                repo_path="owner/repo",
                path="path/to/file.yaml",
                content="updated content",
                message="Update file",
                branch="main",
                sha="existing-sha",
            )

            call_args = mock_instance.put.call_args
            payload = call_args.kwargs["json"]
            assert payload["sha"] == "existing-sha"

    @pytest.mark.asyncio
    async def test_commit_file_with_co_author(self, github_service):
        """Should include Co-authored-by trailer in commit message."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {"commit": {"sha": "abc"}}

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = mock_client.return_value.__aenter__.return_value
            mock_instance.put = AsyncMock(return_value=mock_response)

            await github_service.commit_file(
                repo_path="owner/repo",
                path="file.yaml",
                content="content",
                message="Update",
                branch="main",
                co_author_name="Test User",
                co_author_email="test@example.com",
            )

            call_args = mock_instance.put.call_args
            payload = call_args.kwargs["json"]
            # Should have Co-authored-by trailer in message
            assert "Co-authored-by: Test User <test@example.com>" in payload["message"]
            # Should NOT have author/committer fields (let GitHub use the bot)
            assert "author" not in payload
            assert "committer" not in payload


class TestDeleteFile:
    """Tests for delete_file method."""

    @pytest.mark.asyncio
    async def test_delete_file_success(self, github_service):
        """Should delete file with SHA."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {"commit": {"sha": "delete-commit"}}

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = mock_client.return_value.__aenter__.return_value
            mock_instance.request = AsyncMock(return_value=mock_response)

            result = await github_service.delete_file(
                repo_path="owner/repo",
                path="file.yaml",
                message="Delete file",
                branch="main",
                sha="file-sha",
            )

            assert result["commit"]["sha"] == "delete-commit"


class TestCreatePullRequest:
    """Tests for create_pull_request method."""

    @pytest.mark.asyncio
    async def test_create_pr_success(self, github_service):
        """Should create PR with title and body."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {
            "number": 42,
            "html_url": "https://github.com/owner/repo/pull/42",
            "title": "My PR",
        }

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = mock_client.return_value.__aenter__.return_value
            mock_instance.post = AsyncMock(return_value=mock_response)

            result = await github_service.create_pull_request(
                repo_path="owner/repo",
                head="feature-x",
                base="main",
                title="My PR",
                body="Description",
            )

            assert result["number"] == 42

            call_args = mock_instance.post.call_args
            payload = call_args.kwargs["json"]
            assert payload["head"] == "feature-x"
            assert payload["base"] == "main"
            assert payload["title"] == "My PR"


class TestGetPullRequest:
    """Tests for get_pull_request method."""

    @pytest.mark.asyncio
    async def test_get_pr_exists(self, github_service):
        """Should return existing PR."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [
            {"number": 42, "html_url": "https://..."},
        ]

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response,
            )

            result = await github_service.get_pull_request(
                "owner/repo",
                "feature-x",
                "main",
            )

            assert result["number"] == 42

    @pytest.mark.asyncio
    async def test_get_pr_none(self, github_service):
        """Should return None when no PR exists."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = []

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response,
            )

            result = await github_service.get_pull_request(
                "owner/repo",
                "feature-x",
                "main",
            )

            assert result is None


class TestDeleteBranch:
    """Tests for delete_branch method."""

    @pytest.mark.asyncio
    async def test_delete_branch_success(self, github_service):
        """Should delete branch."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.status_code = 204

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.delete = AsyncMock(
                return_value=mock_response,
            )

            # Should not raise
            await github_service.delete_branch("owner/repo", "feature-x")

    @pytest.mark.asyncio
    async def test_delete_branch_already_deleted(self, github_service):
        """Should not raise when branch already deleted (422)."""
        mock_response = MagicMock()
        mock_response.is_success = False
        mock_response.status_code = 422

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.delete = AsyncMock(
                return_value=mock_response,
            )

            # Should not raise for 422
            await github_service.delete_branch("owner/repo", "feature-x")


class TestErrorHandling:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_error_extracts_message(self, github_service):
        """Should extract error message from GitHub response."""
        mock_response = MagicMock()
        mock_response.is_success = False
        mock_response.status_code = 403
        mock_response.json.return_value = {"message": "API rate limit exceeded"}

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response,
            )

            with pytest.raises(GitHubServiceError) as exc_info:
                await github_service.list_branches("owner/repo")

            assert "API rate limit exceeded" in str(exc_info.value)
            assert exc_info.value.http_status_code == 502
            assert exc_info.value.github_status == 403

    @pytest.mark.asyncio
    async def test_error_handles_non_json_response(self, github_service):
        """Should handle non-JSON error responses."""
        mock_response = MagicMock()
        mock_response.is_success = False
        mock_response.status_code = 500
        mock_response.json.side_effect = Exception("Not JSON")
        mock_response.text = "Internal Server Error"

        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response,
            )

            with pytest.raises(GitHubServiceError) as exc_info:
                await github_service.list_branches("owner/repo")

            assert "Internal Server Error" in str(exc_info.value)
