"""GitHub API service for git operations."""

import base64
import logging
import time
from typing import Optional

import httpx
import jwt

from datajunction_server.errors import DJException
from datajunction_server.utils import get_settings

_logger = logging.getLogger(__name__)


class GitHubServiceError(DJException):
    """Error from GitHub API operations."""

    def __init__(
        self,
        message: str,
        http_status_code: int = 500,
        github_status: Optional[int] = None,
    ):
        super().__init__(message=message, http_status_code=http_status_code)
        self.github_status = github_status


class GitHubService:
    """GitHub API client for branch/file/PR operations.

    Supports two authentication methods:
    1. Simple PAT auth (OSS default): Set GITHUB_SERVICE_TOKEN
    2. GitHub App auth (enterprise): Set GITHUB_APP_ID, GITHUB_APP_PRIVATE_KEY,
       and GITHUB_APP_INSTALLATION_ID

    Commits are attributed to the actual user via author metadata.
    """

    def __init__(self):
        self.settings = get_settings()
        self.base_url = self.settings.github_api_url.rstrip("/")

        # Determine auth method and get token
        self.token = self._get_token()
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github.v3+json",
        }

    def _get_token(self) -> str:
        """Get GitHub token using configured auth method.

        Checks for GitHub App auth first, falls back to PAT.
        """
        # Check for GitHub App auth (all three required)
        if (
            self.settings.github_app_id
            and self.settings.github_app_private_key
            and self.settings.github_app_installation_id
        ):
            return self._get_installation_token()

        # Fall back to simple PAT auth
        if self.settings.github_service_token:
            return self.settings.github_service_token

        raise GitHubServiceError(
            message=(
                "GitHub authentication not configured. "
                "Set GITHUB_SERVICE_TOKEN for PAT auth, or set "
                "GITHUB_APP_ID, GITHUB_APP_PRIVATE_KEY, and "
                "GITHUB_APP_INSTALLATION_ID for GitHub App auth."
            ),
            http_status_code=503,
        )

    def _get_installation_token(self) -> str:
        """Generate an installation access token using GitHub App credentials.

        Uses the App's private key to create a JWT, then exchanges it for
        an installation token scoped to the configured installation.
        """
        # Generate JWT signed with the app's private key
        now = int(time.time())
        payload = {
            "iat": now - 60,  # Issued 60 seconds ago to account for clock drift
            "exp": now + 600,  # Expires in 10 minutes (max allowed)
            "iss": self.settings.github_app_id,
        }

        try:
            app_jwt = jwt.encode(
                payload,
                self.settings.github_app_private_key,
                algorithm="RS256",
            )
        except Exception as e:
            raise GitHubServiceError(
                message=f"Failed to generate GitHub App JWT: {e}",
                http_status_code=503,
            ) from e

        # Exchange JWT for installation token
        installation_id = self.settings.github_app_installation_id
        url = f"{self.base_url}/app/installations/{installation_id}/access_tokens"

        # Use synchronous request since we're in __init__
        with httpx.Client() as client:
            resp = client.post(
                url,
                headers={
                    "Authorization": f"Bearer {app_jwt}",
                    "Accept": "application/vnd.github.v3+json",
                },
                timeout=30.0,
            )

        if not resp.is_success:
            try:
                error_msg = resp.json().get("message", resp.text)
            except Exception:
                error_msg = resp.text
            raise GitHubServiceError(
                message=f"Failed to get GitHub App installation token: {error_msg}",
                http_status_code=503,
                github_status=resp.status_code,
            )

        return resp.json()["token"]

    def _handle_error(self, resp: httpx.Response, operation: str) -> None:
        """Handle GitHub API errors with meaningful messages."""
        if resp.is_success:
            return

        try:
            error_data = resp.json()
            message = error_data.get("message", resp.text)
            if "errors" in error_data:  # pragma: no cover
                detailed_errors = "; ".join(
                    err.get("message", str(err)) for err in error_data["errors"]
                )
                message += f"\n- {detailed_errors}"
        except Exception:
            message = resp.text

        raise GitHubServiceError(
            message=f"GitHub {operation} failed: {message}",
            http_status_code=502,  # Bad Gateway - upstream error
            github_status=resp.status_code,
        )

    async def list_branches(self, repo_path: str) -> list[dict]:
        """List branches in a repository.

        Args:
            repo_path: Repository path (e.g., "owner/repo")

        Returns:
            List of branch objects from GitHub API
        """
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self.base_url}/repos/{repo_path}/branches",
                headers=self.headers,
                timeout=30.0,
            )
            self._handle_error(resp, "list branches")
            return resp.json()

    async def get_branch(self, repo_path: str, branch: str) -> Optional[dict]:
        """Get a specific branch.

        Args:
            repo_path: Repository path (e.g., "owner/repo")
            branch: Branch name

        Returns:
            Branch object or None if not found
        """
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self.base_url}/repos/{repo_path}/branches/{branch}",
                headers=self.headers,
                timeout=30.0,
            )
            if resp.status_code == 404:
                return None
            self._handle_error(resp, "get branch")
            return resp.json()

    async def create_branch(
        self,
        repo_path: str,
        branch: str,
        from_ref: str = "main",
    ) -> dict:
        """Create a new branch from a reference.

        Args:
            repo_path: Repository path (e.g., "owner/repo")
            branch: New branch name
            from_ref: Source branch/ref to branch from

        Returns:
            Created ref object from GitHub API
        """
        async with httpx.AsyncClient() as client:
            # Get SHA of source ref
            ref_resp = await client.get(
                f"{self.base_url}/repos/{repo_path}/git/ref/heads/{from_ref}",
                headers=self.headers,
                timeout=30.0,
            )
            self._handle_error(ref_resp, f"get ref {from_ref}")
            sha = ref_resp.json()["object"]["sha"]

            # Create new branch
            create_resp = await client.post(
                f"{self.base_url}/repos/{repo_path}/git/refs",
                headers=self.headers,
                json={"ref": f"refs/heads/{branch}", "sha": sha},
                timeout=30.0,
            )
            self._handle_error(create_resp, f"create branch {branch}")
            return create_resp.json()

    async def get_file(
        self,
        repo_path: str,
        path: str,
        branch: str,
    ) -> Optional[dict]:
        """Get file content and SHA (for updates).

        Args:
            repo_path: Repository path (e.g., "owner/repo")
            path: File path within the repo
            branch: Branch to read from

        Returns:
            File object with content and sha, or None if not found
        """
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self.base_url}/repos/{repo_path}/contents/{path}",
                headers=self.headers,
                params={"ref": branch},
                timeout=30.0,
            )
            if resp.status_code == 404:
                return None
            self._handle_error(resp, f"get file {path}")
            return resp.json()

    def _add_co_author(
        self,
        message: str,
        co_author_name: Optional[str],
        co_author_email: Optional[str],
    ) -> str:
        """Add Co-authored-by trailer to commit message if co-author info provided.

        Args:
            message: Original commit message
            co_author_name: Name of the co-author
            co_author_email: Email of the co-author

        Returns:
            Message with Co-authored-by trailer appended
        """
        if co_author_name and co_author_email:
            return f"{message}\n\nCo-authored-by: {co_author_name} <{co_author_email}>"
        return message

    async def commit_file(
        self,
        repo_path: str,
        path: str,
        content: str,
        message: str,
        branch: str,
        sha: Optional[str] = None,
        co_author_name: Optional[str] = None,
        co_author_email: Optional[str] = None,
    ) -> dict:
        """Create or update a file in the repository.

        Commits are made by the authenticated bot/app. User attribution is
        added via Co-authored-by trailer in the commit message.

        Args:
            repo_path: Repository path (e.g., "owner/repo")
            path: File path within the repo
            content: File content (will be base64 encoded)
            message: Commit message
            branch: Target branch
            sha: Current file SHA (required for updates, omit for new files)
            co_author_name: Name of user to add as co-author
            co_author_email: Email of user to add as co-author

        Returns:
            Commit result from GitHub API
        """
        final_message = self._add_co_author(message, co_author_name, co_author_email)

        async with httpx.AsyncClient() as client:
            payload: dict = {
                "message": final_message,
                "content": base64.b64encode(content.encode()).decode(),
                "branch": branch,
            }
            if sha:
                payload["sha"] = sha

            resp = await client.put(
                f"{self.base_url}/repos/{repo_path}/contents/{path}",
                headers=self.headers,
                json=payload,
                timeout=30.0,
            )
            self._handle_error(resp, f"commit file {path}")
            return resp.json()

    async def delete_file(
        self,
        repo_path: str,
        path: str,
        message: str,
        branch: str,
        sha: str,
        co_author_name: Optional[str] = None,
        co_author_email: Optional[str] = None,
    ) -> dict:
        """Delete a file from the repository.

        Commits are made by the authenticated bot/app. User attribution is
        added via Co-authored-by trailer in the commit message.

        Args:
            repo_path: Repository path (e.g., "owner/repo")
            path: File path within the repo
            message: Commit message
            branch: Target branch
            sha: Current file SHA (required)
            co_author_name: Name of user to add as co-author
            co_author_email: Email of user to add as co-author

        Returns:
            Commit result from GitHub API
        """
        final_message = self._add_co_author(message, co_author_name, co_author_email)

        async with httpx.AsyncClient() as client:
            payload: dict = {
                "message": final_message,
                "sha": sha,
                "branch": branch,
            }

            resp = await client.request(
                "DELETE",
                f"{self.base_url}/repos/{repo_path}/contents/{path}",
                headers=self.headers,
                json=payload,
                timeout=30.0,
            )
            self._handle_error(resp, f"delete file {path}")
            return resp.json()

    async def commit_files(
        self,
        repo_path: str,
        files: list[dict],
        message: str,
        branch: str,
        co_author_name: Optional[str] = None,
        co_author_email: Optional[str] = None,
    ) -> dict:
        """Commit multiple files in a single commit using Git Data API.

        This is more efficient than commit_file() when updating multiple files,
        as it creates only one commit instead of one per file.

        Commits are made by the authenticated bot/app. User attribution is
        added via Co-authored-by trailer in the commit message.

        Args:
            repo_path: Repository path (e.g., "owner/repo")
            files: List of file dicts with 'path' and 'content' keys
                   e.g., [{"path": "foo.yaml", "content": "..."}, ...]
            message: Commit message
            branch: Target branch
            co_author_name: Name of user to add as co-author
            co_author_email: Email of user to add as co-author

        Returns:
            Commit result with 'sha' and 'html_url'
        """
        if not files:
            raise GitHubServiceError(
                message="No files to commit",
                http_status_code=400,
            )

        _logger.info(
            "Batch committing %d files to %s branch '%s': %s",
            len(files),
            repo_path,
            branch,
            [f["path"] for f in files],
        )

        async with httpx.AsyncClient() as client:
            # 1. Get the current branch ref to find the latest commit
            ref_resp = await client.get(
                f"{self.base_url}/repos/{repo_path}/git/ref/heads/{branch}",
                headers=self.headers,
                timeout=30.0,
            )
            self._handle_error(ref_resp, f"get ref heads/{branch}")
            current_commit_sha = ref_resp.json()["object"]["sha"]

            # 2. Get the tree SHA from the current commit
            commit_resp = await client.get(
                f"{self.base_url}/repos/{repo_path}/git/commits/{current_commit_sha}",
                headers=self.headers,
                timeout=30.0,
            )
            self._handle_error(commit_resp, "get current commit")
            base_tree_sha = commit_resp.json()["tree"]["sha"]

            # 3. Build tree entries with inline content (no separate blob creation needed)
            tree_entries = [
                {
                    "path": file_info["path"],
                    "mode": "100644",  # Regular file
                    "type": "blob",
                    "content": file_info["content"],  # Inline content
                }
                for file_info in files
            ]

            # 4. Create a new tree with all the files
            tree_resp = await client.post(
                f"{self.base_url}/repos/{repo_path}/git/trees",
                headers=self.headers,
                json={
                    "base_tree": base_tree_sha,
                    "tree": tree_entries,
                },
                timeout=60.0,  # Longer timeout for large trees
            )
            self._handle_error(tree_resp, "create tree")
            new_tree_sha = tree_resp.json()["sha"]

            # 5. Create the commit
            final_message = self._add_co_author(
                message,
                co_author_name,
                co_author_email,
            )
            commit_payload: dict = {
                "message": final_message,
                "tree": new_tree_sha,
                "parents": [current_commit_sha],
            }

            new_commit_resp = await client.post(
                f"{self.base_url}/repos/{repo_path}/git/commits",
                headers=self.headers,
                json=commit_payload,
                timeout=30.0,
            )
            self._handle_error(new_commit_resp, "create commit")
            new_commit = new_commit_resp.json()

            # 6. Update the branch ref to point to new commit
            update_ref_resp = await client.patch(
                f"{self.base_url}/repos/{repo_path}/git/refs/heads/{branch}",
                headers=self.headers,
                json={"sha": new_commit["sha"]},
                timeout=30.0,
            )
            self._handle_error(update_ref_resp, f"update ref heads/{branch}")

            return {
                "sha": new_commit["sha"],
                "html_url": new_commit["html_url"],
                "files_committed": len(files),
            }

    async def create_pull_request(
        self,
        repo_path: str,
        head: str,
        base: str,
        title: str,
        body: str = "",
    ) -> dict:
        """Create a pull request.

        Args:
            repo_path: Repository path (e.g., "owner/repo")
            head: Source branch
            base: Target branch
            title: PR title
            body: PR description

        Returns:
            Created PR object from GitHub API
        """
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self.base_url}/repos/{repo_path}/pulls",
                headers=self.headers,
                json={
                    "title": title,
                    "body": body,
                    "head": head,
                    "base": base,
                },
                timeout=30.0,
            )
            self._handle_error(resp, "create pull request")
            return resp.json()

    async def get_pull_request(
        self,
        repo_path: str,
        head: str,
        base: str,
    ) -> Optional[dict]:
        """Get an existing pull request for the given head and base.

        Args:
            repo_path: Repository path (e.g., "owner/repo")
            head: Source branch
            base: Target branch

        Returns:
            PR object if found, None otherwise
        """
        # GitHub API requires head in format "owner:branch" for filtering
        owner = repo_path.split("/")[0]
        head_filter = f"{owner}:{head}"

        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self.base_url}/repos/{repo_path}/pulls",
                headers=self.headers,
                params={
                    "head": head_filter,
                    "base": base,
                    "state": "open",
                },
                timeout=30.0,
            )
            self._handle_error(resp, "list pull requests")
            prs = resp.json()
            return prs[0] if prs else None

    async def delete_branch(self, repo_path: str, branch: str) -> None:
        """Delete a branch.

        Args:
            repo_path: Repository path (e.g., "owner/repo")
            branch: Branch name to delete
        """
        async with httpx.AsyncClient() as client:
            resp = await client.delete(
                f"{self.base_url}/repos/{repo_path}/git/refs/heads/{branch}",
                headers=self.headers,
                timeout=30.0,
            )
            # 422 means ref doesn't exist, which is fine for delete
            if resp.status_code != 422:
                self._handle_error(resp, f"delete branch {branch}")

    async def get_repo(self, repo_path: str) -> Optional[dict]:
        """Get repository info to verify access.

        Args:
            repo_path: Repository path (e.g., "owner/repo")

        Returns:
            Repository object or None if not accessible
        """
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self.base_url}/repos/{repo_path}",
                headers=self.headers,
                timeout=30.0,
            )
            if resp.status_code == 404:
                return None
            self._handle_error(resp, "get repository")
            return resp.json()

    async def verify_commit(self, repo_path: str, commit_sha: str) -> bool:
        """Verify that a commit exists in the repository.

        Args:
            repo_path: Repository path (e.g., "owner/repo")
            commit_sha: The commit SHA to verify

        Returns:
            True if the commit exists, False otherwise
        """
        print("Called the real verify_commit!!!")
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self.base_url}/repos/{repo_path}/commits/{commit_sha}",
                headers=self.headers,
                timeout=30.0,
            )
            return resp.status_code == 200

    async def download_archive(
        self,
        repo_path: str,
        branch: str,
        format: str = "tarball",
    ) -> bytes:
        """Download entire repository archive as tarball or zipball.

        This is much more efficient than calling get_file() multiple times
        when you need to read many files from the repository.

        Args:
            repo_path: Repository path (e.g., "owner/repo")
            branch: Branch name
            format: "tarball" or "zipball"

        Returns:
            Raw archive bytes
        """
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self.base_url}/repos/{repo_path}/{format}/{branch}",
                headers=self.headers,
                timeout=120.0,  # Longer timeout for potentially large repos
                follow_redirects=True,  # GitHub redirects to blob storage
            )
            self._handle_error(resp, f"download {format}")
            return resp.content
