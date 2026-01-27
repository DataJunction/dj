"""GitHub API service for git operations."""

import base64
import os
from typing import Optional

import httpx
from dotenv import load_dotenv

from datajunction_server.config import Settings
from datajunction_server.errors import DJException


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

    Uses a service account token from settings. Commits are
    attributed to the actual user via author metadata.
    """

    def __init__(self):
        # Load settings directly to avoid circular import through utils.py
        dotenv_file = os.environ.get("DOTENV_FILE", ".env")
        load_dotenv(dotenv_file)
        self.settings = Settings()
        if not self.settings.github_service_token:
            raise GitHubServiceError(
                message="GitHub service token not configured. Set GITHUB_SERVICE_TOKEN.",
                http_status_code=503,
            )
        self.token = self.settings.github_service_token
        self.base_url = self.settings.github_api_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github.v3+json",
        }

    def _handle_error(self, resp: httpx.Response, operation: str) -> None:
        """Handle GitHub API errors with meaningful messages."""
        if resp.is_success:
            return

        try:
            error_data = resp.json()
            message = error_data.get("message", resp.text)
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

    async def commit_file(
        self,
        repo_path: str,
        path: str,
        content: str,
        message: str,
        branch: str,
        sha: Optional[str] = None,
        author_name: Optional[str] = None,
        author_email: Optional[str] = None,
    ) -> dict:
        """Create or update a file in the repository.

        Author attribution allows commits made by service account to
        show the actual user who made the change.

        Args:
            repo_path: Repository path (e.g., "owner/repo")
            path: File path within the repo
            content: File content (will be base64 encoded)
            message: Commit message
            branch: Target branch
            sha: Current file SHA (required for updates, omit for new files)
            author_name: Name to attribute the commit to
            author_email: Email to attribute the commit to

        Returns:
            Commit result from GitHub API
        """
        async with httpx.AsyncClient() as client:
            payload: dict = {
                "message": message,
                "content": base64.b64encode(content.encode()).decode(),
                "branch": branch,
            }
            if sha:
                payload["sha"] = sha
            if author_name and author_email:
                payload["author"] = {
                    "name": author_name,
                    "email": author_email,
                }
                payload["committer"] = {
                    "name": author_name,
                    "email": author_email,
                }

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
        author_name: Optional[str] = None,
        author_email: Optional[str] = None,
    ) -> dict:
        """Delete a file from the repository.

        Args:
            repo_path: Repository path (e.g., "owner/repo")
            path: File path within the repo
            message: Commit message
            branch: Target branch
            sha: Current file SHA (required)
            author_name: Name to attribute the commit to
            author_email: Email to attribute the commit to

        Returns:
            Commit result from GitHub API
        """
        async with httpx.AsyncClient() as client:
            payload: dict = {
                "message": message,
                "sha": sha,
                "branch": branch,
            }
            if author_name and author_email:
                payload["author"] = {
                    "name": author_name,
                    "email": author_email,
                }
                payload["committer"] = {
                    "name": author_name,
                    "email": author_email,
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
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self.base_url}/repos/{repo_path}/pulls",
                headers=self.headers,
                params={
                    "head": head,
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
