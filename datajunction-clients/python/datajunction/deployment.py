import json
import os
import re
import socket
import subprocess
import urllib.parse
import urllib.request
from enum import Enum
from pathlib import Path
import time
from typing import Any, Union

import yaml
from rich.console import Console
from rich.status import Status

from datajunction import DJBuilder
from datajunction.exceptions import (
    DJClientException,
    DJDeploymentFailure,
)
from datajunction.models import DeploymentInfo
from datajunction.rendering import print_deployment_header, print_results


# TODO: replace with generated models from OpenAPI spec once client codegen is set up.
# Canonical definitions live in datajunction_server/models/deployment.py.


class DeploymentStatus(str, Enum):
    """Overall deployment status. Mirrors server DeploymentStatus."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class ResultStatus(str, Enum):
    """Per-result node status. Mirrors server DeploymentResult.Status."""

    SUCCESS = "success"
    FAILED = "failed"
    INVALID = "invalid"
    SKIPPED = "skipped"
    NOOP = "noop"


class DeploymentService:
    """
    High-level deployment client for exporting and importing DJ namespaces.
    Intended for CLI scripts but reusable in Python code.
    """

    def __init__(self, client: DJBuilder, console: Console | None = None) -> None:
        self.client = client
        self.console = console or Console()

    def pull(
        self,
        namespace: str,
        target_path: Union[str, Path],
    ):
        """
        Export a namespace to a local project.

        Pulls the YAML files from the server's `/export/yaml` endpoint, which
        runs every node through the same serializer (`node_spec_to_yaml`) used
        by the UI sync-to-git flow. This way `dj pull` and a UI export produce
        identical YAML for the same node state.

        When the target directory already contains YAML files, they are uploaded
        to the server so it can merge new content into them — preserving key
        ordering, inline comments, and scalar styles. This means a `dj pull`
        against an already-populated directory produces minimal diffs.
        """
        import io
        import zipfile

        base_path = Path(target_path)
        base_path.mkdir(parents=True, exist_ok=True)

        existing_zip_bytes = None
        existing_yaml_files = list(base_path.rglob("*.yaml"))
        if existing_yaml_files:
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for yaml_file in existing_yaml_files:
                    zf.writestr(
                        str(yaml_file.relative_to(base_path)),
                        yaml_file.read_bytes(),
                    )
            existing_zip_bytes = buf.getvalue()

        zip_bytes = self.client._export_namespace_yaml_zip(
            namespace,
            existing_zip_bytes,
        )

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            zf.extractall(base_path)

    def push(
        self,
        source_path: str | Path,
        namespace: str | None = None,
        console: Console | None = None,
        verbose: bool = False,
        force: bool = False,
    ):
        """
        Push a local project to a namespace.
        """
        console = console or self.console

        deployment_spec, file_errors = self._reconstruct_deployment_spec(source_path)

        base_namespace = deployment_spec.get("namespace") or ""
        branch = os.getenv("DJ_DEPLOY_BRANCH") or DeploymentService._detect_git_branch(
            cwd=source_path,
        )
        source = deployment_spec.get("source", {})
        if namespace:
            deployment_spec["namespace"] = namespace
        elif branch and base_namespace:
            deployment_spec["namespace"] = DeploymentService._derive_namespace(
                base_namespace,
                branch,
            )

        print_deployment_header(
            mode="push",
            namespace=deployment_spec["namespace"],
            console=console,
            repo=source.get("repository"),
            branch=source.get("branch") or branch,
        )

        # Skip git config when the deployment namespace equals the project's
        # base namespace — no branch derivation happened, and setting parent
        # to the same namespace would error ("a namespace cannot be its own
        # parent"). This is the typical bootstrap / explicit-namespace case.
        if branch and deployment_spec["namespace"] != base_namespace:
            parent_namespace = base_namespace or None
            try:
                self.client._set_namespace_git_config(
                    deployment_spec["namespace"],
                    git_branch=branch,
                    parent_namespace=parent_namespace or None,
                )
            except Exception as e:  # pylint: disable=broad-except
                console.print(
                    f"[yellow]Warning: could not set git config on namespace "
                    f"'{deployment_spec['namespace']}': {e}[/yellow]",
                )
        if force:
            deployment_spec["force"] = True
        deployment_data = self.client.deploy(deployment_spec)
        deployment_uuid = deployment_data["uuid"]

        # console.print(f"[bold]Deployment initiated:[/bold] UUID {deployment_uuid}\n")

        # Max wait time for deployment to finish
        timeout = time.time() + 300  # 5 minutes

        with Status("Deploying...", console=console):
            while deployment_data.get("status") not in ("failed", "success"):
                time.sleep(1)
                deployment_data = self.client.check_deployment(deployment_uuid)

                if time.time() > timeout:
                    raise DJClientException("Deployment timed out after 5 minutes")

        deployment = DeploymentInfo.from_dict(deployment_data)
        print_results(
            deployment_uuid,
            deployment,
            console,
            verbose=verbose,
        )
        if deployment.status == DeploymentStatus.SUCCESS:
            invalid_results = [
                r for r in deployment.results if r.status == ResultStatus.INVALID
            ]
            if invalid_results:
                console.print(
                    "\nDeployment finished: [bold yellow]SUCCESS with invalid nodes[/bold yellow]",
                )
                raise DJDeploymentFailure(
                    project_name=deployment_spec.get("namespace", source_path),
                    errors=[r.__dict__ for r in invalid_results],
                )
            console.print("\nDeployment finished: [bold green]SUCCESS[/bold green]")
        if deployment.status == DeploymentStatus.FAILED:
            errors = [
                r
                for r in deployment.results
                if r.status in (ResultStatus.FAILED, ResultStatus.INVALID)
            ]
            raise DJDeploymentFailure(
                project_name=deployment_spec.get("namespace", source_path),
                errors=[r.__dict__ for r in (errors if errors else deployment.results)],
            )
        if file_errors:
            console.print()
            console.rule("[red bold]Errors[/red bold]", style="red", align="left")
            for err in file_errors:
                console.print(f"[red]  {err}[/red]")
            raise DJClientException(
                "Fix file name mismatches before deploying.",
            )

    def get_impact(
        self,
        source_path: str | Path,
        namespace: str | None = None,
        console: Console | None = None,
        verbose: bool = False,
        display: bool = True,
    ) -> dict[str, Any]:
        """
        Get impact analysis for a deployment without deploying.
        Displays a rich summary of what would change and which downstream nodes
        would be affected (unless display=False), then returns the raw response dict.
        """
        console = console or self.console
        deployment_spec, file_errors = self._reconstruct_deployment_spec(source_path)
        base_namespace = deployment_spec.get("namespace") or ""
        source = deployment_spec.get("source", {})
        branch = os.getenv("DJ_DEPLOY_BRANCH") or DeploymentService._detect_git_branch(
            cwd=source_path,
        )
        if namespace:
            deployment_spec["namespace"] = namespace
        elif branch and base_namespace:
            deployment_spec["namespace"] = DeploymentService._derive_namespace(
                base_namespace,
                branch,
            )
        if display:
            print_deployment_header(
                mode="dry run",
                namespace=deployment_spec["namespace"],
                console=console,
                repo=source.get("repository"),
                branch=source.get("branch") or branch,
            )
        with Status("Analyzing impact...", console=console):
            data = self.client.get_deployment_impact(deployment_spec)
        deployment = DeploymentInfo.from_dict(data)
        if display:
            print_results(
                "dry_run",
                deployment,
                console,
                verbose=verbose,
            )
        if file_errors:
            console.print()
            console.rule("[red bold]Errors[/red bold]", style="red", align="left")
            for err in file_errors:
                console.print(f"[red]  {err}[/red]")
        return data

    @staticmethod
    def read_yaml_file(path: str | Path) -> dict[str, Any]:
        with open(path, "r") as f:
            return yaml.safe_load(f)

    @staticmethod
    def _resolve_email_to_github_username(
        email: str,
        github_api_url: str,
        token: str,
    ) -> str | None:
        """
        Look up a GitHub username by email via the GitHub search API.
        Returns the login string, or None if not found.
        """
        url = f"{github_api_url.rstrip('/')}/search/users?q={urllib.parse.quote(email)}+in:email"
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
                items = data.get("items", [])
                return items[0]["login"] if items else None
        except Exception:
            return None

    @staticmethod
    def build_codeowners(
        base_dir: str | Path,
        output: str | Path = ".github/CODEOWNERS",
        github_api_url: str | None = None,
        github_token_env: str = "GITHUB_TOKEN",
    ) -> int:
        """
        Generate a CODEOWNERS file from the owners fields in DJ node YAML files.

        Walks base_dir recursively, reads every *.yaml file (skipping dj.yaml),
        and maps each file path to its owners list.  Files with no owners are
        omitted.  Paths in the output are relative to base_dir and prefixed with
        / so GitHub resolves them from the repo root.

        If github_api_url is provided (and GITHUB_TOKEN / github_token_env is set),
        email addresses in owners fields are resolved to GitHub usernames via the
        search API.  Unresolvable emails are emitted as-is with a warning comment.

        Returns the number of entries written.
        """
        base = Path(base_dir).resolve()
        _token = os.getenv(github_token_env)
        # Only resolve if both API URL and token are available
        lookup: tuple[str, str] | None = (
            (github_api_url, _token) if (github_api_url and _token) else None
        )

        # Cache email → handle lookups to avoid duplicate API calls
        handle_cache: dict[str, str] = {}
        warnings: list[str] = []

        def to_handle(owner: str) -> str:
            if owner.startswith("@"):
                return owner  # already a handle
            if lookup is None or "@" not in owner:
                return owner  # not an email or no lookup configured
            if owner not in handle_cache:
                api_url, token = lookup
                login = DeploymentService._resolve_email_to_github_username(
                    owner,
                    api_url,
                    token,
                )
                if login:
                    handle_cache[owner] = f"@{login}"
                else:
                    handle_cache[owner] = owner
                    warnings.append(owner)
            return handle_cache[owner]

        entries: list[str] = []
        for path in sorted(base.rglob("*.yaml")):
            if path.name == "dj.yaml":
                continue
            try:
                node = DeploymentService.read_yaml_file(path)
            except Exception:  # skip unreadable / non-dict files
                continue
            if not isinstance(node, dict):
                continue
            owners: list[str] = node.get("owners") or []
            if not owners:
                continue
            rel = "/" + str(path.relative_to(base))
            entries.append(f"{rel} {' '.join(to_handle(o) for o in owners)}")

        lines = [
            "# Auto-generated from DJ YAML owners fields.",
            "# Do not edit manually — regenerate with: dj generate-codeowners",
        ]
        if warnings:
            lines.append("#")
            lines.append(
                "# WARNING: could not resolve these emails to GitHub usernames:",
            )
            for w in warnings:
                lines.append(f"#   {w}")
        lines += ["", *entries, ""]
        content = "\n".join(lines)

        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content, encoding="utf-8")
        return len(entries)

    def _collect_nodes_from_dir(
        self,
        base_dir: str | Path,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """
        Recursively collect all node YAML files under base_dir/nodes.
        Returns (nodes, warnings) where warnings are filename mismatch messages.
        """
        nodes = []
        warnings = []
        nodes_dir = Path(base_dir)
        # Skip dirs that may carry stray YAMLs but aren't part of the project
        # (vendored deps, build artifacts, dot-dirs).
        skip_dir_names = {
            "venv",
            ".venv",
            "node_modules",
            "site-packages",
            "__pycache__",
            ".git",
            "build",
            "dist",
            ".tox",
        }
        for path in nodes_dir.rglob("*.yaml"):
            if path.name == "dj.yaml":
                continue
            parents = path.relative_to(nodes_dir).parts[:-1]
            if any(part in skip_dir_names or part.startswith(".") for part in parents):
                continue
            node_dict = DeploymentService.read_yaml_file(path)
            if not isinstance(node_dict, dict):
                # Not a node definition — stray YAML (e.g. a list-typed file
                # from a vendored package); skip rather than crash.
                continue

            # Check filename matches node name
            node_name = node_dict.get("name", "")
            if node_name:
                short_name = node_name.replace("${prefix}", "").split(".")[-1]
                file_stem = path.stem
                if file_stem != short_name:
                    warnings.append(
                        f"  {path.relative_to(nodes_dir)}: node '{node_name}' "
                        f"(expected: {short_name}.yaml)",
                    )

            nodes.append(node_dict)
        return nodes, warnings

    def _read_project_yaml(self, base_dir: str | Path) -> dict[str, Any]:
        """
        Reads project-level dj.yaml
        """
        project_path = Path(base_dir) / "dj.yaml"
        if project_path.exists():
            return DeploymentService.read_yaml_file(project_path)
        return {}

    def _reconstruct_deployment_spec(
        self,
        base_dir: str | Path,
    ) -> tuple[dict[str, Any], list[str]]:
        """
        Reads exported YAML files and reconstructs a DeploymentSpec-compatible dict.
        Returns (deployment_spec, warnings).
        """
        project_metadata = self._read_project_yaml(base_dir)
        nodes, warnings = self._collect_nodes_from_dir(base_dir)

        # Deduplicate nodes by name (keep last occurrence)
        seen_names: dict[str, dict] = {}
        for node in nodes:
            node_name = node.get("name", "")
            if node_name in seen_names:
                warnings.append(
                    f"  Duplicate node '{node_name}', keeping last occurrence",
                )
            seen_names[node_name] = node
        nodes = list(seen_names.values())

        # Accept either `namespace:` (new) or `prefix:` (legacy seed format) in dj.yaml
        deployment_spec = {
            "namespace": project_metadata.get("namespace")
            or project_metadata.get("prefix", ""),
            "nodes": nodes,
            "tags": project_metadata.get("tags", []),
        }

        # Add deployment source if available from env vars
        source = self._build_deployment_source(cwd=base_dir)
        if source:  # pragma: no branch
            deployment_spec["source"] = source

        return deployment_spec, warnings

    @staticmethod
    def _detect_git_branch(cwd: str | Path | None = None) -> str | None:
        """
        Returns the current git branch name, or None if not in a git repo or
        git is not available.

        In detached HEAD state (common in CI), falls back to recovering the
        branch from remote tracking refs (refs/remotes/origin/<branch>).
        """
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
                cwd=cwd,
            )
            branch = result.stdout.strip()
            if branch and branch != "HEAD":
                return branch
            # Detached HEAD (common in CI) — find remote branches whose tip is
            # exactly this commit.
            result2 = subprocess.run(
                ["git", "branch", "-r", "--points-at", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
                cwd=cwd,
            )
            for line in result2.stdout.splitlines():
                ref = line.strip()
                if "/" in ref:  # pragma: no branch
                    _, branch = ref.split("/", 1)
                    if branch != "HEAD":  # pragma: no branch
                        return branch
            return None
        except (subprocess.CalledProcessError, FileNotFoundError):
            return None

    @staticmethod
    def _detect_git_repo(cwd: str | Path | None = None) -> str | None:
        """
        Returns the remote origin URL of the current git repo, or None if not
        in a git repo, no remote is configured, or git is not available.
        """
        try:
            result = subprocess.run(
                ["git", "remote", "get-url", "origin"],
                capture_output=True,
                text=True,
                check=True,
                cwd=cwd,
            )
            return result.stdout.strip() or None
        except (subprocess.CalledProcessError, FileNotFoundError):
            return None

    @staticmethod
    def _branch_to_namespace_suffix(branch: str) -> str:
        """
        Converts a git branch name to a DJ-safe namespace suffix.
        Replaces slashes and other non-alphanumeric characters with underscores
        and strips leading/trailing underscores.

        Examples:
            "main"               -> "main"
            "feature/my-metric"  -> "feature_my_metric"
            "user/fix_thing"     -> "user_fix_thing"
        """
        return re.sub(r"[^a-zA-Z0-9_]+", "_", branch).strip("_")

    @staticmethod
    def _derive_namespace(base_namespace: str, branch: str) -> str:
        """
        Derives the deployment namespace from the base namespace in dj.yaml and
        the current git branch.

        The base namespace is expected to end with a branch segment (e.g. ".main").
        That segment is replaced with the sanitized branch name so that:
            base="project.main", branch="feature/my-metric" -> "project.feature_my_metric"
            base="project.main", branch="main"              -> "project.main"

        If the base namespace has no dot (i.e. it's a single segment), the branch
        suffix is appended directly: base="project", branch="feature" -> "project.feature".
        """
        suffix = DeploymentService._branch_to_namespace_suffix(branch)
        if "." in base_namespace:
            prefix = base_namespace.rsplit(".", 1)[0]
            return f"{prefix}.{suffix}"
        return f"{base_namespace}.{suffix}"

    @staticmethod
    def _detect_git_commit_author(
        cwd: str | Path | None = None,
    ) -> tuple[str | None, str | None]:
        """
        Returns (email, name) of the most recent git commit author, or (None, None).
        """
        try:
            result = subprocess.run(
                ["git", "log", "-1", "--format=%ae|%an"],
                capture_output=True,
                text=True,
                check=True,
                cwd=cwd,
            )
            parts = result.stdout.strip().split("|", 1)
            if len(parts) == 2:
                email, name = parts[0] or None, parts[1] or None
                return email, name
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
        return None, None

    @staticmethod
    def _build_deployment_source(cwd: str | Path | None = None) -> dict[str, Any]:
        """
        Build deployment source from environment variables.

        For Git deployments (when DJ_DEPLOY_REPO is set):
        - DJ_DEPLOY_REPO: Git repository URL (triggers "git" source type)
        - DJ_DEPLOY_BRANCH: Git branch name
        - DJ_DEPLOY_COMMIT: Git commit SHA
        - DJ_DEPLOY_AUTHOR_EMAIL: Commit author email (falls back to git log)
        - DJ_DEPLOY_AUTHOR_NAME: Commit author name (falls back to git log)
        - DJ_DEPLOY_CI_SYSTEM: CI system name (e.g., "github_actions", "jenkins", "rocket")
        - DJ_DEPLOY_CI_RUN_URL: URL to the CI run/build

        For local deployments (when DJ_DEPLOY_REPO is not set):
        - Hostname is auto-filled from the machine
        - DJ_DEPLOY_REASON: Optional reason for the deployment

        Returns:
            GitDeploymentSource dict if repo is specified,
            LocalDeploymentSource dict otherwise (with hostname auto-filled)
        """
        repo = os.getenv("DJ_DEPLOY_REPO") or DeploymentService._detect_git_repo(
            cwd=cwd,
        )
        branch_for_source = os.getenv(
            "DJ_DEPLOY_BRANCH",
        ) or DeploymentService._detect_git_branch(cwd=cwd)
        if repo:
            # Git deployment source
            source: dict[str, Any] = {
                "type": "git",
                "repository": repo,
            }
            branch = branch_for_source
            if branch:
                source["branch"] = branch
            commit = os.getenv("DJ_DEPLOY_COMMIT")
            if commit:
                source["commit_sha"] = commit
            # Commit author: prefer explicit env vars, fall back to git log
            git_email, git_name = DeploymentService._detect_git_commit_author(cwd=cwd)
            author_email = os.getenv("DJ_DEPLOY_AUTHOR_EMAIL") or git_email
            author_name = os.getenv("DJ_DEPLOY_AUTHOR_NAME") or git_name
            if author_email:
                source["commit_author_email"] = author_email
            if author_name:
                source["commit_author_name"] = author_name
            ci_system = os.getenv("DJ_DEPLOY_CI_SYSTEM")
            if ci_system:
                source["ci_system"] = ci_system
            ci_run_url = os.getenv("DJ_DEPLOY_CI_RUN_URL")
            if ci_run_url:
                source["ci_run_url"] = ci_run_url
            return source

        # Always track local deployments with auto-filled hostname
        source = {
            "type": "local",
            "hostname": socket.gethostname(),
        }
        reason = os.getenv("DJ_DEPLOY_REASON")
        if reason:
            source["reason"] = reason
        return source
