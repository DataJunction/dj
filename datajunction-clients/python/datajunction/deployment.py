import os
import re
import socket
import subprocess
from pathlib import Path
import time
from typing import Any, Union

import yaml
from rich.console import Console, Group
from rich.panel import Panel
from rich.rule import Rule
from rich.status import Status
from rich.text import Text

from datajunction import DJBuilder
from datajunction.exceptions import (
    DJClientException,
    DJDeploymentFailure,
)


class DeploymentService:
    """
    High-level deployment client for exporting and importing DJ namespaces.
    Intended for CLI scripts but reusable in Python code.
    """

    def __init__(self, client: DJBuilder, console: Console | None = None) -> None:
        self.client = client
        self.console = console or Console()

    @staticmethod
    def clean_dict(d: dict) -> dict:
        """
        Recursively remove None, empty list, and empty dict values.
        """
        result = {}
        for k, v in d.items():
            if v is None:
                continue
            if isinstance(v, (list, dict)) and not v:
                continue
            if isinstance(v, dict):
                nested = DeploymentService.clean_dict(v)
                if nested:  # only include if not empty after cleaning
                    result[k] = nested
            else:
                result[k] = v  # type: ignore
        return result

    @staticmethod
    def filter_node_for_export(node: dict) -> dict:
        """
        Filter a node dict for export to YAML.

        For columns:
        - Cubes: columns are always excluded (they're inferred from metrics/dimensions)
        - Other nodes: only includes columns with meaningful customizations
          (display_name different from name, attributes, description, or partition).
          Column types are excluded - let DJ infer them from the query/source.
        """
        result = DeploymentService.clean_dict(node)

        # Cubes should never have columns in export - they're inferred from metrics/dimensions
        if result.get("node_type") == "cube":
            result.pop("columns", None)
        # For other nodes, filter columns to only include meaningful customizations
        elif "columns" in result and result["columns"]:
            filtered_columns = []
            for col in result["columns"]:
                # Check for meaningful customizations
                has_custom_display = col.get("display_name") and col.get(
                    "display_name",
                ) != col.get("name")
                has_attributes = bool(col.get("attributes"))
                has_description = bool(col.get("description"))
                has_partition = bool(col.get("partition"))

                if (
                    has_custom_display
                    or has_attributes
                    or has_description
                    or has_partition
                ):
                    # Include column but exclude type (let DJ infer)
                    filtered_col = {
                        k: v
                        for k, v in col.items()
                        if k != "type" and v  # Exclude type and empty values
                    }
                    filtered_columns.append(filtered_col)

            if filtered_columns:
                result["columns"] = filtered_columns
            else:
                # Remove columns entirely if none have customizations
                del result["columns"]

        return result

    def pull(
        self,
        namespace: str,
        target_path: Union[str, Path],
        ignore_existing_files: bool = False,
    ):
        """
        Export a namespace to a local project.
        """
        path = Path(target_path)
        if any(path.iterdir()) and not ignore_existing_files:
            raise DJClientException("The target path must be empty")
        deployment_spec = self.client._export_namespace_spec(namespace)

        namespace = deployment_spec["namespace"]
        nodes: list[dict[str, Any]] = deployment_spec.get("nodes", [])
        base_path = Path(target_path)
        base_path.mkdir(parents=True, exist_ok=True)

        # Create a YAML for each node in the appropriate namespace folder
        for node in nodes:
            node_name = node["name"]
            # Namespace folder is everything except the last part of the node
            node_parts = node_name.replace("${prefix}", "").split(".")
            node_namespace_path = base_path.joinpath(*node_parts[:-1])
            node_namespace_path.mkdir(parents=True, exist_ok=True)

            # File name is the last part of the node
            file_name = node_parts[-1] + ".yaml"
            file_path = node_namespace_path / file_name

            # Write YAML for this node (filter columns for cleaner output)
            with open(file_path, "w") as yaml_file:
                yaml.dump(
                    DeploymentService.filter_node_for_export(node),
                    yaml_file,
                    sort_keys=False,
                )

        # Write top-level dj.yaml with full deployment info
        dj_yaml_path = base_path / "dj.yaml"
        with open(dj_yaml_path, "w") as yaml_file:
            project_spec = {
                "name": f"Project {namespace} (Autogenerated)",
                "description": f"This is an autogenerated project for namespace {namespace}",
                "namespace": namespace,
            }
            yaml.safe_dump(project_spec, yaml_file, sort_keys=False)

    @staticmethod
    @staticmethod
    def _render_error_bullets(message: str) -> Text:
        """Render a semicolon-separated error message as indented bullet points."""
        bullets = [m.strip() for m in message.split(";") if m.strip()]
        body = Text()
        for i, bullet in enumerate(bullets):
            if i > 0:
                body.append("\n")
            body.append("  • ", style="bold red")
            head, _, tail = bullet.partition(": ")
            for j, part in enumerate(head.split("`")):
                body.append(part, style="bold dark_green" if j % 2 == 1 else "")
            if tail:
                body.append("\n    ")
                for j, part in enumerate(tail.split("`")):
                    body.append(part, style="bold dark_green" if j % 2 == 1 else "")
        return body

    @staticmethod
    def print_results(
        deployment_uuid: str,
        data: dict,
        console: Console,
        verbose: bool = False,
    ) -> None:
        """Render all deployment results inside a single panel."""
        namespace = data.get("namespace", "")
        overall_status = data.get("status", "")
        border_color = "green" if overall_status == "success" else "red"

        icon_map = {"success": "✓", "failed": "✗", "skipped": "–", "pending": "…"}
        color_map = {
            "success": "green",
            "failed": "red",
            "skipped": "grey50",
            "pending": "yellow",
        }

        results = data.get("results", [])
        counts: dict[str, int] = {}
        for r in results:
            counts[r.get("status", "")] = counts.get(r.get("status", ""), 0) + 1

        rows = Text()
        for result in results:
            status = result.get("status", "")
            color = color_map.get(status, "white")
            icon = icon_map.get(status, " ")
            if not verbose and result.get("operation") == "noop":
                continue
            if rows:
                rows.append("\n")
            rows.append(
                f"  {icon}  ",
                style=f"{'bold ' if status == 'failed' else ''}{color}",
            )
            rows.append(f"{result.get('operation', ''):<8}  ", style="dim")
            rows.append(
                result.get("name", ""),
                style=f"{'bold ' if status == 'failed' else ''}{color}",
            )
            if status == "failed":
                rows.append("\n")
                rows.append_text(
                    DeploymentService._render_error_bullets(result.get("message", "")),
                )

        summary_parts = []
        if counts.get("success"):
            summary_parts.append(
                Text(f"✓ {counts['success']} succeeded", style="green"),
            )
        if counts.get("skipped"):
            summary_parts.append(Text(f"– {counts['skipped']} skipped", style="grey50"))
        if not verbose and counts.get("noop"):
            summary_parts.append(Text(f"{counts['noop']} noop", style="dim"))
        if counts.get("failed"):
            summary_parts.append(Text(f"✗ {counts['failed']} failed", style="bold red"))

        summary = Text("  ").join(summary_parts) if summary_parts else Text()

        content = Group(rows, Rule(style="dim"), summary) if summary_parts else rows

        console.print()
        console.print(
            Panel(
                content,
                title=f"[dim]{deployment_uuid}  ·  {namespace}[/dim]",
                title_align="left",
                border_style=border_color,
                padding=(0, 1),
            ),
        )

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
        console.print(f"[bold]Pushing project from:[/bold] {source_path}")

        deployment_spec = self._reconstruct_deployment_spec(source_path)

        base_namespace = deployment_spec.get("namespace") or ""
        branch = DeploymentService._detect_git_branch(cwd=source_path)
        source = deployment_spec.get("source", {})
        if source.get("repository"):
            console.print(
                f"[dim]  repo:    [bold]{source['repository']}[/bold]\n"
                f"  branch:  [bold]{source.get('branch', 'unknown')}[/bold][/dim]",
            )
        if namespace:
            deployment_spec["namespace"] = namespace
        elif branch and base_namespace:
            deployment_spec["namespace"] = DeploymentService._derive_namespace(
                base_namespace,
                branch,
            )
            console.print(
                f"[dim]Detected branch [bold]{branch}[/bold] → "
                f"deploying to [bold]{deployment_spec['namespace']}[/bold][/dim]",
            )

        if branch:
            # Only set parent_namespace when we derived it from dj.yaml (not when
            # --namespace was passed explicitly, since we can't reliably infer the parent)
            parent_namespace = base_namespace if not namespace else None
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

        DeploymentService.print_results(
            deployment_uuid,
            deployment_data,
            console,
            verbose=verbose,
        )
        if deployment_data.get("status") == "success":
            console.print("\nDeployment finished: [bold green]SUCCESS[/bold green]")
        if deployment_data.get("status") == "failed":
            results = deployment_data.get("results", [])
            errors = [r for r in results if r.get("status") == "failed"]
            raise DJDeploymentFailure(
                project_name=deployment_spec.get("namespace", source_path),
                errors=errors if errors else results,
            )

    def get_impact(
        self,
        source_path: str | Path,
        namespace: str | None = None,
    ) -> dict[str, Any]:
        """
        Get impact analysis for a deployment without deploying.
        Returns the impact analysis response from the server.
        """
        deployment_spec = self._reconstruct_deployment_spec(source_path)
        deployment_spec["namespace"] = namespace or deployment_spec.get("namespace")
        return self.client.get_deployment_impact(deployment_spec)

    @staticmethod
    def read_yaml_file(path: str | Path) -> dict[str, Any]:
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def _collect_nodes_from_dir(self, base_dir: str | Path) -> list[dict[str, Any]]:
        """
        Recursively collect all node YAML files under base_dir/nodes.
        """
        nodes = []
        nodes_dir = Path(base_dir)
        for path in nodes_dir.rglob("*.yaml"):
            if path.name == "dj.yaml":
                continue
            node_dict = DeploymentService.read_yaml_file(path)
            nodes.append(node_dict)
        return nodes

    def _read_project_yaml(self, base_dir: str | Path) -> dict[str, Any]:
        """
        Reads project-level dj.yaml
        """
        project_path = Path(base_dir) / "dj.yaml"
        if project_path.exists():
            return DeploymentService.read_yaml_file(project_path)
        return {}

    def _reconstruct_deployment_spec(self, base_dir: str | Path) -> dict[str, Any]:
        """
        Reads exported YAML files and reconstructs a DeploymentSpec-compatible dict.
        """
        project_metadata = self._read_project_yaml(base_dir)
        nodes = self._collect_nodes_from_dir(base_dir)

        # Deduplicate nodes by name (keep last occurrence)
        seen_names: dict[str, dict] = {}
        for node in nodes:
            node_name = node.get("name", "")
            if node_name in seen_names:
                print(  # pragma: no cover
                    f"WARNING: Duplicate node '{node_name}' found, keeping last occurrence",
                )
            seen_names[node_name] = node
        nodes = list(seen_names.values())

        deployment_spec = {
            "namespace": project_metadata.get("namespace", ""),  # fallback to empty
            "nodes": nodes,
            "tags": project_metadata.get("tags", []),
        }

        # Add deployment source if available from env vars
        source = self._build_deployment_source(cwd=base_dir)
        if source:  # pragma: no branch
            deployment_spec["source"] = source

        return deployment_spec

    @staticmethod
    def _detect_git_branch(cwd: str | Path | None = None) -> str | None:
        """
        Returns the current git branch name, or None if not in a git repo or
        git is not available.
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
            return branch if branch and branch != "HEAD" else None
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
