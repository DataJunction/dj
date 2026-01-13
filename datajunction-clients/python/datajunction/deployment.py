import os
import socket
from pathlib import Path
import time
from typing import Any, Union

import yaml
from rich import box
from rich.console import Console
from rich.live import Live
from rich.table import Table

from datajunction import DJBuilder
from datajunction.exceptions import (
    DJClientException,
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
    def build_table(deployment_uuid: str, data: dict) -> Table:
        """Return a fresh Table with current deployment results."""
        table = Table(
            title=f"Deployment [bold green]{deployment_uuid}[/ bold green]\nNamespace [bold green]{data['namespace']}[/ bold green]",
            box=box.SIMPLE_HEAVY,
            expand=True,
        )
        table.add_column("Type", style="cyan", no_wrap=True)
        table.add_column("Name", style="magenta")
        table.add_column("Operation", style="yellow")
        table.add_column("Status", style="green")
        table.add_column("Message", style="white")

        color_mapping = {
            "success": "bold green",
            "failed": "bold red",
            "pending": "yellow",
            "skipped": "bold gray",
        }

        for result in data.get("results", []):
            color = color_mapping.get(result.get("status"), "white")
            table.add_row(
                str(result.get("deploy_type", "")),
                str(result.get("name", "")),
                str(result.get("operation", "")),
                f"[{color}]{result.get('status', '')}[/{color}]",
                f"[gray]{result.get('message', '')}[/gray]",
            )
        return table

    def push(
        self,
        source_path: str | Path,
        namespace: str | None = None,
        console: Console = Console(),
    ):
        """
        Push a local project to a namespace.
        """
        console.print(f"[bold]Pushing project from:[/bold] {source_path}")

        deployment_spec = self._reconstruct_deployment_spec(source_path)
        deployment_spec["namespace"] = namespace or deployment_spec.get("namespace")
        deployment_data = self.client.deploy(deployment_spec)
        deployment_uuid = deployment_data["uuid"]

        # console.print(f"[bold]Deployment initiated:[/bold] UUID {deployment_uuid}\n")

        # Max wait time for deployment to finish
        timeout = time.time() + 300  # 5 minutes

        with Live(
            DeploymentService.build_table(deployment_uuid, deployment_data),
            console=console,
            screen=False,
            refresh_per_second=1,
        ) as live:
            while deployment_data.get("status") not in ("failed", "success"):
                time.sleep(1)
                deployment_data = self.client.check_deployment(deployment_uuid)
                live.update(
                    DeploymentService.build_table(deployment_uuid, deployment_data),
                )

                if time.time() > timeout:
                    raise DJClientException("Deployment timed out after 5 minutes")

            live.update(DeploymentService.build_table(deployment_uuid, deployment_data))
            color = "green" if deployment_data.get("status") == "success" else "red"
            console.print(
                f"\nDeployment finished: [bold {color}]{deployment_data.get('status').upper()}[/bold {color}]",
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
                print(
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
        source = self._build_deployment_source()
        if source:  # pragma: no branch
            deployment_spec["source"] = source

        return deployment_spec

    @staticmethod
    def _build_deployment_source() -> dict[str, Any]:
        """
        Build deployment source from environment variables.

        For Git deployments (when DJ_DEPLOY_REPO is set):
        - DJ_DEPLOY_REPO: Git repository URL (triggers "git" source type)
        - DJ_DEPLOY_BRANCH: Git branch name
        - DJ_DEPLOY_COMMIT: Git commit SHA
        - DJ_DEPLOY_CI_SYSTEM: CI system name (e.g., "github_actions", "jenkins", "rocket")
        - DJ_DEPLOY_CI_RUN_URL: URL to the CI run/build

        For local deployments (when DJ_DEPLOY_REPO is not set):
        - Hostname is auto-filled from the machine
        - DJ_DEPLOY_REASON: Optional reason for the deployment

        Returns:
            GitDeploymentSource dict if repo is specified,
            LocalDeploymentSource dict otherwise (with hostname auto-filled)
        """
        repo = os.getenv("DJ_DEPLOY_REPO")

        if repo:
            # Git deployment source
            source: dict[str, Any] = {
                "type": "git",
                "repository": repo,
            }
            branch = os.getenv("DJ_DEPLOY_BRANCH")
            if branch:
                source["branch"] = branch
            commit = os.getenv("DJ_DEPLOY_COMMIT")
            if commit:
                source["commit_sha"] = commit
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
