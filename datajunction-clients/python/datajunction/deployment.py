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
from datajunction.models import DeploymentInfo, DeploymentResult, DownstreamImpact


# TODO: replace with generated models from OpenAPI spec once client codegen is set up.
# Canonical definitions live in datajunction_server/models/deployment.py.


class TerminalColor:
    """ANSI-safe terminal color names for rich output."""

    RED = "red"
    GREEN = "green"
    YELLOW = "yellow"
    CYAN = "cyan"
    WHITE = "white"
    BRIGHT_BLACK = "bright_black"  # dark grey, replaces non-standard grey50/dark_green


class TextStyle:
    """Rich text style attribute names."""

    BOLD = "bold"
    DIM = "dim"
    ITALIC = "italic"


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


_RESULT_ICONS: dict[str, str] = {
    ResultStatus.SUCCESS: "✓",
    ResultStatus.FAILED: "✗",
    ResultStatus.INVALID: "✗",
    ResultStatus.SKIPPED: "–",
    ResultStatus.NOOP: "·",
}

_RESULT_COLORS: dict[str, str] = {
    ResultStatus.SUCCESS: TerminalColor.GREEN,
    ResultStatus.FAILED: TerminalColor.RED,
    ResultStatus.INVALID: TerminalColor.RED,
    ResultStatus.SKIPPED: TerminalColor.BRIGHT_BLACK,
    ResultStatus.NOOP: "dim",
}

_ERROR_STATUSES = frozenset({ResultStatus.FAILED, ResultStatus.INVALID})


def _strip_summary_lines(message: str) -> str:
    """Remove server-generated success-summary lines from an error message.

    The server bundles lines like "Updated transform (v2.0)" and
    "└─ Updated query, dimension_links" into the same message string as the
    actual error.  Those restate what the result row already shows, so we drop
    any line that starts with "Updated " or with a tree connector ("└─", "├─").
    """
    lines = [
        line
        for line in message.split("\n")
        if not line.startswith("Updated ")
        and not line.startswith("└─")
        and not line.startswith("├─")
    ]
    return "\n".join(line for line in lines if line.strip())


def _short_name(name: str, namespace: str) -> str:
    prefix = namespace + "."
    return name[len(prefix) :] if name.startswith(prefix) else name


def _impact_annotation(
    imp: DownstreamImpact,
    namespace: str,
    current_parent: str = "",
) -> str:
    if imp.node_type == "cube":
        via = [_short_name(c, namespace) for c in imp.caused_by]
        return f"  [dim](via: {', '.join(via)})[/dim]" if via else ""
    others = [_short_name(c, namespace) for c in imp.caused_by if c != current_parent]
    return f"  [dim](also via: {', '.join(others)})[/dim]" if others else ""


def _build_impacts_by_cause(
    impacts: list[DownstreamImpact],
) -> dict[str, list[DownstreamImpact]]:
    index: dict[str, list[DownstreamImpact]] = {}
    for imp in impacts:
        for cause in imp.caused_by:
            index.setdefault(cause, []).append(imp)
    return index


def _collect_transitive_cubes(
    node_name: str,
    impacts_by_cause: dict[str, list[DownstreamImpact]],
) -> list[DownstreamImpact]:
    cubes: dict[str, DownstreamImpact] = {}
    visited: set[str] = set()
    queue = [node_name]
    while queue:
        current = queue.pop()
        for imp in impacts_by_cause.get(current, []):
            if imp.name in visited:
                continue
            visited.add(imp.name)
            if imp.node_type == "cube":
                cubes[imp.name] = imp
            else:
                queue.append(imp.name)
    return list(cubes.values())


def _collect_impact_lines(
    items: list[DownstreamImpact],
    indent: str,
    namespace: str,
    impacts_by_cause: dict[str, list[DownstreamImpact]],
    rendered: set[str],
    current_parent: str,
) -> list[str]:
    """Recursively build downstream impact tree as markup strings."""
    lines: list[str] = []
    for i, impact in enumerate(items):
        branch = indent + ("└" if i == len(items) - 1 else "├")
        continuation = indent + (" " if i == len(items) - 1 else "│")
        name = _short_name(impact.name, namespace)
        annotation = _impact_annotation(impact, namespace, current_parent)
        lines.append(
            f"{branch} [red]⊘[/red] [dim]{impact.node_type}[/dim] [bold red]{name}[/bold red][dim]  → invalid[/dim]{annotation}",
        )
        children = [
            child
            for child in impacts_by_cause.get(impact.name, [])
            if child.name not in rendered and child.node_type != "cube"
        ]
        rendered.update(child.name for child in children)
        if children:
            lines.extend(
                _collect_impact_lines(
                    children,
                    continuation + "  ",
                    namespace,
                    impacts_by_cause,
                    rendered,
                    impact.name,
                ),
            )
    return lines


def _build_downstream_text(
    downstream_impacts: list[DownstreamImpact],
    namespace: str,
) -> "Text | None":
    """Build the downstream impact tree as a single Text renderable (for panel embedding)."""
    # Only surface nodes that will actually be invalidated — predicted_status is authoritative
    # since both dry-run and wet-run go through the same SAVEPOINT-based execution path.
    invalidated = [
        d for d in downstream_impacts if d.predicted_status == ResultStatus.INVALID
    ]
    if not invalidated:
        return None

    impacts_by_cause = _build_impacts_by_cause(invalidated)
    roots = list(
        dict.fromkeys(cause for impact in invalidated for cause in impact.caused_by),
    )

    lines: list[str] = []
    rendered: set[str] = set()
    for root in roots:
        non_cube = [
            impact
            for impact in impacts_by_cause.get(root, [])
            if impact.node_type != "cube" and impact.name not in rendered
        ]
        cubes = [
            impact
            for impact in _collect_transitive_cubes(root, impacts_by_cause)
            if impact.name not in rendered
        ]
        items = non_cube + cubes
        if not items:
            continue
        lines.append(f"  [dim]from[/dim] [bold]{_short_name(root, namespace)}[/bold]")
        rendered.update(impact.name for impact in items)
        lines.extend(
            _collect_impact_lines(
                items,
                "  ",
                namespace,
                impacts_by_cause,
                rendered,
                root,
            ),
        )

    if not lines:
        return None

    from rich.text import Text as RichText

    return RichText.from_markup("\n".join(lines))


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
    def _render_error_bullets(
        message: str,
        color: str = TerminalColor.RED,
        indent: str = "     ",
    ) -> Text:
        """Render a semicolon-separated error message as indented bullet points."""
        bullets = [m.strip() for m in message.split(";") if m.strip()]
        continuation = " " * (len(indent) + 2)  # aligns text after "• "

        def _append_text(body: Text, text: str) -> None:
            """Append text with backtick highlighting, indenting embedded newlines."""
            lines = text.split("\n")
            for li, line in enumerate(lines):
                if li > 0:
                    body.append(f"\n{continuation}")
                for j, part in enumerate(line.split("`")):
                    body.append(
                        part,
                        style=f"{TextStyle.BOLD} {TerminalColor.BRIGHT_BLACK}"
                        if j % 2 == 1
                        else color,
                    )

        body = Text()
        for i, bullet in enumerate(bullets):
            if i > 0:
                body.append("\n")
            body.append(f"{indent}• ", style=f"{TextStyle.BOLD} {color}")
            head, _, tail = bullet.partition(": ")
            _append_text(body, head)
            if tail:
                body.append(f"\n{continuation}")
                _append_text(body, tail)
        return body

    @staticmethod
    def _count_by_status(results: list[DeploymentResult]) -> dict[str, int]:
        """Count results grouped by status."""
        counts: dict[str, int] = {}
        for r in results:
            counts[r.status] = counts.get(r.status, 0) + 1
        return counts

    @staticmethod
    def _group_dim_links(
        results: list[DeploymentResult],
    ) -> tuple[list[DeploymentResult], dict[str, list[DeploymentResult]]]:
        """Split results into regular nodes and dimension link children.

        Dimension link results have names of the form 'parent_node -> dim_node'.
        They are grouped by parent so they can be rendered nested under their parent row.
        """
        regular: list[DeploymentResult] = []
        dim_link_children: dict[str, list[DeploymentResult]] = {}
        for r in results:
            if " -> " in r.name:
                parent = r.name.split(" -> ")[0]
                dim_link_children.setdefault(parent, []).append(r)
            else:
                regular.append(r)
        return regular, dim_link_children

    @staticmethod
    def _render_dim_link_children(
        node_name: str,
        dim_link_children: dict[str, list[DeploymentResult]],
        verbose: bool,
    ) -> Text:
        """Return a Text block of dimension link children for a given parent node."""
        visible = [
            c
            for c in dim_link_children.get(node_name, [])
            if verbose or c.operation != "noop"
        ]
        out = Text()
        if not visible:
            return out
        out.append("\n     ", style="")
        out.append("dimension links", style=f"{TextStyle.DIM} {TextStyle.ITALIC}")
        for ci, child in enumerate(visible):
            connector = "└" if ci == len(visible) - 1 else "├"
            color = _RESULT_COLORS.get(child.status, TerminalColor.WHITE)
            icon = _RESULT_ICONS.get(child.status, " ")
            child_is_error = child.status in _ERROR_STATUSES
            dim_name = child.name.split(" -> ", 1)[-1]
            weight = f"{TextStyle.BOLD} " if child_is_error else ""
            out.append(f"\n     {connector}─ ")
            out.append(f"{icon}  ", style=f"{weight}{color}")
            out.append(f"{child.operation:<8}  ", style=TextStyle.DIM)
            out.append(f"→ {dim_name}", style=f"{weight}{color}")
            if child.changed_fields:
                out.append(
                    f"  [{', '.join(child.changed_fields)}]",
                    style=TextStyle.DIM,
                )
            if child_is_error and child.message:
                out.append("\n")
                out.append_text(
                    DeploymentService._render_error_bullets(
                        child.message,
                        indent="        ",
                    ),
                )
        return out

    @staticmethod
    def _render_result_rows(
        regular_results: list[DeploymentResult],
        dim_link_children: dict[str, list[DeploymentResult]],
        verbose: bool,
    ) -> Text:
        """Build the Text block showing each result row and its dim link children."""
        rows = Text()
        for result in regular_results:
            color = _RESULT_COLORS.get(result.status, TerminalColor.WHITE)
            icon = _RESULT_ICONS.get(result.status, " ")
            if not verbose and result.operation == "noop":
                continue
            if rows:
                rows.append("\n")
            is_error = result.status in _ERROR_STATUSES
            weight = "bold " if is_error else ""
            rows.append(f"  {icon}  ", style=f"{weight}{color}")
            rows.append(f"{result.operation:<8}  ", style=TextStyle.DIM)
            rows.append(result.name, style=f"{weight}{color}")
            if result.changed_fields:
                rows.append(
                    f"  [{', '.join(result.changed_fields)}]",
                    style=TextStyle.DIM,
                )
            if is_error and result.message:
                error_msg = _strip_summary_lines(result.message)
                if error_msg:
                    rows.append("\n")
                    rows.append_text(DeploymentService._render_error_bullets(error_msg))
            dim_text = DeploymentService._render_dim_link_children(
                result.name,
                dim_link_children,
                verbose,
            )
            if dim_text:
                rows.append_text(dim_text)
        return rows

    @staticmethod
    def _build_summary(
        counts: dict[str, int],
        error_count: int,
        downstream_impacts: list[DownstreamImpact],
        verbose: bool,
    ) -> list[Text]:
        """Build the summary line segments shown at the bottom of the panel."""
        invalidated_downstream = [
            d for d in downstream_impacts if d.predicted_status == ResultStatus.INVALID
        ]
        parts: list[Text] = []
        if counts.get(ResultStatus.SUCCESS):
            parts.append(
                Text(
                    f"✓ {counts[ResultStatus.SUCCESS]} succeeded",
                    style=TerminalColor.GREEN,
                ),
            )
        if counts.get(ResultStatus.SKIPPED):
            parts.append(
                Text(
                    f"– {counts[ResultStatus.SKIPPED]} skipped",
                    style=TerminalColor.BRIGHT_BLACK,
                ),
            )
        if not verbose and counts.get(ResultStatus.NOOP):
            parts.append(Text(f"{counts[ResultStatus.NOOP]} noop", style=TextStyle.DIM))
        downstream_count = len(invalidated_downstream)
        total_invalid = error_count + downstream_count
        if total_invalid:
            t = Text(
                f"✗ {total_invalid} invalid",
                style=f"{TextStyle.BOLD} {TerminalColor.RED}",
            )
            if error_count and downstream_count:
                t.append(
                    f"  ({error_count} direct, {downstream_count} downstream)",
                    style=f"{TextStyle.DIM} {TerminalColor.RED}",
                )
            elif downstream_count:
                t.append("  (downstream)", style=f"{TextStyle.DIM} {TerminalColor.RED}")
            parts.append(t)
        return parts

    @staticmethod
    def print_results(
        deployment_uuid: str,
        deployment: DeploymentInfo,
        console: Console,
        verbose: bool = False,
    ) -> None:
        """Render deployment results (and optional downstream impacts) in a single panel."""
        counts = DeploymentService._count_by_status(deployment.results)
        error_count = counts.get(ResultStatus.FAILED, 0) + counts.get(
            ResultStatus.INVALID,
            0,
        )
        border_color = (
            TerminalColor.RED
            if deployment.status == DeploymentStatus.FAILED or error_count > 0
            else TerminalColor.GREEN
        )

        regular, dim_link_children = DeploymentService._group_dim_links(
            deployment.results,
        )
        rows = DeploymentService._render_result_rows(
            regular,
            dim_link_children,
            verbose,
        )
        summary_parts = DeploymentService._build_summary(
            counts,
            error_count,
            deployment.downstream_impacts,
            verbose,
        )
        summary = Text("  ").join(summary_parts) if summary_parts else Text()
        impacts_text = _build_downstream_text(
            deployment.downstream_impacts,
            deployment.namespace,
        )

        panel_parts: list = [rows]
        if impacts_text is not None:
            panel_parts += [
                Rule(style=TextStyle.DIM),
                Text("  Downstream Impacts", style=TextStyle.BOLD),
                impacts_text,
            ]
        if summary_parts:
            panel_parts += [Rule(style=TextStyle.DIM), summary]
        content = Group(*panel_parts) if len(panel_parts) > 1 else rows

        console.print()
        console.print(
            Panel(
                content,
                title=f"[dim]{deployment_uuid}  ·  {deployment.namespace}[/dim]",
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

        deployment = DeploymentInfo.from_dict(deployment_data)
        DeploymentService.print_results(
            deployment_uuid,
            deployment,
            console,
            verbose=verbose,
        )
        if deployment.status == DeploymentStatus.SUCCESS:
            console.print("\nDeployment finished: [bold green]SUCCESS[/bold green]")
        if deployment.status == DeploymentStatus.FAILED:
            errors = [r for r in deployment.results if r.status == ResultStatus.FAILED]
            raise DJDeploymentFailure(
                project_name=deployment_spec.get("namespace", source_path),
                errors=[r.__dict__ for r in (errors if errors else deployment.results)],
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
        deployment_spec = self._reconstruct_deployment_spec(source_path)
        deployment_spec["namespace"] = namespace or deployment_spec.get("namespace")
        data = self.client.get_deployment_impact(deployment_spec)
        deployment = DeploymentInfo.from_dict(data)
        if display:
            DeploymentService.print_results(
                "dry_run",
                deployment,
                console,
                verbose=verbose,
            )
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
