"""DataJunction command-line tool"""

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Optional

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from datajunction import DJBuilder, Project
from datajunction.deployment import DeploymentService
from datajunction.exceptions import DJClientException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def display_impact_analysis(impact: dict, console: Console | None = None) -> None:
    """
    Display deployment impact analysis with rich formatting.

    Args:
        impact: The impact analysis response from the server
        console: Optional Rich console for output
    """
    console = console or Console()
    namespace = impact.get("namespace", "unknown")

    # Header
    console.print()
    console.print(
        f"[bold blue]📊 Impact Analysis for namespace:[/bold blue] [bold green]{namespace}[/bold green]",
    )
    console.print("━" * 60)
    console.print()

    # Direct Changes Table
    changes = impact.get("changes", [])
    if changes:
        changes_table = Table(
            title="[bold]📝 Direct Changes[/bold]",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
        )
        changes_table.add_column("Operation", style="bold", width=10)
        changes_table.add_column("Node", style="magenta")
        changes_table.add_column("Type", style="dim", width=12)
        changes_table.add_column("Changed Fields", style="white")

        operation_styles = {
            "create": ("🟢 Create", "green"),
            "update": ("🟡 Update", "yellow"),
            "delete": ("🔴 Delete", "red"),
            "noop": ("⚪ Skip", "dim"),
        }

        for change in changes:
            op = change.get("operation", "unknown")
            op_display, op_color = operation_styles.get(op, (op.upper(), "white"))
            changed_fields = ", ".join(change.get("changed_fields", [])) or "-"
            changes_table.add_row(
                f"[{op_color}]{op_display}[/{op_color}]",
                change.get("name", ""),
                change.get("node_type", ""),
                changed_fields,
            )

        console.print(changes_table)
        console.print()

    # Summary for direct changes
    create_count = impact.get("create_count", 0)
    update_count = impact.get("update_count", 0)
    delete_count = impact.get("delete_count", 0)
    skip_count = impact.get("skip_count", 0)

    summary_parts = []
    if create_count:
        summary_parts.append(
            f"[green]{create_count} create{'s' if create_count != 1 else ''}[/green]",
        )
    if update_count:
        summary_parts.append(
            f"[yellow]{update_count} update{'s' if update_count != 1 else ''}[/yellow]",
        )
    if delete_count:
        summary_parts.append(
            f"[red]{delete_count} delete{'s' if delete_count != 1 else ''}[/red]",
        )
    if skip_count:
        summary_parts.append(f"[dim]{skip_count} skipped[/dim]")

    if summary_parts:
        console.print(f"[bold]Summary:[/bold] {', '.join(summary_parts)}")
        console.print()

    # Column Changes (if any)
    column_changes_found = []
    for change in changes:
        for col_change in change.get("column_changes", []):
            column_changes_found.append(
                {
                    "node": change.get("name"),
                    **col_change,
                },
            )

    if column_changes_found:
        col_table = Table(
            title="[bold]⚡ Column Changes[/bold]",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
        )
        col_table.add_column("Node", style="magenta")
        col_table.add_column("Change", style="bold", width=10)
        col_table.add_column("Details", style="white")

        change_type_styles = {
            "added": ("🟢 Added", "green"),
            "removed": ("🔴 Removed", "red"),
            "type_changed": ("🟡 Type Changed", "yellow"),
        }

        for col in column_changes_found:
            change_type = col.get("change_type", "unknown")
            display, color = change_type_styles.get(
                change_type,
                (change_type.upper(), "white"),
            )

            if change_type == "type_changed":
                details = f"'{col.get('column')}': {col.get('old_type')} → {col.get('new_type')}"
            elif change_type == "removed":
                details = f"Column '{col.get('column')}' removed"
            else:
                details = f"Column '{col.get('column')}' added"

            col_table.add_row(
                col.get("node", ""),
                f"[{color}]{display}[/{color}]",
                details,
            )

        console.print(col_table)
        console.print()

    # Downstream Impact
    downstream_impacts = impact.get("downstream_impacts", [])
    if downstream_impacts:
        impact_table = Table(
            title="[bold]🔗 Downstream Impact[/bold]",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold cyan",
        )
        impact_table.add_column("Node", style="magenta")
        impact_table.add_column("Impact", style="bold", width=18)
        impact_table.add_column("Reason", style="white")

        impact_styles = {
            "will_invalidate": ("❌ Will Invalidate", "bold red"),
            "may_affect": ("⚠️  May Affect", "yellow"),
            "unchanged": ("✓ Unchanged", "dim"),
        }

        for downstream in downstream_impacts:
            impact_type = downstream.get("impact_type", "unknown")
            display, style = impact_styles.get(
                impact_type,
                (impact_type.upper(), "white"),
            )
            impact_table.add_row(
                downstream.get("name", ""),
                f"[{style}]{display}[/{style}]",
                downstream.get("impact_reason", ""),
            )

        console.print(impact_table)
        console.print()

        # Downstream summary
        will_invalidate = impact.get("will_invalidate_count", 0)
        may_affect = impact.get("may_affect_count", 0)

        impact_summary = []
        if will_invalidate:
            impact_summary.append(f"[red]{will_invalidate} will invalidate[/red]")
        if may_affect:
            impact_summary.append(f"[yellow]{may_affect} may be affected[/yellow]")

        if impact_summary:
            console.print(
                f"[bold]Downstream Summary:[/bold] {', '.join(impact_summary)}",
            )
            console.print()
    else:
        console.print("[green]✅ No downstream impact detected.[/green]")
        console.print()

    # Warnings
    warnings = impact.get("warnings", [])
    if warnings:
        console.print("[bold red]⚠️  Warnings:[/bold red]")
        for warning in warnings:
            console.print(f"  [yellow]• {warning}[/yellow]")
        console.print()
    else:
        console.print("[green]✅ No warnings.[/green]")
        console.print()

    # Final verdict
    has_issues = (
        impact.get("will_invalidate_count", 0) > 0
        or len(warnings) > 0
        or delete_count > 0
    )

    if has_issues:
        console.print(
            "[yellow bold]⚠️  Review the warnings and downstream impacts before deploying.[/yellow bold]",
        )
    else:
        console.print("[green bold]✅ Ready to deploy![/green bold]")

    console.print()


class DJCLI:
    """DJ command-line tool"""

    def __init__(self, builder_client: DJBuilder | None = None):
        """
        Initialize the CLI with a builder client.

        If DJ_URL environment variable is set, it will be used as the server URL.
        """
        if builder_client is None:
            # Read DJ_URL from environment, default to localhost:8000
            dj_url = os.environ.get("DJ_URL", "http://localhost:8000")
            builder_client = DJBuilder(uri=dj_url)
        self.builder_client = builder_client
        self.deployment_service = DeploymentService(client=self.builder_client)

    def push(self, directory: str, namespace: str | None = None):
        """
        Alias for deploy without dryrun.
        """
        self.deployment_service.push(directory, namespace=namespace)

    def dryrun(
        self,
        directory: str,
        namespace: str | None = None,
        format: str = "text",
    ):
        """
        Perform a dry run of deployment, showing impact analysis.
        """
        console = Console()

        try:
            impact = self.deployment_service.get_impact(directory, namespace=namespace)

            if format == "json":
                print(json.dumps(impact, indent=2))
            else:
                console.print(f"[bold]Analyzing deployment from:[/bold] {directory}")
                console.print()
                display_impact_analysis(impact, console=console)
        except DJClientException as exc:
            error_data = exc.args[0] if exc.args else str(exc)
            message = (
                error_data.get("message", str(exc))
                if isinstance(error_data, dict)
                else str(exc)
            )
            if format == "json":
                print(json.dumps({"error": message}, indent=2))
            else:
                console.print(f"[red bold]ERROR:[/red bold] {message}")

    def pull(self, namespace: str, directory: str):
        """
        Export nodes from a specific namespace.
        """
        print(f"Exporting namespace {namespace} to {directory}...")
        self.deployment_service.pull(
            namespace=namespace,
            target_path=directory,
        )
        print(f"Finished exporting namespace {namespace} to {directory}.")

    def delete_node(self, node_name: str, hard: bool = False):
        """
        Delete a node.
        """
        delete_type = "deleting" if hard else "deactivating"
        print(f"{delete_type.capitalize()} node {node_name}...")
        self.builder_client.delete_node(node_name, hard=hard)
        print(f"Finished {delete_type} node {node_name}.")

    def delete_namespace(
        self,
        namespace: str,
        cascade: bool = False,
        hard: bool = False,
    ):
        """
        Delete a namespace.
        """
        delete_type = "deleting" if hard else "deactivating"
        cascade_msg = " with cascade" if cascade else ""
        print(f"{delete_type.capitalize()} namespace {namespace}{cascade_msg}...")
        self.builder_client.delete_namespace(namespace, cascade=cascade, hard=hard)
        print(f"Finished {delete_type} namespace {namespace}.")

    def describe(self, node_name: str, format: str = "text"):
        """
        Describe a node with detailed information.
        """
        try:
            node = self.builder_client.node(node_name)

            if format == "json":
                # Output as JSON
                node_dict = {
                    "name": node.name,
                    "type": node.type,
                    "description": node.description,
                    "display_name": getattr(node, "display_name", None),
                    "query": getattr(node, "query", None),
                    "columns": [
                        {"name": col.name, "type": col.type} for col in node.columns
                    ]
                    if hasattr(node, "columns") and node.columns
                    else [],
                    "status": node.status,
                    "mode": node.mode,
                    "version": getattr(node, "version", None),
                }
                print(json.dumps(node_dict, indent=2))
            else:
                # Output as formatted text
                print(f"\n{'=' * 60}")
                print(f"Node: {node.name}")
                print(f"{'=' * 60}")
                print(f"Type:          {node.type}")
                print(f"Description:   {node.description or 'N/A'}")
                print(f"Status:        {node.status}")
                print(f"Mode:          {node.mode}")
                print(f"Display Name:  {node.display_name or 'N/A'}")
                print(f"Version:       {node.version}")
                if node.primary_key:
                    print(f"Primary Key:   {node.primary_key}")

                if node.query:
                    print(f"\nQuery:\n{'-' * 60}")
                    print(node.query)

                if node.columns and node.type not in ["metric", "cube"]:
                    print(f"\nColumns:\n{'-' * 60}")
                    for col in node.columns:
                        print(f"  {col.name:<30} {col.type}")
                if node.type == "cube":
                    print(f"\nDimensions:\n{'-' * 60}")
                    for dim in node.dimensions:
                        print(f"  {dim}")
                    print(f"\nMetrics:\n{'-' * 60}")
                    for metric in node.metrics:
                        print(f"  {metric}")
                    if node.filters:  # pragma: no cover
                        print(f"\nFilters:\n{'-' * 60}")
                        for filter_ in node.filters:
                            print(f"  {filter_}")
                print(f"{'=' * 60}\n")
        except DJClientException as exc:
            error_data = exc.args[0] if exc.args else str(exc)
            message = (
                error_data.get("message", str(exc))
                if isinstance(error_data, dict)
                else str(exc)
            )
            print(f"ERROR: {message}")

    def list_objects(
        self,
        object_type: str,
        namespace: Optional[str] = None,
        format: str = "text",
    ):
        """
        List objects (namespaces, metrics, dimensions, etc.)
        """
        try:
            results = []
            if object_type == "namespaces":
                results = self.builder_client.list_namespaces(prefix=namespace)
            elif object_type == "metrics":
                results = self.builder_client.list_metrics(namespace=namespace)
            elif object_type == "dimensions":
                results = self.builder_client.list_dimensions(namespace=namespace)
            elif object_type == "cubes":
                results = self.builder_client.list_cubes(namespace=namespace)
            elif object_type == "sources":
                results = self.builder_client.list_sources(namespace=namespace)
            elif object_type == "transforms":
                results = self.builder_client.list_transforms(namespace=namespace)
            elif object_type == "nodes":  # pragma: no cover
                results = self.builder_client.list_nodes(namespace=namespace)

            if format == "json":
                print(json.dumps(results, indent=2))
            else:
                if results:
                    print(f"\n{object_type.capitalize()}:")
                    print("-" * 60)
                    for item in results:
                        print(f"  {item}")
                    print(f"\nTotal: {len(results)}\n")
                else:
                    print(
                        f"No {object_type} found"
                        + (f" in `{namespace}`" if namespace else ""),
                    )
        except DJClientException as exc:  # pragma: no cover
            error_data = exc.args[0] if exc.args else str(exc)
            message = (
                error_data.get("message", str(exc))
                if isinstance(error_data, dict)
                else str(exc)
            )
            print(f"ERROR: {message}")

    def get_sql(
        self,
        node_name: str,
        dimensions: Optional[list[str]] = None,
        filters: Optional[list[str]] = None,
        engine_name: Optional[str] = None,
        engine_version: Optional[str] = None,
    ):
        """
        Generate SQL for a node.
        """
        try:
            sql = self.builder_client.node_sql(
                node_name=node_name,
                dimensions=dimensions,
                filters=filters,
                engine_name=engine_name,
                engine_version=engine_version,
            )
            print(sql)
        except Exception as exc:  # pragma: no cover
            logger.error("Error generating SQL: %s", exc)
            raise

    def show_plan(
        self,
        metrics: list[str],
        dimensions: Optional[list[str]] = None,
        filters: Optional[list[str]] = None,
        dialect: Optional[str] = None,
        format: str = "text",
    ):
        """
        Show query execution plan for metrics.
        """
        try:
            plan = self.builder_client.plan(
                metrics=metrics,
                dimensions=dimensions,
                filters=filters,
                dialect=dialect,
            )
            if format == "json":
                print(json.dumps(plan, indent=2))
            else:
                self._print_plan_text(plan)
        except Exception as exc:  # pragma: no cover
            logger.error("Error generating plan: %s", exc)
            raise

    def _print_plan_text(self, plan: dict):
        """Format and print the query plan using rich formatting."""
        console = Console()

        # Header info
        formulas = plan.get("metric_formulas", [])
        metric_names = [mf.get("name", "") for mf in formulas]
        dims = plan.get("requested_dimensions", [])
        dialect = plan.get("dialect", "spark")

        # Summary table
        summary = Table(show_header=False, box=None, padding=(0, 2))
        summary.add_column(style="bold")
        summary.add_column(style="dim")
        summary.add_row(
            "Metrics",
            ", ".join(metric_names) if metric_names else "(none)",
        )
        summary.add_row("Dimensions", ", ".join(dims) if dims else "(none)")
        summary.add_row("Dialect", dialect)

        console.print(
            Panel(summary, title="[bold]Query Execution Plan[/bold]", border_style=""),
        )

        # Grain Groups
        grain_groups = plan.get("grain_groups", [])
        console.print(f"\n[bold]Grain Groups ({len(grain_groups)})[/bold]")

        for i, gg in enumerate(grain_groups, 1):
            parent = gg.get("parent_name", "")
            grain = ", ".join(gg.get("grain", []))
            aggregability = gg.get("aggregability", "")
            metrics = ", ".join(gg.get("metrics", []))

            # Components table
            comp_table = Table(box=box.SIMPLE, show_header=True, header_style="bold")
            comp_table.add_column("Component")
            comp_table.add_column("Expression", style="dim")
            comp_table.add_column("Agg", style="dim")
            comp_table.add_column("Merge", style="dim")

            for comp in gg.get("components", []):
                comp_table.add_row(
                    comp.get("name", ""),
                    comp.get("expression", ""),
                    comp.get("aggregation") or "-",
                    comp.get("merge") or "-",
                )

            # Group info
            info = Text()
            info.append("Grain: ", style="bold")
            info.append(f"{grain}\n", style="dim")
            info.append("Metrics: ", style="bold")
            info.append(f"{metrics}\n", style="dim")
            info.append("Aggregability: ", style="bold")
            info.append(f"{aggregability}\n\n", style="dim")
            info.append("Components:\n", style="bold")

            console.print(
                Panel(
                    info,
                    title=f"[bold]Group {i}: {parent}[/bold]",
                    border_style="",
                ),
            )
            console.print(comp_table)

            # SQL with syntax highlighting (light theme, no background)
            sql = gg.get("sql", "")
            if sql:
                syntax = Syntax(
                    sql.strip(),
                    "sql",
                    theme="ansi_light",
                    line_numbers=False,
                    background_color=None,
                )
                console.print(Panel(syntax, title="[bold]SQL", border_style="dim"))

        # Metric Formulas
        console.print(f"\n[bold]Metric Formulas ({len(formulas)})[/bold]")

        formula_table = Table(box=box.ROUNDED, show_header=True, header_style="bold")
        formula_table.add_column("Metric")
        formula_table.add_column("Formula", style="dim")
        formula_table.add_column("Components", style="dim")
        formula_table.add_column("Derived", justify="center", style="dim")

        for mf in formulas:
            formula_table.add_row(
                mf.get("name", ""),
                mf.get("combiner", ""),
                ", ".join(mf.get("components", [])),
                "✓" if mf.get("is_derived") else "",
            )

        console.print(formula_table)
        console.print()

    def show_lineage(
        self,
        node_name: str,
        direction: str = "both",
        format: str = "text",
    ):
        """
        Show lineage (upstream/downstream dependencies) for a node.
        """
        try:
            node = self.builder_client.node(node_name)

            upstreams = []
            downstreams = []

            if direction in ["upstream", "both"]:
                upstreams = node.get_upstreams() if node.type != "source" else []

            if direction in ["downstream", "both"]:
                downstreams = node.get_downstreams() if node.type != "cube" else []

            if format == "json":
                lineage = {
                    "node": node_name,
                    "upstream": upstreams,
                    "downstream": downstreams,
                }
                print(json.dumps(lineage, indent=2))
            else:
                print(f"\n{'=' * 60}")
                print(f"Lineage for: {node_name}")
                print(f"{'=' * 60}")

                if direction in ["upstream", "both"]:
                    print(f"\nUpstream dependencies ({len(upstreams)}):")
                    print("-" * 60)
                    if upstreams:
                        for upstream in upstreams:
                            print(f"  ← {upstream}")
                    else:
                        print("  (none)")

                if direction in ["downstream", "both"]:
                    print(f"\nDownstream dependencies ({len(downstreams)}):")
                    print("-" * 60)
                    if downstreams:
                        for downstream in downstreams:
                            print(f"  → {downstream}")
                    else:
                        print("  (none)")

                print(f"{'=' * 60}\n")
        except Exception as exc:  # pragma: no cover
            logger.error("Error fetching lineage: %s", exc)
            raise

    def list_node_dimensions(self, node_name: str, format: str = "text"):
        """
        List available dimensions for a node (typically a metric).
        """
        try:
            # Use the internal API to get dimensions
            dimensions = self.builder_client._get_node_dimensions(node_name)

            if format == "json":
                print(json.dumps(dimensions, indent=2))
            else:
                print(f"\n{'=' * 60}")
                print(f"Available dimensions for: {node_name}")
                print(f"{'=' * 60}\n")

                if dimensions:
                    for dim in dimensions:
                        dim_name = dim.get("name", "")
                        dim_type = dim.get("type", "")
                        node_name_attr = dim.get("node_name", "")
                        path = dim.get("path", [])

                        print(f"  • {dim_name}")
                        print(f"    Type: {dim_type}")
                        print(f"    Node: {node_name_attr}")
                        print(f"    Path: {' → '.join(path)}")
                        print()

                    print(f"Total: {len(dimensions)} dimensions\n")
                else:
                    print("  No dimensions available.\n")

                print(f"{'=' * 60}\n")
        except Exception as exc:  # pragma: no cover
            logger.error("Error fetching dimensions: %s", exc)
            raise

    def create_parser(self):
        """Creates the CLI arg parser"""
        parser = argparse.ArgumentParser(prog="dj", description="DataJunction CLI")
        subparsers = parser.add_subparsers(dest="command", required=True)

        # `dj deploy <directory> --dryrun`
        deploy_parser = subparsers.add_parser(
            "deploy",
            help="Deploy node YAML definitions from a directory",
        )
        deploy_parser.add_argument(
            "directory",
            help="Path to the directory containing YAML files",
        )
        deploy_parser.add_argument(
            "--dryrun",
            action="store_true",
            help="Perform a dry run (show impact analysis without deploying)",
        )
        deploy_parser.add_argument(
            "--format",
            type=str,
            default="text",
            choices=["text", "json"],
            help="Output format for dry run (default: text)",
        )

        # `dj push <directory>` (alias for deploy without dryrun)
        push_parser = subparsers.add_parser(
            "push",
            help="Push node YAML definitions from a directory (alias for deploy)",
        )
        push_parser.add_argument(
            "directory",
            help="Path to the directory containing YAML files",
        )
        push_parser.add_argument(
            "--namespace",
            type=str,
            default=None,
            help="The namespace to push to (optionally overrides the namespace in the YAML files)",
        )
        # Deployment source tracking flags
        push_parser.add_argument(
            "--repo",
            type=str,
            default=None,
            help="Git repository URL for deployment tracking (overrides DJ_DEPLOY_REPO env var)",
        )
        push_parser.add_argument(
            "--branch",
            type=str,
            default=None,
            help="Git branch name (overrides DJ_DEPLOY_BRANCH env var)",
        )
        push_parser.add_argument(
            "--commit",
            type=str,
            default=None,
            help="Git commit SHA (overrides DJ_DEPLOY_COMMIT env var)",
        )
        push_parser.add_argument(
            "--ci-system",
            type=str,
            default=None,
            help="CI system name, e.g. 'github_actions', 'jenkins' (overrides DJ_DEPLOY_CI_SYSTEM)",
        )
        push_parser.add_argument(
            "--ci-run-url",
            type=str,
            default=None,
            help="URL to the CI run/build (overrides DJ_DEPLOY_CI_RUN_URL env var)",
        )

        # `dj pull <namespace> <directory>`
        pull_parser = subparsers.add_parser(
            "pull",
            help="Pull nodes from a namespace to YAML",
        )
        pull_parser.add_argument("namespace", help="The namespace to pull from")
        pull_parser.add_argument(
            "directory",
            help="Path to the directory to output YAML files",
        )

        # `dj seed --type=system` or `dj seed` (for short)
        seed_parser = subparsers.add_parser("seed", help="Seed DJ system nodes")
        seed_parser.add_argument(
            "--type",
            type=str,
            default="system",
            help="The type of nodes to seed (defaults to `system`)",
        )

        # `dj delete-node <node_name> --hard`
        delete_node_parser = subparsers.add_parser(
            "delete-node",
            help="Delete (deactivate) or hard delete a node",
        )
        delete_node_parser.add_argument(
            "node_name",
            help="The name of the node to delete",
        )
        delete_node_parser.add_argument(
            "--hard",
            action="store_true",
            help="Hard delete the node (completely removes it, use with caution)",
        )

        # `dj delete-namespace <namespace> --cascade --hard`
        delete_namespace_parser = subparsers.add_parser(
            "delete-namespace",
            help="Delete (deactivate) or hard delete a namespace",
        )
        delete_namespace_parser.add_argument(
            "namespace",
            help="The name of the namespace to delete",
        )
        delete_namespace_parser.add_argument(
            "--cascade",
            action="store_true",
            help="Delete all nodes in the namespace as well",
        )
        delete_namespace_parser.add_argument(
            "--hard",
            action="store_true",
            help="Hard delete the namespace (completely removes it, use with caution)",
        )

        # `dj describe <node-name> --format json`
        describe_parser = subparsers.add_parser(
            "describe",
            help="Describe a node with detailed information",
        )
        describe_parser.add_argument(
            "node_name",
            help="The name of the node to describe",
        )
        describe_parser.add_argument(
            "--format",
            type=str,
            default="text",
            choices=["text", "json"],
            help="Output format (default: text)",
        )

        # `dj list <type> --namespace <namespace> --format json`
        list_parser = subparsers.add_parser(
            "list",
            help="List objects (namespaces, metrics, dimensions, cubes, sources, transforms, nodes)",
        )
        list_parser.add_argument(
            "type",
            choices=[
                "namespaces",
                "metrics",
                "dimensions",
                "cubes",
                "sources",
                "transforms",
                "nodes",
            ],
            help="Type of objects to list",
        )
        list_parser.add_argument(
            "--namespace",
            type=str,
            default=None,
            help="Filter by namespace (for nodes) or prefix (for namespaces)",
        )
        list_parser.add_argument(
            "--format",
            type=str,
            default="text",
            choices=["text", "json"],
            help="Output format (default: text)",
        )

        # `dj sql <node-name> --dimensions d1,d2 --filters f1,f2`
        sql_parser = subparsers.add_parser(
            "sql",
            help="Generate SQL for a node",
        )
        sql_parser.add_argument("node_name", help="The name of the node")
        sql_parser.add_argument(
            "--dimensions",
            nargs=argparse.ZERO_OR_MORE,
            type=str,
            default=[],
            help="Comma-separated list of dimensions",
        )
        sql_parser.add_argument(
            "--filters",
            nargs=argparse.ZERO_OR_MORE,
            type=str,
            default=[],
            help="Comma-separated list of filters",
        )
        sql_parser.add_argument(
            "--engine",
            type=str,
            default=None,
            help="Engine name",
        )
        sql_parser.add_argument(
            "--engine-version",
            type=str,
            default=None,
            help="Engine version",
        )

        # `dj plan --metrics <metrics> --dimensions <dims> --filters <filters>`
        plan_parser = subparsers.add_parser(
            "plan",
            help="Show query execution plan for metrics",
        )
        plan_parser.add_argument(
            "--metrics",
            nargs=argparse.ONE_OR_MORE,
            type=str,
            required=True,
            help="List of metric names",
        )
        plan_parser.add_argument(
            "--dimensions",
            nargs=argparse.ZERO_OR_MORE,
            type=str,
            default=[],
            help="List of dimensions",
        )
        plan_parser.add_argument(
            "--filters",
            nargs=argparse.ZERO_OR_MORE,
            type=str,
            default=[],
            help="List of filters",
        )
        plan_parser.add_argument(
            "--dialect",
            type=str,
            default=None,
            help="SQL dialect (e.g., spark, trino)",
        )
        plan_parser.add_argument(
            "--format",
            type=str,
            default="text",
            choices=["text", "json"],
            help="Output format (default: text)",
        )

        # `dj lineage <node-name> --direction upstream|downstream|both --format json`
        lineage_parser = subparsers.add_parser(
            "lineage",
            help="Show lineage (upstream/downstream dependencies) for a node",
        )
        lineage_parser.add_argument("node_name", help="The name of the node")
        lineage_parser.add_argument(
            "--direction",
            type=str,
            default="both",
            choices=["upstream", "downstream", "both"],
            help="Direction of lineage to show (default: both)",
        )
        lineage_parser.add_argument(
            "--format",
            type=str,
            default="text",
            choices=["text", "json"],
            help="Output format (default: text)",
        )

        # `dj dimensions <node-name> --format json`
        dimensions_parser = subparsers.add_parser(
            "dimensions",
            help="List available dimensions for a node",
        )
        dimensions_parser.add_argument("node_name", help="The name of the node")
        dimensions_parser.add_argument(
            "--format",
            type=str,
            default="text",
            choices=["text", "json"],
            help="Output format (default: text)",
        )

        return parser

    def dispatch_command(self, args, parser):
        """
        Dispatches the command based on the parsed args
        """
        if args.command == "deploy":
            # deploy is similar to push but supports --dryrun
            if args.dryrun:
                self.dryrun(args.directory, format=args.format)
                return
            self.push(args.directory)
        elif args.command == "push":
            # CLI flags override env vars for deployment source tracking
            if args.repo:
                os.environ["DJ_DEPLOY_REPO"] = args.repo
            if args.branch:
                os.environ["DJ_DEPLOY_BRANCH"] = args.branch
            if args.commit:
                os.environ["DJ_DEPLOY_COMMIT"] = args.commit
            if args.ci_system:
                os.environ["DJ_DEPLOY_CI_SYSTEM"] = args.ci_system
            if args.ci_run_url:
                os.environ["DJ_DEPLOY_CI_RUN_URL"] = args.ci_run_url
            self.push(args.directory, namespace=args.namespace)
        elif args.command == "pull":
            self.pull(args.namespace, args.directory)
        elif args.command == "seed":
            self.seed()
        elif args.command == "delete-node":
            self.delete_node(args.node_name, hard=args.hard)
        elif args.command == "delete-namespace":
            self.delete_namespace(args.namespace, cascade=args.cascade, hard=args.hard)
        elif args.command == "describe":
            self.describe(args.node_name, format=args.format)
        elif args.command == "list":
            self.list_objects(args.type, namespace=args.namespace, format=args.format)
        elif args.command == "sql":
            self.get_sql(
                args.node_name,
                dimensions=args.dimensions,
                filters=args.filters,
                engine_name=args.engine,
                engine_version=args.engine_version,
            )
        elif args.command == "plan":
            self.show_plan(
                metrics=args.metrics,
                dimensions=args.dimensions,
                filters=args.filters,
                dialect=args.dialect,
                format=args.format,
            )
        elif args.command == "lineage":
            self.show_lineage(
                args.node_name,
                direction=args.direction,
                format=args.format,
            )
        elif args.command == "dimensions":
            self.list_node_dimensions(args.node_name, format=args.format)
        else:
            parser.print_help()  # pragma: no cover

    def run(self):
        """
        Parse arguments and run the appropriate command.
        """
        parser = self.create_parser()
        args = parser.parse_args()
        self.builder_client.basic_login()
        self.dispatch_command(args, parser)

    def seed(self, type: str = "nodes"):
        """
        Seed DJ system nodes
        """
        tables = [
            "node",
            "noderevision",
            "users",
            "materialization",
            "node_owners",
            "availabilitystate",
            "backfill",
            "collection",
            "dimensionlink",
        ]
        for table in tables:
            try:
                logger.info("Registering table: %s", table)
                self.builder_client.register_table("dj_metadata", "public", table)
            except DJClientException as exc:  # pragma: no cover
                if "already exists" in str(exc):  # pragma: no cover
                    logger.info("Already exists: %s", table)  # pragma: no cover
                else:  # pragma: no cover
                    logger.error(  # pragma: no cover
                        "Error registering tables: %s",
                        exc,
                    )
        logger.info("Finished registering DJ system metadata tables")

        logger.info("Loading DJ system nodes...")
        script_dir = Path(__file__).resolve().parent
        project_dir = script_dir / "seed" / type
        project = Project.load(str(project_dir))
        logger.info("Finished loading DJ system nodes.")

        logger.info("Compiling DJ system nodes...")
        compiled_project = project.compile()
        logger.info("Finished compiling DJ system nodes.")

        logger.info("Deploying DJ system nodes...")
        compiled_project.deploy(client=self.builder_client)
        logger.info("Finished deploying DJ system nodes.")


def main(builder_client: DJBuilder | None = None):
    """
    Main entrypoint for DJ CLI
    """
    cli = DJCLI(builder_client=builder_client)
    cli.run()


if __name__ == "__main__":
    main()
