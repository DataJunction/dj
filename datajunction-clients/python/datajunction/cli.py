"""DataJunction command-line tool"""

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Optional

from rich import box
from rich.console import Console, Group
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from datajunction import DJBuilder
from datajunction.deployment import DeploymentService
from datajunction.exceptions import DJClientException, DJDeploymentFailure

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DJCLI:
    """DJ command-line tool"""

    def __init__(self, builder_client: DJBuilder | None = None):
        """
        Initialize the CLI with a builder client.

        If DJ_URL environment variable is set, it will be used as the server URL.
        """
        # Track if client was passed in (e.g., for testing) - skip login in that case
        self._client_provided = builder_client is not None
        if builder_client is None:
            # Read DJ_URL from environment, default to localhost:8000
            dj_url = os.environ.get("DJ_URL", "http://localhost:8000")
            builder_client = DJBuilder(uri=dj_url)
        self.builder_client = builder_client
        self.deployment_service = DeploymentService(client=self.builder_client)

    def push(
        self,
        directory: str,
        namespace: str | None = None,
        verbose: bool = False,
        force: bool = False,
    ):
        """
        Alias for deploy without dryrun.
        """
        self.deployment_service.push(
            directory,
            namespace=namespace,
            verbose=verbose,
            force=force,
        )

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
            impact = self.deployment_service.get_impact(
                directory,
                namespace=namespace,
                console=console,
                display=(format != "json"),
            )

            if format == "json":
                print(json.dumps(impact, indent=2))
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
        node_name: Optional[str] = None,
        metrics: Optional[list[str]] = None,
        dimensions: Optional[list[str]] = None,
        filters: Optional[list[str]] = None,
        orderby: Optional[list[str]] = None,
        limit: Optional[int] = None,
        dialect: Optional[str] = None,
        engine_name: Optional[str] = None,
        engine_version: Optional[str] = None,
    ):
        """
        Generate SQL for a node or metrics.
        """
        console = Console()
        try:
            if metrics:
                # Use v3 metrics SQL API
                result = self.builder_client.sql(
                    metrics=metrics,
                    dimensions=dimensions,
                    filters=filters,
                    orderby=orderby,
                    limit=limit,
                    dialect=dialect,
                )
            elif node_name:
                # Use node SQL API
                result = self.builder_client.node_sql(
                    node_name=node_name,
                    dimensions=dimensions,
                    filters=filters,
                    engine_name=engine_name,
                    engine_version=engine_version,
                )
            else:
                console.print(
                    "[bold red]ERROR:[/bold red] Either node_name or --metrics must be provided",
                )
                return

            # Handle error responses (dict) vs SQL string
            if isinstance(result, dict):
                message = result.get("message", str(result))
                console.print(f"[bold red]ERROR:[/bold red] {message}")
            else:
                syntax = Syntax(
                    result.strip(),
                    "sql",
                    theme="ansi_light",
                    line_numbers=False,
                    background_color=None,
                )
                console.print(syntax)
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

            # Components table with SQL syntax highlighting for expressions
            comp_table = Table(box=box.SIMPLE, show_header=True, header_style="bold")
            comp_table.add_column("Component")
            comp_table.add_column("Expression")
            comp_table.add_column("Agg")
            comp_table.add_column("Merge")

            for comp in gg.get("components", []):
                expr = comp.get("expression", "")
                agg = comp.get("aggregation") or "-"
                merge = comp.get("merge") or "-"
                # Use SQL syntax highlighting for expressions and aggregations
                expr_syntax = (
                    Syntax(expr, "sql", theme="ansi_light", background_color=None)
                    if expr
                    else ""
                )
                agg_syntax = (
                    Syntax(agg, "sql", theme="ansi_light", background_color=None)
                    if agg != "-"
                    else "-"
                )
                merge_syntax = (
                    Syntax(merge, "sql", theme="ansi_light", background_color=None)
                    if merge != "-"
                    else "-"
                )
                comp_table.add_row(
                    comp.get("name", ""),
                    expr_syntax,
                    agg_syntax,
                    merge_syntax,
                )

            # Group info
            info = Text()
            info.append("Grain: ", style="bold")
            info.append(f"{grain}\n", style="dim")
            info.append("Metrics: ", style="bold")
            info.append(f"{metrics}\n", style="dim")
            info.append("Aggregability: ", style="bold")
            info.append(f"{aggregability}", style="dim")

            # Combine info and components table into a single group inside the Panel
            components_header = Text("\nComponents:", style="bold")
            panel_content = Group(info, components_header, comp_table)

            console.print(
                Panel(
                    panel_content,
                    title=f"[bold green]Group {i}[/bold green]: {parent}",
                    border_style="",
                ),
            )

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

        # Metric Formulas - table with Metric and Formula columns
        console.print(f"\n[bold]Metric Formulas ({len(formulas)})[/bold]")

        formula_table = Table(
            box=box.ROUNDED,
            show_header=True,
            header_style="bold",
            expand=True,
            show_lines=True,  # Add lines between rows
        )
        formula_table.add_column("Metric", no_wrap=True)
        formula_table.add_column("Formula", overflow="fold")

        for mf in formulas:
            formula = mf.get("combiner", "")
            # Use SQL syntax highlighting for the formula
            formula_syntax = (
                Syntax(
                    formula,
                    "sql",
                    theme="ansi_light",
                    background_color=None,
                    word_wrap=True,
                )
                if formula
                else ""
            )
            formula_table.add_row(
                mf.get("name", ""),
                formula_syntax,
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

    def get_data(
        self,
        node_name: Optional[str] = None,
        metrics: Optional[list[str]] = None,
        dimensions: Optional[list[str]] = None,
        filters: Optional[list[str]] = None,
        engine_name: Optional[str] = None,
        engine_version: Optional[str] = None,
        limit: Optional[int] = None,
        format: str = "table",
    ):
        """
        Fetch and display data for a node or metrics.
        """
        console = Console()
        # Default to 1000 rows to avoid unbounded queries
        effective_limit = limit if limit is not None else 1000
        try:
            if metrics:
                # Use metrics data API
                result = self.builder_client.data(
                    metrics=metrics,
                    dimensions=dimensions,
                    filters=filters,
                    engine_name=engine_name,
                    engine_version=engine_version,
                    limit=effective_limit,
                )
            elif node_name:
                # Use node data API
                result = self.builder_client.node_data(
                    node_name=node_name,
                    dimensions=dimensions,
                    filters=filters,
                    engine_name=engine_name,
                    engine_version=engine_version,
                    limit=effective_limit,
                )
            else:
                console.print(
                    "[bold red]ERROR:[/bold red] Either node_name or --metrics must be provided",
                )
                return

            # Handle error responses
            if isinstance(result, dict) and "message" in result:
                console.print(f"[bold red]ERROR:[/bold red] {result['message']}")
                return

            # result should be a DataFrame (already limited by API)
            if format == "json":
                print(result.to_json(orient="records", indent=2))
            elif format == "csv":
                print(result.to_csv(index=False))
            else:
                # Table format using Rich
                table = Table(
                    box=box.ROUNDED,
                    show_header=True,
                    header_style="bold cyan",
                )

                # Add columns
                for col in result.columns:
                    table.add_column(str(col))

                # Add rows
                for idx, row in result.iterrows():
                    table.add_row(*[str(v) for v in row.values])

                console.print(table)

                # Show row count info
                total_rows = len(result)
                if total_rows == effective_limit:
                    console.print(
                        f"\n[dim]Showing {total_rows} rows (limit: {effective_limit}). "
                        f"Use --limit to adjust.[/dim]",
                    )
                else:
                    console.print(f"\n[dim]{total_rows} row(s)[/dim]")

        except Exception as exc:  # pragma: no cover
            logger.error("Error fetching data: %s", exc)
            console.print(f"[bold red]ERROR:[/bold red] {exc}")
            raise

    #
    # Git config commands
    #
    def git_init(
        self,
        namespace: str,
        repo: str,
        default_branch: str,
        git_path: Optional[str] = None,
        git_only: bool = False,
    ):
        """Initialize git configuration on a namespace."""
        console = Console()
        try:
            config = self.builder_client.init_git_root(
                namespace=namespace,
                github_repo_path=repo,
                default_branch=default_branch,
                git_path=git_path,
                git_only=git_only if git_only else None,
            )
            console.print(
                f"[green]Git config initialized for namespace[/green] [bold]{namespace}[/bold]",
            )
            console.print(f"  Repository:     {config.github_repo_path}")
            console.print(f"  Default Branch: {config.default_branch}")
            if config.git_path:
                console.print(f"  Git Path:       {config.git_path}")
            if config.git_only:
                console.print(f"  Git Only:       {config.git_only}")
        except DJClientException as exc:
            console.print(f"[bold red]ERROR:[/bold red] {exc}")
            raise SystemExit(1)

    def git_show(self, namespace: str, format: str = "text"):
        """Show git configuration for a namespace."""
        console = Console()
        try:
            config = self.builder_client.get_git_config(namespace=namespace)
            if format == "json":
                config_dict = {
                    "namespace": namespace,
                    "github_repo_path": config.github_repo_path,
                    "git_branch": config.git_branch,
                    "default_branch": config.default_branch,
                    "git_path": config.git_path,
                    "git_only": config.git_only,
                }
                print(json.dumps(config_dict, indent=2))
            else:
                console.print(f"\n[bold]Git Configuration for {namespace}[/bold]")
                console.print(f"  Repository:     {config.github_repo_path or 'N/A'}")
                console.print(f"  Git Branch:     {config.git_branch or 'N/A'}")
                console.print(f"  Default Branch: {config.default_branch or 'N/A'}")
                console.print(f"  Git Path:       {config.git_path or 'N/A'}")
                console.print(f"  Git Only:       {config.git_only or False}")
        except DJClientException as exc:
            console.print(f"[bold red]ERROR:[/bold red] {exc}")
            raise SystemExit(1)

    def git_clear(self, namespace: str):
        """Clear git configuration from a namespace."""
        console = Console()
        try:
            self.builder_client.clear_git_config(namespace=namespace)
            console.print(
                f"[green]Git config cleared for namespace[/green] [bold]{namespace}[/bold]",
            )
        except DJClientException as exc:
            console.print(f"[bold red]ERROR:[/bold red] {exc}")
            raise SystemExit(1)

    def git_sync(self, namespace: str):
        """Sync a namespace from the HEAD of its configured git branch."""
        console = Console()
        try:
            info = self.builder_client.sync_from_git(namespace=namespace)
            source = info.get("source") or {}
            branch = source.get("branch", "unknown")
            commit = (source.get("commit_sha") or "")[:12]
            results = info.get("results", [])
            created = sum(1 for r in results if r.get("operation") == "create")
            updated = sum(1 for r in results if r.get("operation") == "update")
            skipped = sum(1 for r in results if r.get("operation") == "noop")
            console.print(
                f"[green]Synced namespace[/green] [bold]{namespace}[/bold] "
                f"from [bold]{branch}[/bold] @ [dim]{commit}[/dim]",
            )
            console.print(
                f"  {created} created, {updated} updated, {skipped} unchanged",
            )
        except DJClientException as exc:
            console.print(f"[bold red]ERROR:[/bold red] {exc}")
            raise SystemExit(1)

    #
    # Branch commands
    #
    def branch_create(self, namespace: str, branch_name: str):
        """Create a branch namespace."""
        console = Console()
        try:
            result = self.builder_client.create_branch(
                namespace=namespace,
                branch_name=branch_name,
            )
            console.print(
                f"[green]Branch created:[/green] [bold]{result.namespace}[/bold]",
            )
            console.print(f"  Git Branch: {branch_name}")
            console.print(f"  Parent:     {namespace}")
        except DJClientException as exc:
            console.print(f"[bold red]ERROR:[/bold red] {exc}")
            raise SystemExit(1)

    def branch_list(self, namespace: str, format: str = "text"):
        """List branches under a namespace."""
        console = Console()
        try:
            branches = self.builder_client.list_branches(namespace=namespace)
            if format == "json":
                branches_list = [
                    {
                        "namespace": b.namespace,
                        "git_branch": b.git_branch,
                        "parent_namespace": b.parent_namespace,
                        "github_repo_path": b.github_repo_path,
                    }
                    for b in branches
                ]
                print(json.dumps(branches_list, indent=2))
            else:
                if not branches:
                    console.print(f"[dim]No branches found under {namespace}[/dim]")
                    return
                console.print(f"\n[bold]Branches under {namespace}[/bold]\n")
                table = Table(show_header=True, header_style="bold")
                table.add_column("Namespace")
                table.add_column("Git Branch")
                table.add_column("Repository")
                for branch in branches:
                    table.add_row(
                        branch.namespace,
                        branch.git_branch,
                        branch.github_repo_path or "",
                    )
                console.print(table)
        except DJClientException as exc:
            console.print(f"[bold red]ERROR:[/bold red] {exc}")
            raise SystemExit(1)

    def branch_delete(
        self,
        namespace: str,
        branch_name: str,
        keep_git_branch: bool = False,
    ):
        """Delete a branch namespace."""
        console = Console()
        try:
            self.builder_client.delete_branch(
                namespace=namespace,
                branch_name=branch_name,
                delete_git_branch=not keep_git_branch,
            )
            git_msg = (
                " (git branch kept)" if keep_git_branch else " (git branch deleted)"
            )
            console.print(
                f"[green]Branch deleted:[/green] [bold]{branch_name}[/bold]{git_msg}",
            )
        except DJClientException as exc:
            console.print(f"[bold red]ERROR:[/bold red] {exc}")
            raise SystemExit(1)

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
        deploy_parser.add_argument(
            "--verbose",
            action="store_true",
            help="Show all results including noops",
        )
        deploy_parser.add_argument(
            "--force",
            action="store_true",
            help="Force update all nodes even if they are unchanged",
        )

        # `dj push <directory>` - primary deployment command
        push_parser = subparsers.add_parser(
            "push",
            help="Push node YAML definitions from a directory to DJ server",
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
        push_parser.add_argument(
            "--dryrun",
            action="store_true",
            help="Perform a dry run (show impact analysis without deploying)",
        )
        push_parser.add_argument(
            "--verbose",
            action="store_true",
            help="Show all results including noops",
        )
        push_parser.add_argument(
            "--force",
            action="store_true",
            help="Force update all nodes even if they are unchanged",
        )
        push_parser.add_argument(
            "--format",
            type=str,
            default="text",
            choices=["text", "json"],
            help="Output format for dry run (default: text)",
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

        # `dj generate-codeowners <directory>`
        codeowners_parser = subparsers.add_parser(
            "generate-codeowners",
            help="Generate a CODEOWNERS file from the owners fields in DJ node YAML files",
        )
        codeowners_parser.add_argument(
            "directory",
            nargs="?",
            default=".",
            help="Path to the DJ repo root (default: current directory)",
        )
        codeowners_parser.add_argument(
            "--output",
            type=str,
            default=".github/CODEOWNERS",
            help="Output file path (default: .github/CODEOWNERS)",
        )
        codeowners_parser.add_argument(
            "--github-api-url",
            type=str,
            default=None,
            help=(
                "GitHub API base URL for resolving emails to usernames "
                "(e.g. https://api.github.com or https://github.example.com/api/v3). "
                "If omitted, no email resolution is performed."
            ),
        )
        codeowners_parser.add_argument(
            "--github-token-env",
            type=str,
            default="GITHUB_TOKEN",
            help="Name of the env var holding the GitHub token (default: GITHUB_TOKEN)",
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

        # `dj sql <node-name>` or `dj sql --metrics m1 m2`
        sql_parser = subparsers.add_parser(
            "sql",
            help="Generate SQL for a node or metrics",
        )
        sql_parser.add_argument(
            "node_name",
            nargs="?",
            default=None,
            help="The name of the node (for single-node SQL)",
        )
        sql_parser.add_argument(
            "--metrics",
            nargs=argparse.ONE_OR_MORE,
            type=str,
            default=None,
            help="List of metrics (for multi-metric SQL using v3 API)",
        )
        sql_parser.add_argument(
            "--dimensions",
            nargs=argparse.ZERO_OR_MORE,
            type=str,
            default=[],
            help="List of dimensions",
        )
        sql_parser.add_argument(
            "--filters",
            nargs=argparse.ZERO_OR_MORE,
            type=str,
            default=[],
            help="List of filters",
        )
        sql_parser.add_argument(
            "--orderby",
            nargs=argparse.ZERO_OR_MORE,
            type=str,
            default=[],
            help="List of ORDER BY clauses (for metrics SQL)",
        )
        sql_parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Limit number of rows (for metrics SQL)",
        )
        sql_parser.add_argument(
            "--dialect",
            type=str,
            default=None,
            help="SQL dialect (e.g., spark, trino)",
        )
        sql_parser.add_argument(
            "--engine",
            type=str,
            default=None,
            help="Engine name (for node SQL, deprecated - use --dialect)",
        )
        sql_parser.add_argument(
            "--engine-version",
            type=str,
            default=None,
            help="Engine version (for node SQL)",
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

        # `dj data <node-name>` or `dj data --metrics m1 m2`
        data_parser = subparsers.add_parser(
            "data",
            help="Fetch and display data for a node or metrics",
        )
        data_parser.add_argument(
            "node_name",
            nargs="?",
            default=None,
            help="The name of the node (for single-node data)",
        )
        data_parser.add_argument(
            "--metrics",
            nargs=argparse.ONE_OR_MORE,
            type=str,
            default=None,
            help="List of metrics (for multi-metric data)",
        )
        data_parser.add_argument(
            "--dimensions",
            nargs=argparse.ZERO_OR_MORE,
            type=str,
            default=[],
            help="List of dimensions to group by",
        )
        data_parser.add_argument(
            "--filters",
            nargs=argparse.ZERO_OR_MORE,
            type=str,
            default=[],
            help="List of filters (e.g., 'default.date.year = 2024')",
        )
        data_parser.add_argument(
            "--engine",
            type=str,
            default=None,
            help="Engine name for query execution",
        )
        data_parser.add_argument(
            "--engine-version",
            type=str,
            default=None,
            help="Engine version for query execution",
        )
        data_parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Limit number of rows returned",
        )
        data_parser.add_argument(
            "--format",
            type=str,
            default="table",
            choices=["table", "json", "csv"],
            help="Output format (default: table)",
        )

        # `dj setup-claude`
        setup_claude_parser = subparsers.add_parser(
            "setup-claude",
            help="Configure Claude Code integration (skills + MCP server)",
        )
        setup_claude_parser.add_argument(
            "--output",
            type=str,
            default=str(Path.home() / ".claude" / "skills"),
            help="Output directory for skill files (default: ~/.claude/skills)",
        )
        setup_claude_parser.add_argument(
            "--skills",
            action="store_true",
            default=True,
            help="Export DJ skills (default: True)",
        )
        setup_claude_parser.add_argument(
            "--no-skills",
            action="store_false",
            dest="skills",
            help="Skip exporting skills",
        )
        setup_claude_parser.add_argument(
            "--mcp",
            action="store_true",
            default=True,
            help="Configure DJ MCP server (default: True)",
        )
        setup_claude_parser.add_argument(
            "--no-mcp",
            action="store_false",
            dest="mcp",
            help="Skip MCP server configuration",
        )
        setup_claude_parser.add_argument(
            "--agents",
            action="store_true",
            default=True,
            help="Install DJ subagent to ~/.claude/agents/ (default: True)",
        )
        setup_claude_parser.add_argument(
            "--no-agents",
            action="store_false",
            dest="agents",
            help="Skip subagent installation",
        )

        # `dj git` command group
        git_parser = subparsers.add_parser(
            "git",
            help="Git configuration management",
        )
        git_subparsers = git_parser.add_subparsers(dest="git_command", required=True)

        # `dj git init <namespace> --repo <org/repo> --default-branch <branch>`
        git_init_parser = git_subparsers.add_parser(
            "init",
            help="Initialize git configuration on a namespace",
        )
        git_init_parser.add_argument(
            "namespace",
            help="The namespace to configure as a git root",
        )
        git_init_parser.add_argument(
            "--repo",
            type=str,
            required=True,
            help="GitHub repository path (e.g., 'org/repo')",
        )
        git_init_parser.add_argument(
            "--default-branch",
            type=str,
            required=True,
            help="Default git branch (e.g., 'main')",
        )
        git_init_parser.add_argument(
            "--git-path",
            type=str,
            default=None,
            help="Subdirectory within repo for node definitions (optional)",
        )
        git_init_parser.add_argument(
            "--git-only",
            action="store_true",
            help="Block UI edits; changes must come via git",
        )

        # `dj git show <namespace>`
        git_show_parser = git_subparsers.add_parser(
            "show",
            help="Show git configuration for a namespace",
        )
        git_show_parser.add_argument(
            "namespace",
            help="The namespace to show git config for",
        )
        git_show_parser.add_argument(
            "--format",
            type=str,
            default="text",
            choices=["text", "json"],
            help="Output format (default: text)",
        )

        # `dj git clear <namespace>`
        git_clear_parser = git_subparsers.add_parser(
            "clear",
            help="Clear git configuration from a namespace",
        )
        git_clear_parser.add_argument(
            "namespace",
            help="The namespace to clear git config from",
        )

        # `dj git sync <namespace>`
        git_sync_parser = git_subparsers.add_parser(
            "sync",
            help="Sync a namespace from the HEAD of its configured git branch",
        )
        git_sync_parser.add_argument(
            "namespace",
            help="The namespace to sync",
        )

        # `dj git create-branch <namespace> <branch-name>`
        git_create_branch_parser = git_subparsers.add_parser(
            "create-branch",
            help="Create a branch namespace for development",
        )
        git_create_branch_parser.add_argument(
            "namespace",
            help="The git root namespace (e.g., 'myns')",
        )
        git_create_branch_parser.add_argument(
            "branch_name",
            help="The git branch name (e.g., 'feature-x')",
        )

        # `dj git list-branches <namespace>`
        git_list_branches_parser = git_subparsers.add_parser(
            "list-branches",
            help="List branch namespaces under a root namespace",
        )
        git_list_branches_parser.add_argument(
            "namespace",
            help="The git root namespace to list branches for",
        )
        git_list_branches_parser.add_argument(
            "--format",
            type=str,
            default="text",
            choices=["text", "json"],
            help="Output format (default: text)",
        )

        # `dj git delete-branch <namespace> <branch-name>`
        git_delete_branch_parser = git_subparsers.add_parser(
            "delete-branch",
            help="Delete a branch namespace",
        )
        git_delete_branch_parser.add_argument(
            "namespace",
            help="The git root namespace (e.g., 'myns')",
        )
        git_delete_branch_parser.add_argument(
            "branch_name",
            help="The git branch name to delete (e.g., 'feature-x')",
        )
        git_delete_branch_parser.add_argument(
            "--keep-git-branch",
            action="store_true",
            help="Keep the git branch in GitHub (only delete DJ namespace)",
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
            try:
                self.push(args.directory, verbose=args.verbose, force=args.force)
            except DJDeploymentFailure:
                raise SystemExit(1)
        elif args.command == "push":
            # Handle dry run first
            if args.dryrun:
                self.dryrun(
                    args.directory,
                    namespace=args.namespace,
                    format=args.format,
                )
                return
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
            try:
                self.push(
                    args.directory,
                    namespace=args.namespace,
                    verbose=args.verbose,
                    force=args.force,
                )
            except DJDeploymentFailure:
                # Errors already displayed in the deployment panel
                raise SystemExit(1)
            except DJClientException as exc:
                Console().print(f"[red bold]ERROR:[/red bold] {exc}")
                raise SystemExit(1)
        elif args.command == "generate-codeowners":
            count = DeploymentService.build_codeowners(
                args.directory,
                output=args.output,
                github_api_url=args.github_api_url,
                github_token_env=args.github_token_env,
            )
            console = Console()
            console.print(
                f"[green]CODEOWNERS written to[/green] [bold]{args.output}[/bold] "
                f"([cyan]{count} entries[/cyan])",
            )
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
                node_name=args.node_name,
                metrics=args.metrics,
                dimensions=args.dimensions,
                filters=args.filters,
                orderby=args.orderby,
                limit=args.limit,
                dialect=args.dialect,
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
        elif args.command == "data":
            self.get_data(
                node_name=args.node_name,
                metrics=args.metrics,
                dimensions=args.dimensions,
                filters=args.filters,
                engine_name=args.engine,
                engine_version=args.engine_version,
                limit=args.limit,
                format=args.format,
            )
        elif args.command == "setup-claude":
            self.setup_claude(
                output_dir=Path(args.output),
                skills=args.skills,
                mcp=args.mcp,
                agents=args.agents,
            )
        elif args.command == "git":
            if args.git_command == "init":
                self.git_init(
                    namespace=args.namespace,
                    repo=args.repo,
                    default_branch=args.default_branch,
                    git_path=args.git_path,
                    git_only=args.git_only,
                )
            elif args.git_command == "show":
                self.git_show(namespace=args.namespace, format=args.format)
            elif args.git_command == "clear":
                self.git_clear(namespace=args.namespace)
            elif args.git_command == "create-branch":
                self.branch_create(
                    namespace=args.namespace,
                    branch_name=args.branch_name,
                )
            elif args.git_command == "list-branches":
                self.branch_list(namespace=args.namespace, format=args.format)
            elif args.git_command == "sync":
                self.git_sync(namespace=args.namespace)
            elif args.git_command == "delete-branch":  # pragma: no branch
                self.branch_delete(
                    namespace=args.namespace,
                    branch_name=args.branch_name,
                    keep_git_branch=args.keep_git_branch,
                )
        else:
            parser.print_help()  # pragma: no cover

    def run(self):
        """
        Parse arguments and run the appropriate command.
        """
        parser = self.create_parser()
        args = parser.parse_args()
        # Skip login if client was provided (e.g., for testing with pre-authenticated client)
        # Also skip login for commands that are purely local (no server needed)
        local_commands = {"setup-claude", "generate-codeowners"}
        if not self._client_provided and args.command not in local_commands:
            self.builder_client.basic_login()  # pragma: no cover
        self.dispatch_command(args, parser)

    def setup_claude(
        self,
        output_dir: Path,
        skills: bool = True,
        mcp: bool = True,
        agents: bool = True,
    ):
        """Configure Claude Code integration with DJ."""
        import json

        console = Console()

        console.print(
            "\n[bold blue]🔧 Setting up Claude Code integration[/bold blue]\n",
        )

        try:
            # Export skills if requested
            if skills:
                # Ensure output directory exists
                output_dir.mkdir(parents=True, exist_ok=True)

                console.print(
                    "[bold]📚 Installing DJ skill[/bold]\n",
                )

                # Load bundled skill from package
                dir_name = "datajunction"
                skill_dir = output_dir / dir_name
                skill_file = skill_dir / "SKILL.md"

                console.print(f"Installing [cyan]{dir_name}[/cyan]...")

                # Find bundled skill file
                from datajunction import __version__ as dj_version
                from importlib.resources import files

                try:
                    # Read bundled skill markdown using importlib.resources (Python 3.9+)
                    skill_file_path = files("datajunction").joinpath(
                        "skills/datajunction.md",
                    )
                    bundled_skill = skill_file_path.read_text(encoding="utf-8")

                    # Create directory
                    skill_dir.mkdir(parents=True, exist_ok=True)

                    # Write SKILL.md
                    with open(skill_file, "w") as f:
                        f.write(bundled_skill)

                    # Write metadata
                    metadata_file = skill_dir / "metadata.json"
                    with open(metadata_file, "w") as f:
                        metadata = {
                            "name": "datajunction",
                            "version": dj_version,
                            "description": "Comprehensive DataJunction semantic layer guide",
                            "keywords": [
                                "DataJunction",
                                "DJ",
                                "semantic layer",
                                "dimension link",
                                "metric",
                                "SQL generation",
                                "YAML nodes",
                                "git workflow",
                                "repo-backed namespace",
                            ],
                            "metadata": {
                                "source": "bundled",
                                "dj_version": dj_version,
                            },
                        }
                        json.dump(metadata, f, indent=2)

                    console.print(f"[green]✓ Installed {skill_dir}/[/green]")
                    console.print(
                        f"  [dim]├─ SKILL.md ({len(bundled_skill)} chars)[/dim]",
                    )
                    console.print(
                        f"  [dim]└─ metadata.json (v{dj_version})[/dim]\n",
                    )

                    console.print(
                        f"[bold green]✓ Skill installed to {output_dir}[/bold green]\n",
                    )

                except FileNotFoundError:  # pragma: no cover
                    console.print(
                        "[red]✗ Bundled skill not found. Please ensure datajunction is properly installed.[/red]",
                    )

            # Install subagent if requested
            if agents:
                agents_dir = Path.home() / ".claude" / "agents"
                agents_dir.mkdir(parents=True, exist_ok=True)
                agent_file = agents_dir / "dj.md"

                console.print("[bold]🤖 Installing DJ subagent[/bold]\n")

                subagent_content = """\
---
name: dj
description: >
  DataJunction semantic layer expert. Use proactively for any DataJunction
  or DJ work — querying metrics, exploring nodes and dimensions, building
  SQL, understanding lineage, and semantic layer design.
skills:
  - datajunction
model: inherit
---
"""
                with open(agent_file, "w") as f:
                    f.write(subagent_content)

                console.print(f"[green]✓ Installed subagent to {agent_file}[/green]\n")

            # Setup MCP if requested
            if mcp:
                self._setup_mcp_server(console)

            # Final success message
            anything_installed = skills or mcp or agents
            if anything_installed:  # pragma: no branch
                console.print(
                    "\n[bold green]✓ Claude Code integration complete[/bold green]",
                )
                parts = []
                if skills:
                    parts.append("skill")
                if agents:
                    parts.append("subagent")
                if mcp:
                    parts.append("MCP server")
                console.print(
                    f"[dim]{', '.join(parts).capitalize()} installed. Restart Claude Code to load changes.[/dim]",
                )

        except Exception as e:  # pragma: no cover
            console.print(f"\n[red]✗ Error: {e}[/red]")
            logger.exception("Failed to setup Claude Code integration")
            raise

    def _setup_mcp_server(self, console: Console):
        """Configure DJ MCP server in Claude config."""
        import json
        import shutil
        from pathlib import Path

        # Find Claude config location
        claude_config_paths = [
            Path.home() / ".claude.json",  # Claude Code CLI (primary)
            Path.home()
            / "Library"
            / "Application Support"
            / "Claude"
            / "claude_desktop_config.json",  # Claude Desktop MacOS
            Path.home()
            / ".config"
            / "Claude"
            / "claude_desktop_config.json",  # Claude Desktop Linux
        ]

        config_path = None
        for path in claude_config_paths:  # pragma: no branch
            # Use the first path that either exists or whose parent exists
            if path.exists() or path.parent.exists():  # pragma: no branch
                config_path = path
                break

        if not config_path:
            console.print(
                "\n[yellow]⚠️  Could not find Claude config directory[/yellow]",
            )
            console.print(
                "[dim]Skipping MCP setup. You can manually add DJ MCP to your Claude config.[/dim]",
            )
            return

        console.print("\n[bold blue]🔧 Configuring DJ MCP server[/bold blue]")
        console.print(f"[dim]Config file: {config_path}[/dim]\n")

        # Find dj-mcp command
        dj_mcp_path = shutil.which("dj-mcp")
        if not dj_mcp_path:
            console.print("[yellow]⚠️  dj-mcp command not found in PATH[/yellow]")
            console.print(
                "[dim]Make sure datajunction client is installed with MCP extras:[/dim]",
            )
            console.print(
                "  pip install 'datajunction[mcp]'",
                style="dim",
                markup=False,
            )
            return

        # Load existing config or create new one
        if config_path.exists():
            try:
                with open(config_path, "r") as f:
                    config = json.load(f)
            except json.JSONDecodeError:
                console.print(
                    "[yellow]⚠️  Invalid JSON in Claude config, creating backup[/yellow]",
                )
                backup_path = config_path.with_suffix(".json.backup")
                shutil.copy(config_path, backup_path)
                console.print(f"[dim]Backup saved to: {backup_path}[/dim]")
                config = {}
        else:
            config = {}

        # Ensure mcpServers section exists
        if "mcpServers" not in config:
            config["mcpServers"] = {}

        # Add DJ MCP server
        dj_url = self.builder_client.uri or "http://localhost:8000"
        config["mcpServers"]["datajunction"] = {
            "command": dj_mcp_path,
            "env": {
                "DJ_API_URL": dj_url,  # Correct env var name
                "DJ_USERNAME": "dj",  # Default username
                "DJ_PASSWORD": "dj",  # Default password
            },
        }

        # Write config back
        config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)

        console.print("[green]✓ DJ MCP server configured[/green]")
        console.print("  [dim]Server: datajunction[/dim]")
        console.print(f"  [dim]Command: {dj_mcp_path}[/dim]")
        console.print(f"  [dim]DJ_API_URL: {dj_url}[/dim]")
        console.print("  [dim]DJ_USERNAME: dj[/dim]")
        console.print("  [dim]DJ_PASSWORD: *** (default)[/dim]\n")
        console.print(
            "[bold yellow]⚠️  Restart Claude Desktop/Code to load the MCP server[/bold yellow]",
        )

    def seed(self, type: str = "nodes"):
        """
        Seed DJ system nodes
        """
        tables = [
            "node",
            "nodenamespace",
            "noderevision",
            "users",
            "materialization",
            "node_owners",
            "availabilitystate",
            "backfill",
            "collection",
            "dimensionlink",
            "deployments",
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

        logger.info("Pushing DJ system nodes...")
        script_dir = Path(__file__).resolve().parent
        project_dir = script_dir / "seed" / type
        # Read the project's declared prefix and pass it explicitly so the
        # push flow doesn't apply git-branch namespace derivation to the
        # bootstrap nodes (they always belong to the canonical system namespace).
        project_yaml_path = project_dir / "dj.yaml"
        project_meta: dict = {}
        if project_yaml_path.exists():  # pragma: no branch
            import yaml as _yaml

            with open(project_yaml_path, encoding="utf-8") as fh:
                project_meta = _yaml.safe_load(fh) or {}
        system_namespace = project_meta.get("namespace") or project_meta.get(
            "prefix",
        )
        self.deployment_service.push(
            str(project_dir),
            namespace=system_namespace,
        )
        logger.info("Finished pushing DJ system nodes.")


def main(builder_client: DJBuilder | None = None):
    """
    Main entrypoint for DJ CLI
    """
    cli = DJCLI(builder_client=builder_client)
    cli.run()


if __name__ == "__main__":
    main()
