"""DataJunction command-line tool"""

import argparse
import json
import logging
from pathlib import Path
from typing import Optional

from datajunction import DJBuilder, Project
from datajunction.deployment import DeploymentService
from datajunction.exceptions import DJClientException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DJCLI:
    """DJ command-line tool"""

    def __init__(self, builder_client: DJBuilder | None = None):
        """
        Initialize the CLI with a builder client.
        """
        self.builder_client = builder_client or DJBuilder()
        self.deployment_service = DeploymentService(client=self.builder_client)

    def push(self, directory: str, namespace: str | None = None):
        """
        Alias for deploy without dryrun.
        """
        self.deployment_service.push(directory, namespace=namespace)

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
            help="Perform a dry run",
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
        if args.command == "push":
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
