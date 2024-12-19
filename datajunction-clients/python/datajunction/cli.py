"""DataJunction command-line tool"""

import argparse

from datajunction import DJBuilder, Project


class DJCLI:
    """DJ command-line tool"""

    def __init__(self, builder_client: DJBuilder | None = None):
        """
        Initialize the CLI with a builder client.
        """
        self.builder_client = builder_client or DJBuilder()

    def deploy(self, directory: str, dryrun: bool):
        """
        Deploy nodes from the specified directory.
        """
        project = Project.load(directory)
        compiled_project = project.compile()
        if dryrun:
            compiled_project.validate(client=self.builder_client)
        else:
            compiled_project.deploy(client=self.builder_client)

    def pull(self, namespace: str, directory: str):
        """
        Export nodes from a specific namespace.
        """
        print(f"Exporting namespace {namespace} to {directory}...")
        Project.pull(
            client=self.builder_client,
            namespace=namespace,
            target_path=directory,
        )
        print(f"Finished exporting namespace {namespace} to {directory}.")

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
        return parser

    def dispatch_command(self, args, parser):
        """
        Dispatches the command based on the parsed args
        """
        if args.command == "deploy":
            self.deploy(args.directory, args.dryrun)
        elif args.command == "pull":
            self.pull(args.namespace, args.directory)
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


def main(builder_client: DJBuilder | None = None):
    """
    Main entrypoint for DJ CLI
    """
    cli = DJCLI(builder_client=builder_client)
    cli.run()


if __name__ == "__main__":
    main()
