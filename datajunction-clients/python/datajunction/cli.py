"""DataJunction command-line tool"""
import argparse

from datajunction import DJBuilder, Project

dj_builder = DJBuilder()


def deploy(directory: str, dryrun: bool = False):
    """
    Deploy nodes from the specified directory.
    """
    project = Project.load(directory)
    compiled_project = project.compile()
    if dryrun:
        compiled_project.validate(client=dj_builder)
    else:
        compiled_project.deploy(client=dj_builder)


def pull(namespace: str, directory: str):
    """
    Pull nodes from a specific namespace.
    """
    Project.pull(client=dj_builder, namespace=namespace, target_path=directory)


def main():
    """DJ CLI"""
    parser = argparse.ArgumentParser(prog="dj", description="DataJunction CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    deploy_parser = subparsers.add_parser(
        "deploy",
        help="Deploy node YAML definitions from a directory",
    )
    deploy_parser.add_argument(
        "directory",
        help="Path to the directory containing YAML files",
    )
    deploy_parser.add_argument(
        "dryrun",
        type=bool,
        help="Whether to do a dry run (validates without actually deploying)",
    )

    pull_parser = subparsers.add_parser(
        "pull",
        help="Pull nodes from a namespace to YAML",
    )
    pull_parser.add_argument("namespace", help="The namespace to pull from")
    pull_parser.add_argument(
        "directory",
        help="Path to the directory to output YAML files",
    )

    args = parser.parse_args()

    dj_builder.basic_login()

    if args.command == "deploy":
        deploy(args.directory, args.dryrun)
    elif args.command == "pull":
        pull(args.namespace, args.directory)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
