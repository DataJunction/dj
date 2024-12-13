import argparse
from datajunction import DJBuilder, Project


djbuilder = DJBuilder()


def deploy(directory: str, dryrun: bool = False):
    """
    Deploy nodes from the specified directory.
    """
    project = Project.load(directory)
    compiled_project = project.compile()
    if dryrun:
        compiled_project.validate(client=djbuilder)
    else:
        compiled_project.deploy(client=djbuilder)


def pull(namespace: str, directory: str):
    """
    Pull nodes from a specific namespace.
    """
    Project.pull(client=djbuilder, namespace=namespace, target_path=directory)
    # os.chdir(tmp_path)
    # Project.pull(client=builder_client, namespace="default", target_path=tmp_path)
    # project = Project.load_current()


def list_namespace(namespace):
    """List items in the specified namespace."""
    print(f"Listing items in namespace: {namespace}")


def main():
    parser = argparse.ArgumentParser(prog="dj", description="DataJunction CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    deploy_parser = subparsers.add_parser("deploy", help="Deploy nodes from a directory")
    deploy_parser.add_argument("directory", help="Path to the directory containing nodes")
    deploy_parser.add_argument("dryrun", type=bool, help="Whether to do a dry run (validates without actually deploying)")

    list_parser = subparsers.add_parser("list", help="List items in a namespace")
    list_parser.add_argument("namespace", help="The namespace to list items from")

    pull_parser = subparsers.add_parser("pull", help="Pull nodes from a namespace to YAML")
    pull_parser.add_argument("namespace", help="The namespace to pull from")
    pull_parser.add_argument("directory", help="Path to the directory to output YAML files")

    args = parser.parse_args()

    djbuilder.basic_login()

    if args.command == "deploy":
        deploy(args.directory, args.dryrun)
    elif args.command == "list":
        list_namespace(args.namespace)
    elif args.command == "pull":
        pull(args.namespace, args.directory)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
