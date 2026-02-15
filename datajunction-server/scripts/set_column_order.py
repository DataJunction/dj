#!/usr/bin/env python
"""
Script to set column order on nodes by revalidating them using the DJ Python client.

This ensures stable column ordering in YAML exports and prevents random
ordering when creating branches from existing nodes.

Usage:
    python scripts/set_column_order.py --uri http://localhost:8000 [OPTIONS]

Options:
    --uri URL               DJ server URL (default: http://localhost:8000)
    --namespace NAMESPACE   Only process nodes in this namespace (can be specified multiple times)
    --workers N             Number of parallel workers (default: 10)
    --dry-run               Show what would be changed without making changes

Examples:
    # Process all nodes (set DJ_USER and DJ_PWD environment variables)
    DJ_USER=myuser DJ_PWD=mypass python scripts/set_column_order.py --uri http://localhost:8000

    # Process specific namespace
    DJ_USER=myuser DJ_PWD=mypass python scripts/set_column_order.py --uri http://localhost:8000 --namespace ads.trial

    # Process multiple namespaces
    DJ_USER=myuser DJ_PWD=mypass python scripts/set_column_order.py --uri http://localhost:8000 --namespace ads.trial --namespace ads.main

    # Dry run
    DJ_USER=myuser DJ_PWD=mypass python scripts/set_column_order.py --uri http://localhost:8000 --dry-run
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

from datajunction import DJClient


def revalidate_node(
    dj: DJClient,
    node_name: str,
    dry_run: bool = False,
) -> tuple[str, bool, Optional[str]]:
    """
    Revalidate a single node using the DJ client.

    Returns:
        (node_name, success, error_message)
    """
    try:
        if dry_run:
            # In dry-run mode, just check if the node exists
            _ = dj.node(node_name)
            return (node_name, True, None)
        else:
            # Actually revalidate the node via POST /nodes/{name}/validate/
            response = dj._session.post(
                f"/nodes/{node_name}/validate/",
                timeout=dj._timeout,
            )
            response.raise_for_status()
            return (node_name, True, None)

    except Exception as e:
        error_msg = str(e)
        # Try to extract more helpful error message from response
        try:
            if hasattr(e, "response"):
                response = getattr(e, "response")
                if hasattr(response, "text"):
                    error_msg = response.text  # type: ignore
        except (AttributeError, TypeError):  # pragma: no cover
            pass  # Just use the string representation
        return (node_name, False, f"Error: {error_msg}")


def set_column_order(
    dj: DJClient,
    namespace: Optional[List[str]] = None,
    dry_run: bool = False,
    max_workers: int = 10,
) -> None:
    """
    Set column order for all nodes by revalidating them.
    """
    print("Fetching node list...")

    # Get all node names
    if namespace:
        node_names = []
        for ns in namespace:
            node_names.extend(dj.list_nodes(namespace=ns))
    else:
        node_names = dj.list_nodes()

    print(f"Processing {len(node_names)} nodes with {max_workers} parallel workers...")
    if dry_run:
        print("DRY RUN MODE - no changes will be made")
    print()

    success_count = 0
    error_count = 0

    # Process nodes in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_node = {
            executor.submit(revalidate_node, dj, node_name, dry_run): node_name
            for node_name in node_names
        }

        # Process results as they complete
        for future in as_completed(future_to_node):
            node_name_str, success, error = future.result()

            if error:
                error_count += 1
                print(f"❌ {node_name_str}: {error}")
            elif success:
                success_count += 1
                print(f"✓ {node_name_str}: Revalidated")

    print()
    print("Summary:")
    print(f"  Success: {success_count}")
    print(f"  Errors: {error_count}")
    print(f"  Total: {len(node_names)}")

    if dry_run and success_count > 0:
        print()
        print("Run without --dry-run to apply these changes.")


def main():
    parser = argparse.ArgumentParser(
        description="Set column order on nodes by revalidating them via DJ Python client",
    )
    parser.add_argument(
        "--uri",
        default="http://localhost:8000",
        help="DJ server URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--namespace",
        action="append",
        help="Only process nodes in this namespace (can be specified multiple times)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of parallel workers (default: 10)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without making changes",
    )

    args = parser.parse_args()

    # Create DJ client and authenticate
    try:
        dj = DJClient(uri=args.uri)
        # Login with basic authentication (uses DJ_USER and DJ_PWD env vars)
        dj.basic_login()
    except Exception as e:
        print(f"Failed to create DJ client or login: {e}")
        print("\nMake sure authentication is configured via:")
        print("  - Environment variables: DJ_USER and DJ_PWD")
        print(
            "  - Example: DJ_USER=myuser DJ_PWD=mypass python scripts/set_column_order.py ...",
        )
        sys.exit(1)

    try:
        if args.namespace:
            for ns in args.namespace:
                print(f"\n{'=' * 60}")
                print(f"Processing namespace: {ns}")
                print("=" * 60)
                set_column_order(
                    dj,
                    namespace=[ns],
                    dry_run=args.dry_run,
                    max_workers=args.workers,
                )
        else:
            set_column_order(
                dj,
                namespace=args.namespace,
                dry_run=args.dry_run,
                max_workers=args.workers,
            )
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()
