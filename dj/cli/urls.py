"""
Print URLs for documentation and APIs.
"""

import sys
import urllib.parse

from fastapi.routing import APIRoute
from yarl import URL

from dj.api.main import app


def run(url: str) -> None:
    """
    Print URLs for documentation and APIs.
    """
    base_url = URL(url)

    sys.stdout.write(f"{base_url / 'docs'}: Documentation.\n")

    seen = set()
    for route in app.routes:
        if isinstance(route, APIRoute) and route.path not in seen:
            description = (
                "GraphQL endpoint."
                if route.path == "/graphql"
                else route.description.split("\n")[0]
            )
            resource_url = urllib.parse.unquote(str(base_url / route.path[1:]))
            sys.stdout.write(f"{resource_url}: {description}\n")
            seen.add(route.path)
