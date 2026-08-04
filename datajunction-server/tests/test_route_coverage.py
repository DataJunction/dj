"""
Route-coverage guard.

RBAC in DJ is opt-in per endpoint: a request is only authorized if the handler
reaches AccessChecker.check().

Coverage detection is static and conservative: it treats any ``*.check(...)`` call
as coverage and follows only bare-name helper calls. A false "uncovered" only costs
an allowlist entry, so we never report a route covered without a literal ``.check(``.

What this guard does NOT prove: that the check asks for the right thing. A route
counts as covered as soon as *some* ``.check()`` is reachable, even if it authorizes
a different action on a different resource. ``upsert_materialization`` was the real
example -- it reached a ``READ`` check on a cube's metrics while writing to the node
itself, and this guard was green. Only a denial test catches that, so every route
here must also have one; see ``tests/api/write_enforcement_test.py``.
"""

import ast
import inspect
import textwrap

from fastapi.routing import APIRoute

from datajunction_server.api.main import app

MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

# We only follow calls into DJ's own code when resolving helpers transitively.
_TARGET_PREFIX = "datajunction_server"


def _normalize(path: str) -> str:
    """Collapse the trailing-slash route variants FastAPI registers into one."""
    return path.rstrip("/") or "/"


def _reaches_check(func, _seen=None) -> bool:
    """
    True if ``func`` calls ``AccessChecker.check()`` directly or via a DJ helper.

    Detection is static: we look for an attribute call ``*.check(...)`` and, for
    bare-name calls (e.g. the ``enforce_scope_management`` helper used by the RBAC
    admin APIs), resolve the name through the function's globals and recurse.
    A false "uncovered" only costs an allowlist entry; we never report covered
    without finding a literal ``.check(`` in the call graph.
    """
    if _seen is None:
        _seen = set()
    if func in _seen:
        return False
    _seen.add(func)
    try:
        tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
    except (OSError, TypeError, SyntaxError):  # pragma: no cover - defensive
        return False
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        callee = node.func
        if isinstance(callee, ast.Attribute) and callee.attr == "check":
            return True
        if isinstance(callee, ast.Name):
            target = getattr(func, "__globals__", {}).get(callee.id)
            if (
                callable(target)
                and getattr(target, "__module__", "").startswith(_TARGET_PREFIX)
                and _reaches_check(target, _seen)
            ):
                return True
    return False


def _mutating_routes():
    """Yield (method, normalized_path, endpoint) for every mutating APIRoute."""
    for route in app.routes:
        if isinstance(route, APIRoute):
            for method in route.methods & MUTATING_METHODS:
                yield method, _normalize(route.path), route.endpoint


def flatten(buckets: dict[str, list[tuple[str, str]]]) -> set[tuple[str, str]]:
    """All routes across reason buckets, for membership checks."""
    return {route for routes in buckets.values() for route in routes}


# Mutating routes deliberately left un-governed, keyed by why. Grouping by reason
# keeps the justification stated once and related routes together.
INTENTIONAL_EXCLUSIONS: dict[str, list[tuple[str, str]]] = {
    "not in #2234 step-0 scope; follow-up": [
        ("POST", "/catalogs"),
        ("POST", "/catalogs/{name}/engines"),
        ("POST", "/engines"),
        ("POST", "/tags"),
        ("PATCH", "/tags/{name}"),
        ("POST", "/attributes"),
        ("POST", "/measures"),
        ("PATCH", "/measures/{measure_name}"),
        ("POST", "/hierarchies"),
        ("DELETE", "/hierarchies/{name}"),
        ("PUT", "/hierarchies/{name}"),
    ],
    # Reports materialization completion, so it is a system call rather than a user
    # action: governing it with user WRITE would break the callback.
    "query-service callback; govern via service identity (follow-up)": [
        ("POST", "/preaggs/{preagg_id}/availability"),
    ],
    # These take a POST body to describe what to read, but mutate nothing.
    "read-only; mutates nothing": [
        ("POST", "/nodes/validate"),
        ("POST", "/semantic/views/list"),
        ("POST", "/semantic/views/{view_name}"),
        ("POST", "/semantic/views/{view_name}/sql"),
    ],
    "GraphQL; access enforced within resolvers, not the route": [
        ("POST", "/graphql"),
    ],
    "authentication endpoint; not node governance": [
        ("POST", "/basic/login"),
        ("POST", "/basic/user"),
        ("POST", "/logout"),
    ],
    "service-account management; not node governance": [
        ("POST", "/service-accounts"),
        ("POST", "/service-accounts/token"),
        ("DELETE", "/service-accounts/{client_id}"),
    ],
    "user-scoped preferences and collections; not node governance": [
        ("POST", "/notifications/mark-read"),
        ("POST", "/notifications/subscribe"),
        ("DELETE", "/notifications/unsubscribe"),
        ("POST", "/collections"),
        ("DELETE", "/collections/{name}"),
        ("POST", "/collections/{name}/nodes"),
        ("POST", "/collections/{name}/remove"),
    ],
}

EXCLUDED_ROUTES = flatten(INTENTIONAL_EXCLUSIONS)


def test_all_mutating_routes_reach_access_check():
    """Every mutating route must reach check() or be explicitly excluded."""
    unlisted = sorted(
        {
            (method, path)
            for method, path, endpoint in _mutating_routes()
            if not _reaches_check(endpoint) and (method, path) not in EXCLUDED_ROUTES
        },
    )
    assert not unlisted, (
        "These mutating routes never reach AccessChecker.check() and are not "
        "excluded (add a check() call, or add an exclusion with a reason):\n"
        + "\n".join(f"  {method} {path}" for method, path in unlisted)
    )


def test_exclusions_have_no_stale_entries():
    """An excluded route must still exist and still be uncovered."""
    routes = list(_mutating_routes())
    current = {(method, path) for method, path, _ in routes}
    covered = {(method, path) for method, path, ep in routes if _reaches_check(ep)}

    stale = []
    for method, path in sorted(EXCLUDED_ROUTES):
        if (method, path) not in current:
            stale.append(f"  {method} {path}: route no longer exists")
        elif (method, path) in covered:
            stale.append(
                f"  {method} {path}: now reaches check(); remove the exclusion",
            )
    assert not stale, "Stale exclusions:\n" + "\n".join(stale)
