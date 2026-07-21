"""
Route-coverage guard.

RBAC in DJ is opt-in per endpoint: a request is only authorized if the handler
reaches AccessChecker.check().
"""

import ast
import inspect
import textwrap

import pytest
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


# PENDING_COVERAGE: mutating routes that are in #2234 step-0 scope but don't yet
# reach check(). This is a TEMPORARY backlog: each entry is removed in the same PR
# that adds its check(), and the set MUST be empty once step 0 is complete
# (asserted by test_step0_backlog_reaches_empty). Do not add non-step-0 routes here.
PENDING_COVERAGE: dict[tuple[str, str], str] = {
    # Preaggregation writes. A preagg is keyed by preagg_id (or a plan body), not a
    # node name, so the correct (resource, action) needs a preagg->node/namespace
    # resolution decision before wiring check(). Tracked as the remaining step0 work.
    ("POST", "/preaggs/plan"): "step0: preaggregation coverage pending",
    ("POST", "/preaggs/register"): "step0: preaggregation coverage pending",
    ("DELETE", "/preaggs/workflows"): "step0: preaggregation coverage pending",
    (
        "POST",
        "/preaggs/{preagg_id}/availability",
    ): "step0: preaggregation coverage pending",
    ("POST", "/preaggs/{preagg_id}/backfill"): "step0: preaggregation coverage pending",
    ("PATCH", "/preaggs/{preagg_id}/config"): "step0: preaggregation coverage pending",
    (
        "POST",
        "/preaggs/{preagg_id}/materialize",
    ): "step0: preaggregation coverage pending",
    (
        "DELETE",
        "/preaggs/{preagg_id}/workflow",
    ): "step0: preaggregation coverage pending",
}

# INTENTIONAL_EXCLUSIONS: mutating routes deliberately not node-RBAC-governed.
INTENTIONAL_EXCLUSIONS: dict[tuple[str, str], str] = {
    # Not among #2234 step-0 coverage fixes (namespace, dimension link,
    # materialization, preagg, cube). Left uncovered for a later effort.
    ("POST", "/catalogs"): "not in #2234 step-0 coverage fixes; follow-up",
    (
        "POST",
        "/catalogs/{name}/engines",
    ): "not in #2234 step-0 coverage fixes; follow-up",
    ("POST", "/engines"): "not in #2234 step-0 coverage fixes; follow-up",
    ("POST", "/tags"): "not in #2234 step-0 coverage fixes; follow-up",
    ("PATCH", "/tags/{name}"): "not in #2234 step-0 coverage fixes; follow-up",
    # Read-only / enforced elsewhere (not a write-governance target).
    ("POST", "/nodes/validate"): "read-only validation, mutates nothing",
    ("POST", "/graphql"): "GraphQL; access enforced within resolvers, not the route",
    # Semantic-layer views: POST carries a query body but the handlers only read
    # (list views, describe a view, generate query SQL); they mutate nothing.
    ("POST", "/semantic/views/list"): "read-only: lists semantic views",
    ("POST", "/semantic/views/{view_name}"): "read-only: describes a semantic view",
    ("POST", "/semantic/views/{view_name}/sql"): "read-only: generates query SQL",
    # Auth / identity endpoints (not node governance).
    ("POST", "/basic/login"): "authentication endpoint",
    ("POST", "/basic/user"): "authentication endpoint",
    ("POST", "/logout"): "authentication endpoint",
    ("POST", "/service-accounts"): "service-account management; not node governance",
    (
        "POST",
        "/service-accounts/token",
    ): "service-account management; not node governance",
    (
        "DELETE",
        "/service-accounts/{client_id}",
    ): "service-account management; not node governance",
    # User-scoped, not node governance.
    ("POST", "/notifications/mark-read"): "user-scoped notification prefs",
    ("POST", "/notifications/subscribe"): "user-scoped notification prefs",
    ("DELETE", "/notifications/unsubscribe"): "user-scoped notification prefs",
    ("DELETE", "/collections/{name}"): "user-scoped collections; not node governance",
    ("POST", "/collections"): "user-scoped collections; not node governance",
    (
        "POST",
        "/collections/{name}/nodes",
    ): "user-scoped collections; not node governance",
    (
        "POST",
        "/collections/{name}/remove",
    ): "user-scoped collections; not node governance",
    # Other semantic-object writes: governance out of #2234 step-0 scope.
    ("POST", "/attributes"): "not in #2234 step-0 scope; follow-up",
    ("POST", "/measures"): "not in #2234 step-0 scope; follow-up",
    ("PATCH", "/measures/{measure_name}"): "not in #2234 step-0 scope; follow-up",
    ("POST", "/hierarchies"): "not in #2234 step-0 scope; follow-up",
    ("DELETE", "/hierarchies/{name}"): "not in #2234 step-0 scope; follow-up",
    ("PUT", "/hierarchies/{name}"): "not in #2234 step-0 scope; follow-up",
}

# A route may appear in at most one bucket; together they form the full allowlist.
ALLOWLIST: dict[tuple[str, str], str] = {**PENDING_COVERAGE, **INTENTIONAL_EXCLUSIONS}


def test_all_mutating_routes_reach_access_check():
    """Every mutating route must reach check() or be explicitly allowlisted."""
    unlisted = sorted(
        {
            (method, path)
            for method, path, endpoint in _mutating_routes()
            if not _reaches_check(endpoint) and (method, path) not in ALLOWLIST
        },
    )
    assert not unlisted, (
        "These mutating routes never reach AccessChecker.check() and are not "
        "allowlisted (add a check() call, or allowlist with a reason):\n"
        + "\n".join(f"  {method} {path}" for method, path in unlisted)
    )


def test_allowlist_has_no_stale_entries():
    """An allowlisted route must still exist and still be uncovered."""
    routes = list(_mutating_routes())
    current = {(method, path) for method, path, _ in routes}
    covered = {(method, path) for method, path, ep in routes if _reaches_check(ep)}

    stale = []
    for key in ALLOWLIST:
        if key not in current:
            stale.append(f"  {key[0]} {key[1]}: route no longer exists")
        elif key in covered:
            stale.append(
                f"  {key[0]} {key[1]}: now reaches check(); remove from ALLOWLIST",
            )
    assert not stale, "Stale ALLOWLIST entries:\n" + "\n".join(stale)


def test_allowlist_buckets_are_disjoint():
    """A route belongs to exactly one bucket, so counts and intent stay unambiguous."""
    overlap = sorted(set(PENDING_COVERAGE) & set(INTENTIONAL_EXCLUSIONS))
    assert not overlap, "Routes in both buckets:\n" + "\n".join(
        f"  {method} {path}" for method, path in overlap
    )


def test_step0_backlog_reaches_empty():
    """
    Progress marker for #2234 step 0: PENDING_COVERAGE must be empty when step 0
    is done. Until then this xfails, so the day the last coverage PR lands it flips
    to a passing signal (xpass) that the backlog is cleared -- and any *new*
    step-0 route parked here keeps it xfailing rather than silently lingering.
    """
    if PENDING_COVERAGE:
        pytest.xfail(
            f"{len(PENDING_COVERAGE)} step-0 route(s) still pending coverage: "
            + ", ".join(f"{m} {p}" for m, p in sorted(PENDING_COVERAGE)),
        )
    assert PENDING_COVERAGE == {}
