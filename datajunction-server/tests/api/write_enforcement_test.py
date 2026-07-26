"""
Write-enforcement tests for the mutating endpoints covered in #2234 step 0.

The route-coverage guard proves each endpoint *reaches* AccessChecker.check();
these prove the check actually *bites* -- i.e. returns HTTP 403 when the caller
lacks the action. Only a denial test can catch a check that fires on the wrong
action or resource, which is why every mutating route needs one (see
test_every_mutating_route_has_a_denial_test below).

Most checks run before the endpoint loads its target resource, so a denial
returns 403 whether or not the resource exists. Cases below therefore name a
route rather than a real node: the request path is derived from the route
template, and only needs to pass FastAPI validation to reach check().
"""

import re
from typing import Any, NamedTuple

import pytest
from httpx import AsyncClient

from datajunction_server.models.access import ResourceAction
from tests.authz import VALIDATOR_AUTH_SERVICE, deny
from tests.test_route_coverage import (
    INTENTIONAL_EXCLUSIONS,
    _mutating_routes,
    _normalize,
)

# Stand-in for every path parameter. The resource does not exist, which is the
# point: a denial must fire before the endpoint looks it up.
STUB = "enforce_test"


class Case(NamedTuple):
    """One endpoint to deny, addressed by its route template."""

    method: str
    route: str
    body: Any = None
    query: str = ""

    @property
    def path(self) -> str:
        """Concrete request path, with every path parameter filled by STUB."""
        return re.sub(r"\{[^}]+\}", STUB, self.route) + self.query


DENY_WRITE_CASES = [
    # dimension links
    Case("DELETE", "/nodes/{node_name}/link", {"dimension_node": "enforce.dim"}),
    Case("DELETE", "/nodes/{name}/columns/{column}", query="?dimension=enforce.dim"),
    # namespaces
    Case("POST", "/namespaces/{namespace}"),
    Case("POST", "/namespaces/{namespace}/restore"),
    # materializations
    Case(
        "POST",
        "/nodes/{node_name}/materialization",
        {"job": "spark_sql", "schedule": "@daily", "strategy": "full"},
    ),
    Case(
        "DELETE",
        "/nodes/{node_name}/materializations",
        query="?materialization_name=m",
    ),
    Case(
        "POST",
        "/nodes/{node_name}/materializations/{materialization_name}/backfill",
        [],
    ),
    # cubes
    Case("POST", "/cubes/{name}/materialize", {"schedule": "0 0 * * *"}),
    Case("DELETE", "/cubes/{name}/materialize"),
    Case("POST", "/cubes/{name}/backfill", {"start_date": "2024-01-01"}),
    # node writes governed by WRITE on the node itself
    Case("POST", "/nodes/{name}/validate"),
    Case("POST", "/nodes/{name}/restore"),
    Case("POST", "/nodes/{name}/refresh"),
    Case("POST", "/nodes/{name}/tags", query="?tag_names=t"),
    Case("POST", "/nodes/{node_name}/columns/{column_name}/attributes", []),
    # Pre-agg bulk deactivation is governed by the node in the query param, so the
    # check fires before loading. The {preagg_id} endpoints check after loading a
    # real pre-agg, so they live in preaggregations_test.py with those fixtures.
    Case("DELETE", "/preaggs/workflows", query=f"?node_name={STUB}"),
]

# Delete-governed endpoints. A DELETE grant implies WRITE, so denying WRITE would
# leave these unexercised.
DENY_DELETE_CASES = [
    Case("DELETE", "/nodes/{name}"),
    Case("DELETE", "/nodes/{name}/hard"),
    Case("DELETE", "/namespaces/{namespace}/hard"),
]

# Routes whose denial test lives in another module, next to the fixtures it needs.
DENIAL_TESTED_ELSEWHERE: dict[tuple[str, str], str] = {
    ("POST", "/register/table/{catalog}/{schema_}/{table}"): "nodes_test.py",
    ("POST", "/preaggs/{preagg_id}/materialize"): "preaggregations_test.py",
    ("PATCH", "/preaggs/{preagg_id}/config"): "preaggregations_test.py",
    ("DELETE", "/preaggs/{preagg_id}/workflow"): "preaggregations_test.py",
    ("POST", "/preaggs/{preagg_id}/backfill"): "preaggregations_test.py",
    ("POST", "/preaggs/plan"): "preaggregations_test.py",
    ("POST", "/preaggs/register"): "preaggregations_test.py",
}

# PENDING_DENIAL_TESTS: routes that reach check() (so the coverage guard is green)
# but have nothing proving the check enforces the right thing. A TEMPORARY backlog,
# keyed by what is missing -- entries leave as their denial tests land.
PENDING_DENIAL_TESTS: dict[str, list[tuple[str, str]]] = {
    "MANAGE-governed (#2230); needs deny(MANAGE) cases": [
        ("POST", "/roles"),
        ("PATCH", "/roles/{role_name}"),
        ("DELETE", "/roles/{role_name}"),
        ("POST", "/roles/{role_name}/scopes"),
        ("DELETE", "/roles/{role_name}/scopes/{action}/{scope_type}/{scope_value}"),
        ("POST", "/roles/{role_name}/assign"),
        ("DELETE", "/roles/{role_name}/assignments/{principal_username}"),
        ("POST", "/groups"),
        ("POST", "/groups/{group_name}/members"),
        ("DELETE", "/groups/{group_name}/members/{member_username}"),
    ],
    "needs a create/update body; WRITE is on the namespace in that body": [
        ("POST", "/nodes/source"),
        ("POST", "/nodes/transform"),
        ("POST", "/nodes/dimension"),
        ("POST", "/nodes/metric"),
        ("POST", "/nodes/cube"),
        ("PATCH", "/nodes/{name}"),
    ],
    "resolves a column on a real node before checking; needs a seeded node": [
        ("POST", "/nodes/{name}/columns/{column}"),
        ("POST", "/nodes/{node_name}/link"),
        ("POST", "/nodes/{node_name}/columns/{node_column}/link"),
        ("DELETE", "/nodes/{node_name}/columns/{node_column}/link"),
        ("PATCH", "/nodes/{node_name}/columns/{column_name}"),
        ("PATCH", "/nodes/{node_name}/columns/{column_name}/description"),
        ("POST", "/nodes/{node_name}/columns/{column_name}/partition"),
        ("DELETE", "/nodes/{node_name}/columns/{column_name}/partition"),
    ],
    "git-backed namespace flow; needs repo fixtures": [
        ("POST", "/namespaces/{namespace}/branches"),
        ("DELETE", "/namespaces/{namespace}/branches/{branch_namespace}"),
        ("PATCH", "/namespaces/{namespace}/git"),
        ("DELETE", "/namespaces/{namespace}/git"),
        ("POST", "/namespaces/{namespace}/sync-to-git"),
        ("POST", "/namespaces/{namespace}/sync-from-git"),
        ("POST", "/namespaces/{namespace}/pull-request"),
        ("POST", "/namespaces/{namespace}/export/yaml"),
        ("POST", "/nodes/{node_name}/sync-to-git"),
    ],
    "needs an availability payload": [
        ("POST", "/data/{node_name}/availability"),
        ("DELETE", "/data/{node_name}/availability"),
    ],
    "deploy path; denial belongs with the internal-path work": [
        ("POST", "/deployments"),
        ("POST", "/deployments/impact"),
    ],
    "check follows the target-namespace lookup, so a stub path 404s first": [
        ("POST", "/nodes/{node_name}/copy"),
    ],
    "mirrors register/table; add alongside it in nodes_test.py": [
        ("POST", "/register/view/{catalog}/{schema_}/{view}"),
    ],
    "deactivate; needs a namespace with no active nodes": [
        ("DELETE", "/namespaces/{namespace}"),
    ],
    "needs catalog/schema fixtures": [
        ("POST", "/namespaces/sources/bulk"),
    ],
}


def _pending_routes() -> set[tuple[str, str]]:
    """Every route in the backlog, flattened out of its reason buckets."""
    return {route for routes in PENDING_DENIAL_TESTS.values() for route in routes}


def _tested_routes() -> set[tuple[str, str]]:
    """Routes with a denial test, wherever that test lives."""
    return {
        (case.method, _normalize(case.route))
        for case in DENY_WRITE_CASES + DENY_DELETE_CASES
    } | set(DENIAL_TESTED_ELSEWHERE)


async def assert_denied(client: AsyncClient, case: Case, action: ResourceAction):
    """The endpoint must answer 403 when `action` is denied."""
    response = await client.request(case.method, case.path, json=case.body)

    assert response.status_code == 403, (
        f"{case.method} {case.path} did not enforce {action.value.upper()} "
        f"(got {response.status_code}: {response.text[:200]})"
    )
    assert "Access denied" in response.json()["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize("case", DENY_WRITE_CASES, ids=lambda case: case.path)
async def test_endpoint_denies_without_write(
    client_with_basic: AsyncClient,
    mocker,
    case,
):
    """Each write-governed endpoint returns 403 when WRITE is denied."""
    mocker.patch(VALIDATOR_AUTH_SERVICE, deny(ResourceAction.WRITE))
    await assert_denied(client_with_basic, case, ResourceAction.WRITE)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", DENY_DELETE_CASES, ids=lambda case: case.path)
async def test_endpoint_denies_without_delete(
    client_with_basic: AsyncClient,
    mocker,
    case,
):
    """Each delete-governed endpoint returns 403 when DELETE is denied."""
    mocker.patch(VALIDATOR_AUTH_SERVICE, deny(ResourceAction.DELETE))
    await assert_denied(client_with_basic, case, ResourceAction.DELETE)


def test_every_mutating_route_has_a_denial_test():
    """
    Every mutating route must have a denial test, be intentionally excluded, or be
    in the backlog with a reason.

    The route-coverage guard cannot tell a correct check from one that fires on the
    wrong action or resource -- upsert_materialization reached a READ check on a
    cube's metrics while writing to the node, and the guard stayed green. This test
    is what closes that gap, so a new mutating route cannot land unnoticed.
    """
    accounted = _tested_routes() | set(INTENTIONAL_EXCLUSIONS) | _pending_routes()
    missing = sorted(
        {(method, path) for method, path, _ in _mutating_routes()} - accounted,
    )
    assert not missing, (
        "These mutating routes have no denial test (add one, or add a "
        "PENDING_DENIAL_TESTS entry with a reason):\n"
        + "\n".join(f"  {method} {path}" for method, path in missing)
    )


def test_denial_test_registry_has_no_stale_entries():
    """Every registered route must still exist and must not be double-listed."""
    live = {(method, path) for method, path, _ in _mutating_routes()}
    tested = _tested_routes()

    assert not sorted((tested | _pending_routes()) - live), (
        "Registered routes that no longer exist: "
        f"{sorted((tested | _pending_routes()) - live)}"
    )
    assert not sorted(tested & _pending_routes()), (
        f"Routes both tested and marked pending: {sorted(tested & _pending_routes())}"
    )
