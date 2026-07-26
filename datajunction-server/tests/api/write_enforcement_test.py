"""
Write-enforcement tests for the mutating endpoints covered in #2234 step 0.

The route-coverage guard proves each endpoint *reaches* AccessChecker.check();
these prove the check actually *bites* -- i.e. returns HTTP 403 when the caller
lacks the action. Together they cover "the check is wired" and "the check
enforces". Only the denial test can catch a check that fires on the wrong action
or resource, which is why every mutating route needs one (see
test_every_mutating_route_has_a_denial_test below).

Most checks run before the endpoint loads its target resource, so a denial
returns 403 whether or not the resource exists; those requests only need to pass
FastAPI validation (valid path/query/body), not reference real nodes/cubes.
"""

from typing import Any

import pytest
from httpx import AsyncClient

from tests.authz import (
    DenyDeleteAuthorizationService,
    DenyWriteAuthorizationService,
    VALIDATOR_AUTH_SERVICE,
)
from tests.test_route_coverage import (
    INTENTIONAL_EXCLUSIONS,
    _mutating_routes,
    _normalize,
)

# (method, route_template, request_path, json_body) per endpoint. The template is
# the FastAPI route (used by the completeness guard below); the request path is a
# concrete URL. Bodies are the minimal payloads that pass FastAPI validation so the
# request reaches check(); paths point at non-existent resources on purpose, since
# the denial fires before the resource is loaded.
DENY_WRITE_ENDPOINTS: list[tuple[str, str, str, Any]] = [
    # dimension links
    (
        "DELETE",
        "/nodes/{node_name}/link",
        "/nodes/enforce_test/link/",
        {"dimension_node": "enforce.dim"},
    ),
    # namespace create / restore
    ("POST", "/namespaces/{namespace}", "/namespaces/enforce_test_ns/", None),
    (
        "POST",
        "/namespaces/{namespace}/restore",
        "/namespaces/enforce_test_ns/restore/",
        None,
    ),
    # materialization writes
    (
        "POST",
        "/nodes/{node_name}/materialization",
        "/nodes/enforce_test/materialization",
        {"job": "spark_sql", "schedule": "@daily", "strategy": "full"},
    ),
    (
        "DELETE",
        "/nodes/{node_name}/materializations",
        "/nodes/enforce_test/materializations/?materialization_name=m",
        None,
    ),
    (
        "POST",
        "/nodes/{node_name}/materializations/{materialization_name}/backfill",
        "/nodes/enforce_test/materializations/m/backfill",
        [],
    ),
    # cube writes
    (
        "POST",
        "/cubes/{name}/materialize",
        "/cubes/enforce_test/materialize",
        {"schedule": "0 0 * * *"},
    ),
    ("DELETE", "/cubes/{name}/materialize", "/cubes/enforce_test/materialize", None),
    (
        "POST",
        "/cubes/{name}/backfill",
        "/cubes/enforce_test/backfill",
        {"start_date": "2024-01-01"},
    ),
    # node writes governed by WRITE on the node itself
    ("POST", "/nodes/{name}/validate", "/nodes/enforce_test/validate/", None),
    ("POST", "/nodes/{name}/restore", "/nodes/enforce_test/restore/", None),
    ("POST", "/nodes/{name}/refresh", "/nodes/enforce_test/refresh/", None),
    ("POST", "/nodes/{name}/tags", "/nodes/enforce_test/tags/?tag_names=t", None),
    (
        "POST",
        "/nodes/{node_name}/columns/{column_name}/attributes",
        "/nodes/enforce_test/columns/c/attributes/",
        [],
    ),
    (
        "DELETE",
        "/nodes/{name}/columns/{column}",
        "/nodes/enforce_test/columns/c/?dimension=enforce.dim",
        None,
    ),
    # preaggregation bulk workflow deactivation (governed by the node in the query
    # param, so the check fires before loading -- no real node needed here). The
    # other preagg endpoints check after loading a real pre-agg and are tested in
    # preaggregations_test.py where pre-agg fixtures exist.
    ("DELETE", "/preaggs/workflows", "/preaggs/workflows?node_name=enforce_test", None),
]

# Endpoints governed by DELETE rather than WRITE. A DELETE grant implies WRITE, so
# denying WRITE would not exercise these.
DENY_DELETE_ENDPOINTS: list[tuple[str, str, str, Any]] = [
    ("DELETE", "/nodes/{name}", "/nodes/enforce_test/", None),
    ("DELETE", "/nodes/{name}/hard", "/nodes/enforce_test/hard/", None),
    ("DELETE", "/namespaces/{namespace}/hard", "/namespaces/enforce_ns/hard/", None),
]

# Routes whose denial test lives in another module, next to the fixtures it needs.
DENIAL_TESTED_ELSEWHERE: dict[tuple[str, str], str] = {
    ("POST", "/register/table/{catalog}/{schema_}/{table}"): "nodes_test.py",
    ("POST", "/preaggs/{preagg_id}/materialize"): "preaggregations_test.py",
    ("PATCH", "/preaggs/{preagg_id}/config"): "preaggregations_test.py",
    ("DELETE", "/preaggs/{preagg_id}/workflow"): "preaggregations_test.py",
    ("POST", "/preaggs/{preagg_id}/backfill"): "preaggregations_test.py",
    ("POST", "/preaggs/plan"): "preaggregations_test.py",
}

# PENDING_DENIAL_TESTS: mutating routes that reach check() (the coverage guard is
# green) but have no test proving the check enforces the right thing. This is a
# TEMPORARY backlog -- each entry is removed in the PR that adds its denial test.
# Do not add a route here to silence the guard without a reason.
PENDING_DENIAL_TESTS: dict[tuple[str, str], str] = {
    # RBAC and group administration: governed by MANAGE, so neither the deny-WRITE
    # nor the deny-DELETE service exercises them.
    ("POST", "/roles"): "MANAGE-governed (#2230); needs a deny-MANAGE test",
    (
        "PATCH",
        "/roles/{role_name}",
    ): "MANAGE-governed (#2230); needs a deny-MANAGE test",
    (
        "DELETE",
        "/roles/{role_name}",
    ): "MANAGE-governed (#2230); needs a deny-MANAGE test",
    (
        "POST",
        "/roles/{role_name}/scopes",
    ): "MANAGE-governed (#2230); needs a deny-MANAGE test",
    (
        "DELETE",
        "/roles/{role_name}/scopes/{action}/{scope_type}/{scope_value}",
    ): "MANAGE-governed (#2230); needs a deny-MANAGE test",
    (
        "POST",
        "/roles/{role_name}/assign",
    ): "MANAGE-governed (#2230); needs a deny-MANAGE test",
    (
        "DELETE",
        "/roles/{role_name}/assignments/{principal_username}",
    ): "MANAGE-governed (#2230); needs a deny-MANAGE test",
    ("POST", "/groups"): "MANAGE-governed (#2230); needs a deny-MANAGE test",
    (
        "POST",
        "/groups/{group_name}/members",
    ): "MANAGE-governed (#2230); needs a deny-MANAGE test",
    (
        "DELETE",
        "/groups/{group_name}/members/{member_username}",
    ): "MANAGE-governed (#2230); needs a deny-MANAGE test",
    # Node create: the check is WRITE on the namespace in the request body, so each
    # needs a create payload that passes validation.
    (
        "POST",
        "/nodes/source",
    ): "needs a valid create body; WRITE is on the body namespace",
    (
        "POST",
        "/nodes/transform",
    ): "needs a valid create body; WRITE is on the body namespace",
    (
        "POST",
        "/nodes/dimension",
    ): "needs a valid create body; WRITE is on the body namespace",
    (
        "POST",
        "/nodes/metric",
    ): "needs a valid create body; WRITE is on the body namespace",
    (
        "POST",
        "/nodes/cube",
    ): "needs a valid create body; WRITE is on the body namespace",
    ("PATCH", "/nodes/{name}"): "needs an update body",
    # Column, partition and dimension-link writes: these resolve a column on a real
    # node before checking, so they need a seeded node rather than a stub path.
    ("POST", "/nodes/{name}/columns/{column}"): "needs a real column on a node",
    ("POST", "/nodes/{node_name}/link"): "needs a real column on a node",
    (
        "POST",
        "/nodes/{node_name}/columns/{node_column}/link",
    ): "needs a real column on a node",
    (
        "DELETE",
        "/nodes/{node_name}/columns/{node_column}/link",
    ): "needs a real column on a node",
    (
        "PATCH",
        "/nodes/{node_name}/columns/{column_name}",
    ): "needs a real column on a node",
    (
        "PATCH",
        "/nodes/{node_name}/columns/{column_name}/description",
    ): "needs a real column on a node",
    (
        "POST",
        "/nodes/{node_name}/columns/{column_name}/partition",
    ): "needs a real column on a node",
    (
        "DELETE",
        "/nodes/{node_name}/columns/{column_name}/partition",
    ): "needs a real column on a node",
    (
        "POST",
        "/nodes/{node_name}/copy",
    ): "check follows the target-namespace lookup, so a bad path 404s first",
    # Availability: the query-service reports table state, so these need a payload.
    ("POST", "/data/{node_name}/availability"): "needs a valid availability payload",
    ("DELETE", "/data/{node_name}/availability"): "needs a valid availability payload",
    # Git-backed namespace flows need a repo fixture to get past validation.
    (
        "POST",
        "/namespaces/{namespace}/branches",
    ): "git-backed namespace flow; needs repo fixtures",
    (
        "DELETE",
        "/namespaces/{namespace}/branches/{branch_namespace}",
    ): "git-backed namespace flow; needs repo fixtures",
    (
        "PATCH",
        "/namespaces/{namespace}/git",
    ): "git-backed namespace flow; needs repo fixtures",
    (
        "DELETE",
        "/namespaces/{namespace}/git",
    ): "git-backed namespace flow; needs repo fixtures",
    (
        "POST",
        "/namespaces/{namespace}/sync-to-git",
    ): "git-backed namespace flow; needs repo fixtures",
    (
        "POST",
        "/namespaces/{namespace}/sync-from-git",
    ): "git-backed namespace flow; needs repo fixtures",
    (
        "POST",
        "/namespaces/{namespace}/pull-request",
    ): "git-backed namespace flow; needs repo fixtures",
    (
        "POST",
        "/namespaces/{namespace}/export/yaml",
    ): "git-backed namespace flow; needs repo fixtures",
    (
        "POST",
        "/nodes/{node_name}/sync-to-git",
    ): "git-backed namespace flow; needs repo fixtures",
    # Remaining namespace / deployment / register writes.
    (
        "DELETE",
        "/namespaces/{namespace}",
    ): "deactivate; needs a namespace with no active nodes",
    ("POST", "/namespaces/sources/bulk"): "needs catalog/schema fixtures",
    (
        "POST",
        "/deployments",
    ): "deploy path; denial belongs with the internal-path work",
    (
        "POST",
        "/deployments/impact",
    ): "deploy path; denial belongs with the internal-path work",
    ("POST", "/preaggs/register"): "deferred with the register restructure",
    (
        "POST",
        "/register/view/{catalog}/{schema_}/{view}",
    ): "mirrors register/table; add alongside it in nodes_test.py",
}


def _tested_routes() -> set[tuple[str, str]]:
    """Routes with a denial test, wherever that test lives."""
    return (
        {
            (method, _normalize(template))
            for method, template, _, _ in DENY_WRITE_ENDPOINTS
        }
        | {
            (method, _normalize(template))
            for method, template, _, _ in DENY_DELETE_ENDPOINTS
        }
        | set(DENIAL_TESTED_ELSEWHERE)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,path,json_body",
    [(method, path, body) for method, _, path, body in DENY_WRITE_ENDPOINTS],
)
async def test_mutating_endpoint_denies_without_write(
    client_with_basic: AsyncClient,
    mocker,
    method,
    path,
    json_body,
):
    """Each covered mutating endpoint returns 403 when WRITE is denied."""
    mocker.patch(VALIDATOR_AUTH_SERVICE, lambda: DenyWriteAuthorizationService())

    response = await client_with_basic.request(method, path, json=json_body)

    assert response.status_code == 403, (
        f"{method} {path} did not enforce WRITE "
        f"(got {response.status_code}: {response.text[:200]})"
    )
    assert "Access denied" in response.json()["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,path,json_body",
    [(method, path, body) for method, _, path, body in DENY_DELETE_ENDPOINTS],
)
async def test_mutating_endpoint_denies_without_delete(
    client_with_basic: AsyncClient,
    mocker,
    method,
    path,
    json_body,
):
    """Each delete-governed endpoint returns 403 when DELETE is denied."""
    mocker.patch(VALIDATOR_AUTH_SERVICE, lambda: DenyDeleteAuthorizationService())

    response = await client_with_basic.request(method, path, json=json_body)

    assert response.status_code == 403, (
        f"{method} {path} did not enforce DELETE "
        f"(got {response.status_code}: {response.text[:200]})"
    )
    assert "Access denied" in response.json()["message"]


def test_every_mutating_route_has_a_denial_test():
    """
    Every mutating route must have a denial test, be intentionally excluded, or be
    in the backlog with a reason.

    The route-coverage guard cannot tell a correct check from one that fires on the
    wrong action or resource -- upsert_materialization reached a READ check on a
    cube's metrics while writing to the node, and the guard was green. This test is
    what closes that gap, so a new mutating route cannot land unnoticed.
    """
    accounted = (
        _tested_routes() | set(INTENTIONAL_EXCLUSIONS) | set(PENDING_DENIAL_TESTS)
    )
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

    stale = sorted((tested | set(PENDING_DENIAL_TESTS)) - live)
    assert not stale, f"Registered routes that no longer exist: {stale}"

    both = sorted(tested & set(PENDING_DENIAL_TESTS))
    assert not both, f"Routes both tested and marked pending: {both}"
