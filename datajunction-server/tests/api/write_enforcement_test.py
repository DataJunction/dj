"""
Write-enforcement tests for the mutating endpoints covered in #2234 step 0.

The route-coverage guard proves each endpoint *reaches* AccessChecker.check();
these prove the check actually *bites* -- i.e. returns HTTP 403 when the caller
lacks WRITE. Together they cover "the check is wired" and "the check enforces".

check() runs before the endpoint loads its target resource, so a denied WRITE
returns 403 whether or not the resource exists; the requests below only need to
pass FastAPI validation (valid path/query/body), not reference real nodes/cubes.
"""

from typing import Any

import pytest
from httpx import AsyncClient

from tests.authz import DenyWriteAuthorizationService, VALIDATOR_AUTH_SERVICE

# (method, path, json_body) for each endpoint whose check was added in PRs 2-5.
# Bodies are the minimal payloads that pass FastAPI validation so the request
# reaches check(); paths point at non-existent resources on purpose (the denial
# fires first).
COVERED_WRITE_ENDPOINTS: list[tuple[str, str, Any]] = [
    # PR2: dimension-link removal
    ("DELETE", "/nodes/enforce_test/link/", {"dimension_node": "enforce.dim"}),
    # PR3: namespace create
    ("POST", "/namespaces/enforce_test_ns/", None),
    # PR4: materialization writes
    (
        "DELETE",
        "/nodes/enforce_test/materializations/?materialization_name=m",
        None,
    ),
    ("POST", "/nodes/enforce_test/materializations/m/backfill", []),
    # PR5: cube writes
    ("POST", "/cubes/enforce_test/materialize", {"schedule": "0 0 * * *"}),
    ("DELETE", "/cubes/enforce_test/materialize", None),
    ("POST", "/cubes/enforce_test/backfill", {"start_date": "2024-01-01"}),
    # PR4: preaggregation bulk workflow deactivation (governed by the node in the
    # query param, so the check fires before loading -- no real node needed here).
    # The other preagg endpoints check after loading a real pre-agg and are tested
    # in preaggregations_test.py where pre-agg fixtures exist.
    ("DELETE", "/preaggs/workflows?node_name=enforce_test", None),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("method,path,json_body", COVERED_WRITE_ENDPOINTS)
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
