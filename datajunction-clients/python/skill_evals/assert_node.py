"""promptfoo python assertion: the model produced a single node conforming to the
server deployment schema (``datajunction_server.models.deployment``). For multi-node
decomposition outputs use ``assert_deployment.py``.

Rules live in ``node_rules`` (shared with the deployment assert). Per-case expectations
come from test vars:
  - ``expected_node_type``  — the node_type the case asked for;
  - ``require_in_query``    — regex the node's ``query`` MUST match (e.g. ``CASE\\s+WHEN``);
  - ``forbid_in_query``     — regex the node's ``query`` must NOT match (e.g. a ``WHERE``).
"""

import re

import node_rules


def get_assert(output, context):
    variables = context.get("vars", {})
    expected = variables.get("expected_node_type")

    nodes = node_rules.parse_nodes(output)
    if not nodes:
        return {"pass": False, "score": 0, "reason": "no YAML node found in output"}
    if len(nodes) > 1:
        return {
            "pass": False,
            "score": 0,
            "reason": f"expected a single node, found {len(nodes)} (use a deployment case)",
        }

    data = nodes[0]
    checks = node_rules.CheckRun()
    node_rules.validate_node(data, checks, expected)

    query = str(data.get("query") or "")
    require = variables.get("require_in_query")
    if require:
        checks.check(
            bool(re.search(require, query)),
            f"query does not match required pattern {require!r}",
        )
    forbid = variables.get("forbid_in_query")
    if forbid:
        checks.check(
            not re.search(forbid, query),
            f"query matches forbidden pattern {forbid!r}",
        )

    return checks.result(
        f"valid {data.get('node_type')} node per deployment schema",
    )
