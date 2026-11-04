"""promptfoo python assertion for multi-node decomposition outputs (the turn-2 YAML of
a two-turn case, or any answer that emits several nodes / a deployment doc).

It validates every node structurally (via ``node_rules``) and then checks the
case-specific modeling shape from test vars:
  - ``min_nodes_by_type``     — mapping like ``{metric: 3, transform: 1}``: at least
                                this many nodes of each type must be present;
  - ``require_derived_ratio`` — at least one metric must be a derived ratio (an
                                expression composing other metrics, no raw aggregate);
  - ``require_dimension_link``— at least one node must declare ``dimension_links``;
  - ``forbid_join``           — no metric/transform query may contain a ``JOIN`` (joins
                                belong in dimension links, not baked into queries).

If the output contains the turn-2 marker, only the part after it (the YAML turn) is
validated — the proposal turn is graded by the llm-rubric, not here.
"""

import re

import node_rules

_JOIN = re.compile(r"\bJOIN\b", re.IGNORECASE)


def get_assert(output, context):
    variables = context.get("vars", {})

    yaml_part = output
    if node_rules.TURN2_MARKER in output:
        yaml_part = output.split(node_rules.TURN2_MARKER, 1)[1]

    nodes = [
        n
        for n in node_rules.parse_nodes(yaml_part)
        if not node_rules.is_reference_stub(n)
    ]
    if not nodes:
        return {
            "pass": False,
            "score": 0,
            "reason": "no authored nodes found in deployment output",
        }

    checks = node_rules.CheckRun()
    for node in nodes:
        node_rules.validate_node(node, checks)

    counts: dict[str, int] = {}
    for node in nodes:
        counts[node.get("node_type")] = counts.get(node.get("node_type"), 0) + 1

    for node_type, minimum in (variables.get("min_nodes_by_type") or {}).items():
        checks.check(
            counts.get(node_type, 0) >= minimum,
            f"expected >= {minimum} {node_type} node(s), found {counts.get(node_type, 0)}",
        )

    if variables.get("require_derived_ratio"):
        checks.check(
            any(node_rules.is_derived_metric(n) for n in nodes),
            "no derived ratio metric (expected a metric composing other metrics by name, "
            "not a single query with the whole ratio inlined)",
        )

    if variables.get("require_dimension_link"):
        checks.check(
            any(n.get("dimension_links") for n in nodes),
            "no node declares `dimension_links` (the join should be a dim link)",
        )

    if variables.get("forbid_join"):
        # One check per node, so partial credit shows how many queries are clean.
        for node in nodes:
            if node.get("node_type") in ("metric", "transform"):
                checks.check(
                    not _JOIN.search(str(node.get("query") or "")),
                    f"{node_rules.node_label(node)}: query contains a JOIN — joins belong "
                    f"in dimension links, not baked into the query",
                )

    summary = ", ".join(f"{v}×{k}" for k, v in sorted(counts.items())) or "0 nodes"
    return checks.result(f"valid deployment ({summary})")
