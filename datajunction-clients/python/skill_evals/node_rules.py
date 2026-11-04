"""Shared structural rules for DataJunction node specs, mirroring the server
deployment schema (``datajunction_server.models.deployment``).

We validate structurally rather than importing the server models: importing
``datajunction_server.models.deployment`` standalone currently triggers a circular
import outside the full pytest harness, and promptfoo runs the asserts in a bare
Python. The rules below mirror that schema and match real deployed nodes in ads-dj:

  - the node-type discriminator is ``node_type`` (NOT a top-level ``type:``);
  - SourceSpec needs ``catalog`` + ``table``; transform/dimension/metric need ``query``;
  - a dimension declares a primary key (top-level ``primary_key`` OR a column carrying
    ``primary_key`` in its ``attributes`` — the schema treats these as equivalent).

Both ``assert_node`` (single node) and ``assert_deployment`` (multi-node) build on
these helpers so a rule change lands in one place.
"""

import re

import yaml

# Separates turn 1 (the decomposition proposal) from turn 2 (the deployment YAML) in
# a two-turn provider response. Shared by provider.py and assert_deployment.py.
TURN2_MARKER = "<<<TURN2-YAML>>>"

NODE_TYPES = {"source", "transform", "dimension", "metric", "cube"}
REQUIRED_FIELDS: dict[str, list[str]] = {
    "source": ["catalog", "table"],
    "transform": ["query"],
    "dimension": ["query"],
    "metric": ["query"],
    "cube": [],
}

# Aggregate functions a *base* metric uses. A derived/ratio metric composes other
# metrics and should contain none of these — it references metric names instead.
_AGG = re.compile(
    r"\b(SUM|COUNT|AVG|MIN|MAX|APPROX_COUNT_DISTINCT|VAR_POP|STDDEV_POP|"
    r"PERCENTILE_APPROX)\s*\(",
    re.IGNORECASE,
)


def extract_yaml_blocks(output: str) -> list[str]:
    """All fenced ```yaml blocks, in order. Falls back to the whole output if it has
    no fences but looks like YAML (a bare node)."""
    blocks = re.findall(r"```ya?ml\n(.*?)\n```", output, re.DOTALL)
    if blocks:
        return blocks
    return [output] if ":" in output else []


def parse_nodes(output: str) -> list[dict]:
    """Flatten every node spec in the output. Handles both a deployment doc
    (``nodes: [...]``) and one-or-more standalone node blocks."""
    nodes: list[dict] = []
    for block in extract_yaml_blocks(output):
        try:
            doc = yaml.safe_load(block)
        except yaml.YAMLError:
            continue
        if isinstance(doc, dict) and isinstance(doc.get("nodes"), list):
            nodes.extend(n for n in doc["nodes"] if isinstance(n, dict))
        elif isinstance(doc, dict):
            nodes.append(doc)
    return nodes


_CONTENT_FIELDS = ("query", "catalog", "table", "columns", "dimension_links")


def is_reference_stub(data: dict) -> bool:
    """A bare pointer to an existing node — name + node_type and no content fields. The
    model may list these in a deployment to reference parents it was told already exist;
    they aren't nodes we're authoring, so the deployment assert ignores them (and does
    NOT count them toward min-nodes, so a genuinely incomplete node still fails)."""
    if not data.get("name") or not data.get("node_type"):
        return False
    return not any(data.get(field) for field in _CONTENT_FIELDS)


def has_primary_key(data: dict) -> bool:
    if data.get("primary_key"):
        return True
    return any(
        isinstance(col, dict) and "primary_key" in (col.get("attributes") or [])
        for col in (data.get("columns") or [])
    )


def node_label(data: dict) -> str:
    return data.get("name") or data.get("node_type") or "<unnamed>"


class CheckRun:
    """Tallies individual checks so an assertion scores partial credit.

    Binary pass/fail hides progress: a five-node deployment with one bad node looks
    the same as unparseable junk. Each rule records one check here, and the result
    carries the fraction satisfied. ``pass`` stays strict — every check must hold —
    so a partial score never reads as success.
    """

    def __init__(self) -> None:
        self.total = 0
        self.failed: list[str] = []

    def check(self, ok: bool, problem: str) -> bool:
        """Record one check. ``problem`` is only used when it fails."""
        self.total += 1
        if not ok:
            self.failed.append(problem)
        return ok

    @property
    def passed(self) -> int:
        return self.total - len(self.failed)

    @property
    def score(self) -> float:
        return 1.0 if not self.total else self.passed / self.total

    def result(self, summary: str) -> dict:
        """A promptfoo assertion result: fractional score, strict pass."""
        tally = f"{self.passed}/{self.total} checks"
        if self.failed:
            return {
                "pass": False,
                "score": round(self.score, 4),
                "reason": f"{tally} — {'; '.join(self.failed)}",
            }
        return {"pass": True, "score": 1.0, "reason": f"{tally} — {summary}"}


def validate_node(data: dict, checks: CheckRun, expected: str | None = None) -> None:
    """Record the structural checks for a single node spec."""
    label = node_label(data)
    node_type = data.get("node_type")

    if node_type in NODE_TYPES:
        problem = ""
    elif node_type is not None:
        problem = f"{label}: node_type {node_type!r} not one of {sorted(NODE_TYPES)}"
    elif data.get("type") in NODE_TYPES:
        problem = (
            f"{label}: uses legacy top-level `type: {data['type']}` — the deployment "
            f"schema discriminator is `node_type` "
            f"(datajunction_server.models.deployment)"
        )
    else:
        problem = f"{label}: missing `node_type`"
    checks.check(not problem, problem)

    if expected:
        checks.check(
            node_type == expected,
            f"{label}: node_type is {node_type!r}, expected {expected!r}",
        )

    checks.check("name" in data, f"{label}: missing `name`")

    if isinstance(node_type, str):
        for field in REQUIRED_FIELDS.get(node_type, []):
            checks.check(
                bool(data.get(field)),
                f"{label}: {node_type} node missing `{field}`",
            )

    if node_type == "dimension":
        checks.check(
            has_primary_key(data),
            f"{label}: dimension has no primary key (top-level `primary_key` or a "
            f"column with `attributes: [primary_key]`)",
        )


def is_derived_metric(data: dict) -> bool:
    """A metric that composes other metrics: it's a ratio/expression (has an operator)
    and contains no raw aggregate of its own."""
    if data.get("node_type") != "metric":
        return False
    query = str(data.get("query") or "")
    has_operator = any(op in query for op in ("/", "+", "-", "*"))
    return has_operator and not _AGG.search(query)


def query_has_aggregate(data: dict) -> bool:
    return bool(_AGG.search(str(data.get("query") or "")))
