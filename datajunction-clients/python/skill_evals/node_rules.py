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


def validate_node(data: dict, expected: str | None = None) -> list[str]:
    """Structural problems with a single node spec (empty list == valid)."""
    problems: list[str] = []
    label = node_label(data)

    node_type = data.get("node_type")
    if node_type is None:
        if data.get("type") in NODE_TYPES:
            problems.append(
                f"{label}: uses legacy top-level `type: {data['type']}` — the deployment "
                f"schema discriminator is `node_type` "
                f"(datajunction_server.models.deployment)",
            )
        else:
            problems.append(f"{label}: missing `node_type`")
    elif node_type not in NODE_TYPES:
        problems.append(
            f"{label}: node_type {node_type!r} not one of {sorted(NODE_TYPES)}",
        )
    elif expected and node_type != expected:
        problems.append(f"{label}: node_type is {node_type!r}, expected {expected!r}")

    if "name" not in data:
        problems.append(f"{label}: missing `name`")
    if isinstance(node_type, str):
        for field in REQUIRED_FIELDS.get(node_type, []):
            if not data.get(field):
                problems.append(f"{label}: {node_type} node missing `{field}`")
    if node_type == "dimension" and not has_primary_key(data):
        problems.append(
            f"{label}: dimension has no primary key (top-level `primary_key` or a "
            f"column with `attributes: [primary_key]`)",
        )
    return problems


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
