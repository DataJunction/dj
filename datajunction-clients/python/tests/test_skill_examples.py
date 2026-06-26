"""Validate that deployable node-spec examples in the bundled skills conform to the
current server-side deployment schema (``datajunction_server.models.deployment``).

Why: the skills hand users example node YAML. As the orchestrator's deployment schema
evolves, those examples silently drift. This "compiles" each example against the real
Pydantic spec models — no running server needed (the spec models are imported directly,
and the client test suite already depends on ``datajunction_server``).

Each fenced ```yaml block is classified:

  - **spec** — a single node (top-level ``node_type``) or a deployment (``nodes:``).
    Validated against the server schema; a mismatch fails the build.
  - **legacy** — the deprecated client shape (top-level ``type: source|metric|…``
    instead of ``node_type``). Failed with a migration message, so drifted examples
    can't silently dodge validation by not matching the new schema.
  - **fragment** — anything else (a bare ``query:`` / ``columns:`` snippet). Skipped.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml
from pydantic import TypeAdapter, ValidationError

from datajunction_server.models.deployment import DeploymentSpec, NodeUnion

SKILLS_DIR = Path(__file__).parent.parent / "datajunction" / "skills"
_NODE_ADAPTER = TypeAdapter(NodeUnion)
_FENCED_YAML = re.compile(r"```ya?ml\n(.*?)\n```", re.DOTALL)
_LEGACY_TYPES = {"source", "transform", "dimension", "metric", "cube"}


def _classify(data) -> str | None:
    """Return 'spec' | 'legacy' for a deployable block, or None to skip it."""
    if not isinstance(data, dict):
        return None
    if "node_type" in data or "nodes" in data:
        return "spec"
    if data.get("type") in _LEGACY_TYPES:  # top-level node type the old way
        return "legacy"
    return None


def _candidate_examples() -> list[tuple[str, dict, str]]:
    out: list[tuple[str, dict, str]] = []
    for path in sorted(SKILLS_DIR.glob("*.md")):
        for i, match in enumerate(_FENCED_YAML.finditer(path.read_text())):
            try:
                data = yaml.safe_load(match.group(1))
            except yaml.YAMLError:
                continue
            kind = _classify(data)
            if kind:
                out.append((f"{path.stem}[{i}]", data, kind))
    return out


_EXAMPLES = _candidate_examples()


@pytest.mark.parametrize(
    "ident,data,kind",
    _EXAMPLES or [("__none__", {}, "spec")],
    ids=[e[0] for e in _EXAMPLES] or ["no-deployable-examples"],
)
def test_skill_node_examples_conform_to_deployment_schema(ident, data, kind) -> None:
    if ident == "__none__":
        pytest.skip("No deployable node-spec examples found in the skills.")

    if kind == "legacy":
        pytest.fail(
            f"{ident}: uses the deprecated client node shape (top-level `type: "
            f"{data.get('type')}`). Migrate to the server deployment schema "
            f"(`node_type:` + the current spec fields) so it matches what the "
            f"orchestrator accepts.",
        )

    try:
        if "nodes" in data:
            DeploymentSpec.model_validate(data)
        else:
            _NODE_ADAPTER.validate_python(data)
    except ValidationError as exc:
        pytest.fail(
            f"{ident}: skill example does not conform to the current deployment schema "
            f"(datajunction_server.models.deployment):\n{exc}",
        )
