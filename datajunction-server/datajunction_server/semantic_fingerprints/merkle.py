"""Pure graph and component hashing for semantic fingerprints."""

import hashlib

from datajunction_server.models.semantic_fingerprint import SemanticFingerprint
from datajunction_server.semantic_fingerprints.normalization import canonical_json


def strongly_connected_components(
    graph: dict[str, list[str]],
) -> list[tuple[str, ...]]:
    """Find graph components iteratively in deterministic order."""
    visited: set[str] = set()
    finish_order: list[str] = []
    for start in sorted(graph):
        if start in visited:
            continue
        visited.add(start)
        dfs_stack = [(start, 0)]
        while dfs_stack:
            name, parent_index = dfs_stack[-1]
            parents = graph[name]
            if parent_index < len(parents):
                parent = parents[parent_index]
                dfs_stack[-1] = (name, parent_index + 1)
                if parent not in visited:
                    visited.add(parent)
                    dfs_stack.append((parent, 0))
                continue
            dfs_stack.pop()
            finish_order.append(name)

    reverse_graph: dict[str, list[str]] = {name: [] for name in graph}
    for child, parents in graph.items():
        for parent in parents:
            reverse_graph[parent].append(child)
    for children in reverse_graph.values():
        children.sort()

    components: list[tuple[str, ...]] = []
    visited.clear()
    for start in reversed(finish_order):
        if start in visited:
            continue
        visited.add(start)
        members = []
        component_stack = [start]
        while component_stack:
            name = component_stack.pop()
            members.append(name)
            for child in reversed(reverse_graph[name]):
                if child not in visited:
                    visited.add(child)
                    component_stack.append(child)
        components.append(tuple(sorted(members)))
    return components


def cycle_component_fingerprint(
    members: tuple[str, ...],
    local_fingerprints: dict[str, SemanticFingerprint],
    internal_edges: list[tuple[str, str]],
    external_edges: list[tuple[str, str, SemanticFingerprint]],
) -> SemanticFingerprint:
    """Hash the members and edge structure of one cyclic component."""
    version = local_fingerprints[members[0]].version
    payload = {
        "domain": "datajunction/node-semantic-scc",
        "version": version,
        "members": [
            {
                "name": name,
                "fingerprint": local_fingerprints[name].model_dump(mode="json"),
            }
            for name in members
        ],
        "internal_edges": [
            {"child": child, "parent": parent}
            for child, parent in sorted(internal_edges)
        ],
        "external_edges": [
            {
                "child": child,
                "parent": parent,
                "fingerprint": fingerprint.model_dump(mode="json"),
            }
            for child, parent, fingerprint in sorted(
                external_edges,
                key=lambda edge: (edge[0], edge[1]),
            )
        ],
    }
    return SemanticFingerprint(
        version=version,
        digest=hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest(),
    )
