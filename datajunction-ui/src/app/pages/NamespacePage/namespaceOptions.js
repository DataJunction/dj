// src/app/pages/NamespacePage/namespaceOptions.js

// A namespace is a git ROOT (vs a git branch) when its git config is a GitRootConfig.
const isGitRoot = ns => ns.git?.__typename === 'GitRootConfig';
const isTopLevel = ns => !ns.namespace.includes('.');

/**
 * Build grouped react-select options for the namespace selector.
 * Group 1 "Git-backed": every git root, at any depth.
 * Group 2 "Top-level namespaces": top-levels that are not themselves git roots.
 * Deep non-git namespaces are intentionally excluded (reached via drill-in).
 */
export function buildNamespaceOptions(namespaces) {
  const gitBacked = [];
  const topLevel = [];

  for (const ns of namespaces) {
    if (isGitRoot(ns)) {
      gitBacked.push({
        value: ns.namespace,
        label: ns.namespace,
        isGitRoot: true,
        count: ns.numNodes ?? 0,
      });
    } else if (isTopLevel(ns)) {
      topLevel.push({
        value: ns.namespace,
        label: ns.namespace,
        isGitRoot: false,
        count: ns.numNodes ?? 0,
      });
    }
  }

  const byValue = (a, b) => a.value.localeCompare(b.value);
  gitBacked.sort(byValue);
  topLevel.sort(byValue);

  const groups = [];
  if (gitBacked.length)
    groups.push({ label: 'Git-backed', options: gitBacked });
  if (topLevel.length) groups.push({ label: 'Top-level', options: topLevel });
  return groups;
}

/**
 * Sorted full paths of EVERY namespace (any depth) whose dotted path matches `text`.
 * Used while filtering so search reaches nested namespaces — not just the git roots /
 * top-levels in the curated default list.
 */
export function searchNamespaces(namespaces, text) {
  const t = (text || '').trim().toLowerCase();
  if (!t) return [];
  return namespaces
    .map(ns => ns.namespace)
    .filter(name => name.toLowerCase().includes(t))
    .sort((a, b) => a.localeCompare(b));
}

/** Recursively find the hierarchy node whose full dotted `path` matches. */
export function findHierarchyNode(hierarchy, path) {
  if (!path) return null;
  for (const node of hierarchy) {
    if (node.path === path) return node;
    const found = findHierarchyNode(node.children || [], path);
    if (found) return found;
  }
  return null;
}

/** Direct children of the hierarchy node at `path` (empty if leaf/missing). */
export function immediateChildren(hierarchy, path) {
  const node = findHierarchyNode(hierarchy, path);
  return node?.children || [];
}

/**
 * Flat rows for the rail "jump tree": every sibling at each level from the root
 * down to `currentPath`, descending ONLY along the current path. The current
 * node is included (marked) but its children are not — those live in the main
 * panel. Each row: { path, namespace, depth, isCurrent, isAncestor }.
 */
export function buildJumpTree(hierarchy, currentPath) {
  if (!currentPath) return [];
  const rows = [];
  const walk = (level, depth) => {
    for (const node of level) {
      const isCurrent = node.path === currentPath;
      const isAncestor = currentPath.startsWith(node.path + '.');
      rows.push({
        path: node.path,
        namespace: node.namespace,
        depth,
        isCurrent,
        isAncestor,
      });
      if (isAncestor) walk(node.children || [], depth + 1);
    }
  };
  walk(hierarchy, 0);
  return rows;
}
