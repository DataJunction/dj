import React, { useState } from 'react';
import Explorer from './Explorer';
import NamespaceTypeSummary from './NamespaceTypeSummary';
import CollapsedIcon from '../../icons/CollapsedIcon';
import ExpandedIcon from '../../icons/ExpandedIcon';
import {
  buildNamespaceOptions,
  searchNamespaces,
  findHierarchyNode,
} from './namespaceOptions';
import { getPinned, togglePinned } from './namespaceShortcuts';

// Git context for a selected namespace, derived from the raw namespace list.
function gitContext(gitByNs, ns) {
  const g = gitByNs[ns];
  if (!g) return null;
  if (g.__typename === 'GitRootConfig') {
    return {
      root: ns,
      defaultBranch: g.defaultBranch,
      activeBranch: g.defaultBranch,
      isRoot: true,
    };
  }
  if (g.__typename === 'GitBranchConfig') {
    return {
      root: g.parentNamespace,
      defaultBranch: g.root?.defaultBranch,
      activeBranch: g.branch,
      isRoot: false,
    };
  }
  return null;
}

export default function NamespaceNav({
  namespaces,
  hierarchy,
  currentNamespace,
  gitRoots,
  onSelect,
}) {
  const [filter, setFilter] = useState('');
  const [collapsed, setCollapsed] = useState({ 'Top-level': true });
  const [pinned, setPinned] = useState(() => getPinned());

  const isFiltering = filter.trim() !== '';
  // Filtering searches ALL namespaces (any depth) as one flat result list; the
  // unfiltered default stays the curated jump-list (Git-backed roots + top-levels).
  const matches = isFiltering ? searchNamespaces(namespaces || [], filter) : [];
  const groups = isFiltering ? [] : buildNamespaceOptions(namespaces || []);
  const showList = isFiltering || !currentNamespace;

  const gitByNs = {};
  for (const ns of namespaces || []) {
    gitByNs[ns.namespace] = ns.git;
  }
  const pinnedSet = new Set(pinned);

  const isOpen = label => isFiltering || !collapsed[label];
  const toggleGroup = label =>
    setCollapsed(c => ({ ...c, [label]: !c[label] }));

  const select = value => {
    setFilter('');
    onSelect(value);
  };
  const onPin = (e, ns) => {
    e.stopPropagation();
    setPinned(togglePinned(ns));
  };

  const renderRow = (ns, keyPrefix) => (
    <div
      key={`${keyPrefix}-${ns}`}
      className="dj-ns-nav-item"
      role="button"
      tabIndex={0}
      title={ns}
      onClick={() => select(ns)}
      onKeyDown={e => {
        if (e.key === 'Enter') select(ns);
      }}
    >
      <span className="dj-ns-nav-name">{ns}</span>
      <button
        type="button"
        className={`dj-ns-star${pinnedSet.has(ns) ? ' pinned' : ''}`}
        aria-label={`${pinnedSet.has(ns) ? 'Unpin' : 'Pin'} ${ns}`}
        aria-pressed={pinnedSet.has(ns)}
        onClick={e => onPin(e, ns)}
      >
        {pinnedSet.has(ns) ? '★' : '☆'}
      </button>
    </div>
  );

  // One uniform collapsible section used for Pinned and the git groups.
  const renderGroup = (label, names, keyPrefix) => {
    const open = isOpen(label);
    return (
      <div key={keyPrefix}>
        <button
          type="button"
          className="dj-ns-group-heading"
          aria-expanded={open}
          onClick={() => toggleGroup(label)}
        >
          <span className="dj-ns-chevron">
            {open ? <ExpandedIcon /> : <CollapsedIcon />}
          </span>
          <span className="dj-ns-group-label">{label}</span>
          <span className="dj-ns-group-count">{names.length}</span>
        </button>
        {open ? names.map(ns => renderRow(ns, keyPrefix)) : null}
      </div>
    );
  };

  // Selected (subtree) state, with git default-branch + branch switcher.
  const ctx = currentNamespace ? gitContext(gitByNs, currentNamespace) : null;
  const branchList = ctx?.root
    ? (namespaces || [])
        .filter(
          ns =>
            ns.git?.__typename === 'GitBranchConfig' &&
            ns.git.parentNamespace === ctx.root,
        )
        .map(ns => ({ branch: ns.git.branch, namespace: ns.namespace }))
        .sort((a, b) => a.branch.localeCompare(b.branch))
    : [];
  const subtreePath =
    ctx?.isRoot && ctx.defaultBranch
      ? `${ctx.root}.${ctx.defaultBranch}`
      : currentNamespace;
  const subtreeNode = currentNamespace
    ? findHierarchyNode(hierarchy || [], subtreePath)
    : null;
  // At the branch root the switcher already names the branch, so render its
  // children directly instead of repeating the branch as the tree root.
  const atBranchRoot =
    !!ctx && subtreePath === `${ctx.root}.${ctx.activeBranch}`;
  const currentPinned = currentNamespace
    ? pinnedSet.has(currentNamespace)
    : false;
  // For a git namespace, show just the root in the box (e.g. "arc") — the branch part
  // is owned by the branch switcher below, so the full "arc.<branch>" would be redundant.
  const scopeLabel = ctx?.root || currentNamespace || '';

  return (
    <div>
      {/* The filter box doubles as the scope selector: when a namespace is selected it
          shows that namespace (with a pin + a ✕ that returns to All namespaces); typing
          turns it back into a filter over the list below. */}
      <div className="dj-ns-filter">
        <input
          type="text"
          className="dj-ns-filter-input"
          value={isFiltering ? filter : scopeLabel}
          title={
            !isFiltering && currentNamespace ? currentNamespace : undefined
          }
          onChange={e => setFilter(e.target.value)}
          onFocus={e => {
            // Showing a selection → select-all so the first keystroke replaces it
            // with filter text rather than appending to the namespace name.
            if (!isFiltering && currentNamespace) e.target.select();
          }}
          placeholder="Filter namespaces…"
          aria-label="Filter or select a namespace"
        />
        {currentNamespace && !isFiltering ? (
          <button
            type="button"
            className={`dj-ns-filter-pin${currentPinned ? ' pinned' : ''}`}
            aria-label={`${
              currentPinned ? 'Unpin' : 'Pin'
            } ${currentNamespace}`}
            aria-pressed={currentPinned}
            title={currentPinned ? 'Pinned' : 'Pin this namespace'}
            onClick={() => setPinned(togglePinned(currentNamespace))}
          >
            {currentPinned ? '★' : '☆'}
          </button>
        ) : null}
        {isFiltering || currentNamespace ? (
          <button
            type="button"
            className="dj-ns-filter-clear"
            aria-label={isFiltering ? 'Clear filter' : 'All namespaces'}
            title={isFiltering ? 'Clear filter' : 'All namespaces'}
            onClick={() => (isFiltering ? setFilter('') : select(null))}
          >
            ✕
          </button>
        ) : null}
      </div>
      {showList ? (
        <>
          {!isFiltering && pinned.length > 0
            ? renderGroup('Pinned', pinned, 'pin')
            : null}
          {isFiltering && matches.length === 0 ? (
            <div style={{ padding: '6px', color: '#64748b', fontSize: '12px' }}>
              No namespaces match "{filter.trim()}".
            </div>
          ) : isFiltering ? (
            renderGroup('Matches', matches, 'matches')
          ) : (
            groups.map(group =>
              renderGroup(
                group.label,
                group.options.map(o => o.value),
                `grp-${group.label}`,
              ),
            )
          )}
        </>
      ) : (
        <div>
          {/* The selected namespace + pin + back-to-all now live in the filter box
              above; here we just nest its branch switcher and node counts. */}
          {ctx && branchList.length > 0 ? (
            // Borderless branch switcher: the icon + branch name + caret read as plain
            // text (no box, so it doesn't look like a second selector); a transparent
            // native <select> overlays the row for the real dropdown + accessibility.
            <div className="dj-ns-scope-branch-row">
              <svg
                className="dj-ns-branch-glyph"
                aria-hidden="true"
                xmlns="http://www.w3.org/2000/svg"
                width="13"
                height="13"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <line x1="6" y1="3" x2="6" y2="15" />
                <circle cx="18" cy="6" r="3" />
                <circle cx="6" cy="18" r="3" />
                <path d="M18 9a9 9 0 0 1-9 9" />
              </svg>
              <span className="dj-ns-branch-value">{ctx.activeBranch}</span>
              <svg
                className="dj-ns-branch-caret"
                aria-hidden="true"
                xmlns="http://www.w3.org/2000/svg"
                width="18"
                height="18"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <polyline points="6 9 12 15 18 9" />
              </svg>
              <select
                className="dj-ns-branch-native"
                aria-label="Branch"
                title="Branch"
                value={ctx.activeBranch || ''}
                onChange={e => {
                  const b = branchList.find(x => x.branch === e.target.value);
                  if (b) select(b.namespace);
                }}
              >
                {branchList.map(b => (
                  <option key={b.namespace} value={b.branch}>
                    {b.branch}
                  </option>
                ))}
              </select>
            </div>
          ) : null}
          {/* This branch only renders when a namespace is selected (showList is false),
              so currentNamespace is always truthy here — no guard needed. */}
          <NamespaceTypeSummary namespace={subtreePath} showHeading={false} />
          {(
            atBranchRoot
              ? (subtreeNode?.children || []).length > 0
              : !!subtreeNode
          ) ? (
            <div className="dj-ns-tree-heading">Namespaces</div>
          ) : null}
          {subtreeNode && atBranchRoot ? (
            (subtreeNode.children || []).map(child => (
              <Explorer
                item={child}
                current={subtreePath}
                key={child.namespace}
                gitRoots={gitRoots}
                pinnedSet={pinnedSet}
                onTogglePin={ns => setPinned(togglePinned(ns))}
              />
            ))
          ) : subtreeNode ? (
            <Explorer
              item={subtreeNode}
              current={subtreePath}
              key={subtreeNode.namespace}
              gitRoots={gitRoots}
              pinnedSet={pinnedSet}
              onTogglePin={ns => setPinned(togglePinned(ns))}
            />
          ) : null}
        </div>
      )}
    </div>
  );
}
