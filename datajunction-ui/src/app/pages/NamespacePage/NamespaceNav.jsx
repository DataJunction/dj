import React, { useState } from 'react';
import ChevronIcon from '../../icons/ChevronIcon';
import {
  buildNamespaceOptions,
  searchNamespaces,
  immediateChildren,
} from './namespaceOptions';
import { getPinned, togglePinned } from './namespaceShortcuts';
import FolderTree from './FolderTree';
import NewSubNamespace from './NewSubNamespace';

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
  canCreateNamespace = false,
  onCreateNamespace,
}) {
  const [collapsed, setCollapsed] = useState({ 'Top-level': true });
  const [pinned, setPinned] = useState(() => getPinned());
  // Filter only exists in the no-namespace ("All namespaces") view, where there's
  // no current scope to confuse it with. Once a namespace is selected, jumping
  // elsewhere is done via the header's namespace switcher.
  const [filter, setFilter] = useState('');

  const isFiltering = filter.trim() !== '';
  const matches = isFiltering ? searchNamespaces(namespaces || [], filter) : [];
  const showList = !currentNamespace;
  const groups = showList ? buildNamespaceOptions(namespaces || []) : [];

  const gitByNs = {};
  for (const ns of namespaces || []) {
    gitByNs[ns.namespace] = ns.git;
  }
  const pinnedSet = new Set(pinned);

  const isOpen = label => !collapsed[label];
  const toggleGroup = label =>
    setCollapsed(c => ({ ...c, [label]: !c[label] }));

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
      onClick={() => onSelect(ns)}
      onKeyDown={e => {
        if (e.key === 'Enter') onSelect(ns);
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
            <ChevronIcon open={open} />
          </span>
          <span className="dj-ns-group-label">{label}</span>
          <span className="dj-ns-group-count">{names.length}</span>
        </button>
        {open ? names.map(ns => renderRow(ns, keyPrefix)) : null}
      </div>
    );
  };

  // Selected (subtree) state — ctx is still needed for subtreePath (git-root folders).
  const ctx = currentNamespace ? gitContext(gitByNs, currentNamespace) : null;
  const subtreePath =
    ctx?.isRoot && ctx.defaultBranch
      ? `${ctx.root}.${ctx.defaultBranch}`
      : currentNamespace;

  return (
    <div>
      {showList ? (
        <>
          <div className="dj-ns-filter">
            <input
              type="text"
              className="dj-ns-filter-input"
              value={filter}
              onChange={e => setFilter(e.target.value)}
              placeholder="Filter namespaces…"
              aria-label="Filter namespaces"
            />
            {isFiltering ? (
              <button
                type="button"
                className="dj-ns-filter-clear"
                aria-label="Clear filter"
                title="Clear filter"
                onClick={() => setFilter('')}
              >
                ✕
              </button>
            ) : null}
          </div>
          {isFiltering ? (
            matches.length === 0 ? (
              <div
                style={{ padding: '6px', color: '#64748b', fontSize: '12px' }}
              >
                No namespaces match "{filter.trim()}".
              </div>
            ) : (
              renderGroup('Matches', matches, 'matches')
            )
          ) : (
            <>
              {pinned.length > 0 ? renderGroup('Pinned', pinned, 'pin') : null}
              {groups.map(group =>
                renderGroup(
                  group.label,
                  group.options.map(o => o.value),
                  `grp-${group.label}`,
                ),
              )}
            </>
          )}
        </>
      ) : (
        // Folder navigation: the current namespace's sub-namespace tree. Chevron
        // expands a level in place; clicking a name drills in. Going up is handled
        // by the header breadcrumb; switching namespace by the filter box
        // (no-namespace view) or the header switcher. Keyed by the current path so
        // expansion resets when the tree re-roots.
        <>
          <FolderTree
            key={subtreePath}
            folders={immediateChildren(hierarchy || [], subtreePath)}
            onSelect={onSelect}
          />
          {canCreateNamespace ? (
            <NewSubNamespace
              parent={subtreePath}
              onCreate={onCreateNamespace}
            />
          ) : null}
        </>
      )}
    </div>
  );
}
