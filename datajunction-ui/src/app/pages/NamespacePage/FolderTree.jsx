import React, { useState } from 'react';

// A clean chevron that points right when collapsed and rotates down when open.
function Chevron({ open }) {
  return (
    <svg
      width="12"
      height="12"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="3"
      strokeLinecap="round"
      strokeLinejoin="round"
      style={{
        transform: open ? 'rotate(90deg)' : 'none',
        transition: 'transform 0.12s ease',
      }}
    >
      <polyline points="9 6 15 12 9 18" />
    </svg>
  );
}

// Recursive folder tree for the namespace rail. `nodes` are hierarchy nodes
// ({ namespace, path, children }) — the descendants of the current namespace.
// Clicking the chevron expands/collapses that level in place (no navigation);
// clicking the name navigates into it (onSelect(path)). Collapsed beyond the
// top level so a deep tree only grows on demand.
function FolderTreeRows({ nodes, depth, expanded, onToggle, onSelect }) {
  return nodes.map(node => {
    const hasChildren = (node.children || []).length > 0;
    const isOpen = expanded.has(node.path);
    return (
      <React.Fragment key={node.path}>
        <div
          className="dj-ns-nav-item"
          role="button"
          tabIndex={0}
          title={node.path}
          style={{ paddingLeft: `${depth * 14}px` }}
          onClick={() => onSelect(node.path)}
          onKeyDown={e => {
            if (e.key === 'Enter') onSelect(node.path);
          }}
        >
          {hasChildren ? (
            <button
              type="button"
              aria-label={isOpen ? 'Collapse' : 'Expand'}
              aria-expanded={isOpen}
              onClick={e => {
                e.stopPropagation();
                onToggle(node.path);
              }}
              style={{
                width: '18px',
                minWidth: '18px',
                flexShrink: 0,
                padding: 0,
                margin: 0,
                border: 'none',
                background: 'none',
                cursor: 'pointer',
                color: '#64748b',
                display: 'inline-flex',
                alignItems: 'center',
                justifyContent: 'center',
              }}
            >
              <Chevron open={isOpen} />
            </button>
          ) : (
            <span style={{ width: '18px', minWidth: '18px', flexShrink: 0 }} />
          )}
          <span className="dj-ns-nav-name">{node.namespace}</span>
        </div>
        {hasChildren && isOpen ? (
          <FolderTreeRows
            nodes={node.children}
            depth={depth + 1}
            expanded={expanded}
            onToggle={onToggle}
            onSelect={onSelect}
          />
        ) : null}
      </React.Fragment>
    );
  });
}

// Rail folder tree rooted at the current namespace's children. Remount it
// (via a `key` on the current path) to reset expansion when the root changes.
export default function FolderTree({ folders, onSelect }) {
  const [expanded, setExpanded] = useState(() => new Set());
  if (!folders || folders.length === 0) return null;
  const onToggle = path =>
    setExpanded(prev => {
      const next = new Set(prev);
      if (next.has(path)) {
        next.delete(path);
      } else {
        next.add(path);
      }
      return next;
    });
  return (
    <div className="dj-ns-folder-nav">
      <div className="dj-ns-tree-heading">Folders</div>
      <FolderTreeRows
        nodes={folders}
        depth={0}
        expanded={expanded}
        onToggle={onToggle}
        onSelect={onSelect}
      />
    </div>
  );
}
