import React from 'react';

// Immediate sub-namespaces of the current namespace, shown as folder rows.
// Clicking a row drills into that namespace (onOpen receives its full path).
export default function FolderList({ folders, onOpen }) {
  if (!folders || folders.length === 0) return null;
  return (
    <div className="dj-folder-list">
      <div className="dj-ns-tree-heading">FOLDERS</div>
      {folders.map(f => (
        <div
          key={f.path}
          className="dj-folder-row"
          role="button"
          tabIndex={0}
          title={f.path}
          onClick={() => onOpen(f.path)}
          onKeyDown={e => {
            if (e.key === 'Enter') onOpen(f.path);
          }}
        >
          <span className="dj-folder-icon" aria-hidden="true">
            📁
          </span>
          <span className="dj-folder-name">{f.namespace}</span>
          <span className="dj-folder-caret" aria-hidden="true">
            ›
          </span>
        </div>
      ))}
    </div>
  );
}
