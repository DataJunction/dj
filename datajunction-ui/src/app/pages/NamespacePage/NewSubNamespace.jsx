import React, { useState } from 'react';

// Inline "new folder" affordance for the rail folder tree: a subtle button that
// expands into a text field for the leaf name. Enter creates `<parent>.<name>`
// (via onCreate), Esc / empty-blur cancels. Only rendered for non-git-backed
// namespaces (namespaces under git control are managed via git, not the UI).
export default function NewSubNamespace({ parent, onCreate }) {
  const [adding, setAdding] = useState(false);
  const [name, setName] = useState('');
  const [error, setError] = useState(null);

  // Sanitize to a single namespace segment for the create call.
  const segment = name.trim().replace(/[^a-zA-Z0-9_]/g, '_');

  const reset = () => {
    setAdding(false);
    setName('');
    setError(null);
  };

  const submit = async () => {
    if (!segment) {
      reset();
      return;
    }
    const res = await onCreate(`${parent}.${segment}`);
    if (res?._error) {
      setError(res.message || 'Failed to create namespace');
    } else {
      reset();
    }
  };

  if (!adding) {
    // Match the folder rows (dj-ns-nav-item): same 15px size/padding, with the
    // ＋ sitting in the same column the folder chevrons occupy. Muted color marks
    // it as a secondary "add" affordance.
    return (
      <button
        type="button"
        className="dj-ns-nav-item"
        onClick={() => setAdding(true)}
        style={{ paddingLeft: 0, color: '#64748b' }}
      >
        <span
          style={{
            width: '18px',
            minWidth: '18px',
            flexShrink: 0,
            display: 'inline-flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '16px',
            lineHeight: 1,
          }}
        >
          ＋
        </span>
        <span className="dj-ns-nav-name">New folder</span>
      </button>
    );
  }

  return (
    <div style={{ marginTop: '4px', paddingLeft: '18px', paddingRight: '8px' }}>
      <input
        autoFocus
        type="text"
        value={name}
        onChange={e => setName(e.target.value)}
        onKeyDown={e => {
          if (e.key === 'Enter') submit();
          else if (e.key === 'Escape') reset();
        }}
        onBlur={() => {
          if (!name.trim()) reset();
        }}
        placeholder="Folder name"
        aria-label="New sub-namespace name"
        style={{
          width: '100%',
          boxSizing: 'border-box',
          padding: '6px 8px',
          fontSize: '15px',
          border: '1px solid #cbd5e1',
          borderRadius: '6px',
          outline: 'none',
        }}
      />
      {error ? (
        <div style={{ marginTop: '4px', fontSize: '12px', color: '#b91c1c' }}>
          {error}
        </div>
      ) : null}
    </div>
  );
}
