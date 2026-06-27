import React from 'react';

// Clickable path crumbs (growth › metrics › dau). Each crumb navigates to its
// own full ancestor path; the final crumb is the current namespace (plain text).
export default function NamespaceBreadcrumb({ path, onNavigate }) {
  if (!path) return null;
  const segments = path.split('.');
  return (
    <nav className="dj-breadcrumb" aria-label="Namespace path">
      {segments.map((seg, i) => {
        const full = segments.slice(0, i + 1).join('.');
        const isLast = i === segments.length - 1;
        return (
          <span key={full} className="dj-breadcrumb-item">
            {isLast ? (
              <span className="dj-breadcrumb-current">{seg}</span>
            ) : (
              <>
                <button
                  type="button"
                  className="dj-breadcrumb-link"
                  onClick={() => onNavigate(full)}
                >
                  {seg}
                </button>
                <span className="dj-breadcrumb-sep" aria-hidden="true">
                  {' '}
                  ›{' '}
                </span>
              </>
            )}
          </span>
        );
      })}
    </nav>
  );
}
