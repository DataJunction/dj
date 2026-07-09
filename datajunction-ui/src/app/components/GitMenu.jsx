import React from 'react';
import { secondaryButtonStyle } from './buttonStyles';

const itemStyle = {
  display: 'block',
  width: '100%',
  textAlign: 'left',
  padding: '10px 16px',
  border: 'none',
  background: '#ffffff',
  color: '#1e293b',
  fontSize: '13px',
  fontFamily: 'inherit',
  textTransform: 'none',
  cursor: 'pointer',
  textDecoration: 'none',
  boxSizing: 'border-box',
};

const onItemOver = e => {
  e.currentTarget.style.background = '#f3eeff';
};
const onItemOut = e => {
  e.currentTarget.style.background = '#ffffff';
};

/**
 * Low-frequency git actions for a namespace, in a hover dropdown. Renders only
 * the items whose handlers/urls are provided, so read-only and editable callers
 * pass exactly the valid set. Reuses the shared `.dropdown` hover CSS.
 */
export default function GitMenu({
  repoPath,
  branch,
  viewInGitUrl,
  onOpenSettings,
  onNewBranch,
  onDelete,
}) {
  return (
    <div className="dropdown" data-testid="git-menu">
      <button type="button" style={secondaryButtonStyle} aria-label="Git menu">
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="14"
          height="14"
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
        Git ▾
      </button>
      <div
        className="dropdown-content add-node-dropdown-content"
        role="menu"
        // marginTop:0 removes the shared 6px gap between trigger and menu; the
        // gap is a hover dead zone that closes the menu before the cursor
        // reaches it. Flush menu keeps the :hover area continuous.
        style={{ textTransform: 'none', marginTop: 0 }}
      >
        {repoPath && (
          <div
            style={{
              padding: '10px 16px',
              fontSize: '12px',
              color: '#64748b',
              borderBottom: '1px solid #e2e8f0',
              background: '#ffffff',
            }}
          >
            <code style={{ fontSize: '11px' }}>{repoPath}</code>
            {branch && (
              <>
                {' '}
                @ <code style={{ fontSize: '11px' }}>{branch}</code>
              </>
            )}
          </div>
        )}
        {onNewBranch && (
          <button
            type="button"
            style={itemStyle}
            onClick={onNewBranch}
            onMouseOver={onItemOver}
            onMouseOut={onItemOut}
          >
            + New Branch
          </button>
        )}
        {viewInGitUrl && (
          <a
            style={itemStyle}
            href={viewInGitUrl}
            target="_blank"
            rel="noreferrer"
            onMouseOver={onItemOver}
            onMouseOut={onItemOut}
          >
            View in git
          </a>
        )}
        {onOpenSettings && (
          <button
            type="button"
            style={itemStyle}
            onClick={onOpenSettings}
            onMouseOver={onItemOver}
            onMouseOut={onItemOut}
          >
            Git settings
          </button>
        )}
        {onDelete && (
          <button
            type="button"
            style={{ ...itemStyle, color: '#dc2626' }}
            onClick={onDelete}
            onMouseOver={onItemOver}
            onMouseOut={onItemOut}
          >
            Delete branch
          </button>
        )}
      </div>
    </div>
  );
}
