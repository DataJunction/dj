import * as React from 'react';
import { Link } from 'react-router-dom';
import { NodeBadge, NodeLink } from '../../components/NodeComponents';
import NodeListActions from '../../components/NodeListActions';

// Format timestamp to relative time
function formatRelativeTime(dateStr) {
  const date = new Date(dateStr);
  const now = new Date();
  const diffMs = now - date;
  const diffMins = Math.floor(diffMs / (1000 * 60));
  const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
  const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

  if (diffMins < 60) return `${diffMins}m`;
  if (diffHours < 24) return `${diffHours}h`;
  if (diffDays < 30) return `${diffDays}d`;
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

// Capitalize first letter
function capitalize(str) {
  return str.charAt(0).toUpperCase() + str.slice(1);
}

// Type Card Component
function TypeCard({ type, nodes, count, username, activeTab }) {
  const maxDisplay = 10;
  const displayNodes = nodes.slice(0, maxDisplay);
  const remaining = count - maxDisplay;

  // Build filter URL based on active tab and type
  const getFilterUrl = () => {
    const params = new URLSearchParams();

    if (activeTab === 'owned') {
      params.append('ownedBy', username);
    } else if (activeTab === 'watched') {
      // For watched, we'd need a different filter - for now use type only
      params.append('type', type);
    } else if (activeTab === 'edited') {
      params.append('updatedBy', username);
    }

    params.append('type', type);
    return `/?${params.toString()}`;
  };

  return (
    <div className="type-group-card">
      <div className="type-group-header">
        <span className="type-group-title">
          {capitalize(type)}s ({count})
        </span>
      </div>

      <div className="type-group-nodes">
        {displayNodes.map(node => {
          const isInvalid = node.status === 'invalid';
          const isDraft = node.mode === 'draft';
          const gitRepo = node.gitInfo?.repo;
          const gitBranch = node.gitInfo?.branch;
          const defaultBranch = node.gitInfo?.defaultBranch;
          const isDefaultBranch = gitBranch && gitBranch === defaultBranch;
          const hasGitInfo = gitRepo || gitBranch;

          return (
            <div key={node.name} className="type-group-node">
              <NodeBadge type={node.type} size="small" abbreviated={true} />
              {isInvalid && (
                <span
                  className="type-group-node-status-icon"
                  title="Invalid node"
                >
                  ⚠️
                </span>
              )}
              {isDraft && (
                <span
                  className="type-group-node-status-icon"
                  title="Draft mode"
                >
                  📝
                </span>
              )}
              <NodeLink
                node={node}
                size="small"
                ellipsis={true}
                style={{ flex: 1, minWidth: 0 }}
              />
              {hasGitInfo && (
                <span
                  className={`type-group-node-git-info ${
                    isDefaultBranch ? 'type-group-node-git-info--default' : ''
                  }`}
                  title={`${gitRepo || ''}${gitRepo && gitBranch ? ' / ' : ''}${
                    gitBranch || ''
                  }${isDefaultBranch ? ' (default)' : ''}`}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '4px',
                    maxWidth: '200px',
                  }}
                >
                  {isDefaultBranch && (
                    <span style={{ lineHeight: 1, flexShrink: 0 }}>⭐</span>
                  )}
                  {gitRepo && (
                    <span
                      style={{
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                        minWidth: 0,
                      }}
                    >
                      {gitRepo}
                    </span>
                  )}
                  {gitRepo && gitBranch && (
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      width="10"
                      height="10"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      style={{ flexShrink: 0 }}
                    >
                      <line x1="6" y1="3" x2="6" y2="15" />
                      <circle cx="18" cy="6" r="3" />
                      <circle cx="6" cy="18" r="3" />
                      <path d="M18 9a9 9 0 0 1-9 9" />
                    </svg>
                  )}
                  {gitBranch && (
                    <span
                      style={{
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                        minWidth: 0,
                      }}
                    >
                      {gitBranch}
                    </span>
                  )}
                </span>
              )}
              {node.current?.updatedAt && (
                <span className="type-group-node-time">
                  {formatRelativeTime(node.current.updatedAt)}
                </span>
              )}
              <div className="type-group-node-actions">
                <NodeListActions nodeName={node.name} />
              </div>
            </div>
          );
        })}
      </div>

      {remaining > 0 && (
        <Link to={getFilterUrl()} className="type-group-more">
          +{remaining} more →
        </Link>
      )}
    </div>
  );
}

// Type Group Grid Component (2-column layout)
export function TypeGroupGrid({ groupedData, username, activeTab }) {
  if (!groupedData || groupedData.length === 0) {
    return (
      <div
        style={{
          padding: '1rem',
          textAlign: 'center',
          color: '#666',
          fontSize: '12px',
        }}
      >
        No nodes to display
      </div>
    );
  }

  return (
    <div className="type-group-grid">
      {groupedData.map(group => (
        <TypeCard
          key={group.type}
          type={group.type}
          nodes={group.nodes}
          count={group.count}
          username={username}
          activeTab={activeTab}
        />
      ))}
    </div>
  );
}
