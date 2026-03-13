import * as React from 'react';
import { Link } from 'react-router-dom';
import DashboardCard from '../../components/DashboardCard';
import { NodeList } from './NodeList';
import { TypeGroupGrid } from './TypeGroupGrid';

// Node type display order
const NODE_TYPE_ORDER = ['metric', 'cube', 'dimension', 'transform', 'source'];

// Helper to group nodes by type
function groupNodesByType(nodes, hasMoreMap = {}) {
  const groups = {};
  nodes.forEach(node => {
    const type = (node.type || 'unknown').toLowerCase();
    if (!groups[type]) groups[type] = [];
    groups[type].push(node);
  });

  // Return types in defined order, only including types with nodes
  return NODE_TYPE_ORDER.filter(type => groups[type]?.length > 0).map(type => ({
    type,
    nodes: groups[type],
    hasMore: hasMoreMap[type] ?? false,
  }));
}

// My Nodes Section (owned + watched, with tabs)
export function MyNodesSection({
  ownedNodes,
  ownedHasMore = {},
  watchedNodes,
  recentlyEdited,
  editedHasMore = {},
  username,
  loading,
}) {
  const [activeTab, setActiveTab] = React.useState('owned');
  const hasAutoSwitchedTab = React.useRef(false);

  // Once data loads, auto-switch to "edited" if user has no owned nodes but has edits
  React.useEffect(() => {
    if (hasAutoSwitchedTab.current) return;
    if (ownedNodes.length === 0 && recentlyEdited.length > 0) {
      setActiveTab('edited');
      hasAutoSwitchedTab.current = true;
    } else if (ownedNodes.length > 0) {
      hasAutoSwitchedTab.current = true; // owned nodes exist, stick with default
    }
  }, [ownedNodes.length, recentlyEdited.length]);
  const [groupByType, setGroupByType] = React.useState(() => {
    const stored = localStorage.getItem('workspace_groupByType');
    return stored === null ? true : stored === 'true';
  });

  const ownedNames = new Set(ownedNodes.map(n => n.name));
  const watchedOnly = watchedNodes.filter(n => !ownedNames.has(n.name));

  const allMyNodeNames = new Set([
    ...ownedNames,
    ...watchedNodes.map(n => n.name),
  ]);
  const editedOnly = recentlyEdited.filter(n => !allMyNodeNames.has(n.name));

  const getDisplayNodes = () => {
    switch (activeTab) {
      case 'owned':
        return ownedNodes;
      case 'watched':
        return watchedOnly;
      case 'edited':
        return recentlyEdited;
      default:
        return ownedNodes;
    }
  };
  const displayNodes = getDisplayNodes();

  const hasAnyContent =
    ownedNodes.length > 0 ||
    watchedOnly.length > 0 ||
    recentlyEdited.length > 0;
  const maxDisplay = 8;

  // Handle group by type toggle
  const handleGroupByTypeChange = e => {
    const checked = e.target.checked;
    setGroupByType(checked);
    localStorage.setItem('workspace_groupByType', checked.toString());
  };

  // Pick the right hasMore map for the active tab
  const activeHasMore =
    activeTab === 'owned'
      ? ownedHasMore
      : activeTab === 'edited'
      ? editedHasMore
      : {};

  // Group nodes by type if enabled
  const groupedData = groupByType
    ? groupNodesByType(displayNodes, activeHasMore)
    : null;

  return (
    <DashboardCard
      title="My Nodes"
      actionLink={`/?ownedBy=${username}`}
      loading={loading}
      cardStyle={{
        padding: '0.25rem 0.75rem',
      }}
      emptyState={
        <div style={{ padding: '0.75rem 0' }}>
          <p
            style={{ fontSize: '12px', color: '#666', marginBottom: '0.75rem' }}
          >
            No nodes yet.
          </p>
          <div style={{ display: 'flex', gap: '0.75rem' }}>
            <div
              style={{
                flex: 1,
                padding: '0.75rem',
                backgroundColor: 'var(--card-bg, #f8f9fa)',
                border: '1px dashed var(--border-color, #dee2e6)',
                borderRadius: '6px',
                textAlign: 'center',
              }}
            >
              <div style={{ fontSize: '16px', marginBottom: '0.25rem' }}>
                ➕
              </div>
              <div
                style={{
                  fontSize: '11px',
                  fontWeight: '500',
                  marginBottom: '0.25rem',
                }}
              >
                Create a node
              </div>
              <p
                style={{
                  fontSize: '10px',
                  color: '#666',
                  marginBottom: '0.5rem',
                }}
              >
                Build your data model
              </p>
              <Link
                to="/create/source"
                style={{
                  display: 'inline-block',
                  padding: '3px 8px',
                  fontSize: '10px',
                  backgroundColor: 'var(--primary-color, #4a90d9)',
                  color: '#fff',
                  borderRadius: '4px',
                  textDecoration: 'none',
                }}
              >
                Create →
              </Link>
            </div>
            <div
              style={{
                flex: 1,
                padding: '0.75rem',
                backgroundColor: 'var(--card-bg, #f8f9fa)',
                border: '1px dashed var(--border-color, #dee2e6)',
                borderRadius: '6px',
                textAlign: 'center',
              }}
            >
              <div style={{ fontSize: '16px', marginBottom: '0.25rem' }}>
                👤
              </div>
              <div
                style={{
                  fontSize: '11px',
                  fontWeight: '500',
                  marginBottom: '0.25rem',
                }}
              >
                Claim ownership
              </div>
              <p
                style={{
                  fontSize: '10px',
                  color: '#666',
                  marginBottom: '0.5rem',
                }}
              >
                Add yourself as owner
              </p>
              <Link
                to="/"
                style={{
                  display: 'inline-block',
                  padding: '3px 8px',
                  fontSize: '10px',
                  backgroundColor: '#6c757d',
                  color: '#fff',
                  borderRadius: '4px',
                  textDecoration: 'none',
                }}
              >
                Browse →
              </Link>
            </div>
          </div>
        </div>
      }
    >
      {hasAnyContent && (
        <>
          {/* Tabs and Group by Type Toggle */}
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              gap: '0.5rem',
              marginBottom: '0.5rem',
              paddingTop: '0.5rem',
              paddingBottom: displayNodes.length > 0 ? '0.5rem' : '0',
              borderBottom:
                displayNodes.length > 0
                  ? '1px solid var(--border-color, #eee)'
                  : 'none',
            }}
          >
            <div style={{ display: 'flex', gap: '0.5rem' }}>
              <button
                onClick={() => setActiveTab('owned')}
                style={{
                  padding: '4px 10px',
                  fontSize: '11px',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  backgroundColor:
                    activeTab === 'owned'
                      ? 'var(--primary-color, #4a90d9)'
                      : '#e9ecef',
                  color: activeTab === 'owned' ? '#fff' : '#495057',
                }}
              >
                Owned
              </button>
              <button
                onClick={() => setActiveTab('watched')}
                style={{
                  padding: '4px 10px',
                  fontSize: '11px',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  backgroundColor:
                    activeTab === 'watched'
                      ? 'var(--primary-color, #4a90d9)'
                      : '#e9ecef',
                  color: activeTab === 'watched' ? '#fff' : '#495057',
                }}
              >
                Watched ({watchedOnly.length})
              </button>
              <button
                onClick={() => setActiveTab('edited')}
                style={{
                  padding: '4px 10px',
                  fontSize: '11px',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  backgroundColor:
                    activeTab === 'edited'
                      ? 'var(--primary-color, #4a90d9)'
                      : '#e9ecef',
                  color: activeTab === 'edited' ? '#fff' : '#495057',
                }}
              >
                Recent Edits
              </button>
            </div>

            {/* Group by Type Toggle */}
            {displayNodes.length > 0 && (
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem',
                }}
              >
                <input
                  type="checkbox"
                  id="groupByType"
                  checked={groupByType}
                  onChange={handleGroupByTypeChange}
                  style={{ cursor: 'pointer' }}
                />
                <label
                  htmlFor="groupByType"
                  style={{
                    fontSize: '11px',
                    cursor: 'pointer',
                    userSelect: 'none',
                    color: '#495057',
                    fontWeight: '500',
                  }}
                >
                  Group by Type
                </label>
              </div>
            )}
          </div>

          {displayNodes.length > 0 ? (
            <>
              {groupByType ? (
                <TypeGroupGrid
                  groupedData={groupedData}
                  username={username}
                  activeTab={activeTab}
                />
              ) : (
                <>
                  <NodeList
                    nodes={displayNodes.slice(0, maxDisplay)}
                    showUpdatedAt={true}
                  />
                  {displayNodes.length > maxDisplay && (
                    <div
                      style={{
                        textAlign: 'center',
                        padding: '0.5rem',
                        fontSize: '12px',
                        color: '#666',
                      }}
                    >
                      +{displayNodes.length - maxDisplay} more
                    </div>
                  )}
                </>
              )}
            </>
          ) : (
            <div
              style={{
                padding: '1rem',
                textAlign: 'center',
                color: '#666',
                fontSize: '12px',
              }}
            >
              {activeTab === 'owned' && 'No owned nodes'}
              {activeTab === 'watched' && 'No watched nodes'}
              {activeTab === 'edited' && 'No recent edits'}
            </div>
          )}
        </>
      )}
    </DashboardCard>
  );
}
