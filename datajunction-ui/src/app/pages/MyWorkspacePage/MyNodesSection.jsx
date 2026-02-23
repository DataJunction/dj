import * as React from 'react';
import { Link } from 'react-router-dom';
import DashboardCard from '../../components/DashboardCard';
import { NodeList } from './NodeList';

// My Nodes Section (owned + watched, with tabs)
export function MyNodesSection({
  ownedNodes,
  watchedNodes,
  recentlyEdited,
  username,
  loading,
}) {
  const [activeTab, setActiveTab] = React.useState('owned');

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

  return (
    <DashboardCard
      title="My Nodes"
      actionLink={`/?ownedBy=${username}`}
      loading={loading}
      cardStyle={{
        padding: '0.25rem 0.75rem',
        maxHeight: '400px',
        overflowY: 'auto',
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
          {/* Tabs */}
          <div
            style={{
              display: 'flex',
              gap: '0.5rem',
              marginBottom: '0.5rem',
              paddingTop: '0.5rem',
            }}
          >
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
              Owned ({ownedNodes.length})
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
              Recent Edits ({recentlyEdited.length})
            </button>
          </div>
          {displayNodes.length > 0 ? (
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
