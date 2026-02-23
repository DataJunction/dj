import * as React from 'react';
import { Link } from 'react-router-dom';
import LoadingIcon from '../../icons/LoadingIcon';
import { NodeChip } from '../../components/NodeComponents';

// Needs Attention Section with single-line categories
export function NeedsAttentionSection({
  nodesMissingDescription,
  invalidNodes,
  staleDrafts,
  staleMaterializations,
  orphanedDimensions,
  username,
  hasItems,
  loading,
  personalNamespace,
  hasPersonalNamespace,
  namespaceLoading,
}) {
  const categories = [
    {
      id: 'invalid',
      icon: '❌',
      label: 'Invalid',
      nodes: invalidNodes,
      viewAllLink: `/?ownedBy=${username}&statuses=INVALID`,
    },
    {
      id: 'stale-drafts',
      icon: '⏰',
      label: 'Stale Drafts',
      nodes: staleDrafts,
      viewAllLink: `/?ownedBy=${username}&mode=DRAFT`,
    },
    {
      id: 'stale-materializations',
      icon: '📦',
      label: 'Stale Materializations',
      nodes: staleMaterializations,
      viewAllLink: `/?ownedBy=${username}&hasMaterialization=true`,
    },
    {
      id: 'no-description',
      icon: '📝',
      label: 'No Description',
      nodes: nodesMissingDescription,
      viewAllLink: `/?ownedBy=${username}&missingDescription=true`,
    },
    {
      id: 'orphaned-dimensions',
      icon: '🔗',
      label: 'Orphaned Dimensions',
      nodes: orphanedDimensions,
      viewAllLink: `/?ownedBy=${username}&orphanedDimension=true`,
    },
  ];

  const categoriesList = categories.map(cat => (
    <div
      key={cat.id}
      className="settings-card"
      style={{ padding: '0.5rem 0.75rem', minWidth: 0 }}
    >
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: '0.3rem',
        }}
      >
        <span style={{ fontSize: '11px', fontWeight: '600', color: '#555' }}>
          {cat.icon} {cat.label}
          <span
            style={{
              color: cat.nodes.length > 0 ? '#dc3545' : '#666',
              marginLeft: '4px',
            }}
          >
            ({cat.nodes.length})
          </span>
        </span>
        {cat.nodes.length > 0 && (
          <Link to={cat.viewAllLink} style={{ fontSize: '10px' }}>
            View all →
          </Link>
        )}
      </div>
      {cat.nodes.length > 0 ? (
        <div style={{ display: 'flex', gap: '0.3rem', overflow: 'hidden' }}>
          {cat.nodes.slice(0, 10).map(node => (
            <NodeChip key={node.name} node={node} />
          ))}
        </div>
      ) : (
        <div style={{ fontSize: '10px', color: '#28a745' }}>✓ All good!</div>
      )}
    </div>
  ));

  return (
    <section style={{ minWidth: 0, width: '100%' }}>
      <h2 className="settings-section-title">Needs Attention</h2>
      <div style={{ maxHeight: '300px', overflowY: 'auto' }}>
        {loading ? (
          <div
            className="settings-card"
            style={{ textAlign: 'center', padding: '1rem' }}
          >
            <LoadingIcon />
          </div>
        ) : (
          <div
            style={{
              display: 'flex',
              flexDirection: 'column',
              width: '100%',
              gap: '0.5rem',
            }}
          >
            {categoriesList}
            {/* Personal namespace prompt if missing */}
            {!namespaceLoading && !hasPersonalNamespace && (
              <div
                style={{
                  padding: '0.75rem',
                  backgroundColor: 'var(--card-bg, #f8f9fa)',
                  border: '1px dashed var(--border-color, #dee2e6)',
                  borderRadius: '6px',
                  textAlign: 'center',
                }}
              >
                <div style={{ fontSize: '16px', marginBottom: '0.25rem' }}>
                  📁
                </div>
                <div
                  style={{
                    fontSize: '11px',
                    fontWeight: '500',
                    marginBottom: '0.25rem',
                  }}
                >
                  Set up your namespace
                </div>
                <p
                  style={{
                    fontSize: '10px',
                    color: '#666',
                    marginBottom: '0.5rem',
                  }}
                >
                  Create{' '}
                  <code
                    style={{
                      backgroundColor: '#e9ecef',
                      padding: '1px 4px',
                      borderRadius: '3px',
                      fontSize: '9px',
                    }}
                  >
                    {personalNamespace}
                  </code>
                </p>
                <Link
                  to={`/namespaces/${personalNamespace}`}
                  style={{
                    display: 'inline-block',
                    padding: '3px 8px',
                    fontSize: '10px',
                    backgroundColor: '#28a745',
                    color: '#fff',
                    borderRadius: '4px',
                    textDecoration: 'none',
                  }}
                >
                  Create →
                </Link>
              </div>
            )}
          </div>
        )}
      </div>
    </section>
  );
}
