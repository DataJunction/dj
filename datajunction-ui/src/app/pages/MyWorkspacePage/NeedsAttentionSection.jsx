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

  return (
    <section style={{ minWidth: 0, width: '100%' }}>
      <h2 className="settings-section-title">Needs Attention</h2>
      <div className="settings-card" style={{ padding: '0.25rem 0.75rem' }}>
        {loading ? (
          <div style={{ textAlign: 'center', padding: '1rem' }}>
            <LoadingIcon />
          </div>
        ) : (
          <>
            {categories.map((cat, i) => (
              <div
                key={cat.id}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem',
                  padding: '0.35rem 0',
                  borderBottom:
                    i < categories.length - 1
                      ? '1px solid var(--border-color, #f0f0f0)'
                      : 'none',
                  minWidth: 0,
                }}
              >
                {/* Label + count */}
                <span
                  style={{
                    fontSize: '11px',
                    fontWeight: 600,
                    color: '#555',
                    whiteSpace: 'nowrap',
                    flexShrink: 0,
                  }}
                >
                  {cat.label}{' '}
                  <span
                    style={{
                      color: cat.nodes.length > 0 ? '#dc3545' : '#28a745',
                      fontWeight: 500,
                    }}
                  >
                    ({cat.nodes.length})
                  </span>
                </span>

                {/* Chips with right-side fade */}
                {cat.nodes.length > 0 ? (
                  <div
                    style={{
                      flex: 1,
                      overflow: 'hidden',
                      maskImage:
                        'linear-gradient(to right, black 75%, transparent 100%)',
                      WebkitMaskImage:
                        'linear-gradient(to right, black 75%, transparent 100%)',
                    }}
                  >
                    <div
                      style={{
                        display: 'flex',
                        gap: '0.25rem',
                        flexWrap: 'nowrap',
                      }}
                    >
                      {cat.nodes.slice(0, 20).map(node => (
                        <NodeChip key={node.name} node={node} />
                      ))}
                    </div>
                  </div>
                ) : (
                  <span style={{ fontSize: '10px', color: '#28a745', flex: 1 }}>
                    All good
                  </span>
                )}

                {/* Arrow link — always visible, outside the fade */}
                {cat.nodes.length > 0 && (
                  <Link
                    to={cat.viewAllLink}
                    style={{ fontSize: '11px', flexShrink: 0 }}
                  >
                    →
                  </Link>
                )}
              </div>
            ))}

            {!namespaceLoading && !hasPersonalNamespace && (
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem',
                  padding: '0.35rem 0',
                  fontSize: '11px',
                }}
              >
                <span style={{ fontWeight: 600, color: '#555', flexShrink: 0 }}>
                  Personal namespace
                </span>
                <Link
                  to={`/namespaces/${personalNamespace}`}
                  style={{ fontSize: '10px' }}
                >
                  Create {personalNamespace} →
                </Link>
              </div>
            )}
          </>
        )}
      </div>
    </section>
  );
}
