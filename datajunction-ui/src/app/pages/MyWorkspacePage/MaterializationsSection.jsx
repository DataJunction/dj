import * as React from 'react';
import { Link } from 'react-router-dom';
import DashboardCard from '../../components/DashboardCard';
import { NodeDisplay } from '../../components/NodeComponents';

// Materializations Section
export function MaterializationsSection({ nodes, loading }) {
  // Ensure nodes is always an array
  const sortedNodes = (nodes || []).slice().sort((a, b) => {
    const aTs = a.current?.availability?.validThroughTs;
    const bTs = b.current?.availability?.validThroughTs;
    if (!aTs && !bTs) return 0;
    if (!aTs) return 1;
    if (!bTs) return -1;
    return bTs - aTs;
  });

  const getAvailabilityStatus = availability => {
    if (!availability) {
      return { icon: '⏳', text: 'Pending', color: '#6c757d' };
    }
    const validThrough = availability.validThroughTs
      ? new Date(availability.validThroughTs)
      : null;
    const now = new Date();
    const hoursSinceUpdate = validThrough
      ? (now - validThrough) / (1000 * 60 * 60)
      : null;
    if (!validThrough) {
      return { icon: '⏳', text: 'Pending', color: '#6c757d' };
    } else if (hoursSinceUpdate <= 24) {
      return {
        icon: '🟢',
        text: formatTimeAgo(validThrough),
        color: '#28a745',
      };
    } else if (hoursSinceUpdate <= 72) {
      return {
        icon: '🟡',
        text: formatTimeAgo(validThrough),
        color: '#ffc107',
      };
    } else {
      return {
        icon: '🔴',
        text: formatTimeAgo(validThrough),
        color: '#dc3545',
      };
    }
  };

  const formatTimeAgo = date => {
    const now = new Date();
    const diffMs = now - date;
    const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
    const diffDays = Math.floor(diffHours / 24);
    if (diffHours < 1) return 'just now';
    if (diffHours < 24) return `${diffHours}h ago`;
    if (diffDays === 1) return 'yesterday';
    return `${diffDays}d ago`;
  };

  const maxDisplay = 5;

  const materializationsList = sortedNodes.slice(0, maxDisplay).map(node => {
    const status = getAvailabilityStatus(node.current?.availability);
    return (
      <div
        key={node.name}
        style={{
          padding: '0.5rem',
          border: '1px solid var(--border-color, #e0e0e0)',
          borderRadius: '4px',
          backgroundColor: 'var(--card-bg, #fff)',
        }}
      >
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            marginBottom: '4px',
          }}
        >
          <NodeDisplay node={node} size="medium" />
          <span style={{ fontSize: '10px', color: status.color }}>
            {status.icon} {status.text}
          </span>
        </div>
        <div style={{ fontSize: '10px', color: '#666' }}>
          {node.current?.materializations?.map(mat => (
            <span key={mat.name} style={{ marginRight: '8px' }}>
              🕐 {mat.schedule || 'No schedule'}
            </span>
          ))}
          {node.current?.availability?.table && (
            <span style={{ color: '#888' }}>
              → {node.current.availability.table}
            </span>
          )}
        </div>
      </div>
    );
  });

  return (
    <DashboardCard
      title="Materializations"
      actionLink="/?hasMaterialization=true"
      loading={loading}
      cardStyle={{
        padding: '0.75rem 1rem',
        maxHeight: '300px',
        overflowY: 'auto',
      }}
      emptyState={
        <div style={{ padding: '0' }}>
          <p
            style={{ fontSize: '12px', color: '#666', marginBottom: '0.75rem' }}
          >
            No materializations configured.
          </p>
          <div
            style={{
              padding: '0.75rem',
              backgroundColor: 'var(--card-bg, #f8f9fa)',
              border: '1px dashed var(--border-color, #dee2e6)',
              borderRadius: '6px',
              textAlign: 'center',
            }}
          >
            <div style={{ fontSize: '16px', marginBottom: '0.25rem' }}>📦</div>
            <div
              style={{
                fontSize: '11px',
                fontWeight: '500',
                marginBottom: '0.25rem',
              }}
            >
              Materialize a node
            </div>
            <p
              style={{
                fontSize: '10px',
                color: '#666',
                marginBottom: '0.5rem',
              }}
            >
              Speed up queries with cached data
            </p>
            <Link
              to="/"
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
              Browse nodes →
            </Link>
          </div>
        </div>
      }
    >
      {sortedNodes.length > 0 && (
        <div
          style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}
        >
          {materializationsList}
          {sortedNodes.length > maxDisplay && (
            <div
              style={{
                textAlign: 'center',
                padding: '0.5rem',
                fontSize: '12px',
                color: '#666',
              }}
            >
              +{sortedNodes.length - maxDisplay} more
            </div>
          )}
        </div>
      )}
    </DashboardCard>
  );
}
