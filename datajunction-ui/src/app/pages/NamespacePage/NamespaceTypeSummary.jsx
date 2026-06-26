import React, { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { NODE_TYPE_ORDER, NODE_TYPE_COLORS } from './nodeTypes';

// A compact, clickable summary of the node counts (by type) under a namespace.
// Each row deep-links to the type-filtered node list for the selected branch.
// Counts come from a single count-only request (djClient.nodeTypeCounts).
export default function NamespaceTypeSummary({
  namespace,
  showHeading = true,
}) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [counts, setCounts] = useState(null);

  useEffect(() => {
    let cancelled = false;
    if (!namespace) {
      setCounts(null);
      return;
    }
    setCounts(null);
    djClient
      .nodeTypeCounts(namespace, NODE_TYPE_ORDER)
      .then(byType => {
        if (!cancelled) {
          setCounts(
            NODE_TYPE_ORDER.map(type => ({ type, count: byType[type] ?? 0 })),
          );
        }
      })
      .catch(() => {
        if (!cancelled) setCounts([]);
      });
    return () => {
      cancelled = true;
    };
  }, [djClient, namespace]);

  if (!counts) return null;
  const nonEmpty = counts.filter(c => c.count > 0);
  if (nonEmpty.length === 0) return null;

  return (
    <div className="dj-ns-type-summary">
      {showHeading ? (
        <div className="dj-ns-type-summary-heading">Nodes</div>
      ) : null}
      {nonEmpty.map(({ type, count }) => (
        <a
          key={type}
          className="dj-ns-type-row"
          href={`/namespaces/${namespace}?type=${type}`}
        >
          <span className="dj-ns-type-name">{`${type}s`}</span>
          <span
            className="dj-ns-type-count"
            style={{
              backgroundColor: NODE_TYPE_COLORS[type]?.bg ?? '#f1f5f9',
              color: NODE_TYPE_COLORS[type]?.color ?? '#475569',
            }}
          >
            {count}
          </span>
        </a>
      ))}
    </div>
  );
}
