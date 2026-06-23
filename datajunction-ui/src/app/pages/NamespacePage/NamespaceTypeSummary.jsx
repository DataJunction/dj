import React, { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';

const NODE_TYPE_ORDER = ['metric', 'cube', 'dimension', 'transform', 'source'];
const NODE_TYPE_COLORS = {
  metric: { bg: '#fad7dd', color: '#a2283e' },
  cube: { bg: '#dbafff', color: '#580076' },
  dimension: { bg: '#ffefd0', color: '#a96621' },
  transform: { bg: '#ccefff', color: '#0063b4' },
  source: { bg: '#ccf7e5', color: '#00b368' },
};

// A compact, clickable summary of the node counts (by type) under a namespace.
// Each row deep-links to the type-filtered node list for the selected branch.
// One cheap limit:1 count query per type — we only read totalCount.
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
    Promise.all(
      NODE_TYPE_ORDER.map(type =>
        djClient
          .listNodesForLanding(
            namespace,
            [type.toUpperCase()],
            [],
            null,
            null,
            null,
            1,
            { key: 'name', direction: 'ascending' },
            null,
            {},
          )
          .then(result => ({
            type,
            count: result?.data?.findNodesPaginated?.totalCount ?? 0,
          }))
          .catch(() => ({ type, count: 0 })),
      ),
    ).then(results => {
      if (!cancelled) setCounts(results);
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
