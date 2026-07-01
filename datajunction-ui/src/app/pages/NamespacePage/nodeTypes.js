// Canonical node-type ordering + badge colors for the NamespacePage views.
// Single source of truth shared by the namespace landing preview (index.jsx) and the
// rail's per-namespace type summary (NamespaceTypeSummary.jsx) so the order and colors
// can't drift between them.

export const NODE_TYPE_ORDER = [
  'metric',
  'cube',
  'dimension',
  'transform',
  'source',
];

export const NODE_TYPE_COLORS = {
  metric: { bg: '#fad7dd', color: '#a2283e' },
  cube: { bg: '#dbafff', color: '#580076' },
  dimension: { bg: '#ffefd0', color: '#a96621' },
  transform: { bg: '#ccefff', color: '#0063b4' },
  source: { bg: '#ccf7e5', color: '#00b368' },
};
