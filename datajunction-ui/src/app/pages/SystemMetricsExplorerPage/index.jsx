import { memo, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import Select from 'react-select';
import {
  ResponsiveContainer,
  LineChart,
  Line,
  AreaChart,
  Area,
  BarChart,
  Bar,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
} from 'recharts';
import DJClientContext from '../../providers/djclient';
import { DataJunctionAPI } from '../../services/DJService';
import './styles.css';

const PALETTE = [
  '#3b51d6',
  '#00C49F',
  '#FFBB28',
  '#FF91A3',
  '#AA46BE',
  '#0088FE',
  '#FF8042',
  '#82ca9d',
];

// Brighter cousins of the canonical DJ node-type colors
// (the canonical ones in src/styles/index.css are tuned for badges on a
// white background — they're too desaturated for chart series).
const NODE_TYPE_COLORS = {
  source: '#22c55e', // bright emerald
  transform: '#3b82f6', // bright blue
  metric: '#f43f5e', // bright rose
  dimension: '#f59e0b', // bright amber
  cube: '#a855f7', // bright purple
  tag: '#8b5cf6', // bright violet
};

const colorForSeries = (label, fallback) => {
  if (!label) return { color: fallback, opacity: 0.85 };
  const key = String(label).toLowerCase();
  if (NODE_TYPE_COLORS[key]) {
    return { color: NODE_TYPE_COLORS[key], opacity: 0.85 };
  }
  return { color: fallback, opacity: 0.85 };
};

const OPERATORS = [
  { value: '=', label: '=' },
  { value: '!=', label: '≠' },
  { value: 'in', label: 'is one of' },
  { value: '>', label: '>' },
  { value: '<', label: '<' },
  { value: '>=', label: '≥' },
  { value: '<=', label: '≤' },
];

const isTemporal = name =>
  /(_date|_week|_month|_quarter|_year|_day|dateint|created_at)/i.test(name);

const typeIcon = type => {
  if (!type) return '#';
  const t = String(type).toLowerCase();
  if (/(bool)/.test(t)) return '✓';
  if (/(date|time)/.test(t)) return '📅';
  if (/(int|long|double|float|decimal|numeric|number)/.test(t)) return '123';
  if (/(string|varchar|char|text)/.test(t)) return 'Aa';
  if (/(list|array)/.test(t)) return '[ ]';
  return '#';
};

// Shared react-select styles so options inside groups are visibly indented
// under their group header, and the header has a tight, distinct look.
const groupedSelectStyles = {
  groupHeading: base => ({
    ...base,
    fontSize: 10.5,
    fontWeight: 600,
    textTransform: 'uppercase',
    letterSpacing: '0.04em',
    color: '#64748b',
    padding: '6px 12px 2px',
  }),
  option: (base, state) => ({
    ...base,
    paddingLeft: state.data && state.data.group ? 28 : base.paddingLeft,
  }),
  group: base => ({
    ...base,
    paddingTop: 4,
    paddingBottom: 4,
  }),
};

// Recharts tooltip that drops zero-valued series — when stacked breakdowns
// have lots of empty cells (zero-filled for stacking), the default tooltip
// fills the screen with "X: 0" rows that aren't useful.
function NonZeroTooltip({ active, payload, label }) {
  if (!active || !payload || !payload.length) return null;
  const nonZero = payload.filter(p => p && p.value !== 0 && p.value != null);
  if (!nonZero.length) return null;
  return (
    <div
      style={{
        background: '#fff',
        border: '1px solid #e2e8f0',
        borderRadius: 6,
        padding: '8px 10px',
        fontSize: 12,
        boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
      }}
    >
      <div style={{ fontWeight: 600, marginBottom: 4, color: '#1e293b' }}>
        {label}
      </div>
      {nonZero.map(p => (
        <div
          key={p.dataKey}
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 6,
            color: '#334155',
          }}
        >
          <span
            style={{
              display: 'inline-block',
              width: 9,
              height: 9,
              background: p.color,
              borderRadius: 2,
            }}
          />
          <span>{p.name}</span>
          <span
            style={{ marginLeft: 'auto', fontVariantNumeric: 'tabular-nums' }}
          >
            {typeof p.value === 'number' ? p.value.toLocaleString() : p.value}
          </span>
        </div>
      ))}
    </div>
  );
}

const isNumericLike = v =>
  typeof v === 'number' || (typeof v === 'string' && /^-?\d+(\.\d+)?$/.test(v));

// Recharts subtree, memoed so unrelated parent re-renders (e.g. URL syncs,
// filter-input keystrokes) don't re-mount the chart's SVG paths.
const ChartView = memo(function ChartView({
  chartData,
  resolvedChartType,
  startAtZero,
}) {
  const stacked = chartData.series.length > 1;
  const isLine = resolvedChartType === 'line';
  const isArea = resolvedChartType === 'area';
  const ChartCmp = isArea ? AreaChart : isLine ? LineChart : BarChart;
  return (
    <div className="sme-chart-body">
      <ResponsiveContainer width="100%" height="100%">
        <ChartCmp
          data={chartData.data}
          margin={{ top: 10, right: 30, left: 10, bottom: 20 }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
          <XAxis dataKey={chartData.xKey} tick={{ fontSize: 12 }} />
          <YAxis
            tick={{ fontSize: 12 }}
            domain={startAtZero ? [0, 'auto'] : ['auto', 'auto']}
          />
          <Tooltip
            content={<NonZeroTooltip />}
            isAnimationActive={false}
            cursor={{ fill: 'rgba(15, 23, 42, 0.04)' }}
          />
          {stacked ? <Legend /> : null}
          {chartData.series.map((s, i) => {
            const name = chartData.labels[s] || s;
            const { color, opacity } = colorForSeries(
              name,
              PALETTE[i % PALETTE.length],
            );
            if (isArea) {
              return (
                <Area
                  key={s}
                  type="monotone"
                  dataKey={s}
                  name={name}
                  stackId={stacked ? '1' : undefined}
                  stroke={color}
                  fill={color}
                  fillOpacity={opacity}
                  isAnimationActive={false}
                  activeDot={false}
                />
              );
            }
            if (isLine) {
              return (
                <Line
                  key={s}
                  type="monotone"
                  dataKey={s}
                  name={name}
                  stroke={color}
                  strokeWidth={2}
                  dot={false}
                  isAnimationActive={false}
                  activeDot={false}
                />
              );
            }
            return (
              <Bar
                key={s}
                dataKey={s}
                name={name}
                fill={color}
                fillOpacity={opacity}
                stackId={stacked ? '1' : undefined}
                isAnimationActive={false}
                activeBar={false}
              />
            );
          })}
        </ChartCmp>
      </ResponsiveContainer>
    </div>
  );
});

// Parse URL search params into the explorer's initial state.
function parseSearch(search) {
  const p = new URLSearchParams(search);
  const filters = p
    .getAll('filter')
    .map(raw => {
      const [dim = '', op = '=', ...rest] = raw.split('|');
      return { dim, op, value: rest.join('|') };
    })
    .filter(f => f.dim);
  return {
    metric: p.get('metric') || null,
    xAxis: p.get('x') || null,
    compareBy: (p.get('by') || '').split(',').filter(Boolean),
    filters,
    view: p.get('view') === 'table' ? 'table' : 'chart',
    chartType: ['line', 'area', 'bar', 'auto'].includes(p.get('chart'))
      ? p.get('chart')
      : 'auto',
    startAtZero: p.get('zero') === '1',
  };
}

export function SystemMetricsExplorerPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const location = useLocation();
  const navigate = useNavigate();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const initial = useMemo(() => parseSearch(location.search), []);

  const [metricsList, setMetricsList] = useState([]);
  const [metricSearch, setMetricSearch] = useState('');
  const [dimSearch, setDimSearch] = useState('');

  const [selectedMetric, setSelectedMetric] = useState(initial.metric);
  const [availableDims, setAvailableDims] = useState([]); // [{name,type,path,label}]
  const [xAxisDim, setXAxisDim] = useState(null);
  const [compareBy, setCompareBy] = useState([]);
  const [filters, setFilters] = useState(initial.filters);

  // Pending selections from the URL that need to be resolved against
  // availableDims once they load.
  const pendingXAxis = useRef(initial.xAxis);
  const pendingCompareBy = useRef(initial.compareBy);

  const [rows, setRows] = useState(null);
  const [columns, setColumns] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const [view, setView] = useState(initial.view);
  const [chartType, setChartType] = useState(initial.chartType);
  const [startAtZero, setStartAtZero] = useState(initial.startAtZero);

  // Load system metrics
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const list = await djClient.system.list();
        if (cancelled) return;
        if (!Array.isArray(list)) {
          setError(
            `Unexpected response from /system/metrics: ${JSON.stringify(
              list,
            ).slice(0, 200)}`,
          );
          setMetricsList([]);
          return;
        }
        // Endpoint may return either ["name", ...] (legacy) or
        // [{name, display_name, description}, ...] (new). Normalize.
        const normalized = list.map(item => {
          if (typeof item === 'string') {
            return {
              name: item,
              display_name: item,
              description: '',
              group: 'Other',
              subgroup: 'Other',
              suggestedCompareBy: [],
            };
          }
          const meta = item.custom_metadata || {};
          return {
            name: item.name,
            display_name: item.display_name || item.name,
            description: item.description || '',
            group: meta.group || 'Other',
            subgroup: meta.subgroup || 'Other',
            suggestedCompareBy: Array.isArray(meta.suggested_compare_by)
              ? meta.suggested_compare_by
              : [],
          };
        });
        setMetricsList(normalized);
        if (normalized.length && !selectedMetric)
          setSelectedMetric(normalized[0].name);
      } catch (e) {
        if (!cancelled) setError(String(e));
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [djClient]);

  // Load dimensions for selected metric
  useEffect(() => {
    if (!selectedMetric) {
      setAvailableDims([]);
      return;
    }
    let cancelled = false;
    setXAxisDim(null);
    setCompareBy([]);
    setRows(null);
    setColumns([]);
    setError(null);
    (async () => {
      try {
        const dims = await djClient.commonDimensions([selectedMetric]);
        if (cancelled) return;
        const opts = (Array.isArray(dims) ? dims : []).map(d => {
          // d.name is e.g. "system.dj.date.dateint[created_at]". Pull the
          // role suffix out for the group header.
          const roleMatch = d.name.match(/\[([^\]]+)\]$/);
          const role = roleMatch ? roleMatch[1] : null;
          const baseName = role
            ? d.name.slice(0, -roleMatch[0].length)
            : d.name;
          const segments = baseName.split('.');
          const attr = segments[segments.length - 1];
          const nodeDisplay =
            d.node_display_name || segments.slice(0, -1).join('.');
          const colDisplay = d.column_display_name || attr;
          const group = role ? `${nodeDisplay} [${role}]` : nodeDisplay;
          return {
            value: d.name,
            label: colDisplay,
            // Combined label used in search and in some narrow contexts.
            combinedLabel: `${group} · ${colDisplay}`,
            attr,
            nodeDisplay,
            colDisplay,
            role,
            group,
            path: d.path.join(' ▶ '),
            type: d.type,
          };
        });
        setAvailableDims(opts);

        // Apply pending URL state first if present; otherwise default to a
        // temporal X-axis. Pending state is one-shot — clear after applying.
        if (pendingXAxis.current) {
          const match = opts.find(o => o.value === pendingXAxis.current);
          if (match) setXAxisDim(match);
          pendingXAxis.current = null;
        } else {
          // Prefer a "week" temporal column; fall back to any temporal one.
          const weekly = opts.find(o => /_week\b/i.test(o.value));
          const firstTemporal = weekly || opts.find(o => isTemporal(o.value));
          if (firstTemporal) setXAxisDim(firstTemporal);
        }
        if (pendingCompareBy.current && pendingCompareBy.current.length) {
          const matches = pendingCompareBy.current
            .map(v => opts.find(o => o.value === v))
            .filter(Boolean);
          if (matches.length) setCompareBy(matches);
          pendingCompareBy.current = null;
        } else {
          // Apply the metric's suggested compare-by defaults (from
          // custom_metadata.suggested_compare_by). Only kicks in when the
          // user has no pending compare-by from the URL.
          const metric = metricsList.find(m => m.name === selectedMetric);
          const suggested = (metric && metric.suggestedCompareBy) || [];
          if (suggested.length) {
            const matches = suggested
              .map(v => opts.find(o => o.value === v))
              .filter(Boolean);
            if (matches.length) setCompareBy(matches);
          }
        }
      } catch (e) {
        if (!cancelled) setError(String(e));
      }
    })();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedMetric, djClient]);

  // Sync state back to URL so links are shareable.
  useEffect(() => {
    const p = new URLSearchParams();
    if (selectedMetric) p.set('metric', selectedMetric);
    if (xAxisDim) p.set('x', xAxisDim.value);
    if (compareBy.length) p.set('by', compareBy.map(d => d.value).join(','));
    filters.forEach(f => {
      if (f.dim && f.op) {
        p.append('filter', `${f.dim}|${f.op}|${f.value ?? ''}`);
      }
    });
    if (view !== 'chart') p.set('view', view);
    if (chartType !== 'auto') p.set('chart', chartType);
    if (startAtZero) p.set('zero', '1');
    const next = p.toString();
    const current = window.location.search.replace(/^\?/, '');
    if (next !== current) {
      navigate(
        { pathname: window.location.pathname, search: next ? `?${next}` : '' },
        { replace: true },
      );
    }
  }, [
    selectedMetric,
    xAxisDim,
    compareBy,
    filters,
    view,
    chartType,
    startAtZero,
    navigate,
  ]);

  // Run query whenever the user changes inputs
  useEffect(() => {
    if (!selectedMetric) return;
    let cancelled = false;
    (async () => {
      setLoading(true);
      setError(null);
      try {
        const dimensions = [
          ...(xAxisDim ? [xAxisDim.value] : []),
          ...compareBy.map(d => d.value),
        ];
        const filterClauses = filters
          .filter(f => f.dim && f.op && f.value !== '')
          .map(f => {
            if (f.op === 'in') {
              const vals = f.value
                .split(',')
                .map(v => v.trim())
                .filter(Boolean)
                .map(v => (isNumericLike(v) ? v : `'${v.replace(/'/g, "''")}'`))
                .join(', ');
              return `${f.dim} IN (${vals})`;
            }
            const lhs = f.dim;
            const rhs = isNumericLike(f.value)
              ? f.value
              : `'${String(f.value).replace(/'/g, "''")}'`;
            return `${lhs} ${f.op} ${rhs}`;
          });

        const orderby = xAxisDim ? [xAxisDim.value] : [];

        const result = await djClient.querySystemMetric({
          metric: selectedMetric,
          dimensions,
          filters: filterClauses,
          orderby,
        });
        if (cancelled) return;
        // New compact shape: { columns: [...], rows: [[...], ...] }.
        // Rows are already aligned to columns, no per-cell envelope.
        setColumns(result.columns || []);
        setRows(result.rows || []);
      } catch (e) {
        if (!cancelled) setError(String(e));
        setRows(null);
        setColumns([]);
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [selectedMetric, xAxisDim, compareBy, filters, djClient]);

  const metricDisplay = useMemo(() => {
    const map = {};
    for (const m of metricsList) map[m.name] = m.display_name || m.name;
    return map;
  }, [metricsList]);

  // Shape data for the chart.
  // Recharts uses lodash-style get(obj, dataKey), so keys containing dots
  // (like "system.dj.number_of_nodes") would be interpreted as nested
  // paths and produce undefined. Flatten to safe keys (__x, s0, s1, ...)
  // and keep a label map for tooltip/legend display.
  const chartData = useMemo(() => {
    const empty = { data: [], series: [], xKey: null, labels: {} };
    if (!rows || !selectedMetric) return empty;
    const xKey = xAxisDim?.value;
    const breakdownKey = compareBy[0]?.value || null;
    const metricCol = selectedMetric;
    const metricIdx = columns.indexOf(metricCol);
    if (metricIdx === -1) return empty;
    const num = v => {
      if (v === null || v === undefined) return 0;
      const n = typeof v === 'number' ? v : parseFloat(v);
      return Number.isFinite(n) ? n : 0;
    };
    const X = '__x';

    // Strip rows (X buckets) where every series is 0/null and series that
    // are 0/null at every X. Both flavors of "empty" are noise: an X bucket
    // with no data adds an unused tick; a series with no data adds a legend
    // entry and a phantom stack member.
    const pruneEmpty = ({ data, series, labels, xKey: xk }) => {
      const seriesWithValue = series.filter(s =>
        data.some(r => r[s] !== 0 && r[s] != null),
      );
      const keptLabels = {};
      for (const s of seriesWithValue) keptLabels[s] = labels[s];
      const keptData = data
        .map(r => {
          const out = { [xk]: r[xk] };
          for (const s of seriesWithValue) out[s] = r[s];
          return out;
        })
        .filter(r => seriesWithValue.some(s => r[s] !== 0 && r[s] != null));
      return {
        data: keptData,
        series: seriesWithValue,
        labels: keptLabels,
        xKey: xk,
      };
    };

    if (!xKey) {
      const bdIdx = breakdownKey ? columns.indexOf(breakdownKey) : -1;
      const sKey = 's0';
      const baseLabel =
        metricDisplay[metricCol] || metricCol.replace(/^system\.dj\./, '');
      if (bdIdx === -1) {
        return {
          data: [{ [X]: metricCol, [sKey]: num(rows[0]?.[metricIdx]) }],
          series: [sKey],
          xKey: X,
          labels: { [sKey]: baseLabel },
        };
      }
      return pruneEmpty({
        data: rows.map(r => ({
          [X]: String(r[bdIdx]),
          [sKey]: num(r[metricIdx]),
        })),
        series: [sKey],
        xKey: X,
        labels: { [sKey]: baseLabel },
      });
    }

    const xIdx = columns.indexOf(xKey);
    if (xIdx === -1) return empty;

    const baseLabel =
      metricDisplay[metricCol] || metricCol.replace(/^system\.dj\./, '');

    if (!breakdownKey) {
      const sKey = 's0';
      return pruneEmpty({
        data: rows.map(r => ({
          [X]: String(r[xIdx]),
          [sKey]: num(r[metricIdx]),
        })),
        series: [sKey],
        xKey: X,
        labels: { [sKey]: baseLabel },
      });
    }

    const bdIdx = columns.indexOf(breakdownKey);
    const byX = new Map();
    const seriesLabels = new Map(); // safeKey -> displayLabel
    const seriesOrder = [];
    for (const r of rows) {
      const xVal = String(r[xIdx]);
      const bdVal = String(r[bdIdx]);
      if (!seriesLabels.has(bdVal)) {
        const safe = `s${seriesLabels.size}`;
        seriesLabels.set(bdVal, safe);
        seriesOrder.push(safe);
      }
      const safe = seriesLabels.get(bdVal);
      if (!byX.has(xVal)) byX.set(xVal, { [X]: xVal });
      byX.get(xVal)[safe] = num(r[metricIdx]);
    }
    const labels = {};
    seriesLabels.forEach((safe, orig) => {
      labels[safe] = orig;
    });
    const filled = [];
    byX.forEach(row => {
      const out = { ...row };
      for (const safe of seriesOrder) {
        if (out[safe] === undefined || out[safe] === null) out[safe] = 0;
      }
      filled.push(out);
    });
    return pruneEmpty({
      data: filled,
      series: seriesOrder,
      xKey: X,
      labels,
    });
  }, [rows, columns, xAxisDim, compareBy, selectedMetric, metricDisplay]);

  const filteredMetrics = useMemo(() => {
    const q = metricSearch.toLowerCase();
    return metricsList.filter(
      m =>
        m.name.toLowerCase().includes(q) ||
        (m.display_name && m.display_name.toLowerCase().includes(q)),
    );
  }, [metricsList, metricSearch]);

  // Two-level grouping: group > subgroup > metric[]. Preserves insertion
  // order so the seed's intended ordering is honored.
  // NOTE: avoid `[...map.entries()]` here — this bundle has a polyfill that
  // breaks Map's iterator and silently produces empty arrays. forEach works.
  const groupedMetrics = useMemo(() => {
    const groupOrder = [];
    const groupMap = {};
    for (const m of filteredMetrics) {
      const g = m.group || 'Other';
      const sg = m.subgroup || 'Other';
      if (!groupMap[g]) {
        groupMap[g] = { subgroupOrder: [], subgroups: {} };
        groupOrder.push(g);
      }
      if (!groupMap[g].subgroups[sg]) {
        groupMap[g].subgroups[sg] = [];
        groupMap[g].subgroupOrder.push(sg);
      }
      groupMap[g].subgroups[sg].push(m);
    }
    return groupOrder.map(group => ({
      group,
      subgroups: groupMap[group].subgroupOrder.map(subgroup => ({
        subgroup,
        metrics: groupMap[group].subgroups[subgroup],
      })),
    }));
  }, [filteredMetrics]);

  const selectedMetricDisplay = selectedMetric
    ? metricDisplay[selectedMetric] ||
      selectedMetric.replace(/^system\.dj\./, '')
    : null;

  const selectedMetricDescription = selectedMetric
    ? (metricsList.find(m => m.name === selectedMetric) || {}).description || ''
    : '';

  const filteredDims = useMemo(() => {
    const q = dimSearch.toLowerCase();
    return availableDims.filter(
      d =>
        d.value.toLowerCase().includes(q) ||
        d.combinedLabel.toLowerCase().includes(q),
    );
  }, [availableDims, dimSearch]);

  // Group filtered dims by their parent dim node (+ role) for use in
  // react-select option groups and in the rail's sectioned list.
  const groupedDims = useMemo(() => {
    const order = [];
    const byGroup = new Map();
    for (const d of filteredDims) {
      if (!byGroup.has(d.group)) {
        byGroup.set(d.group, []);
        order.push(d.group);
      }
      byGroup.get(d.group).push(d);
    }
    return order.map(group => ({ label: group, options: byGroup.get(group) }));
  }, [filteredDims]);

  const resolvedChartType = useMemo(() => {
    if (chartType !== 'auto') return chartType;
    const temporal = xAxisDim && isTemporal(xAxisDim.value);
    if (!temporal) return 'bar';
    return compareBy.length > 0 ? 'area' : 'line';
  }, [chartType, xAxisDim, compareBy]);

  const addFilter = () =>
    setFilters([...filters, { dim: '', op: '=', value: '' }]);

  const updateFilter = (i, patch) =>
    setFilters(filters.map((f, idx) => (idx === i ? { ...f, ...patch } : f)));

  const removeFilter = i => setFilters(filters.filter((_, idx) => idx !== i));

  const renderChart = () => {
    if (loading)
      return (
        <div className="sme-empty">
          <span className="sme-spinner" />
          Loading…
        </div>
      );
    if (!rows || rows.length === 0)
      return (
        <div className="sme-empty">
          <div className="sme-empty-icon">📈</div>
          No data.
        </div>
      );
    return (
      <ChartView
        chartData={chartData}
        resolvedChartType={resolvedChartType}
        startAtZero={startAtZero}
      />
    );
  };

  const renderTable = () => {
    if (loading)
      return (
        <div className="sme-empty">
          <span className="sme-spinner" />
          Loading…
        </div>
      );
    if (!rows || rows.length === 0)
      return <div className="sme-empty">No data.</div>;
    return (
      <div className="sme-table-wrap">
        <table className="sme-table">
          <thead>
            <tr>
              {columns.map(c => (
                <th key={c}>{c}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr key={`row-${i}`}>
                {row.map((v, j) => (
                  <td key={`${i}-${j}`}>
                    {v === null || v === undefined ? '' : String(v)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  };

  return (
    <div className="sme-page">
      {/* Left rail */}
      <aside className="sme-rail">
        <div className="sme-rail-section grow">
          <div className="sme-rail-header">
            <span>System Metrics</span>
            <span className="sme-rail-count">{filteredMetrics.length}</span>
          </div>
          <input
            className="sme-rail-search"
            placeholder="Search metrics"
            value={metricSearch}
            onChange={e => setMetricSearch(e.target.value)}
          />
          <div className="sme-rail-list">
            {groupedMetrics.map(g => (
              <div key={g.group} className="sme-rail-group">
                <div className="sme-rail-group-header">{g.group}</div>
                {g.subgroups.map(sg => (
                  <div key={sg.subgroup} className="sme-rail-subgroup">
                    <div className="sme-rail-subgroup-header">
                      {sg.subgroup}
                    </div>
                    {sg.metrics.map(m => (
                      <div
                        key={m.name}
                        className={`sme-rail-item indent${
                          selectedMetric === m.name ? ' active' : ''
                        }`}
                        onClick={() => setSelectedMetric(m.name)}
                        title={
                          m.description
                            ? `${m.name}\n\n${m.description}`
                            : m.name
                        }
                      >
                        <span className="sme-rail-icon">Σ</span>
                        {m.display_name || m.name.replace(/^system\.dj\./, '')}
                      </div>
                    ))}
                  </div>
                ))}
              </div>
            ))}
          </div>
        </div>

        <div className="sme-rail-section grow">
          <div className="sme-rail-header">
            <span>Dimensions</span>
            <span className="sme-rail-count">{filteredDims.length}</span>
          </div>
          <input
            className="sme-rail-search"
            placeholder="Search dimensions"
            value={dimSearch}
            onChange={e => setDimSearch(e.target.value)}
            disabled={!selectedMetric}
          />
          <div className="sme-rail-list">
            {groupedDims.map(group => (
              <div key={group.label} className="sme-rail-group">
                <div className="sme-rail-group-header">{group.label}</div>
                {group.options.map(d => {
                  const selected =
                    compareBy.some(c => c.value === d.value) ||
                    xAxisDim?.value === d.value;
                  return (
                    <div
                      key={d.value}
                      className={`sme-rail-item indent${
                        selected ? ' active' : ''
                      }`}
                      title={`${d.value}\n${d.path}\nClick to toggle in Compare-by`}
                      onClick={() => {
                        if (xAxisDim?.value === d.value) return;
                        setCompareBy(prev =>
                          prev.some(c => c.value === d.value)
                            ? prev.filter(c => c.value !== d.value)
                            : [...prev, d],
                        );
                      }}
                    >
                      <span className="sme-rail-icon">{typeIcon(d.type)}</span>
                      {d.label}
                    </div>
                  );
                })}
              </div>
            ))}
          </div>
        </div>
      </aside>

      {/* Main pane */}
      <div className="sme-main">
        <div className="sme-toolbar">
          <div className="sme-row">
            <span className="sme-label">X-axis</span>
            <span className="sme-select-wide">
              <Select
                isClearable
                placeholder="Choose an X-axis dimension"
                options={groupedDims}
                value={xAxisDim}
                onChange={setXAxisDim}
                isDisabled={!selectedMetric}
                styles={groupedSelectStyles}
              />
            </span>
          </div>
          <div className="sme-row">
            <span className="sme-label">Compare by</span>
            <span className="sme-select-wide">
              <Select
                isMulti
                isClearable
                placeholder="Break down by dimensions"
                options={groupedDims
                  .map(g => ({
                    ...g,
                    options: g.options.filter(d => d.value !== xAxisDim?.value),
                  }))
                  .filter(g => g.options.length > 0)}
                value={compareBy}
                onChange={vals => setCompareBy(vals || [])}
                isDisabled={!selectedMetric}
                styles={groupedSelectStyles}
              />
            </span>
          </div>
          {filters.map((f, i) => (
            <div className="sme-filter-row" key={`f-${i}`}>
              <span className="sme-label">{i === 0 ? 'Filter' : 'and'}</span>
              <span className="sme-select">
                <Select
                  placeholder="Dimension"
                  options={groupedDims}
                  value={availableDims.find(d => d.value === f.dim) || null}
                  onChange={o => updateFilter(i, { dim: o?.value || '' })}
                  styles={groupedSelectStyles}
                />
              </span>
              <span style={{ minWidth: 140 }}>
                <Select
                  options={OPERATORS}
                  value={OPERATORS.find(o => o.value === f.op)}
                  onChange={o => updateFilter(i, { op: o.value })}
                />
              </span>
              <input
                className="sme-text-input"
                placeholder={f.op === 'in' ? 'comma-separated values' : 'value'}
                value={f.value}
                onChange={e => updateFilter(i, { value: e.target.value })}
              />
              <button
                className="sme-remove-filter"
                onClick={() => removeFilter(i)}
                aria-label="Remove filter"
              >
                ×
              </button>
            </div>
          ))}
          <div className="sme-row">
            <button
              className="sme-add-filter"
              onClick={addFilter}
              disabled={!selectedMetric}
            >
              + Add filter
            </button>
          </div>
        </div>

        <div className="sme-content">
          <div className="sme-chart-controls">
            <div className="sme-view-toggle">
              <button
                className={`sme-view-btn${view === 'chart' ? ' active' : ''}`}
                onClick={() => setView('chart')}
              >
                📈 Chart
              </button>
              <button
                className={`sme-view-btn${view === 'table' ? ' active' : ''}`}
                onClick={() => setView('table')}
              >
                ▦ Table
              </button>
            </div>
            {view === 'chart' ? (
              <div className="sme-view-toggle">
                {[
                  { key: 'auto', label: 'Auto' },
                  { key: 'line', label: 'Line' },
                  { key: 'area', label: 'Area' },
                  { key: 'bar', label: 'Bar' },
                ].map(t => (
                  <button
                    key={t.key}
                    className={`sme-view-btn${
                      chartType === t.key ? ' active' : ''
                    }`}
                    onClick={() => setChartType(t.key)}
                  >
                    {t.label}
                  </button>
                ))}
              </div>
            ) : null}
            <div className="sme-options">
              <label>
                <input
                  type="checkbox"
                  checked={startAtZero}
                  onChange={e => setStartAtZero(e.target.checked)}
                />
                Start scale at zero
              </label>
            </div>
          </div>

          <h2 className="sme-chart-title">
            {selectedMetric ? (
              <a
                href={`/nodes/${selectedMetric}`}
                target="_blank"
                rel="noreferrer"
                title={`Open ${selectedMetric}`}
                className="sme-chart-title-link"
              >
                {selectedMetricDisplay}
              </a>
            ) : (
              'Pick a system metric'
            )}
          </h2>
          {selectedMetricDescription ? (
            <p className="sme-chart-description">{selectedMetricDescription}</p>
          ) : null}

          {error ? <div className="sme-error">{error}</div> : null}

          {view === 'chart' ? renderChart() : renderTable()}
        </div>
      </div>
    </div>
  );
}

SystemMetricsExplorerPage.defaultProps = {
  djClient: DataJunctionAPI,
};
