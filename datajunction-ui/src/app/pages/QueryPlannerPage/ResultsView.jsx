import { useState, useCallback, useMemo, useEffect } from 'react';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import sql from 'react-syntax-highlighter/dist/esm/languages/hljs/sql';
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';

SyntaxHighlighter.registerLanguage('sql', sql);

const SERIES_COLORS = [
  '#3b82f6',
  '#a2283e',
  '#059669',
  '#d97706',
  '#8b5cf6',
  '#0ea5e9',
  '#ec4899',
];

// Threshold for switching from multi-series to small multiples
const SMALL_MULTIPLES_THRESHOLD = 2;

function isTimeColumn(col) {
  const name = col.name.toLowerCase();
  const type = (col.type || '').toLowerCase();
  return (
    type.includes('date') ||
    type.includes('timestamp') ||
    type.includes('time') ||
    name === 'date' ||
    name === 'day' ||
    name === 'week' ||
    name === 'month' ||
    name === 'year' ||
    name === 'quarter' ||
    name.endsWith('_date') ||
    name.endsWith('_day') ||
    name.endsWith('_week') ||
    name.endsWith('_month') ||
    name.endsWith('_year') ||
    name.endsWith('_at') ||
    name.startsWith('date') ||
    name.startsWith('ds')
  );
}

function isNumericColumn(col) {
  const type = (col.type || '').toLowerCase();
  return (
    type.includes('int') ||
    type.includes('float') ||
    type.includes('double') ||
    type.includes('decimal') ||
    type.includes('numeric') ||
    type.includes('real') ||
    type.includes('number')
  );
}

function detectChartConfig(columns, rows) {
  if (!columns.length || !rows.length) return null;

  const tagged = columns.map((c, i) => ({ ...c, idx: i }));
  const timeCols = tagged.filter(c => isTimeColumn(c));
  const numericCols = tagged.filter(c => isNumericColumn(c));
  const nonNumericCols = tagged.filter(c => !isNumericColumn(c));

  // Time dimension present → line chart (handles integer time cols like week/year)
  if (timeCols.length > 0) {
    const xCol = timeCols[0];
    const metricCols = numericCols.filter(c => c.idx !== xCol.idx);
    if (metricCols.length > 0) return { type: 'line', xCol, metricCols };
  }

  // String/categorical dimension → bar chart
  if (nonNumericCols.length > 0 && numericCols.length > 0) {
    const xCol = nonNumericCols[0];
    const metricCols = numericCols.filter(c => c.idx !== xCol.idx);
    if (metricCols.length > 0) return { type: 'bar', xCol, metricCols };
  }

  // Multiple numeric columns, no string/time dim → treat first as x-axis (line)
  if (numericCols.length > 1) {
    const xCol = numericCols[0];
    const metricCols = numericCols.slice(1);
    return { type: 'line', xCol, metricCols };
  }

  // Scalar result → KPI cards
  if (numericCols.length > 0) {
    return { type: 'kpi', metricCols: numericCols };
  }

  return null;
}

function buildChartData(columns, rows, xCol) {
  const data = rows.map(row => {
    const obj = {};
    columns.forEach((col, i) => {
      obj[col.name] = row[i];
    });
    return obj;
  });
  const key = xCol.name;
  data.sort((a, b) => {
    const av = a[key];
    const bv = b[key];
    if (av === null && bv === null) return 0;
    if (av === null) return 1;
    if (bv === null) return -1;
    if (typeof av === 'number' && typeof bv === 'number') return av - bv;
    return String(av).localeCompare(String(bv));
  });
  return data;
}

function formatYAxis(value) {
  if (Math.abs(value) >= 1_000_000_000)
    return (value / 1_000_000_000).toFixed(1) + 'B';
  if (Math.abs(value) >= 1_000_000) return (value / 1_000_000).toFixed(1) + 'M';
  if (Math.abs(value) >= 1_000) return (value / 1_000).toFixed(1) + 'K';
  return value;
}

function KpiCards({ rows, metricCols }) {
  const row = rows[0] || [];
  return (
    <div className="kpi-cards">
      {metricCols.map(col => {
        const val = row[col.idx];
        const formatted =
          val == null
            ? '—'
            : typeof val === 'number'
            ? val.toLocaleString(undefined, { maximumFractionDigits: 4 })
            : String(val);
        return (
          <div key={col.idx} className="kpi-card">
            <div className="kpi-label">{col.name}</div>
            <div className="kpi-value">{formatted}</div>
            {col.type && <div className="kpi-type">{col.type}</div>}
          </div>
        );
      })}
    </div>
  );
}

const CHART_MARGIN = { top: 8, right: 24, left: 8, bottom: 40 };
const AXIS_TICK = { fontSize: 11, fill: '#64748b' };
const TOOLTIP_STYLE = { fontSize: 12, border: '1px solid #e2e8f0' };

function Chart({
  type,
  xCol,
  metricCols,
  chartData,
  seriesColors = SERIES_COLORS,
}) {
  const showDots = chartData.length <= 60;
  const xInterval = type === 'line' ? 'preserveStartEnd' : 0;
  if (type === 'line') {
    return (
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={chartData} margin={CHART_MARGIN}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
          <XAxis
            dataKey={xCol.name}
            tick={AXIS_TICK}
            angle={-35}
            textAnchor="end"
            interval={xInterval}
          />
          <YAxis tickFormatter={formatYAxis} tick={AXIS_TICK} width={60} />
          <Tooltip contentStyle={TOOLTIP_STYLE} />
          {metricCols.map((col, i) => (
            <Line
              key={col.idx}
              type="monotone"
              dataKey={col.name}
              stroke={seriesColors[i % seriesColors.length]}
              dot={showDots}
              strokeWidth={2}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    );
  }
  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={chartData} margin={CHART_MARGIN}>
        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
        <XAxis
          dataKey={xCol.name}
          tick={AXIS_TICK}
          angle={-35}
          textAnchor="end"
          interval={xInterval}
        />
        <YAxis tickFormatter={formatYAxis} tick={AXIS_TICK} width={60} />
        <Tooltip contentStyle={TOOLTIP_STYLE} />
        {metricCols.map((col, i) => (
          <Bar
            key={col.idx}
            dataKey={col.name}
            fill={seriesColors[i % seriesColors.length]}
          />
        ))}
      </BarChart>
    </ResponsiveContainer>
  );
}

function ChartView({ chartConfig, chartData, rows, columns }) {
  if (chartConfig.type === 'kpi') {
    return <KpiCards rows={rows} metricCols={chartConfig.metricCols} />;
  }

  const { type, xCol, metricCols } = chartConfig;
  const useSmallMultiples = metricCols.length > SMALL_MULTIPLES_THRESHOLD;

  if (useSmallMultiples) {
    return (
      <div className="small-multiples">
        {metricCols.map((col, i) => (
          <div key={col.idx} className="small-multiple">
            <div className="small-multiple-label">{col.name}</div>
            <div className="small-multiple-chart">
              <Chart
                type={type}
                xCol={xCol}
                metricCols={[col]}
                chartData={chartData}
                seriesColors={[SERIES_COLORS[i % SERIES_COLORS.length]]}
              />
            </div>
          </div>
        ))}
      </div>
    );
  }

  return (
    <Chart
      type={type}
      xCol={xCol}
      metricCols={metricCols}
      chartData={chartData}
    />
  );
}

/**
 * ResultsView - Displays query results with SQL and data table
 * Layout: SQL in top ~25%, results in bottom ~75% with Table/Chart tabs
 */
export function ResultsView({
  sql: sqlQuery,
  results,
  loading,
  error,
  elapsedTime,
  onBackToPlan,
  selectedMetrics,
  selectedDimensions,
  filters,
  dialect,
  cubeName,
  availability,
  links,
}) {
  const [copied, setCopied] = useState(false);
  const [sortColumn, setSortColumn] = useState(null);
  const [sortDirection, setSortDirection] = useState('asc');
  const [activeTab, setActiveTab] = useState('table');

  const handleCopySql = useCallback(() => {
    if (sqlQuery) {
      navigator.clipboard.writeText(sqlQuery);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  }, [sqlQuery]);

  const columns = useMemo(
    () => results?.results?.[0]?.columns || [],
    [results],
  );
  const rows = useMemo(() => results?.results?.[0]?.rows || [], [results]);
  const rowCount = rows.length;

  const handleSort = useCallback(
    columnIndex => {
      if (sortColumn === columnIndex) {
        setSortDirection(d => (d === 'asc' ? 'desc' : 'asc'));
      } else {
        setSortColumn(columnIndex);
        setSortDirection('asc');
      }
    },
    [sortColumn],
  );

  const sortedRows = useMemo(() => {
    if (sortColumn === null) return rows;
    return [...rows].sort((a, b) => {
      const aVal = a[sortColumn];
      const bVal = b[sortColumn];
      if (aVal === null && bVal === null) return 0;
      if (aVal === null) return 1;
      if (bVal === null) return -1;
      let cmp;
      if (typeof aVal === 'number' && typeof bVal === 'number') {
        cmp = aVal - bVal;
      } else {
        cmp = String(aVal).localeCompare(String(bVal));
      }
      return sortDirection === 'asc' ? cmp : -cmp;
    });
  }, [rows, sortColumn, sortDirection]);

  const chartConfig = useMemo(
    () => detectChartConfig(columns, rows),
    [columns, rows],
  );
  const chartData = useMemo(
    () =>
      chartConfig && chartConfig.xCol
        ? buildChartData(columns, rows, chartConfig.xCol)
        : [],
    [columns, rows, chartConfig],
  );

  const canChart = chartConfig !== null && rowCount > 0;

  // Reset to table view if new results can't be charted
  useEffect(() => {
    if (!canChart && activeTab === 'chart') setActiveTab('table');
  }, [canChart, activeTab]);

  return (
    <div className="results-view">
      {/* Header */}
      <div className="results-header">
        <button className="back-to-plan-btn" onClick={onBackToPlan}>
          <span className="back-arrow">←</span>
          <span>Back to Plan</span>
        </button>
        <div className="results-summary">
          {loading ? (
            <span className="results-loading-text">Running query...</span>
          ) : error ? (
            <span className="results-error-text">Query failed</span>
          ) : (
            <>
              <span className="results-count">
                {rowCount.toLocaleString()} rows
              </span>
              {elapsedTime != null && (
                <span className="results-time">{elapsedTime.toFixed(2)}s</span>
              )}
            </>
          )}
        </div>
      </div>

      {/* Two-pane layout: SQL (top) + Results (bottom) */}
      <div className="results-panes">
        {/* SQL Pane */}
        <div className="sql-pane">
          <div className="sql-pane-header">
            <span className="sql-pane-title">SQL Query</span>
            {cubeName && (
              <span
                className="sql-pane-info"
                title={
                  availability
                    ? `Querying materialized dataset ${[
                        availability.catalog,
                        availability.schema_,
                        availability.table,
                      ]
                        .filter(Boolean)
                        .join('.')}, last refreshed for data through ${new Date(
                        availability.validThroughTs,
                      ).toLocaleDateString()}`
                    : undefined
                }
              >
                <span className="info-materialized">
                  <span style={{ fontFamily: 'sans-serif' }}>⚡</span> Using
                  materialized cube
                </span>
                {availability?.validThroughTs && (
                  <>
                    {' · Valid thru '}
                    {new Date(availability.validThroughTs).toLocaleDateString()}
                  </>
                )}
              </span>
            )}
            <button
              className={`copy-btn ${copied ? 'copied' : ''}`}
              onClick={handleCopySql}
              disabled={!sqlQuery}
            >
              {copied ? '✓ Copied' : 'Copy'}
            </button>
          </div>
          <div className="sql-pane-content">
            {sqlQuery ? (
              <SyntaxHighlighter
                language="sql"
                style={foundation}
                wrapLongLines={true}
                customStyle={{
                  margin: 0,
                  padding: '12px 16px',
                  background: '#f8fafc',
                  fontSize: '12px',
                  height: '100%',
                  overflow: 'auto',
                }}
              >
                {sqlQuery}
              </SyntaxHighlighter>
            ) : (
              <div className="sql-pane-empty">Generating SQL...</div>
            )}
          </div>
        </div>

        {/* Results Pane */}
        <div className="results-pane">
          {loading ? (
            <div className="results-loading">
              <div className="loading-spinner large" />
              <span>Executing query...</span>
              <span className="loading-hint">
                Querying {selectedMetrics.length} metric(s) with{' '}
                {selectedDimensions.length} dimension(s)
              </span>
              {links && links.length > 0 && (
                <span className="results-links">
                  {links.map((link, idx) => (
                    <a
                      key={idx}
                      href={link}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="results-link"
                    >
                      View query ↗
                    </a>
                  ))}
                </span>
              )}
            </div>
          ) : error ? (
            <div className="results-error">
              <div className="error-icon">⚠</div>
              <h3>Query Failed</h3>
              <p className="error-message">{error}</p>
              <button
                className="action-btn action-btn-primary"
                onClick={onBackToPlan}
              >
                Back to Plan
              </button>
            </div>
          ) : (
            <div className="results-table-section">
              {/* Tab bar */}
              <div className="results-tabs-bar">
                <div className="results-tabs">
                  <button
                    className={`results-tab ${
                      activeTab === 'table' ? 'active' : ''
                    }`}
                    onClick={() => setActiveTab('table')}
                  >
                    Table
                  </button>
                  <button
                    className={`results-tab ${
                      activeTab === 'chart' ? 'active' : ''
                    } ${!canChart ? 'disabled' : ''}`}
                    onClick={() => canChart && setActiveTab('chart')}
                    title={
                      !canChart
                        ? 'No chartable data (need at least one numeric column)'
                        : undefined
                    }
                  >
                    Chart
                  </button>
                </div>
                <div className="results-tabs-meta">
                  <span className="table-count">
                    {rowCount.toLocaleString()} rows
                  </span>
                  {filters && filters.length > 0 && (
                    <div className="table-filters">
                      {filters.map((filter, idx) => (
                        <span key={idx} className="filter-chip small">
                          {filter}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              </div>

              {/* Content */}
              {activeTab === 'table' ? (
                <div className="results-table-wrapper">
                  {rowCount === 0 ? (
                    <div className="table-empty">
                      <p>No results returned</p>
                    </div>
                  ) : (
                    <table className="results-table">
                      <thead>
                        <tr>
                          {columns.map((col, idx) => (
                            <th
                              key={idx}
                              title={col.semantic_name || col.name}
                              onClick={() => handleSort(idx)}
                              className={sortColumn === idx ? 'sorted' : ''}
                            >
                              <span className="col-header-content">
                                {col.name}
                                <span className="sort-arrows">
                                  <span
                                    className={`sort-arrow up ${
                                      sortColumn === idx &&
                                      sortDirection === 'asc'
                                        ? 'active'
                                        : ''
                                    }`}
                                  >
                                    ▲
                                  </span>
                                  <span
                                    className={`sort-arrow down ${
                                      sortColumn === idx &&
                                      sortDirection === 'desc'
                                        ? 'active'
                                        : ''
                                    }`}
                                  >
                                    ▼
                                  </span>
                                </span>
                              </span>
                              <span className="col-type">{col.type}</span>
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {sortedRows.map((row, rowIdx) => (
                          <tr key={rowIdx}>
                            {row.map((cell, cellIdx) => (
                              <td key={cellIdx}>
                                {cell === null ? (
                                  <span className="null-value">NULL</span>
                                ) : (
                                  String(cell)
                                )}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              ) : (
                <div className="results-chart-wrapper">
                  {canChart ? (
                    <ChartView
                      chartConfig={chartConfig}
                      chartData={chartData}
                      rows={rows}
                      columns={columns}
                    />
                  ) : (
                    <div className="chart-no-data">
                      No chartable data detected
                    </div>
                  )}
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default ResultsView;
