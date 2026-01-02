import { Link } from 'react-router-dom';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { atomOneLight } from 'react-syntax-highlighter/src/styles/hljs';

/**
 * Helper to extract dimension node name from a dimension path
 * e.g., "v3.customer.name" -> "v3.customer"
 * e.g., "v3.date.month[order]" -> "v3.date"
 */
function getDimensionNodeName(dimPath) {
  // Remove role suffix if present (e.g., "[order]")
  const pathWithoutRole = dimPath.split('[')[0];
  // Split by dot and remove the last segment (column name)
  const parts = pathWithoutRole.split('.');
  if (parts.length > 1) {
    return parts.slice(0, -1).join('.');
  }
  return pathWithoutRole;
}

/**
 * QueryOverviewPanel - Default view showing metrics SQL and pre-agg summary
 *
 * Shown when no node is selected in the graph
 */
export function QueryOverviewPanel({
  measuresResult,
  metricsResult,
  selectedMetrics,
  selectedDimensions,
}) {
  const copyToClipboard = text => {
    navigator.clipboard.writeText(text);
  };

  // No selection yet
  if (!selectedMetrics?.length || !selectedDimensions?.length) {
    return (
      <div className="details-panel details-panel-empty">
        <div className="empty-hint">
          <div className="empty-icon">⊞</div>
          <h4>Query Planner</h4>
          <p>
            Select metrics and dimensions from the left panel to see the
            generated SQL and pre-aggregation plan
          </p>
        </div>
      </div>
    );
  }

  // Loading or no results yet
  if (!measuresResult || !metricsResult) {
    return (
      <div className="details-panel details-panel-empty">
        <div className="empty-hint">
          <div className="loading-spinner" />
          <p>Building query plan...</p>
        </div>
      </div>
    );
  }

  const grainGroups = measuresResult.grain_groups || [];
  const metricFormulas = measuresResult.metric_formulas || [];
  const sql = metricsResult.sql || '';

  return (
    <div className="details-panel">
      {/* Header */}
      <div className="details-header">
        <h2 className="details-title">Generated Query Overview</h2>
        <p className="details-full-name">
          {selectedMetrics.length} metric
          {selectedMetrics.length !== 1 ? 's' : ''} ×{' '}
          {selectedDimensions.length} dimension
          {selectedDimensions.length !== 1 ? 's' : ''}
        </p>
      </div>

      {/* Pre-aggregations Summary */}
      <div className="details-section">
        <h3 className="section-title">
          <span className="section-icon">◫</span>
          Pre-Aggregations ({grainGroups.length})
        </h3>
        <div className="preagg-summary-list">
          {grainGroups.map((gg, i) => {
            const shortName = gg.parent_name?.split('.').pop() || 'Unknown';
            const relatedMetrics = metricFormulas.filter(m =>
              m.components?.some(comp =>
                gg.components?.some(pc => pc.name === comp),
              ),
            );
            return (
              <div key={i} className="preagg-summary-card">
                <div className="preagg-summary-header">
                  <span className="preagg-summary-name">{shortName}</span>
                  <span
                    className={`aggregability-pill aggregability-${gg.aggregability?.toLowerCase()}`}
                  >
                    {gg.aggregability}
                  </span>
                </div>
                <div className="preagg-summary-details">
                  <div className="preagg-summary-row">
                    <span className="label">Grain:</span>
                    <span className="value">
                      {gg.grain?.join(', ') || 'None'}
                    </span>
                  </div>
                  <div className="preagg-summary-row">
                    <span className="label">Measures:</span>
                    <span className="value">{gg.components?.length || 0}</span>
                  </div>
                  <div className="preagg-summary-row">
                    <span className="label">Metrics:</span>
                    <span className="value">
                      {relatedMetrics.map(m => m.short_name).join(', ') ||
                        'None'}
                    </span>
                  </div>
                </div>
                <div className="preagg-summary-status">
                  <span className="status-indicator status-not-materialized">
                    ○
                  </span>
                  <span>Not materialized</span>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Metrics & Dimensions Summary - Two columns */}
      <div className="details-section">
        <div className="selection-summary-grid">
          {/* Metrics Column */}
          <div className="selection-summary-column">
            <h3 className="section-title">
              <span className="section-icon">◈</span>
              Metrics ({metricFormulas.length})
            </h3>
            <div className="selection-summary-list">
              {metricFormulas.map((m, i) => (
                <Link
                  key={i}
                  to={`/nodes/${m.name}`}
                  className="selection-summary-item metric clickable"
                >
                  <span className="selection-summary-name">{m.short_name}</span>
                  {m.is_derived && (
                    <span className="compact-node-badge">Derived</span>
                  )}
                </Link>
              ))}
            </div>
          </div>

          {/* Dimensions Column */}
          <div className="selection-summary-column">
            <h3 className="section-title">
              <span className="section-icon">⊞</span>
              Dimensions ({selectedDimensions.length})
            </h3>
            <div className="selection-summary-list">
              {selectedDimensions.map((dim, i) => {
                const shortName = dim.split('.').pop().split('[')[0]; // Remove role suffix too
                const nodeName = getDimensionNodeName(dim);
                return (
                  <Link
                    key={i}
                    to={`/nodes/${nodeName}`}
                    className="selection-summary-item dimension clickable"
                  >
                    <span className="selection-summary-name">{shortName}</span>
                  </Link>
                );
              })}
            </div>
          </div>
        </div>
      </div>

      {/* SQL Section */}
      {sql && (
        <div className="details-section details-section-full details-sql-section">
          <div className="section-header-row">
            <h3 className="section-title">
              <span className="section-icon">⌘</span>
              Generated SQL
            </h3>
            <button
              className="copy-sql-btn"
              onClick={() => copyToClipboard(sql)}
              type="button"
            >
              Copy SQL
            </button>
          </div>
          <div className="sql-code-wrapper">
            <SyntaxHighlighter
              language="sql"
              style={atomOneLight}
              customStyle={{
                margin: 0,
                borderRadius: '6px',
                fontSize: '11px',
                background: '#f8fafc',
                border: '1px solid #e2e8f0',
              }}
            >
              {sql}
            </SyntaxHighlighter>
          </div>
        </div>
      )}
    </div>
  );
}

/**
 * PreAggDetailsPanel - Detailed view of a selected pre-aggregation
 *
 * Shows comprehensive info when a preagg node is selected in the graph
 */
export function PreAggDetailsPanel({ preAgg, metricFormulas, onClose }) {
  if (!preAgg) {
    return null;
  }

  // Get friendly names
  const sourceName = preAgg.parent_name || 'Pre-aggregation';
  const shortName = sourceName.split('.').pop();

  // Find metrics that use this preagg's components
  const relatedMetrics =
    metricFormulas?.filter(m =>
      m.components?.some(comp =>
        preAgg.components?.some(pc => pc.name === comp),
      ),
    ) || [];

  const copyToClipboard = text => {
    navigator.clipboard.writeText(text);
  };

  return (
    <div className="details-panel">
      {/* Header */}
      <div className="details-header">
        <div className="details-title-row">
          <div className="details-type-badge preagg">Pre-aggregation</div>
          <button
            className="details-close"
            onClick={onClose}
            title="Close panel"
          >
            ×
          </button>
        </div>
        <h2 className="details-title">{shortName}</h2>
        <p className="details-full-name">{sourceName}</p>
      </div>

      {/* Grain Section */}
      <div className="details-section">
        <h3 className="section-title">
          <span className="section-icon">⊞</span>
          Grain (GROUP BY)
        </h3>
        <div className="grain-pills">
          {preAgg.grain?.length > 0 ? (
            preAgg.grain.map(g => (
              <code key={g} className="grain-pill">
                {g}
              </code>
            ))
          ) : (
            <span className="empty-text">No grain columns</span>
          )}
        </div>
      </div>

      {/* Metrics Using This */}
      <div className="details-section">
        <h3 className="section-title">
          <span className="section-icon">◈</span>
          Metrics Using This
        </h3>
        <div className="metrics-list">
          {relatedMetrics.length > 0 ? (
            relatedMetrics.map((m, i) => (
              <div key={i} className="related-metric">
                <span className="metric-name">{m.short_name}</span>
                {m.is_derived && <span className="derived-badge">Derived</span>}
              </div>
            ))
          ) : (
            <span className="empty-text">No metrics found</span>
          )}
        </div>
      </div>

      {/* Components Table */}
      <div className="details-section details-section-full">
        <h3 className="section-title">
          <span className="section-icon">⚙</span>
          Components ({preAgg.components?.length || 0})
        </h3>
        <div className="components-table-wrapper">
          <table className="details-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Expression</th>
                <th>Agg</th>
                <th>Re-agg</th>
              </tr>
            </thead>
            <tbody>
              {preAgg.components?.map((comp, i) => (
                <tr key={comp.name || i}>
                  <td className="comp-name-cell">
                    <code>{comp.name}</code>
                  </td>
                  <td className="comp-expr-cell">
                    <code>{comp.expression}</code>
                  </td>
                  <td className="comp-agg-cell">
                    <span className="agg-func">{comp.aggregation || '—'}</span>
                  </td>
                  <td className="comp-merge-cell">
                    <span className="merge-func">{comp.merge || '—'}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* SQL Section */}
      {preAgg.sql && (
        <div className="details-section details-section-full details-sql-section">
          <div className="section-header-row">
            <h3 className="section-title">
              <span className="section-icon">⌘</span>
              Pre-Aggregation SQL
            </h3>
            <button
              className="copy-sql-btn"
              onClick={() => copyToClipboard(preAgg.sql)}
              type="button"
            >
              Copy SQL
            </button>
          </div>
          <div className="sql-code-wrapper">
            <SyntaxHighlighter
              language="sql"
              style={atomOneLight}
              customStyle={{
                margin: 0,
                borderRadius: '6px',
                fontSize: '11px',
                background: '#f8fafc',
                border: '1px solid #e2e8f0',
              }}
            >
              {preAgg.sql}
            </SyntaxHighlighter>
          </div>
        </div>
      )}
    </div>
  );
}

/**
 * MetricDetailsPanel - Detailed view of a selected metric
 */
export function MetricDetailsPanel({ metric, grainGroups, onClose }) {
  if (!metric) return null;

  // Find preaggs that this metric depends on
  const relatedPreaggs =
    grainGroups?.filter(gg =>
      metric.components?.some(comp =>
        gg.components?.some(pc => pc.name === comp),
      ),
    ) || [];

  return (
    <div className="details-panel">
      {/* Header */}
      <div className="details-header">
        <div className="details-title-row">
          <div
            className={`details-type-badge ${
              metric.is_derived ? 'derived' : 'metric'
            }`}
          >
            {metric.is_derived ? 'Derived Metric' : 'Metric'}
          </div>
          <button
            className="details-close"
            onClick={onClose}
            title="Close panel"
          >
            ×
          </button>
        </div>
        <h2 className="details-title">{metric.short_name}</h2>
        <p className="details-full-name">{metric.name}</p>
      </div>

      {/* Formula */}
      <div className="details-section">
        <h3 className="section-title">
          <span className="section-icon">∑</span>
          Combiner Formula
        </h3>
        <div className="formula-display">
          <code>{metric.combiner}</code>
        </div>
      </div>

      {/* Components Used */}
      <div className="details-section">
        <h3 className="section-title">
          <span className="section-icon">⚙</span>
          Components Used
        </h3>
        <div className="component-tags">
          {metric.components?.map((comp, i) => (
            <span key={i} className="component-tag">
              {comp}
            </span>
          ))}
        </div>
      </div>

      {/* Source Pre-aggregations */}
      <div className="details-section">
        <h3 className="section-title">
          <span className="section-icon">◫</span>
          Source Pre-aggregations
        </h3>
        <div className="preagg-sources">
          {relatedPreaggs.length > 0 ? (
            relatedPreaggs.map((gg, i) => (
              <div key={i} className="preagg-source-item">
                <span className="preagg-source-name">
                  {gg.parent_name?.split('.').pop()}
                </span>
                <span className="preagg-source-full">{gg.parent_name}</span>
              </div>
            ))
          ) : (
            <span className="empty-text">No source found</span>
          )}
        </div>
      </div>
    </div>
  );
}

export default PreAggDetailsPanel;
