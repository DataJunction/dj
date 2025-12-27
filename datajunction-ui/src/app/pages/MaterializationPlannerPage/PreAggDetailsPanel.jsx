import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { atomOneDark } from 'react-syntax-highlighter/src/styles/hljs';

/**
 * PreAggDetailsPanel - Detailed view of a selected pre-aggregation
 * 
 * Shows comprehensive info when a preagg node is selected in the graph
 */
export function PreAggDetailsPanel({ preAgg, metricFormulas, onClose }) {
  if (!preAgg) {
    return (
      <div className="details-panel details-panel-empty">
        <div className="empty-hint">
          <div className="empty-icon">◎</div>
          <h4>Select a Node</h4>
          <p>Click on a pre-aggregation or metric in the graph to view its details</p>
        </div>
      </div>
    );
  }

  // Get friendly names
  const sourceName = preAgg.parent_name || 'Pre-aggregation';
  const shortName = sourceName.split('.').pop();

  // Find metrics that use this preagg's components
  const relatedMetrics = metricFormulas?.filter(m => 
    m.components?.some(comp => 
      preAgg.components?.some(pc => pc.name === comp)
    )
  ) || [];

  const copyToClipboard = (text) => {
    navigator.clipboard.writeText(text);
  };

  return (
    <div className="details-panel">
      {/* Header */}
      <div className="details-header">
        <div className="details-title-row">
          <div className="details-type-badge preagg">Pre-aggregation</div>
          <button className="details-close" onClick={onClose} title="Close panel">×</button>
        </div>
        <h2 className="details-title">{shortName}</h2>
        <p className="details-full-name">{sourceName}</p>
      </div>

      {/* Aggregability Badge */}
      <div className="details-section details-aggregability-section">
        <span className={`aggregability-badge aggregability-${preAgg.aggregability?.toLowerCase()}`}>
          {preAgg.aggregability} Aggregability
        </span>
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
              <code key={g} className="grain-pill">{g}</code>
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
              Generated SQL
            </h3>
            <button 
              className="copy-sql-btn"
              onClick={() => copyToClipboard(preAgg.sql)}
            >
              Copy SQL
            </button>
          </div>
          <div className="sql-code-wrapper">
            <SyntaxHighlighter 
              language="sql" 
              style={atomOneDark}
              customStyle={{
                margin: 0,
                borderRadius: '6px',
                fontSize: '12px',
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
  const relatedPreaggs = grainGroups?.filter(gg => 
    metric.components?.some(comp => 
      gg.components?.some(pc => pc.name === comp)
    )
  ) || [];

  return (
    <div className="details-panel">
      {/* Header */}
      <div className="details-header">
        <div className="details-title-row">
          <div className={`details-type-badge ${metric.is_derived ? 'derived' : 'metric'}`}>
            {metric.is_derived ? 'Derived Metric' : 'Metric'}
          </div>
          <button className="details-close" onClick={onClose} title="Close panel">×</button>
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
            <span key={i} className="component-tag">{comp}</span>
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
                <span className="preagg-source-name">{gg.parent_name?.split('.').pop()}</span>
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

