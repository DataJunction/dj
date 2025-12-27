import { useState } from 'react';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';

/**
 * PreAggregationCard - Displays a single pre-aggregation (grain group) with its components
 * 
 * Shows:
 * - Source node name
 * - Grain (GROUP BY columns)
 * - Metrics covered
 * - Component table with aggregation details
 * - Collapsible SQL
 */
export function PreAggregationCard({ preAgg, index }) {
  const [showSql, setShowSql] = useState(false);

  // Get a friendly name from the parent (e.g., "v3.order_details" -> "order_details")
  const sourceName = preAgg.parent_name || `Pre-aggregation ${index + 1}`;
  const shortName = sourceName.split('.').pop();

  return (
    <div className="preagg-card">
      <div className="preagg-header" onClick={() => setShowSql(!showSql)}>
        <div className="preagg-title">
          <span className="preagg-expand-icon">{showSql ? '▼' : '►'}</span>
          <h4>Pre-aggregation {index + 1}: {shortName}</h4>
        </div>
        <span className={`preagg-aggregability preagg-aggregability-${preAgg.aggregability?.toLowerCase()}`}>
          {preAgg.aggregability}
        </span>
      </div>

      <div className="preagg-meta">
        <div className="preagg-meta-item">
          <label>Source:</label>
          <span>{sourceName}</span>
        </div>
        <div className="preagg-meta-item">
          <label>Grain:</label>
          <span className="preagg-grain">
            {preAgg.grain?.length > 0 
              ? preAgg.grain.map(g => <code key={g}>{g}</code>)
              : <em>No additional grain columns</em>
            }
          </span>
        </div>
        <div className="preagg-meta-item">
          <label>Metrics:</label>
          <span className="preagg-metrics">
            {preAgg.metrics?.map(m => (
              <span key={m} className="badge badge-metric">{m.split('.').pop()}</span>
            ))}
          </span>
        </div>
      </div>

      {/* Components Table */}
      <div className="preagg-components">
        <h5>Metric Components</h5>
        <table className="components-table">
          <thead>
            <tr>
              <th>Component</th>
              <th>Expression</th>
              <th>Aggregation</th>
              <th>Re-aggregation</th>
              <th>Aggregability</th>
            </tr>
          </thead>
          <tbody>
            {preAgg.components?.map((comp, i) => (
              <tr key={comp.name || i}>
                <td><code>{comp.name}</code></td>
                <td><code>{comp.expression}</code></td>
                <td>{comp.aggregation || <em>(grain column)</em>}</td>
                <td>{comp.merge || '—'}</td>
                <td>
                  <span className={`badge-aggregability badge-${comp.aggregability?.toLowerCase()}`}>
                    {comp.aggregability}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Collapsible SQL */}
      {showSql && (
        <div className="preagg-sql">
          <div className="preagg-sql-header">
            <h5>SQL</h5>
            <button 
              className="copy-btn"
              onClick={(e) => {
                e.stopPropagation();
                navigator.clipboard.writeText(preAgg.sql);
              }}
            >
              Copy SQL
            </button>
          </div>
          <SyntaxHighlighter language="sql" style={foundation}>
            {preAgg.sql}
          </SyntaxHighlighter>
        </div>
      )}
    </div>
  );
}

export default PreAggregationCard;

