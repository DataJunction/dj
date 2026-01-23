import { useState, useCallback } from 'react';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import sql from 'react-syntax-highlighter/dist/esm/languages/hljs/sql';

SyntaxHighlighter.registerLanguage('sql', sql);

/**
 * ResultsView - Displays query results with SQL and data table
 * Layout: SQL in top 1/3, results in bottom 2/3
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
}) {
  const [copied, setCopied] = useState(false);

  const handleCopySql = useCallback(() => {
    if (sqlQuery) {
      navigator.clipboard.writeText(sqlQuery);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  }, [sqlQuery]);

  // Parse results data - handle new v3 format
  const columns = results?.results?.[0]?.columns || [];
  const rows = results?.results?.[0]?.rows || [];
  const rowCount = rows.length;

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
              <span className="results-count">{rowCount.toLocaleString()} rows</span>
              {elapsedTime != null && (
                <span className="results-time">{elapsedTime.toFixed(2)}s</span>
              )}
            </>
          )}
        </div>
      </div>

      {/* Two-pane layout: SQL (top 1/3) + Results (bottom 2/3) */}
      <div className="results-panes">
        {/* SQL Pane - always visible, top 1/3 */}
        <div className="sql-pane">
          <div className="sql-pane-header">
            <span className="sql-pane-title">SQL Query</span>
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

        {/* Results Pane - bottom 2/3 */}
        <div className="results-pane">
          {loading ? (
            <div className="results-loading">
              <div className="loading-spinner large" />
              <span>Executing query...</span>
              <span className="loading-hint">
                Querying {selectedMetrics.length} metric(s) with {selectedDimensions.length} dimension(s)
              </span>
            </div>
          ) : error ? (
            <div className="results-error">
              <div className="error-icon">⚠</div>
              <h3>Query Failed</h3>
              <p className="error-message">{error}</p>
              <button className="action-btn action-btn-primary" onClick={onBackToPlan}>
                Back to Plan
              </button>
            </div>
          ) : (
            <div className="results-table-section">
              <div className="table-header">
                <span className="table-title">Results</span>
                <span className="table-count">{rowCount.toLocaleString()} rows</span>
                {filters && filters.length > 0 && (
                  <div className="table-filters">
                    {filters.map((filter, idx) => (
                      <span key={idx} className="filter-chip small">{filter}</span>
                    ))}
                  </div>
                )}
              </div>
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
                          <th key={idx} title={col.semantic_name || col.name}>
                            {col.name}
                            <span className="col-type">{col.type}</span>
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((row, rowIdx) => (
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
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default ResultsView;
