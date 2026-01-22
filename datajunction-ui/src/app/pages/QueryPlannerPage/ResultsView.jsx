import { useState, useCallback } from 'react';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import sql from 'react-syntax-highlighter/dist/esm/languages/hljs/sql';

SyntaxHighlighter.registerLanguage('sql', sql);

/**
 * ResultsView - Displays query results with SQL and data table
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
  const [sqlExpanded, setSqlExpanded] = useState(true);
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

      {/* Main content */}
      <div className="results-content">
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
          <>
            {/* SQL Section */}
            <div className="results-sql-section">
              <div
                className="sql-section-header"
                onClick={() => setSqlExpanded(!sqlExpanded)}
              >
                <span className="expand-icon">{sqlExpanded ? '▼' : '▶'}</span>
                <span className="sql-section-title">SQL Query</span>
                <div className="sql-section-actions" onClick={e => e.stopPropagation()}>
                  <button
                    className={`copy-btn ${copied ? 'copied' : ''}`}
                    onClick={handleCopySql}
                    disabled={!sqlQuery}
                  >
                    {copied ? '✓ Copied' : 'Copy'}
                  </button>
                </div>
              </div>
              {sqlExpanded && sqlQuery && (
                <div className="sql-content">
                  <SyntaxHighlighter
                    language="sql"
                    style={foundation}
                    wrapLongLines={true}
                    customStyle={{
                      margin: 0,
                      padding: '12px 16px',
                      background: '#f8fafc',
                      borderRadius: '0 0 8px 8px',
                      fontSize: '12px',
                    }}
                  >
                    {sqlQuery}
                  </SyntaxHighlighter>
                </div>
              )}
            </div>

            {/* Results Table */}
            <div className="results-table-section">
              <div className="table-header">
                <span className="table-title">Results</span>
                <span className="table-count">{rowCount.toLocaleString()} rows</span>
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

            {/* Query Info Footer */}
            {filters && filters.length > 0 && (
              <div className="results-filters-info">
                <span className="filters-label">Filters applied:</span>
                <div className="filters-list">
                  {filters.map((filter, idx) => (
                    <span key={idx} className="filter-chip">{filter}</span>
                  ))}
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

export default ResultsView;
