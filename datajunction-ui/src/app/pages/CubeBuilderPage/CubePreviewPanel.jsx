/**
 * Preview panel for cube builder showing selection summary and generated SQL.
 * Matches Query Planner styling exactly.
 */
import React, { useContext, useEffect, useState, useCallback } from 'react';
import { useFormikContext } from 'formik';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import sql from 'react-syntax-highlighter/dist/esm/languages/hljs/sql';
import { atomOneLight } from 'react-syntax-highlighter/dist/esm/styles/hljs';
import DJClientContext from '../../providers/djclient';
import {
  formatBytes,
  formatScanEstimate,
} from '../QueryPlannerPage/PreAggDetailsPanel';

SyntaxHighlighter.registerLanguage('sql', sql);

const debounce = (fn, ms) => {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), ms);
  };
};

export const CubePreviewPanel = () => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const { values } = useFormikContext();
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const metrics = values.metrics || [];
  const dimensions = values.dimensions || [];

  // Fetch SQL when metrics/dimensions change
  const fetchSql = useCallback(
    debounce(async (m, d) => {
      if (m.length === 0 || d.length === 0) {
        setResult(null);
        return;
      }
      setLoading(true);
      setError(null);
      try {
        const res = await djClient.metricsV3(m, d, '');
        if (res.errors && res.errors.length > 0) {
          setError(
            res.errors
              .map(e =>
                typeof e === 'string' ? e : e.message || JSON.stringify(e),
              )
              .join(', '),
          );
          setResult(null);
        } else if (res.message) {
          setError(res.message);
          setResult(null);
        } else {
          setResult(res);
        }
      } catch (err) {
        setError(err.message || 'Failed to generate SQL');
        setResult(null);
      } finally {
        setLoading(false);
      }
    }, 500),
    [djClient],
  );

  useEffect(() => {
    fetchSql(metrics, dimensions);
  }, [metrics, dimensions, fetchSql]);

  // Get short name from full metric/dimension name
  const getShortName = fullName => {
    if (!fullName) return '';
    const parts = fullName.split('.');
    return parts[parts.length - 1];
  };

  const scanInfo = formatScanEstimate(result?.scan_estimate);

  return (
    <div className="cube-preview-panel">
      <div className="preview-section-header">
        <span className="preview-section-icon">⌘</span>
        <span className="preview-section-title">Generated SQL</span>
      </div>

      {/* Scan Cost Banner */}
      {scanInfo && (
        <div className={`scan-estimate-banner scan-estimate-${scanInfo.level}`}>
          <span className="scan-estimate-icon">{scanInfo.icon}</span>
          <div className="scan-estimate-content">
            <div className="scan-estimate-header">
              <strong>Scan Cost:</strong>{' '}
              {scanInfo.totalBytes !== null && scanInfo.totalBytes !== undefined
                ? (scanInfo.hasMissingData ? '≥ ' : '') +
                  formatBytes(scanInfo.totalBytes)
                : 'Unknown'}
            </div>
            <div className="scan-estimate-sources">
              {scanInfo.sources.map((source, idx) => {
                let displayName = source.source_name;
                if (source.schema_ && source.table) {
                  displayName = `${source.schema_}.${source.table}`;
                } else if (source.table) {
                  displayName = source.table;
                }
                return (
                  <div key={idx} className="scan-source-item">
                    <span
                      className="scan-source-name"
                      title={source.source_name}
                    >
                      {displayName}
                    </span>
                    <span className="scan-source-size">
                      {source.total_bytes !== null &&
                      source.total_bytes !== undefined
                        ? formatBytes(source.total_bytes)
                        : 'no size data'}
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}

      <div className="preview-sql-container">
        {loading && <div className="preview-loading">Generating SQL...</div>}
        {error && <div className="preview-error">{error}</div>}
        {!loading && !error && !result?.sql && (
          <div className="preview-empty">
            Select metrics and dimensions to preview SQL
          </div>
        )}
        {!loading && !error && result?.sql && (
          <SyntaxHighlighter
            language="sql"
            style={atomOneLight}
            customStyle={{
              margin: 0,
              padding: 0,
              fontSize: '11px',
              background: 'transparent',
              border: 'none',
            }}
          >
            {result.sql}
          </SyntaxHighlighter>
        )}
      </div>
    </div>
  );
};
