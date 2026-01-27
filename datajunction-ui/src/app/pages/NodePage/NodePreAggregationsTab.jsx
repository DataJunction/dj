import { useEffect, useState, useMemo, useContext } from 'react';
import DJClientContext from '../../providers/djclient';
import { labelize } from '../../../utils/form';
import '../../../styles/preaggregations.css';

const cronstrue = require('cronstrue');

/**
 * Pre-aggregations tab for non-cube nodes (transform, metric, dimension).
 * Shows pre-aggs grouped by staleness (current vs stale versions).
 */
export default function NodePreAggregationsTab({ node }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [preaggs, setPreaggs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [expandedIds, setExpandedIds] = useState(new Set());
  const [expandedGrainIds, setExpandedGrainIds] = useState(new Set());
  const [deactivating, setDeactivating] = useState(new Set());

  const MAX_VISIBLE_GRAIN = 10;

  // Fetch pre-aggregations for this node
  useEffect(() => {
    const fetchPreaggs = async () => {
      if (!node?.name) return;

      setLoading(true);
      setError(null);

      try {
        const result = await djClient.listPreaggs({
          node_name: node.name,
          include_stale: true,
        });
        if (result._error) {
          setError(result.message);
        } else {
          setPreaggs(result.items || []);
        }
      } catch (err) {
        setError(err.message || 'Failed to load pre-aggregations');
      } finally {
        setLoading(false);
      }
    };

    fetchPreaggs();
  }, [node?.name, djClient]);

  // Group pre-aggs by staleness
  const { currentPreaggs, stalePreaggs } = useMemo(() => {
    const currentVersion = node?.version;
    const current = [];
    const stale = [];

    preaggs.forEach(preagg => {
      if (preagg.node_version === currentVersion) {
        current.push(preagg);
      } else {
        stale.push(preagg);
      }
    });

    return { currentPreaggs: current, stalePreaggs: stale };
  }, [preaggs, node?.version]);

  // Auto-expand the first current pre-agg when data loads
  useEffect(() => {
    if (currentPreaggs.length > 0 && expandedIds.size === 0) {
      setExpandedIds(new Set([currentPreaggs[0].id]));
    }
  }, [currentPreaggs]);

  // Toggle expanded state for a pre-agg row
  const toggleExpanded = id => {
    setExpandedIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  // Deactivate a single pre-agg workflow
  const handleDeactivate = async preaggId => {
    if (
      !window.confirm(
        'Are you sure you want to deactivate this workflow? ' +
          'The materialization will stop running.',
      )
    ) {
      return;
    }

    setDeactivating(prev => new Set(prev).add(preaggId));

    try {
      const result = await djClient.deactivatePreaggWorkflow(preaggId);
      if (result._error) {
        alert(`Failed to deactivate: ${result.message}`);
      } else {
        // Refresh the list
        const refreshed = await djClient.listPreaggs({
          node_name: node.name,
          include_stale: true,
        });
        if (!refreshed._error) {
          setPreaggs(refreshed.items || []);
        }
      }
    } catch (err) {
      alert(`Error: ${err.message}`);
    } finally {
      setDeactivating(prev => {
        const next = new Set(prev);
        next.delete(preaggId);
        return next;
      });
    }
  };

  // Bulk deactivate all stale workflows
  const handleDeactivateAllStale = async () => {
    const activeStale = stalePreaggs.filter(
      p => p.workflow_status === 'active',
    );
    if (activeStale.length === 0) {
      alert('No active stale workflows to deactivate.');
      return;
    }

    if (
      !window.confirm(
        `Are you sure you want to deactivate ${activeStale.length} stale workflow(s)? ` +
          'These materializations are from older node versions and will stop running.',
      )
    ) {
      return;
    }

    setDeactivating(prev => {
      const next = new Set(prev);
      activeStale.forEach(p => next.add(p.id));
      return next;
    });

    try {
      const result = await djClient.bulkDeactivatePreaggWorkflows(
        node.name,
        true,
      );
      if (result._error) {
        alert(`Failed to deactivate: ${result.message}`);
      } else {
        // Refresh the list
        const refreshed = await djClient.listPreaggs({
          node_name: node.name,
          include_stale: true,
        });
        if (!refreshed._error) {
          setPreaggs(refreshed.items || []);
        }
      }
    } catch (err) {
      alert(`Error: ${err.message}`);
    } finally {
      setDeactivating(new Set());
    }
  };

  // Format cron expression to human-readable
  const formatSchedule = schedule => {
    if (!schedule) return 'Not scheduled';
    try {
      return cronstrue.toString(schedule);
    } catch {
      return schedule;
    }
  };

  // Render a single pre-agg row
  const renderPreaggRow = (preagg, isStale = false) => {
    const isExpanded = expandedIds.has(preagg.id);
    const isDeactivating = deactivating.has(preagg.id);
    const hasActiveWorkflow = preagg.workflow_status === 'active';

    return (
      <div
        key={preagg.id}
        className={`preagg-row ${isStale ? 'preagg-row--stale' : ''}`}
      >
        {/* Collapsed header row */}
        <div
          className="preagg-row-header"
          onClick={() => toggleExpanded(preagg.id)}
        >
          <span className="preagg-row-toggle">
            {isExpanded ? '\u25BC' : '\u25B6'}
          </span>

          {preagg.preagg_hash && (
            <code
              className="preagg-row-hash"
              title="Pre-aggregation hash - used in table and workflow names"
            >
              {preagg.preagg_hash}
            </code>
          )}

          <div className="preagg-row-grain-chips">
            {(() => {
              const grainCols = preagg.grain_columns || [];
              const maxVisible = MAX_VISIBLE_GRAIN;
              const visibleCols = grainCols.slice(0, maxVisible);
              const hiddenCount = grainCols.length - maxVisible;

              return (
                <>
                  {visibleCols.map((col, idx) => {
                    const parts = col.split('.');
                    const shortName = parts[parts.length - 1];
                    return (
                      <span key={idx} className="preagg-grain-chip">
                        {shortName}
                      </span>
                    );
                  })}
                  {hiddenCount > 0 && (
                    <span className="preagg-grain-chip preagg-grain-chip--more">
                      +{hiddenCount}
                    </span>
                  )}
                </>
              );
            })()}
          </div>

          <span className="preagg-row-measures">
            {preagg.measures?.length || 0} measure
            {(preagg.measures?.length || 0) !== 1 ? 's' : ''}
          </span>

          {preagg.related_metrics?.length > 0 && (
            <span className="preagg-metric-count-badge">
              {preagg.related_metrics.length} metric
              {preagg.related_metrics.length !== 1 ? 's' : ''}
            </span>
          )}

          {hasActiveWorkflow ? (
            <span className="preagg-status-badge preagg-status-badge--active">
              Active
            </span>
          ) : preagg.workflow_status === 'paused' ? (
            <span className="preagg-status-badge preagg-status-badge--paused">
              Paused
            </span>
          ) : (
            <span className="preagg-status-badge preagg-status-badge--pending">
              Pending
            </span>
          )}

          {preagg.schedule && (
            <span className="preagg-row-schedule">
              {formatSchedule(preagg.schedule).toLowerCase()}
            </span>
          )}

          {isStale && (
            <span className="preagg-row-version">
              was {preagg.node_version}
            </span>
          )}
        </div>

        {/* Expanded details */}
        {isExpanded && (
          <div
            className={`preagg-details ${
              isStale ? 'preagg-details--stale' : ''
            }`}
          >
            {isStale && (
              <div className="preagg-stale-banner">
                <span className="preagg-stale-banner-icon">⚠️</span>
                <div>
                  <strong>Built for {preagg.node_version}</strong> — current is{' '}
                  {node.version}
                  <br />
                  <span className="preagg-stale-banner-text">
                    This workflow is still running but won't be used for
                    queries.
                  </span>
                </div>
              </div>
            )}

            <div className="preagg-stack">
              {/* Config + Grain side by side */}
              <div className="preagg-two-column">
                {/* Config */}
                <div>
                  <div className="preagg-card-label">Config</div>
                  <div className="preagg-card">
                    {/* Table-style key-value pairs */}
                    <table className="preagg-config-table">
                      <tbody>
                        <tr>
                          <td className="preagg-config-key">Strategy</td>
                          <td className="preagg-config-value">
                            {preagg.strategy
                              ? labelize(preagg.strategy)
                              : 'Not set'}
                          </td>
                        </tr>
                        {preagg.preagg_hash && (
                          <tr>
                            <td className="preagg-config-key">Hash</td>
                            <td className="preagg-config-value">
                              <code
                                className="preagg-hash-badge"
                                title="Unique identifier for this pre-aggregation. Used in table and workflow names."
                              >
                                {preagg.preagg_hash}
                              </code>
                            </td>
                          </tr>
                        )}
                        <tr>
                          <td className="preagg-config-key">Schedule</td>
                          <td className="preagg-config-value">
                            {preagg.schedule ? (
                              <>
                                {formatSchedule(preagg.schedule)}
                                <span className="preagg-config-schedule-cron">
                                  ({preagg.schedule})
                                </span>
                              </>
                            ) : (
                              'Not scheduled'
                            )}
                          </td>
                        </tr>
                        {preagg.lookback_window && (
                          <tr>
                            <td className="preagg-config-key">Lookback</td>
                            <td className="preagg-config-value">
                              {preagg.lookback_window}
                            </td>
                          </tr>
                        )}
                        {preagg.max_partition &&
                          preagg.max_partition.length > 0 && (
                            <tr>
                              <td className="preagg-config-key">
                                Max Partition
                              </td>
                              <td className="preagg-config-value">
                                <code>{preagg.max_partition.join(', ')}</code>
                              </td>
                            </tr>
                          )}
                      </tbody>
                    </table>

                    {/* Actions */}
                    <div className="preagg-actions">
                      {/* Workflow buttons - one per URL */}
                      {preagg.workflow_urls?.map((wf, idx) => {
                        const label = wf.label || 'Workflow';
                        const capitalizedLabel =
                          label.charAt(0).toUpperCase() + label.slice(1);
                        return (
                          <a
                            key={idx}
                            href={wf.url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="preagg-action-btn"
                          >
                            {capitalizedLabel}
                          </a>
                        );
                      })}

                      {hasActiveWorkflow && (
                        <button
                          className={`preagg-action-btn preagg-action-btn--danger`}
                          disabled={isDeactivating}
                          onClick={e => {
                            e.stopPropagation();
                            handleDeactivate(preagg.id);
                          }}
                        >
                          {isDeactivating ? 'Deactivating...' : 'Deactivate'}
                        </button>
                      )}
                    </div>
                  </div>
                </div>

                {/* Grain */}
                <div>
                  <div className="preagg-card-label">Grain</div>
                  <div className="preagg-card preagg-card--compact">
                    <div className="preagg-grain-list">
                      {(() => {
                        const grainCols = preagg.grain_columns || [];
                        const isGrainExpanded = expandedGrainIds.has(preagg.id);
                        const visibleCols = isGrainExpanded
                          ? grainCols
                          : grainCols.slice(0, MAX_VISIBLE_GRAIN);
                        const hiddenCount =
                          grainCols.length - MAX_VISIBLE_GRAIN;

                        return (
                          <>
                            {visibleCols.map((col, idx) => {
                              const parts = col.split('.');
                              const nodeName = parts.slice(0, -1).join('.');
                              return (
                                <a
                                  key={idx}
                                  href={`/nodes/${nodeName}`}
                                  title={`View ${nodeName}`}
                                  className="preagg-grain-badge"
                                >
                                  {col}
                                </a>
                              );
                            })}
                            {!isGrainExpanded && hiddenCount > 0 && (
                              <button
                                className="preagg-expand-btn"
                                onClick={e => {
                                  e.stopPropagation();
                                  setExpandedGrainIds(prev => {
                                    const next = new Set(prev);
                                    next.add(preagg.id);
                                    return next;
                                  });
                                }}
                              >
                                +{hiddenCount} more
                              </button>
                            )}
                            {isGrainExpanded && hiddenCount > 0 && (
                              <button
                                className="preagg-expand-btn"
                                onClick={e => {
                                  e.stopPropagation();
                                  setExpandedGrainIds(prev => {
                                    const next = new Set(prev);
                                    next.delete(preagg.id);
                                    return next;
                                  });
                                }}
                              >
                                Show less
                              </button>
                            )}
                          </>
                        );
                      })()}
                    </div>
                  </div>
                </div>
              </div>

              {/* Measures */}
              <div>
                <div className="preagg-card-label preagg-card-label--with-info">
                  Measures
                  <span
                    className="preagg-info-icon"
                    title="Pre-computed aggregations stored in this pre-aggregation. At query time, DJ uses these to avoid re-scanning raw data."
                  >
                    ⓘ
                  </span>
                </div>
                <div
                  className="preagg-card preagg-card--table"
                  style={{ border: '1px solid #e2e8f0' }}
                >
                  <table className="preagg-measures-table">
                    <thead>
                      <tr>
                        <th>Name</th>
                        <th>
                          Aggregation
                          <span
                            className="preagg-info-icon"
                            title="Phase 1: How raw data is aggregated when building the pre-agg table"
                          >
                            ⓘ
                          </span>
                        </th>
                        <th>
                          Merge
                          <span
                            className="preagg-info-icon"
                            title="Phase 2: How pre-aggregated values are combined at query time"
                          >
                            ⓘ
                          </span>
                        </th>
                        <th>
                          Rule
                          <span
                            className="preagg-info-icon"
                            title="Additivity: FULL = can roll up across any dimension"
                          >
                            ⓘ
                          </span>
                        </th>
                        <th>
                          Used By
                          <span
                            className="preagg-info-icon"
                            title="Metrics that use this measure"
                          >
                            ⓘ
                          </span>
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {preagg.measures?.map((measure, idx) => (
                        <tr key={idx}>
                          <td className="preagg-measure-name">
                            {measure.name}
                          </td>
                          <td>
                            <code className="preagg-agg-badge">
                              {measure.aggregation
                                ? `${measure.aggregation}(${measure.expression})`
                                : measure.expression}
                            </code>
                          </td>
                          <td>
                            {measure.merge && (
                              <code className="preagg-merge-badge">
                                {measure.merge}
                              </code>
                            )}
                          </td>
                          <td>
                            {measure.rule && (
                              <span className="preagg-rule-badge">
                                {typeof measure.rule === 'object'
                                  ? measure.rule.type || ''
                                  : measure.rule}
                              </span>
                            )}
                          </td>
                          <td>
                            {measure.used_by_metrics?.length > 0 && (
                              <div className="preagg-metrics-list">
                                {measure.used_by_metrics.map((metric, mIdx) => (
                                  <a
                                    key={mIdx}
                                    href={`/nodes/${metric.name}`}
                                    title={metric.name}
                                    className="preagg-metric-badge"
                                  >
                                    {metric.display_name ||
                                      metric.name.split('.').pop()}
                                  </a>
                                ))}
                              </div>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    );
  };

  // Loading state
  if (loading) {
    return <div className="preagg-loading">Loading pre-aggregations...</div>;
  }

  // Error state
  if (error) {
    return (
      <div className="message alert preagg-error">
        Error loading pre-aggregations: {error}
      </div>
    );
  }

  // No pre-aggs
  if (preaggs.length === 0) {
    return (
      <div className="preagg-no-data">
        <div className="message alert preagg-no-data-alert">
          No pre-aggregations found for this node.
        </div>
        <p className="preagg-no-data-text">
          Pre-aggregations are created when you use the{' '}
          <a href="/query-planner">Query Planner</a> to plan materializations
          for metrics derived from this node.
        </p>
      </div>
    );
  }

  // Calculate if there are active stale workflows
  const activeStaleCount = stalePreaggs.filter(
    p => p.workflow_status === 'active',
  ).length;

  return (
    <div className="preagg-container">
      {/* Current Version Section */}
      <div className="preagg-section">
        <div className="preagg-section-header">
          <h3 className="preagg-section-title">
            Current Pre-Aggregations ({node.version})
          </h3>
          <span className="preagg-section-count">
            {currentPreaggs.length} pre-aggregation
            {currentPreaggs.length !== 1 ? 's' : ''}
          </span>
        </div>

        {currentPreaggs.length > 0 ? (
          currentPreaggs.map(preagg => renderPreaggRow(preagg, false))
        ) : (
          <div className="preagg-empty">
            No pre-aggregations for the current version.
          </div>
        )}
      </div>

      {/* Stale Section */}
      {stalePreaggs.length > 0 && (
        <div className="preagg-section">
          <div className="preagg-section-header preagg-section-header--stale">
            <div className="preagg-section-header-left">
              <h3 className="preagg-section-title preagg-section-title--stale">
                Stale Pre-Aggregations ({stalePreaggs.length})
              </h3>
              <span className="preagg-section-count preagg-section-count--stale">
                {activeStaleCount} active workflow
                {activeStaleCount !== 1 ? 's' : ''}
              </span>
            </div>

            {activeStaleCount > 0 && (
              <button
                className="preagg-action-btn preagg-action-btn--danger-fill"
                onClick={handleDeactivateAllStale}
              >
                Deactivate All Stale
              </button>
            )}
          </div>

          {stalePreaggs.map(preagg => renderPreaggRow(preagg, true))}
        </div>
      )}
    </div>
  );
}
