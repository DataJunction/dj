import { useState } from 'react';
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
 * Helper to normalize grain columns to short names for lookup key
 */
function normalizeGrain(grainCols) {
  return (grainCols || [])
    .map(col => col.split('.').pop())
    .sort()
    .join(',');
}

/**
 * Helper to get a human-readable schedule summary from cron expression
 */
function getScheduleSummary(schedule) {
  if (!schedule) return null;

  // Basic cron parsing for common patterns
  const parts = schedule.split(' ');
  if (parts.length < 5) return schedule;

  const [minute, hour, dayOfMonth, month, dayOfWeek] = parts;

  // Daily at specific hour
  if (dayOfMonth === '*' && month === '*' && dayOfWeek === '*') {
    const hourNum = parseInt(hour, 10);
    const minuteNum = parseInt(minute, 10);
    if (!isNaN(hourNum) && !isNaN(minuteNum)) {
      const period = hourNum >= 12 ? 'pm' : 'am';
      const displayHour = hourNum > 12 ? hourNum - 12 : hourNum || 12;
      const displayMinute = minuteNum.toString().padStart(2, '0');
      return `Daily @ ${displayHour}:${displayMinute}${period}`;
    }
  }

  // Weekly
  if (dayOfMonth === '*' && month === '*' && dayOfWeek !== '*') {
    const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    const dayNum = parseInt(dayOfWeek, 10);
    if (!isNaN(dayNum) && dayNum >= 0 && dayNum <= 6) {
      return `Weekly on ${days[dayNum]}`;
    }
  }

  return schedule;
}

/**
 * Helper to get status display info
 */
function getStatusInfo(preagg) {
  if (!preagg) {
    return {
      icon: '○',
      text: 'Not planned',
      className: 'status-not-planned',
      color: '#94a3b8',
    };
  }

  // Check if this is a compatible (superset) pre-agg, not exact match
  if (preagg._isCompatible) {
    // Show that this grain is covered by an existing pre-agg with more dimensions
    const preaggGrain = preagg.grain_columns
      ?.map(g => g.split('.').pop())
      .join(', ');
    
    // Check if that compatible pre-agg has data
    if (preagg.availability || preagg.status === 'active') {
      return {
        icon: '✓',
        text: `Covered (${preaggGrain})`,
        className: 'status-compatible-materialized',
        color: '#059669',
        isCompatible: true,
      };
    }
    return {
      icon: '◐',
      text: `Covered (${preaggGrain})`,
      className: 'status-compatible',
      color: '#d97706',
      isCompatible: true,
    };
  }

  // Check for running status (job is in progress)
  if (preagg.status === 'running') {
    return {
      icon: '◉',
      text: 'Running',
      className: 'status-running',
      color: '#2563eb',
    };
  }

  // Check availability for materialization status (active = has data)
  if (preagg.availability || preagg.status === 'active') {
    return {
      icon: '●',
      text: 'Materialized',
      className: 'status-materialized',
      color: '#059669',
    };
  }

  // Check if workflow is active
  if (preagg.workflow_status === 'active') {
    return {
      icon: '◐',
      text: 'Workflow Active',
      className: 'status-workflow-active',
      color: '#2563eb',
    };
  }

  // Check if workflow is paused
  if (preagg.workflow_status === 'paused') {
    return {
      icon: '◐',
      text: 'Workflow Paused',
      className: 'status-workflow-paused',
      color: '#94a3b8',
    };
  }

  // Check if configured but not yet materialized
  if (preagg.strategy) {
    return {
      icon: '◐',
      text: 'Pending',
      className: 'status-pending',
      color: '#d97706',
    };
  }

  return {
    icon: '○',
    text: 'Not planned',
    className: 'status-not-planned',
    color: '#94a3b8',
  };
}

/**
 * QueryOverviewPanel - Default view showing metrics SQL and pre-agg summary
 *
 * Shown when no node is selected in the graph
 */
/**
 * Get recommended schedule based on granularity
 */
function getRecommendedSchedule(granularity) {
  switch (granularity?.toUpperCase()) {
    case 'HOUR':
      return { cron: '0 * * * *', label: 'Hourly' };
    case 'DAY':
    default:
      return { cron: '0 6 * * *', label: 'Daily at 6:00 AM' };
  }
}

/**
 * Check if any grain group has temporal partitions
 */
function hasTemporalPartition(grainGroups) {
  // Check if any grain group has columns that look like temporal partitions
  // Common patterns: dateint, date_id, ds, hour, etc.
  const temporalPatterns = ['date', 'dateint', 'ds', 'hour', 'day', 'month', 'year', 'timestamp'];
  for (const gg of grainGroups || []) {
    for (const col of gg.grain || []) {
      const colName = col.split('.').pop().toLowerCase();
      if (temporalPatterns.some(p => colName.includes(p))) {
        return true;
      }
    }
  }
  return false;
}

/**
 * Get granularity hint from grain columns
 */
function inferGranularity(grainGroups) {
  for (const gg of grainGroups || []) {
    for (const col of gg.grain || []) {
      const colName = col.split('.').pop().toLowerCase();
      if (colName.includes('hour')) return 'HOUR';
    }
  }
  return 'DAY'; // Default
}

export function QueryOverviewPanel({
  measuresResult,
  metricsResult,
  selectedMetrics,
  selectedDimensions,
  plannedPreaggs = {},
  onPlanMaterialization,
  onUpdateConfig,
  onTriggerMaterialization,
  onCreateWorkflow,
  onRunBackfill,
  onRunAdhoc,
  onFetchRawSql,
  materializationError,
  onClearError,
}) {
  const [expandedCards, setExpandedCards] = useState({});
  const [configuringCard, setConfiguringCard] = useState(null); // '__all__' for section-level
  const [editingCard, setEditingCard] = useState(null); // grainKey of existing card being edited
  
  // Enhanced config form state
  const [configForm, setConfigForm] = useState({
    strategy: 'incremental_time',
    backfillFrom: '',
    backfillTo: 'today', // 'today' or specific date
    backfillToDate: '',
    continueAfterBackfill: true,
    schedule: '',
    scheduleType: 'auto', // 'auto' or 'custom'
    lookbackWindow: '1 day',
  });
  const [isSaving, setIsSaving] = useState(false);
  const [loadingAction, setLoadingAction] = useState(null); // Track which action is loading: 'workflow', 'backfill', 'trigger'
  
  // Backfill modal state (for existing pre-aggs)
  const [backfillModal, setBackfillModal] = useState(null);
  
  // Toast state for job URLs
  const [toastMessage, setToastMessage] = useState(null);

  // SQL view toggle state: 'optimized' (uses pre-aggs) or 'raw' (from source tables)
  const [sqlViewMode, setSqlViewMode] = useState('optimized');
  const [rawSql, setRawSql] = useState(null);
  const [loadingRawSql, setLoadingRawSql] = useState(false);

  // Handle SQL view toggle
  const handleSqlViewToggle = async (mode) => {
    setSqlViewMode(mode);
    // Fetch raw SQL lazily when switching to raw mode
    if (mode === 'raw' && !rawSql && onFetchRawSql) {
      setLoadingRawSql(true);
      const sql = await onFetchRawSql();
      setRawSql(sql);
      setLoadingRawSql(false);
    }
  };

  // Initialize config form with smart defaults when opening
  const openConfigForm = () => {
    const grainGroups = measuresResult?.grain_groups || [];
    const hasTemporal = hasTemporalPartition(grainGroups);
    const granularity = inferGranularity(grainGroups);
    const recommended = getRecommendedSchedule(granularity);
    
    // Default backfill start to 30 days ago
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000)
      .toISOString().split('T')[0];
    
    setConfigForm({
      strategy: hasTemporal ? 'incremental_time' : 'full',
      runBackfill: true, // Option to skip backfill
      backfillFrom: thirtyDaysAgo,
      backfillTo: 'today',
      backfillToDate: '',
      continueAfterBackfill: true,
      schedule: recommended.cron,
      scheduleType: 'auto',
      lookbackWindow: '1 day',
      _recommendedSchedule: recommended, // Store for display
      _granularity: granularity,
    });
    setConfiguringCard('__all__');
  };

  // Helper to start editing an existing pre-agg's config
  const startEditingConfig = (grainKey, existingPreagg) => {
    setConfigForm({
      strategy: existingPreagg.strategy || 'incremental_time',
      backfillFrom: '',
      backfillTo: 'today',
      backfillToDate: '',
      continueAfterBackfill: true,
      schedule: existingPreagg.schedule || '',
      scheduleType: existingPreagg.schedule ? 'custom' : 'auto',
      lookbackWindow: existingPreagg.lookback_window || '',
    });
    setEditingCard(grainKey);
  };

  const copyToClipboard = text => {
    navigator.clipboard.writeText(text);
  };

  const toggleCardExpanded = cardKey => {
    setExpandedCards(prev => ({
      ...prev,
      [cardKey]: !prev[cardKey],
    }));
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
        <div className="section-header-row">
          <h3 className="section-title">
            <span className="section-icon">◫</span>
            Pre-Aggregations ({grainGroups.length})
          </h3>
        </div>

        {/* Prominent call-to-action when no pre-aggs are configured */}
        {onPlanMaterialization && 
         grainGroups.length > 0 && 
         !Object.values(plannedPreaggs).some(p => p?.strategy) &&
         configuringCard !== '__all__' && (
          <div className="plan-materialization-cta">
            <div className="cta-content">
              <div className="cta-icon">⚡</div>
              <div className="cta-text">
                <strong>Ready to materialize?</strong>
                <span>Configure scheduled materialization for faster queries</span>
              </div>
            </div>
            <button
              className="action-btn action-btn-primary"
              type="button"
              onClick={openConfigForm}
            >
              Plan Materialization
            </button>
          </div>
        )}

        {/* Section-level Configuration Form - Enhanced */}
        {configuringCard === '__all__' && (
          <div className="materialization-config-form section-level-config">
            <div className="config-form-header">
              <span>Configure Materialization</span>
              <button
                className="config-close-btn"
                type="button"
                onClick={() => setConfiguringCard(null)}
              >
                ×
              </button>
            </div>
            <div className="config-form-body">
              {/* Strategy */}
              <div className="config-form-row">
                <label className="config-form-label">Strategy</label>
                <div className="config-form-options">
                  <label className="radio-option">
                    <input
                      type="radio"
                      name="strategy-all"
                      value="full"
                      checked={configForm.strategy === 'full'}
                      onChange={e =>
                        setConfigForm(prev => ({
                          ...prev,
                          strategy: e.target.value,
                        }))
                      }
                    />
                    <span>Full</span>
                  </label>
                  <label 
                    className={`radio-option ${!hasTemporalPartition(measuresResult?.grain_groups) ? 'disabled' : ''}`}
                    title={!hasTemporalPartition(measuresResult?.grain_groups) 
                      ? 'Incremental requires temporal partition columns on the source node (e.g., dateint, ds)' 
                      : ''}
                  >
                    <input
                      type="radio"
                      name="strategy-all"
                      value="incremental_time"
                      checked={configForm.strategy === 'incremental_time'}
                      disabled={!hasTemporalPartition(measuresResult?.grain_groups)}
                      onChange={e =>
                        setConfigForm(prev => ({
                          ...prev,
                          strategy: e.target.value,
                        }))
                      }
                    />
                    <span>Incremental</span>
                    {!hasTemporalPartition(measuresResult?.grain_groups) && (
                      <span className="option-hint">(requires temporal partition)</span>
                    )}
                  </label>
                </div>
              </div>

              {/* Run Backfill Option (only for incremental) */}
              {configForm.strategy === 'incremental_time' && (
                <div className="config-form-row">
                  <label className="checkbox-option">
                    <input
                      type="checkbox"
                      checked={configForm.runBackfill}
                      onChange={e =>
                        setConfigForm(prev => ({
                          ...prev,
                          runBackfill: e.target.checked,
                        }))
                      }
                    />
                    <span>Run initial backfill</span>
                  </label>
                  <span className="config-form-hint">
                    Populate historical data. Uncheck to only set up ongoing materialization.
                  </span>
                </div>
              )}

              {/* Backfill Date Range (only if runBackfill is checked) */}
              {configForm.strategy === 'incremental_time' && configForm.runBackfill && (
                <div className="config-form-section">
                  <label className="config-form-section-label">Backfill Date Range</label>
                  <div className="backfill-range">
                    <div className="backfill-field">
                      <label>From</label>
                      <input
                        type="date"
                        value={configForm.backfillFrom}
                        onChange={e =>
                          setConfigForm(prev => ({
                            ...prev,
                            backfillFrom: e.target.value,
                          }))
                        }
                      />
                    </div>
                    <div className="backfill-field">
                      <label>To</label>
                      <select
                        value={configForm.backfillTo}
                        onChange={e =>
                          setConfigForm(prev => ({
                            ...prev,
                            backfillTo: e.target.value,
                          }))
                        }
                      >
                        <option value="today">Today</option>
                        <option value="specific">Specific date</option>
                      </select>
                      {configForm.backfillTo === 'specific' && (
                        <input
                          type="date"
                          value={configForm.backfillToDate}
                          onChange={e =>
                            setConfigForm(prev => ({
                              ...prev,
                              backfillToDate: e.target.value,
                            }))
                          }
                          style={{ marginTop: '6px' }}
                        />
                      )}
                    </div>
                  </div>
                </div>
              )}

              {/* Schedule - always shown since we always create workflows */}
              <div className="config-form-row">
                  <label className="config-form-label">Schedule</label>
                  <select
                    className="config-form-select"
                    value={configForm.scheduleType}
                    onChange={e => {
                      const type = e.target.value;
                      setConfigForm(prev => ({
                        ...prev,
                        scheduleType: type,
                        schedule: type === 'auto' 
                          ? (prev._recommendedSchedule?.cron || '0 6 * * *')
                          : prev.schedule,
                      }));
                    }}
                  >
                    <option value="auto">
                      {configForm._recommendedSchedule?.label || 'Daily at 6:00 AM'} (recommended)
                    </option>
                    <option value="hourly">Hourly</option>
                    <option value="custom">Custom cron...</option>
                  </select>
                  {configForm.scheduleType === 'custom' && (
                    <input
                      type="text"
                      className="config-form-input"
                      placeholder="0 6 * * *"
                      value={configForm.schedule}
                      onChange={e =>
                        setConfigForm(prev => ({
                          ...prev,
                          schedule: e.target.value,
                        }))
                      }
                      style={{ marginTop: '6px' }}
                    />
                  )}
                  {configForm.scheduleType === 'auto' && (
                    <span className="config-form-hint">
                      Based on {configForm._granularity?.toLowerCase() || 'daily'} partition granularity
                    </span>
                  )}
                </div>

              {/* Lookback Window (only for incremental) */}
              {configForm.strategy === 'incremental_time' && (
                <div className="config-form-row">
                  <label className="config-form-label">Lookback Window</label>
                  <input
                    type="text"
                    className="config-form-input"
                    placeholder="1 day"
                    value={configForm.lookbackWindow}
                    onChange={e =>
                      setConfigForm(prev => ({
                        ...prev,
                        lookbackWindow: e.target.value,
                      }))
                    }
                  />
                  <span className="config-form-hint">
                    For late-arriving data (e.g., "1 day", "3 days")
                  </span>
                </div>
              )}
            </div>
            <div className="config-form-actions">
              <button
                className="action-btn action-btn-secondary"
                type="button"
                onClick={() => setConfiguringCard(null)}
              >
                Cancel
              </button>
              <button
                className="action-btn action-btn-primary"
                type="button"
                disabled={isSaving}
                onClick={async () => {
                  setIsSaving(true);
                  try {
                    // Compute the actual schedule value
                    let finalSchedule = configForm.schedule;
                    if (configForm.scheduleType === 'auto') {
                      finalSchedule = configForm._recommendedSchedule?.cron || '0 6 * * *';
                    } else if (configForm.scheduleType === 'hourly') {
                      finalSchedule = '0 * * * *';
                    }
                    
                    // Compute backfill end date
                    let backfillEndDate = null;
                    if (configForm.strategy === 'incremental_time') {
                      backfillEndDate = configForm.backfillTo === 'today'
                        ? new Date().toISOString().split('T')[0]
                        : configForm.backfillToDate;
                    }
                    
                    // Build the config object for the API
                    const apiConfig = {
                      strategy: configForm.strategy,
                      schedule: finalSchedule,  // Always set - we always create workflows
                      lookbackWindow: configForm.strategy === 'incremental_time' 
                        ? configForm.lookbackWindow 
                        : null,
                      // Backfill info (only for incremental + runBackfill checked)
                      runBackfill: configForm.runBackfill,
                      backfillFrom: (configForm.strategy === 'incremental_time' && configForm.runBackfill) 
                        ? configForm.backfillFrom 
                        : null,
                      backfillTo: (configForm.strategy === 'incremental_time' && configForm.runBackfill)
                        ? backfillEndDate
                        : null,
                    };
                    
                    await onPlanMaterialization(null, apiConfig);
                    setConfiguringCard(null);
                    // Reset form to defaults
                    setConfigForm({
                      strategy: 'incremental_time',
                      runBackfill: true,
                      backfillFrom: '',
                      backfillTo: 'today',
                      backfillToDate: '',
                      continueAfterBackfill: true,
                      schedule: '',
                      scheduleType: 'auto',
                      lookbackWindow: '1 day',
                    });
                  } catch (err) {
                    console.error('Failed to plan:', err);
                  }
                  setIsSaving(false);
                }}
              >
                {isSaving 
                  ? <><span className="spinner" /> Creating...</>
                  : configForm.strategy === 'incremental_time' && configForm.runBackfill
                    ? 'Create Workflow & Start Backfill'
                    : 'Create Workflow'
                }
              </button>
            </div>
          </div>
        )}

        {/* Materialization Error Banner */}
        {materializationError && (
          <div className="materialization-error">
            <div className="error-content">
              <span className="error-icon">⚠</span>
              <span className="error-message">{materializationError}</span>
            </div>
            <button
              className="error-dismiss"
              onClick={onClearError}
              aria-label="Dismiss error"
            >
              ×
            </button>
          </div>
        )}

        <div className="preagg-summary-list">
          {grainGroups.map((gg, i) => {
            const shortName = gg.parent_name?.split('.').pop() || 'Unknown';
            const relatedMetrics = metricFormulas.filter(m =>
              m.components?.some(comp =>
                gg.components?.some(pc => pc.name === comp),
              ),
            );

            // Look up existing pre-agg by normalized grain key
            const grainKey = `${gg.parent_name}|${normalizeGrain(gg.grain)}`;
            const existingPreagg = plannedPreaggs[grainKey];
            const statusInfo = getStatusInfo(existingPreagg);
            const isExpanded = expandedCards[grainKey] || false;
            const scheduleSummary = existingPreagg?.schedule
              ? getScheduleSummary(existingPreagg.schedule)
              : null;

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

                {/* Materialization Status Header */}
                {editingCard === grainKey ? (
                  /* Edit Config Form (only for existing pre-aggs) */
                  <div className="materialization-config-form">
                    <div className="config-form-header">
                      <span>Edit Materialization Config</span>
                      <button
                        className="config-close-btn"
                        type="button"
                        onClick={() => {
                          setEditingCard(null);
                        }}
                      >
                        ×
                      </button>
                    </div>
                    <div className="config-form-body">
                      <div className="config-form-row">
                        <label className="config-form-label">Strategy</label>
                        <div className="config-form-options">
                          <label className="radio-option">
                            <input
                              type="radio"
                              name={`strategy-${i}`}
                              value="full"
                              checked={configForm.strategy === 'full'}
                              onChange={e =>
                                setConfigForm(prev => ({
                                  ...prev,
                                  strategy: e.target.value,
                                }))
                              }
                            />
                            <span>Full</span>
                          </label>
                          <label className="radio-option">
                            <input
                              type="radio"
                              name={`strategy-${i}`}
                              value="incremental_time"
                              checked={
                                configForm.strategy === 'incremental_time'
                              }
                              onChange={e =>
                                setConfigForm(prev => ({
                                  ...prev,
                                  strategy: e.target.value,
                                }))
                              }
                            />
                            <span>Incremental (Time)</span>
                          </label>
                        </div>
                      </div>
                      <div className="config-form-row">
                        <label className="config-form-label">
                          Schedule (cron)
                        </label>
                        <input
                          type="text"
                          className="config-form-input"
                          placeholder="0 6 * * * (daily at 6am)"
                          value={configForm.schedule}
                          onChange={e =>
                            setConfigForm(prev => ({
                              ...prev,
                              schedule: e.target.value,
                            }))
                          }
                        />
                      </div>
                      {configForm.strategy === 'incremental_time' && (
                        <div className="config-form-row">
                          <label className="config-form-label">
                            Lookback Window
                          </label>
                          <input
                            type="text"
                            className="config-form-input"
                            placeholder="3 days"
                            value={configForm.lookbackWindow}
                            onChange={e =>
                              setConfigForm(prev => ({
                                ...prev,
                                lookbackWindow: e.target.value,
                              }))
                            }
                          />
                        </div>
                      )}
                    </div>
                    <div className="config-form-actions">
                      <button
                        className="action-btn action-btn-secondary"
                        type="button"
                        onClick={() => setEditingCard(null)}
                      >
                        Cancel
                      </button>
                      <button
                        className="action-btn action-btn-primary"
                        type="button"
                        disabled={isSaving}
                        onClick={async () => {
                          setIsSaving(true);
                          try {
                            if (onUpdateConfig && existingPreagg?.id) {
                              await onUpdateConfig(existingPreagg.id, configForm);
                            }
                            setEditingCard(null);
                            setConfigForm({
                              strategy: 'full',
                              schedule: '',
                              lookbackWindow: '',
                            });
                          } catch (err) {
                            console.error('Failed to save:', err);
                          }
                          setIsSaving(false);
                        }}
                      >
                        {isSaving ? <><span className="spinner" /> Saving...</> : 'Save'}
                      </button>
                    </div>
                  </div>
                ) : existingPreagg ? (
                  /* Existing Pre-agg Status Header */
                  <>
                    <div
                      className="materialization-header clickable"
                      onClick={() => toggleCardExpanded(grainKey)}
                    >
                      <div className="materialization-status">
                        <span
                          className={`status-indicator ${statusInfo.className}`}
                          style={{ color: statusInfo.color }}
                        >
                          {statusInfo.icon}
                        </span>
                        <span className="status-text">{statusInfo.text}</span>
                        {scheduleSummary && (
                          <>
                            <span className="status-separator">|</span>
                            <span className="schedule-summary">
                              {scheduleSummary}
                            </span>
                          </>
                        )}
                      </div>
                      <button
                        className="expand-toggle"
                        type="button"
                        aria-label={isExpanded ? 'Collapse' : 'Expand'}
                      >
                        {isExpanded ? '▲' : '▼'}
                      </button>
                    </div>

                    {/* Expandable Materialization Details */}
                    {isExpanded && (
                      <div className="materialization-details">
                        {/* Note for compatible (superset) pre-aggs */}
                        {existingPreagg._isCompatible && (
                          <div className="compatible-preagg-note">
                            <span className="note-icon">ℹ️</span>
                            <span>
                              This query can use an existing pre-agg with finer grain: 
                              <strong> {existingPreagg.grain_columns?.map(g => g.split('.').pop()).join(', ')}</strong>
                            </span>
                          </div>
                        )}
                        <div className="materialization-config">
                          <div className="config-row">
                            <span className="config-label">Strategy:</span>
                            <span className="config-value">
                              {existingPreagg.strategy === 'incremental_time'
                                ? 'Incremental (Time-based)'
                                : existingPreagg.strategy === 'full'
                                ? 'Full'
                                : existingPreagg.strategy || 'Not set'}
                            </span>
                          </div>
                          {existingPreagg.schedule && (
                            <div className="config-row">
                              <span className="config-label">Schedule:</span>
                              <span className="config-value config-mono">
                                {existingPreagg.schedule}
                              </span>
                            </div>
                          )}
                          {existingPreagg.lookback_window && (
                            <div className="config-row">
                              <span className="config-label">Lookback:</span>
                              <span className="config-value">
                                {existingPreagg.lookback_window}
                              </span>
                            </div>
                          )}
                          {existingPreagg.availability?.updated_at && (
                            <div className="config-row">
                              <span className="config-label">Last Run:</span>
                              <span className="config-value">
                                {new Date(
                                  existingPreagg.availability.updated_at,
                                ).toLocaleString()}{' '}
                                <span className="run-status success">✓</span>
                              </span>
                            </div>
                          )}
                          {/* Workflow URLs (shown when job is running or has been triggered) */}
                          {existingPreagg.workflow_urls?.length > 0 && (
                            <div className="config-row">
                              <span className="config-label">Workflows:</span>
                              <div className="workflow-links">
                                {existingPreagg.workflow_urls.map((url, idx) => (
                                  <a
                                    key={idx}
                                    href={url}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="workflow-link"
                                  >
                                    {url.includes('scheduled')
                                      ? '📅 Scheduled'
                                      : url.includes('adhoc')
                                      ? '▶ Ad-hoc'
                                      : `Job ${idx + 1}`}
                                    <span className="link-icon">↗</span>
                                  </a>
                                ))}
                              </div>
                            </div>
                          )}
                        </div>
                        <div className="materialization-actions">
                          {onUpdateConfig && existingPreagg.id && (
                            <button
                              className="action-btn action-btn-secondary"
                              type="button"
                              onClick={e => {
                                e.stopPropagation();
                                startEditingConfig(grainKey, existingPreagg);
                              }}
                            >
                              Edit Config
                            </button>
                          )}
                          
                          {/* Workflow actions */}
                          {existingPreagg.strategy && existingPreagg.schedule && (
                            <>
                              {!existingPreagg.scheduled_workflow_url && onCreateWorkflow && (
                                <button
                                  className="action-btn action-btn-secondary"
                                  type="button"
                                  disabled={loadingAction === `workflow-${existingPreagg.id}`}
                                  onClick={async e => {
                                    e.stopPropagation();
                                    setLoadingAction(`workflow-${existingPreagg.id}`);
                                    try {
                                      const result = await onCreateWorkflow(existingPreagg.id);
                                      if (result?.workflow_url) {
                                        setToastMessage(`Workflow created: ${result.workflow_url}`);
                                        setTimeout(() => setToastMessage(null), 5000);
                                      }
                                    } finally {
                                      setLoadingAction(null);
                                    }
                                  }}
                                >
                                  {loadingAction === `workflow-${existingPreagg.id}` 
                                    ? <><span className="spinner" /> Creating...</>
                                    : 'Create Workflow'}
                                </button>
                              )}
                              {existingPreagg.scheduled_workflow_url && (
                                <>
                                  <a
                                    href={existingPreagg.scheduled_workflow_url}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="action-btn action-btn-secondary"
                                    onClick={e => e.stopPropagation()}
                                  >
                                    View Workflow ↗
                                  </a>
                                  <button
                                    className="action-btn action-btn-secondary"
                                    type="button"
                                    title="Refresh workflow (re-push to scheduler)"
                                    disabled={loadingAction === `refresh-${existingPreagg.id}`}
                                    onClick={async e => {
                                      e.stopPropagation();
                                      setLoadingAction(`refresh-${existingPreagg.id}`);
                                      try {
                                        // Re-push the workflow
                                        await onCreateWorkflow(existingPreagg.id, true);
                                      } finally {
                                        setLoadingAction(null);
                                      }
                                    }}
                                  >
                                    {loadingAction === `refresh-${existingPreagg.id}`
                                      ? <><span className="spinner" /> Refreshing...</>
                                      : '↻ Refresh'}
                                  </button>
                                </>
                              )}
                            </>
                          )}
                          
                          {/* Backfill button */}
                          {existingPreagg.strategy && onRunBackfill && (
                            <button
                              className="action-btn action-btn-secondary"
                              type="button"
                              onClick={e => {
                                e.stopPropagation();
                                // Open backfill modal
                                const today = new Date().toISOString().split('T')[0];
                                const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
                                setBackfillModal({
                                  preaggId: existingPreagg.id,
                                  startDate: weekAgo,
                                  endDate: today,
                                });
                              }}
                            >
                              Run Backfill
                            </button>
                          )}
                          
                          {/* Legacy trigger button */}
                          {onTriggerMaterialization && existingPreagg.id && !existingPreagg.scheduled_workflow_url && (
                            <button
                              className="action-btn action-btn-primary"
                              type="button"
                              disabled={existingPreagg.status === 'running' || loadingAction === `trigger-${existingPreagg.id}`}
                              onClick={async e => {
                                e.stopPropagation();
                                setLoadingAction(`trigger-${existingPreagg.id}`);
                                try {
                                  await onTriggerMaterialization(existingPreagg.id);
                                } finally {
                                  setLoadingAction(null);
                                }
                              }}
                            >
                              {loadingAction === `trigger-${existingPreagg.id}`
                                ? <><span className="spinner" /> Running...</>
                                : existingPreagg.status === 'running'
                                  ? 'Running...'
                                  : 'Trigger Now'}
                            </button>
                          )}
                        </div>
                      </div>
                    )}
                  </>
                ) : (
                  /* Not Planned - Show status only (use section-level button to plan) */
                  <div className="materialization-header">
                    <div className="materialization-status">
                      <span
                        className={`status-indicator ${statusInfo.className}`}
                        style={{ color: statusInfo.color }}
                      >
                        {statusInfo.icon}
                      </span>
                      <span className="status-text">{statusInfo.text}</span>
                    </div>
                  </div>
                )}
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
            <div className="sql-view-toggle">
              <button
                className={`sql-toggle-btn ${sqlViewMode === 'optimized' ? 'active' : ''}`}
                onClick={() => handleSqlViewToggle('optimized')}
                type="button"
                title="SQL using pre-aggregations (when available)"
              >
                Optimized
              </button>
              <button
                className={`sql-toggle-btn ${sqlViewMode === 'raw' ? 'active' : ''}`}
                onClick={() => handleSqlViewToggle('raw')}
                type="button"
                title="SQL computed directly from source tables"
                disabled={loadingRawSql}
              >
                {loadingRawSql ? '...' : 'Raw'}
              </button>
            </div>
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
              {sqlViewMode === 'raw' ? (rawSql || 'Loading...') : sql}
            </SyntaxHighlighter>
          </div>
        </div>
      )}

      {/* Backfill Modal */}
      {backfillModal && (
        <div className="backfill-modal-overlay" onClick={() => setBackfillModal(null)}>
          <div className="backfill-modal" onClick={e => e.stopPropagation()}>
            <div className="backfill-modal-header">
              <h3>Run Backfill</h3>
              <button
                className="modal-close"
                onClick={() => setBackfillModal(null)}
              >
                ×
              </button>
            </div>
            <div className="backfill-modal-body">
              <div className="backfill-form-row">
                <label>Start Date</label>
                <input
                  type="date"
                  value={backfillModal.startDate}
                  onChange={e =>
                    setBackfillModal(prev => ({
                      ...prev,
                      startDate: e.target.value,
                    }))
                  }
                />
              </div>
              <div className="backfill-form-row">
                <label>End Date</label>
                <input
                  type="date"
                  value={backfillModal.endDate}
                  onChange={e =>
                    setBackfillModal(prev => ({
                      ...prev,
                      endDate: e.target.value,
                    }))
                  }
                />
              </div>
            </div>
            <div className="backfill-modal-actions">
              <button
                className="action-btn action-btn-secondary"
                onClick={() => setBackfillModal(null)}
                disabled={loadingAction === 'backfill-modal'}
              >
                Cancel
              </button>
              <button
                className="action-btn action-btn-primary"
                disabled={loadingAction === 'backfill-modal'}
                onClick={async () => {
                  setLoadingAction('backfill-modal');
                  try {
                    const result = await onRunBackfill(
                      backfillModal.preaggId,
                      backfillModal.startDate,
                      backfillModal.endDate,
                    );
                    setBackfillModal(null);
                    if (result?.job_url) {
                      setToastMessage(
                        <span>
                          Backfill started:{' '}
                          <a href={result.job_url} target="_blank" rel="noopener noreferrer">
                            View Job ↗
                          </a>
                        </span>
                      );
                      setTimeout(() => setToastMessage(null), 10000);
                    }
                  } finally {
                    setLoadingAction(null);
                  }
                }}
              >
                {loadingAction === 'backfill-modal' 
                  ? <><span className="spinner" /> Starting...</>
                  : 'Start Backfill'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Toast Message */}
      {toastMessage && (
        <div className="toast-message">
          {toastMessage}
          <button
            className="toast-close"
            onClick={() => setToastMessage(null)}
          >
            ×
          </button>
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
