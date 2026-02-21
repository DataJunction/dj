import { useState, useEffect, useCallback, useRef } from 'react';
import { Link } from 'react-router-dom';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { atomOneLight } from 'react-syntax-highlighter/src/styles/hljs';
import {
  SCAN_WARNING_THRESHOLD,
  SCAN_CRITICAL_THRESHOLD,
} from '../../constants';

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
  if (!preagg || preagg.workflow_urls?.length === 0) {
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

  // Check if workflow is being configured (has URLs but not yet active)
  // Only show "Pending" if workflows exist but aren't active yet
  if (preagg.workflow_urls?.length > 0) {
    return {
      icon: '◐',
      text: 'Pending',
      className: 'status-pending',
      color: '#d97706',
    };
  }

  // No workflow configured - show "Not planned"
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

/**
 * Format bytes to human-readable string
 */
function formatBytes(bytes) {
  if (!bytes || bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
}

/**
 * Get warning level for scan size
 */
function getScanWarningLevel(bytes) {
  if (bytes > SCAN_CRITICAL_THRESHOLD) return 'critical';
  if (bytes > SCAN_WARNING_THRESHOLD) return 'warning';
  return 'ok';
}

/**
 * Format scan estimate for display
 */
function formatScanEstimate(scanEstimate) {
  if (
    !scanEstimate ||
    !scanEstimate.sources ||
    scanEstimate.sources.length === 0
  ) {
    return null;
  }

  // Check if any sources are missing size data
  const hasMissingData = scanEstimate.sources.some(
    s => s.total_bytes === null || s.total_bytes === undefined,
  );

  // Determine warning level based on total_bytes (if available)
  let level = 'unknown';
  let icon = 'ℹ️';
  if (
    scanEstimate.total_bytes !== null &&
    scanEstimate.total_bytes !== undefined
  ) {
    level = getScanWarningLevel(scanEstimate.total_bytes);
    icon = level === 'critical' ? '⚠️' : level === 'warning' ? '⚡' : '✓';
  }

  return {
    icon,
    level,
    totalBytes: scanEstimate.total_bytes,
    sources: scanEstimate.sources || [],
    hasMissingData,
  };
}

/**
 * CubeBackfillModal - Simple modal to collect start/end dates for backfill
 */
function CubeBackfillModal({ onClose, onSubmit, loading }) {
  // Default to last 7 days
  const today = new Date().toISOString().split('T')[0];
  const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)
    .toISOString()
    .split('T')[0];

  const [startDate, setStartDate] = useState(weekAgo);
  const [endDate, setEndDate] = useState(today);

  return (
    <div className="backfill-modal-overlay" onClick={onClose}>
      <div className="backfill-modal" onClick={e => e.stopPropagation()}>
        <div className="backfill-modal-header">
          <h3>Run Cube Backfill</h3>
          <button className="modal-close" onClick={onClose}>
            ×
          </button>
        </div>
        <div className="backfill-modal-body">
          <p className="backfill-description">
            Run a backfill for the specified date range:
          </p>
          <div className="backfill-date-inputs">
            <div className="date-input-group">
              <label htmlFor="backfill-start">Start Date</label>
              <input
                id="backfill-start"
                type="date"
                value={startDate}
                onChange={e => setStartDate(e.target.value)}
                disabled={loading}
              />
            </div>
            <div className="date-input-group">
              <label htmlFor="backfill-end">End Date</label>
              <input
                id="backfill-end"
                type="date"
                value={endDate}
                onChange={e => setEndDate(e.target.value)}
                disabled={loading}
              />
            </div>
          </div>
        </div>
        <div className="backfill-modal-actions">
          <button
            className="action-btn action-btn-secondary"
            onClick={onClose}
            disabled={loading}
          >
            Cancel
          </button>
          <button
            className="action-btn action-btn-primary"
            disabled={loading || !startDate}
            onClick={() => onSubmit(startDate, endDate || null)}
          >
            {loading ? (
              <>
                <span className="spinner" /> Starting...
              </>
            ) : (
              'Start Backfill'
            )}
          </button>
        </div>
      </div>
    </div>
  );
}

export function QueryOverviewPanel({
  measuresResult,
  metricsResult,
  selectedMetrics,
  selectedDimensions,
  plannedPreaggs = {},
  onPlanMaterialization,
  onUpdateConfig,
  onCreateWorkflow,
  onRunBackfill,
  onRunAdhoc,
  onFetchRawSql,
  onSetPartition,
  onRefreshMeasures,
  onFetchNodePartitions,
  materializationError,
  onClearError,
  workflowUrls = [],
  onClearWorkflowUrls,
  loadedCubeName = null, // Existing cube name if loaded from preset
  cubeMaterialization = null, // Full cube materialization info {schedule, strategy, lookbackWindow, ...}
  onUpdateCubeConfig,
  onRefreshCubeWorkflow,
  onRunCubeBackfill,
  onDeactivatePreaggWorkflow,
  onDeactivateCubeWorkflow,
}) {
  // Extract default namespace from the first selected metric (e.g., "v3.total_revenue" -> "v3")
  const getDefaultNamespace = useCallback(() => {
    if (selectedMetrics && selectedMetrics.length > 0) {
      const firstMetric = selectedMetrics[0];
      const parts = firstMetric.split('.');
      // Take all parts except the last one (the metric name)
      if (parts.length > 1) {
        return parts.slice(0, -1).join('.');
      }
    }
    return 'default';
  }, [selectedMetrics]);

  const [expandedCards, setExpandedCards] = useState({});
  const [configuringCard, setConfiguringCard] = useState(null); // '__all__' for section-level
  const [editingCard, setEditingCard] = useState(null); // grainKey of existing card being edited
  const [editingCube, setEditingCube] = useState(false); // Whether cube config form is open
  const [cubeBackfillModal, setCubeBackfillModal] = useState(false); // Cube backfill modal state
  const [cubeConfigForm, setCubeConfigForm] = useState({
    strategy: 'incremental_time',
    schedule: '0 6 * * *',
    lookbackWindow: '1 DAY',
  });
  const [isSavingCube, setIsSavingCube] = useState(false);

  // Partition setup form state (for setting up temporal partition inline)
  // Map of nodeName -> { column, granularity, format }
  const [showPartitionSetup, setShowPartitionSetup] = useState(false);
  const [partitionForms, setPartitionForms] = useState({});
  const [settingPartitionFor, setSettingPartitionFor] = useState(null); // nodeName being set
  const [partitionErrors, setPartitionErrors] = useState({}); // nodeName -> error

  // Actual temporal partitions from source nodes (fetched via API)
  // Map of nodeName -> { columns, temporalPartitions }
  const [allNodePartitions, setAllNodePartitions] = useState({});
  const [partitionsLoading, setPartitionsLoading] = useState(false);

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
    // Druid cube materialization settings
    enableDruidCube: true,
    druidCubeNamespace: '', // Will be initialized with user's namespace
    druidCubeName: '', // Short name without namespace
  });
  const [isSaving, setIsSaving] = useState(false);
  const [loadingAction, setLoadingAction] = useState(null); // Track which action is loading: 'workflow', 'backfill', 'trigger'

  // Backfill modal state (for existing pre-aggs)
  const [backfillModal, setBackfillModal] = useState(null);

  // Toast state for job URLs
  const [toastMessage, setToastMessage] = useState(null);

  // SQL view toggle state: 'optimized' (uses pre-aggs) or 'raw' (from source tables)
  const [sqlViewMode, setSqlViewMode] = useState('optimized');
  const [rawResult, setRawResult] = useState(null);
  const [loadingRawSql, setLoadingRawSql] = useState(false);

  // Handle SQL view toggle
  const handleSqlViewToggle = async mode => {
    setSqlViewMode(mode);
    // Fetch raw SQL lazily when switching to raw mode
    if (mode === 'raw' && !rawResult && onFetchRawSql) {
      setLoadingRawSql(true);
      const result = await onFetchRawSql();
      setRawResult(result);
      setLoadingRawSql(false);
    }
  };

  // Get unique parent nodes from grain groups
  const getUniqueParentNodes = useCallback(() => {
    const grainGroups = measuresResult?.grain_groups || [];
    const uniqueNodes = new Set();
    grainGroups.forEach(gg => {
      if (gg.parent_name) uniqueNodes.add(gg.parent_name);
    });
    return Array.from(uniqueNodes);
  }, [measuresResult?.grain_groups]);

  // Fetch actual partition info for ALL source nodes when config form opens
  // Returns a map of nodeName -> { columns, temporalPartitions }
  const fetchAllNodePartitions = useCallback(async () => {
    const parentNodes = getUniqueParentNodes();
    if (parentNodes.length === 0 || !onFetchNodePartitions) {
      setAllNodePartitions({});
      return {};
    }

    setPartitionsLoading(true);
    try {
      const results = {};
      // Fetch partitions for all nodes in parallel
      const promises = parentNodes.map(async nodeName => {
        try {
          const result = await onFetchNodePartitions(nodeName);
          results[nodeName] = result;
        } catch (err) {
          console.error(`Failed to fetch partitions for ${nodeName}:`, err);
          results[nodeName] = { columns: [], temporalPartitions: [] };
        }
      });
      await Promise.all(promises);
      console.log('[fetchAllNodePartitions] results:', results);
      setAllNodePartitions(results);
      return results;
    } catch (err) {
      console.error('Failed to fetch node partitions:', err);
      setAllNodePartitions({});
      return {};
    } finally {
      setPartitionsLoading(false);
    }
  }, [getUniqueParentNodes, onFetchNodePartitions]);

  // Check if ALL parent nodes have temporal partitions
  const hasActualTemporalPartitions = useCallback(() => {
    const parentNodes = getUniqueParentNodes();
    if (parentNodes.length === 0) return false;

    // All nodes must have temporal partitions
    const allHaveTemporal = parentNodes.every(nodeName => {
      const nodePartitions = allNodePartitions[nodeName];
      return nodePartitions?.temporalPartitions?.length > 0;
    });

    console.log('[hasActualTemporalPartitions]', {
      parentNodes,
      allNodePartitions,
      allHaveTemporal,
    });
    return allHaveTemporal;
  }, [getUniqueParentNodes, allNodePartitions]);

  // Get nodes that are missing temporal partitions
  const getNodesMissingPartitions = useCallback(() => {
    const parentNodes = getUniqueParentNodes();
    return parentNodes.filter(nodeName => {
      const nodePartitions = allNodePartitions[nodeName];
      return !nodePartitions?.temporalPartitions?.length;
    });
  }, [getUniqueParentNodes, allNodePartitions]);

  // Initialize config form with smart defaults when opening
  const openConfigForm = async () => {
    const grainGroups = measuresResult?.grain_groups || [];
    const granularity = inferGranularity(grainGroups);
    const recommended = getRecommendedSchedule(granularity);

    // Default backfill start to 30 days ago
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000)
      .toISOString()
      .split('T')[0];

    // Fetch actual partition info for ALL source nodes
    const partitionResults = await fetchAllNodePartitions();

    // Check if ALL nodes have temporal partitions
    const parentNodes = getUniqueParentNodes();
    const allHaveTemporal = parentNodes.every(
      nodeName => partitionResults[nodeName]?.temporalPartitions?.length > 0,
    );
    console.log(
      '[openConfigForm] allHaveTemporal:',
      allHaveTemporal,
      'partitionResults:',
      partitionResults,
    );

    // Initialize partition forms for nodes missing partitions
    const datePattern =
      /date|time|day|month|year|hour|ds|dt|dateint|timestamp/i;
    const initialForms = {};
    parentNodes.forEach(nodeName => {
      const nodePartitions = partitionResults[nodeName];
      if (!nodePartitions?.temporalPartitions?.length) {
        // Find best default column (date-like, unpartitioned)
        const cols = (nodePartitions?.columns || []).filter(c => !c.partition);
        const sortedCols = [...cols].sort((a, b) => {
          const aIsDate = datePattern.test(a.name);
          const bIsDate = datePattern.test(b.name);
          if (aIsDate && !bIsDate) return -1;
          if (!aIsDate && bIsDate) return 1;
          return a.name.localeCompare(b.name);
        });
        initialForms[nodeName] = {
          column: sortedCols[0]?.name || '',
          granularity: 'day',
          format: 'yyyyMMdd',
        };
      }
    });
    setPartitionForms(initialForms);
    setPartitionErrors({});

    setConfigForm({
      strategy: allHaveTemporal ? 'incremental_time' : 'full',
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
      // Druid cube settings
      enableDruidCube: true,
      druidCubeNamespace: getDefaultNamespace(), // Default to first metric's namespace
      druidCubeName: '', // Short name without namespace
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

  // Determine if materialization is already configured (has active workflows)
  const isMaterialized =
    workflowUrls.length > 0 ||
    Object.values(plannedPreaggs).some(p => p?.workflow_urls?.length > 0);

  // Get materialization summary for collapsed state
  const getMaterializationSummary = () => {
    const hasCube = workflowUrls.length > 0;
    const preaggCount = Object.values(plannedPreaggs).filter(
      p => p?.workflow_urls?.length > 0,
    ).length;
    const strategy =
      Object.values(plannedPreaggs).find(p => p?.strategy)?.strategy ||
      'incremental_time';
    const schedule =
      Object.values(plannedPreaggs).find(p => p?.schedule)?.schedule ||
      '0 6 * * *';
    return {
      hasCube,
      preaggCount,
      strategy: strategy === 'incremental_time' ? 'Incremental' : 'Full',
      schedule: getScheduleSummary(schedule),
    };
  };

  return (
    <div className="details-panel">
      {/* Header */}
      <div className="details-header">
        <h2 className="details-title">Query Plan</h2>
        <p className="details-full-name">
          {selectedMetrics.length} metric
          {selectedMetrics.length !== 1 ? 's' : ''} ×{' '}
          {selectedDimensions.length} dimension
          {selectedDimensions.length !== 1 ? 's' : ''}
        </p>
      </div>

      {/* Global Materialization Error Banner - always visible when there's an error */}
      {materializationError && (
        <div className="materialization-error global-error">
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

      {/* Materialization Config Section - Only show when there's content */}
      {grainGroups.length > 0 &&
        onPlanMaterialization &&
        (!isMaterialized || configuringCard === '__all__') && (
          <div className="details-section">
            {/* State A: Not materialized - show CTA */}
            {!isMaterialized && configuringCard !== '__all__' && (
              <div className="plan-materialization-cta">
                <div className="cta-content">
                  <div className="cta-icon">⚡</div>
                  <div className="cta-text">
                    <strong>Ready to materialize?</strong>
                    <span>
                      Configure scheduled materialization for faster queries
                    </span>
                  </div>
                </div>
                <button
                  className="action-btn action-btn-primary"
                  type="button"
                  onClick={openConfigForm}
                >
                  Configure
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
                      <label className="radio-option">
                        <input
                          type="radio"
                          name="strategy-all"
                          value="incremental_time"
                          checked={configForm.strategy === 'incremental_time'}
                          onChange={e => {
                            setConfigForm(prev => ({
                              ...prev,
                              strategy: e.target.value,
                            }));
                            // Auto-show partition setup if any node is missing temporal partition
                            if (
                              !hasActualTemporalPartitions() &&
                              !partitionsLoading
                            ) {
                              setShowPartitionSetup(true);
                            }
                          }}
                        />
                        <span>Incremental</span>
                        {configForm.strategy === 'incremental_time' &&
                          (partitionsLoading ? (
                            <span className="option-hint">(checking...)</span>
                          ) : hasActualTemporalPartitions() ? (
                            <span className="partition-badge">
                              {getUniqueParentNodes()
                                .map(
                                  nodeName =>
                                    allNodePartitions[nodeName]
                                      ?.temporalPartitions?.[0]?.name,
                                )
                                .filter(Boolean)
                                .join(', ')}
                            </span>
                          ) : null)}
                      </label>
                    </div>
                  </div>

                  {/* Inline Partition Setup Form - Per Node */}
                  {showPartitionSetup &&
                    configForm.strategy === 'incremental_time' &&
                    !hasActualTemporalPartitions() && (
                      <div className="partition-setup-form">
                        <div className="partition-setup-header">
                          <span className="partition-setup-icon">⚠️</span>
                          <span>
                            Set up temporal partitions for incremental builds
                          </span>
                        </div>

                        <div className="partition-setup-body">
                          {getUniqueParentNodes().map(nodeName => {
                            const nodePartitions = allNodePartitions[nodeName];
                            const hasTemporal =
                              nodePartitions?.temporalPartitions?.length > 0;
                            const form = partitionForms[nodeName] || {
                              column: '',
                              granularity: 'day',
                              format: 'yyyyMMdd',
                            };
                            const error = partitionErrors[nodeName];
                            const isSettingThis =
                              settingPartitionFor === nodeName;
                            const shortName = nodeName.split('.').pop();

                            // If this node already has temporal partitions, show success
                            if (hasTemporal) {
                              return (
                                <div
                                  key={nodeName}
                                  className="partition-node-section partition-node-done"
                                >
                                  <div className="partition-node-header">
                                    <span className="partition-node-name">
                                      <span className="partition-node-icon">
                                        ✓
                                      </span>
                                      {shortName}
                                    </span>
                                  </div>
                                  <div className="partition-node-status">
                                    <span className="partition-badge">
                                      {
                                        nodePartitions.temporalPartitions[0]
                                          ?.name
                                      }
                                    </span>
                                  </div>
                                </div>
                              );
                            }

                            // Show setup form for this node
                            return (
                              <div
                                key={nodeName}
                                className="partition-node-section"
                              >
                                <div className="partition-node-header">
                                  <span className="partition-node-name">
                                    <span className="partition-node-icon">
                                      📦
                                    </span>
                                    {shortName}
                                  </span>
                                </div>

                                {error && (
                                  <div className="partition-setup-error">
                                    {error}
                                  </div>
                                )}

                                <div className="partition-node-form">
                                  <div className="partition-field">
                                    <label>Column</label>
                                    <select
                                      value={form.column}
                                      onChange={e =>
                                        setPartitionForms(prev => ({
                                          ...prev,
                                          [nodeName]: {
                                            ...form,
                                            column: e.target.value,
                                          },
                                        }))
                                      }
                                    >
                                      <option value="">Select...</option>
                                      {(() => {
                                        const datePattern =
                                          /date|time|day|month|year|hour|ds|dt|dateint|timestamp/i;
                                        return (nodePartitions?.columns || [])
                                          .filter(col => !col.partition)
                                          .map(col => ({
                                            ...col,
                                            isDateLike: datePattern.test(
                                              col.name,
                                            ),
                                          }))
                                          .sort((a, b) => {
                                            if (a.isDateLike && !b.isDateLike)
                                              return -1;
                                            if (!a.isDateLike && b.isDateLike)
                                              return 1;
                                            return a.name.localeCompare(b.name);
                                          })
                                          .map(col => (
                                            <option
                                              key={col.name}
                                              value={col.name}
                                            >
                                              {col.name}
                                              {col.isDateLike ? ' ★' : ''}
                                            </option>
                                          ));
                                      })()}
                                    </select>
                                  </div>
                                  <div className="partition-field partition-field-small">
                                    <label>Granularity</label>
                                    <select
                                      value={form.granularity}
                                      onChange={e =>
                                        setPartitionForms(prev => ({
                                          ...prev,
                                          [nodeName]: {
                                            ...form,
                                            granularity: e.target.value,
                                          },
                                        }))
                                      }
                                    >
                                      <option value="day">Day</option>
                                      <option value="hour">Hour</option>
                                      <option value="month">Month</option>
                                    </select>
                                  </div>
                                  <div className="partition-field partition-field-small">
                                    <label>Format</label>
                                    <input
                                      type="text"
                                      placeholder="yyyyMMdd"
                                      value={form.format}
                                      onChange={e =>
                                        setPartitionForms(prev => ({
                                          ...prev,
                                          [nodeName]: {
                                            ...form,
                                            format: e.target.value,
                                          },
                                        }))
                                      }
                                    />
                                  </div>
                                  <button
                                    type="button"
                                    className="partition-set-btn"
                                    disabled={!form.column || isSettingThis}
                                    onClick={async () => {
                                      if (!form.column) return;

                                      setSettingPartitionFor(nodeName);
                                      setPartitionErrors(prev => ({
                                        ...prev,
                                        [nodeName]: null,
                                      }));

                                      try {
                                        const result = await onSetPartition(
                                          nodeName,
                                          form.column,
                                          'temporal',
                                          form.format || 'yyyyMMdd',
                                          form.granularity,
                                        );

                                        if (result?.status >= 400) {
                                          throw new Error(
                                            result.json?.message ||
                                              'Failed to set partition',
                                          );
                                        }

                                        // Refresh partitions to pick up the new one
                                        await fetchAllNodePartitions();

                                        // Check if all nodes now have partitions
                                        if (hasActualTemporalPartitions()) {
                                          setShowPartitionSetup(false);
                                        }
                                      } catch (err) {
                                        setPartitionErrors(prev => ({
                                          ...prev,
                                          [nodeName]:
                                            err.message ||
                                            'Failed to set partition',
                                        }));
                                      } finally {
                                        setSettingPartitionFor(null);
                                      }
                                    }}
                                  >
                                    {isSettingThis ? '...' : 'Set'}
                                  </button>
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    )}

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
                        Populate historical data. Uncheck to only set up ongoing
                        materialization.
                      </span>
                    </div>
                  )}

                  {/* Backfill Date Range (only if runBackfill is checked) */}
                  {configForm.strategy === 'incremental_time' &&
                    configForm.runBackfill && (
                      <div className="config-form-section">
                        <label className="config-form-section-label">
                          Backfill Date Range
                        </label>
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
                          schedule:
                            type === 'auto'
                              ? prev._recommendedSchedule?.cron || '0 6 * * *'
                              : prev.schedule,
                        }));
                      }}
                    >
                      <option value="auto">
                        {configForm._recommendedSchedule?.label ||
                          'Daily at 6:00 AM'}{' '}
                        (recommended)
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
                        Based on{' '}
                        {configForm._granularity?.toLowerCase() || 'daily'}{' '}
                        partition granularity
                      </span>
                    )}
                  </div>

                  {/* Lookback Window (only for incremental) */}
                  {configForm.strategy === 'incremental_time' && (
                    <div className="config-form-row">
                      <label className="config-form-label">
                        Lookback Window
                      </label>
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

                  {/* Druid Cube Materialization Section */}
                  <div className="config-form-divider">
                    <span>Cube Materialization (Druid)</span>
                  </div>

                  <div className="config-form-row">
                    <label className="checkbox-option">
                      <input
                        type="checkbox"
                        checked={configForm.enableDruidCube}
                        onChange={e =>
                          setConfigForm(prev => ({
                            ...prev,
                            enableDruidCube: e.target.checked,
                          }))
                        }
                      />
                      Enable Druid cube materialization
                    </label>
                    <span className="config-form-hint">
                      Combines pre-aggs into a single Druid datasource for fast
                      interactive queries
                    </span>
                  </div>

                  {/* Druid cube config (shown when enabled and there is no associated cube) */}
                  {configForm.enableDruidCube && !loadedCubeName && (
                    <div className="config-form-section druid-cube-config">
                      {/* Show existing cube or prompt for new name */}
                      {loadedCubeName ? (
                        <div className="config-form-row">
                          <label className="config-form-label">
                            Using Cube
                          </label>
                          <div className="existing-cube-name">
                            <span className="cube-badge">📦</span>
                            <code>{loadedCubeName}</code>
                          </div>
                          <span className="config-form-hint">
                            Materialization will be added to this existing cube
                          </span>
                        </div>
                      ) : (
                        <div className="config-form-row">
                          <label className="config-form-label">Cube Name</label>
                          <div className="cube-name-input-group">
                            <input
                              type="text"
                              className="config-form-input namespace-input"
                              placeholder="users.myname"
                              value={configForm.druidCubeNamespace}
                              onChange={e =>
                                setConfigForm(prev => ({
                                  ...prev,
                                  druidCubeNamespace: e.target.value,
                                }))
                              }
                            />
                            <span className="namespace-separator">.</span>
                            <input
                              type="text"
                              className="config-form-input name-input"
                              placeholder="my_cube"
                              value={configForm.druidCubeName}
                              onChange={e =>
                                setConfigForm(prev => ({
                                  ...prev,
                                  druidCubeName: e.target.value,
                                }))
                              }
                            />
                          </div>
                          <span className="config-form-hint">
                            Full name:{' '}
                            <code>
                              {configForm.druidCubeNamespace}.
                              {configForm.druidCubeName || 'my_cube'}
                            </code>
                          </span>
                        </div>
                      )}

                      {/* Preview of what will be combined */}
                      <div className="druid-cube-preview">
                        <div className="preview-label">
                          Pre-aggregations to combine:
                        </div>
                        <div className="preview-list">
                          {(measuresResult?.grain_groups || []).map((gg, i) => (
                            <div key={i} className="preview-item">
                              <span className="preview-source">
                                {gg.parent_name?.split('.').pop()}
                              </span>
                              <span className="preview-grain">
                                (
                                {(gg.grain || [])
                                  .map(g => g.split('.').pop())
                                  .join(', ')}
                                )
                              </span>
                            </div>
                          ))}
                        </div>
                        <div className="preview-info">
                          <span className="info-icon">ℹ️</span>
                          Pre-aggs will be combined with FULL OUTER JOIN on
                          shared dimensions and ingested to Druid
                        </div>
                      </div>
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
                          finalSchedule =
                            configForm._recommendedSchedule?.cron ||
                            '0 6 * * *';
                        } else if (configForm.scheduleType === 'hourly') {
                          finalSchedule = '0 * * * *';
                        }

                        // Compute backfill end date
                        let backfillEndDate = null;
                        if (configForm.strategy === 'incremental_time') {
                          backfillEndDate =
                            configForm.backfillTo === 'today'
                              ? new Date().toISOString().split('T')[0]
                              : configForm.backfillToDate;
                        }

                        // Build the config object for the API
                        const apiConfig = {
                          strategy: configForm.strategy,
                          schedule: finalSchedule, // Always set - we always create workflows
                          lookbackWindow:
                            configForm.strategy === 'incremental_time'
                              ? configForm.lookbackWindow
                              : null,
                          // Backfill info (only for incremental + runBackfill checked)
                          runBackfill: configForm.runBackfill,
                          backfillFrom:
                            configForm.strategy === 'incremental_time' &&
                            configForm.runBackfill
                              ? configForm.backfillFrom
                              : null,
                          backfillTo:
                            configForm.strategy === 'incremental_time' &&
                            configForm.runBackfill
                              ? backfillEndDate
                              : null,
                          // Druid cube config
                          enableDruidCube: configForm.enableDruidCube,
                          // Only send cube name if creating a new cube (no existing cube loaded)
                          // Combine namespace and name: "users.myname.my_cube"
                          druidCubeName:
                            configForm.enableDruidCube &&
                            configForm.druidCubeName
                              ? `${configForm.druidCubeNamespace}.${configForm.druidCubeName}`
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
                          enableDruidCube: true,
                          druidCubeNamespace: getDefaultNamespace(),
                          druidCubeName: '',
                        });
                      } catch (err) {
                        console.error('Failed to plan:', err);
                      }
                      setIsSaving(false);
                    }}
                  >
                    {isSaving ? (
                      <>
                        <span className="spinner" /> Creating...
                      </>
                    ) : configForm.enableDruidCube ? (
                      'Create Pre-Agg Workflows & Schedule Cube'
                    ) : configForm.strategy === 'incremental_time' &&
                      configForm.runBackfill ? (
                      'Create Workflow & Start Backfill'
                    ) : (
                      'Create Workflow'
                    )}
                  </button>
                </div>
              </div>
            )}
          </div>
        )}

      {/* Druid Cube Section - shown when cube is scheduled */}
      {workflowUrls.length > 0 && (
        <div className="details-section">
          <div className="section-header-row">
            <h3 className="section-title">
              <span className="section-icon cube-icon">◆</span>
              Druid Cube
            </h3>
          </div>

          <div className="preagg-summary-card cube-card">
            {editingCube ? (
              /* Edit Cube Config Form - matches pre-agg edit form exactly */
              <div className="materialization-config-form">
                <div className="config-form-header">
                  <span>Edit Materialization Config</span>
                  <button
                    className="config-close-btn"
                    type="button"
                    onClick={() => setEditingCube(false)}
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
                          name="cube-strategy"
                          value="full"
                          checked={cubeConfigForm.strategy === 'full'}
                          onChange={e =>
                            setCubeConfigForm(prev => ({
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
                          name="cube-strategy"
                          value="incremental_time"
                          checked={
                            cubeConfigForm.strategy === 'incremental_time'
                          }
                          onChange={e =>
                            setCubeConfigForm(prev => ({
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
                    <label className="config-form-label">Schedule (cron)</label>
                    <input
                      type="text"
                      className="config-form-input"
                      placeholder="0 6 * * * (daily at 6am)"
                      value={cubeConfigForm.schedule}
                      onChange={e =>
                        setCubeConfigForm(prev => ({
                          ...prev,
                          schedule: e.target.value,
                        }))
                      }
                    />
                  </div>
                  {cubeConfigForm.strategy === 'incremental_time' && (
                    <div className="config-form-row">
                      <label className="config-form-label">
                        Lookback Window
                      </label>
                      <input
                        type="text"
                        className="config-form-input"
                        placeholder="3 days"
                        value={cubeConfigForm.lookbackWindow}
                        onChange={e =>
                          setCubeConfigForm(prev => ({
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
                    onClick={() => setEditingCube(false)}
                  >
                    Cancel
                  </button>
                  <button
                    className="action-btn action-btn-primary"
                    type="button"
                    disabled={isSavingCube}
                    onClick={async () => {
                      setIsSavingCube(true);
                      try {
                        if (onUpdateCubeConfig) {
                          await onUpdateCubeConfig(cubeConfigForm);
                        }
                        setEditingCube(false);
                      } catch (err) {
                        console.error('Failed to save cube config:', err);
                      }
                      setIsSavingCube(false);
                    }}
                  >
                    {isSavingCube ? (
                      <>
                        <span className="spinner" /> Saving...
                      </>
                    ) : (
                      'Save'
                    )}
                  </button>
                </div>
              </div>
            ) : (
              /* Cube Summary View - matches pre-agg expandable pattern */
              <>
                {/* Card header with name */}
                <div className="preagg-summary-header">
                  <span className="preagg-summary-name cube-name">
                    {cubeMaterialization?.druidDatasource ||
                      (loadedCubeName
                        ? `dj__${loadedCubeName.replace(/\./g, '_')}`
                        : 'dj__cube')}
                  </span>
                  <span className="status-pill status-active">● Active</span>
                </div>

                {/* Clickable workflow status bar */}
                <div
                  className="materialization-header clickable"
                  onClick={() =>
                    setExpandedCards(prev => ({ ...prev, cube: !prev.cube }))
                  }
                >
                  <div className="materialization-status">
                    <span
                      className="status-indicator status-materialized"
                      style={{ color: '#059669' }}
                    >
                      ●
                    </span>
                    <span className="status-text">Workflow active</span>
                    {cubeMaterialization?.schedule && (
                      <>
                        <span className="status-separator">|</span>
                        <span className="schedule-summary">
                          {getScheduleSummary(cubeMaterialization.schedule)}
                        </span>
                      </>
                    )}
                  </div>
                  <button
                    className="expand-toggle"
                    type="button"
                    aria-label={expandedCards.cube ? 'Collapse' : 'Expand'}
                  >
                    {expandedCards.cube ? '▲' : '▼'}
                  </button>
                </div>

                {/* Expandable Details */}
                {expandedCards.cube && (
                  <div className="materialization-details">
                    <div className="materialization-config">
                      <div className="config-row">
                        <span className="config-label">Strategy:</span>
                        <span className="config-value">
                          {cubeMaterialization?.strategy === 'incremental_time'
                            ? 'Incremental (Time-based)'
                            : cubeMaterialization?.strategy === 'full'
                            ? 'Full'
                            : cubeMaterialization?.strategy || 'Not set'}
                        </span>
                      </div>
                      {cubeMaterialization?.schedule && (
                        <div className="config-row">
                          <span className="config-label">Schedule:</span>
                          <span className="config-value config-mono">
                            {cubeMaterialization.schedule}
                          </span>
                        </div>
                      )}
                      {cubeMaterialization?.lookbackWindow && (
                        <div className="config-row">
                          <span className="config-label">Lookback:</span>
                          <span className="config-value">
                            {cubeMaterialization.lookbackWindow}
                          </span>
                        </div>
                      )}
                      <div className="config-row">
                        <span className="config-label">Dependencies:</span>
                        <span className="config-value">
                          {cubeMaterialization?.preaggTables?.length ||
                            Object.keys(plannedPreaggs).length ||
                            grainGroups.length}{' '}
                          pre-agg(s)
                        </span>
                      </div>
                      {/* Workflow URLs */}
                      {workflowUrls.length > 0 && (
                        <div className="config-row">
                          <span className="config-label">Workflows:</span>
                          <div className="workflow-links">
                            {workflowUrls.map((wf, idx) => {
                              // Support both {label, url} objects and plain strings
                              const url = typeof wf === 'string' ? wf : wf.url;
                              const label =
                                typeof wf === 'string'
                                  ? wf.includes('adhoc') ||
                                    wf.includes('backfill')
                                    ? 'Backfill'
                                    : 'Scheduled'
                                  : wf.label || 'Workflow';
                              return (
                                <a
                                  key={idx}
                                  href={url}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  className="action-btn action-btn-secondary"
                                >
                                  {label === 'backfill'
                                    ? 'Backfill'
                                    : label === 'scheduled'
                                    ? 'Scheduled'
                                    : label}
                                </a>
                              );
                            })}
                          </div>
                        </div>
                      )}
                    </div>
                    {/* Action Buttons */}
                    <div className="materialization-actions">
                      {onUpdateCubeConfig && (
                        <button
                          className="action-btn action-btn-secondary"
                          type="button"
                          onClick={e => {
                            e.stopPropagation();
                            setCubeConfigForm({
                              strategy:
                                cubeMaterialization?.strategy ||
                                'incremental_time',
                              schedule:
                                cubeMaterialization?.schedule || '0 6 * * *',
                              lookbackWindow:
                                cubeMaterialization?.lookbackWindow || '1 DAY',
                            });
                            setEditingCube(true);
                          }}
                        >
                          Edit Config
                        </button>
                      )}

                      {onRefreshCubeWorkflow && (
                        <button
                          className="action-btn action-btn-secondary"
                          type="button"
                          title="Refresh workflow (re-push to scheduler)"
                          onClick={async e => {
                            e.stopPropagation();
                            setLoadingAction('refresh-cube');
                            try {
                              await onRefreshCubeWorkflow();
                            } finally {
                              setLoadingAction(null);
                            }
                          }}
                          disabled={loadingAction === 'refresh-cube'}
                        >
                          {loadingAction === 'refresh-cube' ? (
                            <>
                              <span className="spinner" /> Refreshing...
                            </>
                          ) : (
                            '↻ Refresh'
                          )}
                        </button>
                      )}

                      {onRunCubeBackfill && (
                        <button
                          className="action-btn action-btn-secondary"
                          type="button"
                          onClick={e => {
                            e.stopPropagation();
                            setCubeBackfillModal(true);
                          }}
                        >
                          Run Backfill
                        </button>
                      )}

                      {onDeactivateCubeWorkflow && (
                        <button
                          className="action-btn action-btn-danger"
                          type="button"
                          title="Deactivate this cube materialization"
                          onClick={async e => {
                            e.stopPropagation();
                            if (
                              window.confirm(
                                'Are you sure you want to deactivate this cube materialization? The workflow will be stopped.',
                              )
                            ) {
                              setLoadingAction('deactivate-cube');
                              try {
                                await onDeactivateCubeWorkflow();
                              } finally {
                                setLoadingAction(null);
                              }
                            }
                          }}
                          disabled={loadingAction === 'deactivate-cube'}
                        >
                          {loadingAction === 'deactivate-cube' ? (
                            <>
                              <span className="spinner" /> Deactivating...
                            </>
                          ) : (
                            '⏹ Deactivate'
                          )}
                        </button>
                      )}
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      )}

      {/* Pre-Aggregations Section */}
      <div className="details-section">
        <div className="section-header-row">
          <h3 className="section-title">
            <span className="section-icon">◫</span>
            Pre-Aggregations ({grainGroups.length})
          </h3>
        </div>

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

            // Determine status badge
            const isActive =
              existingPreagg?.strategy && existingPreagg?.schedule;
            const statusPillClass = isActive
              ? 'status-active'
              : 'status-not-set';
            const statusPillText = isActive ? '● Active' : '○ Not Set';

            return (
              <div key={i} className="preagg-summary-card">
                <div className="preagg-summary-header">
                  <span className="preagg-summary-name">{shortName}</span>
                  <span className={`status-pill ${statusPillClass}`}>
                    {statusPillText}
                  </span>
                </div>
                <div className="preagg-summary-details">
                  <div className="preagg-summary-row">
                    <span className="label">Grain:</span>
                    <span className="value">
                      {gg.grain?.join(', ') || 'None'}
                    </span>
                  </div>
                  {isActive && scheduleSummary && (
                    <div className="preagg-summary-row">
                      <span className="label">Schedule:</span>
                      <span className="value">{scheduleSummary}</span>
                    </div>
                  )}
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
                              await onUpdateConfig(
                                existingPreagg.id,
                                configForm,
                              );
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
                        {isSaving ? (
                          <>
                            <span className="spinner" /> Saving...
                          </>
                        ) : (
                          'Save'
                        )}
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
                              This query can use an existing pre-agg with finer
                              grain:
                              <strong>
                                {' '}
                                {existingPreagg.grain_columns
                                  ?.map(g => g.split('.').pop())
                                  .join(', ')}
                              </strong>
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
                                {existingPreagg.workflow_urls.map((wf, idx) => {
                                  // Support both {label, url} objects and plain strings
                                  const url =
                                    typeof wf === 'string' ? wf : wf.url;
                                  const label =
                                    typeof wf === 'string'
                                      ? wf.includes('backfill') ||
                                        wf.includes('adhoc')
                                        ? 'backfill'
                                        : 'scheduled'
                                      : wf.label || 'workflow';
                                  return (
                                    <a
                                      key={idx}
                                      href={url}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="action-btn action-btn-secondary"
                                    >
                                      {label === 'scheduled'
                                        ? 'Scheduled'
                                        : label === 'backfill'
                                        ? 'Backfill'
                                        : label}
                                    </a>
                                  );
                                })}
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
                              {(!existingPreagg.workflow_urls ||
                                existingPreagg.workflow_urls.length === 0) &&
                                onCreateWorkflow && (
                                  <button
                                    className="action-btn action-btn-secondary"
                                    type="button"
                                    disabled={
                                      loadingAction ===
                                      `workflow-${existingPreagg.id}`
                                    }
                                    onClick={async e => {
                                      e.stopPropagation();
                                      setLoadingAction(
                                        `workflow-${existingPreagg.id}`,
                                      );
                                      try {
                                        const result = await onCreateWorkflow(
                                          existingPreagg.id,
                                        );
                                        if (result?.workflow_urls?.length > 0) {
                                          const firstUrl =
                                            typeof result.workflow_urls[0] ===
                                            'string'
                                              ? result.workflow_urls[0]
                                              : result.workflow_urls[0].url;
                                          setToastMessage(
                                            `Workflow created: ${firstUrl}`,
                                          );
                                          setTimeout(
                                            () => setToastMessage(null),
                                            5000,
                                          );
                                        }
                                      } finally {
                                        setLoadingAction(null);
                                      }
                                    }}
                                  >
                                    {loadingAction ===
                                    `workflow-${existingPreagg.id}` ? (
                                      <>
                                        <span className="spinner" /> Creating...
                                      </>
                                    ) : (
                                      'Create Workflow'
                                    )}
                                  </button>
                                )}
                              {existingPreagg.workflow_urls?.length > 0 && (
                                <>
                                  <button
                                    className="action-btn action-btn-secondary"
                                    type="button"
                                    title="Refresh workflow (re-push to scheduler)"
                                    disabled={
                                      loadingAction ===
                                      `refresh-${existingPreagg.id}`
                                    }
                                    onClick={async e => {
                                      e.stopPropagation();
                                      setLoadingAction(
                                        `refresh-${existingPreagg.id}`,
                                      );
                                      try {
                                        // Re-push the workflow
                                        await onCreateWorkflow(
                                          existingPreagg.id,
                                          true,
                                        );
                                      } finally {
                                        setLoadingAction(null);
                                      }
                                    }}
                                  >
                                    {loadingAction ===
                                    `refresh-${existingPreagg.id}` ? (
                                      <>
                                        <span className="spinner" />{' '}
                                        Refreshing...
                                      </>
                                    ) : (
                                      '↻ Refresh'
                                    )}
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
                                const today = new Date()
                                  .toISOString()
                                  .split('T')[0];
                                const weekAgo = new Date(
                                  Date.now() - 7 * 24 * 60 * 60 * 1000,
                                )
                                  .toISOString()
                                  .split('T')[0];
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

                          {/* Deactivate button */}
                          {existingPreagg.workflow_urls?.length > 0 &&
                            onDeactivatePreaggWorkflow && (
                              <button
                                className="action-btn action-btn-danger"
                                type="button"
                                title="Deactivate (pause) this workflow"
                                onClick={async e => {
                                  e.stopPropagation();
                                  if (
                                    window.confirm(
                                      'Are you sure you want to deactivate this workflow? It will be paused and can be re-activated later.',
                                    )
                                  ) {
                                    setLoadingAction(
                                      `deactivate-${existingPreagg.id}`,
                                    );
                                    try {
                                      await onDeactivatePreaggWorkflow(
                                        existingPreagg.id,
                                      );
                                    } finally {
                                      setLoadingAction(null);
                                    }
                                  }
                                }}
                                disabled={
                                  loadingAction ===
                                  `deactivate-${existingPreagg.id}`
                                }
                              >
                                {loadingAction ===
                                `deactivate-${existingPreagg.id}` ? (
                                  <>
                                    <span className="spinner" /> Deactivating...
                                  </>
                                ) : (
                                  '⏹ Deactivate'
                                )}
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
                className={`sql-toggle-btn ${
                  sqlViewMode === 'optimized' ? 'active' : ''
                }`}
                onClick={() => handleSqlViewToggle('optimized')}
                type="button"
                title="SQL using pre-aggregations (when available)"
              >
                Optimized
              </button>
              <button
                className={`sql-toggle-btn ${
                  sqlViewMode === 'raw' ? 'active' : ''
                }`}
                onClick={() => handleSqlViewToggle('raw')}
                type="button"
                title="SQL computed directly from source tables"
                disabled={loadingRawSql}
              >
                {loadingRawSql ? '...' : 'Raw'}
              </button>
            </div>
          </div>

          {/* Scan Estimate Info */}
          {(() => {
            const currentScanEstimate =
              sqlViewMode === 'raw'
                ? rawResult?.scan_estimate
                : metricsResult?.scan_estimate;
            const scanInfo = formatScanEstimate(currentScanEstimate);

            if (scanInfo) {
              return (
                <div
                  className={`scan-estimate-banner scan-estimate-${scanInfo.level}`}
                >
                  <span className="scan-estimate-icon">{scanInfo.icon}</span>
                  <div className="scan-estimate-content">
                    <div className="scan-estimate-header">
                      <strong>Scan Cost:</strong>{' '}
                      {scanInfo.totalBytes !== null &&
                      scanInfo.totalBytes !== undefined
                        ? (scanInfo.hasMissingData ? '≥ ' : '') +
                          formatBytes(scanInfo.totalBytes)
                        : 'Unknown'}
                    </div>
                    <div className="scan-estimate-sources">
                      {scanInfo.sources.map((source, idx) => {
                        // Prefer schema.table display, fall back to node name
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
              );
            }
            return null;
          })()}

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
              {sqlViewMode === 'raw' ? rawResult?.sql || 'Loading...' : sql}
            </SyntaxHighlighter>
          </div>
        </div>
      )}

      {/* Metrics & Dimensions Summary - Two columns */}
      <div className="details-section">
        <div className="section-header-row">
          <h3 className="section-title">
            <span className="section-icon">◈</span>
            Selection Summary
          </h3>
        </div>
        <div className="selection-summary-grid">
          {/* Metrics Column */}
          <div className="selection-summary-column">
            <div className="selection-summary-label">
              Metrics ({metricFormulas.length})
            </div>
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
            <div className="selection-summary-label">
              Dimensions ({selectedDimensions.length})
            </div>
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

      {/* Backfill Modal */}
      {backfillModal && (
        <div
          className="backfill-modal-overlay"
          onClick={() => setBackfillModal(null)}
        >
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
                          <a
                            href={result.job_url}
                            target="_blank"
                            rel="noopener noreferrer"
                          >
                            View Job ↗
                          </a>
                        </span>,
                      );
                      setTimeout(() => setToastMessage(null), 10000);
                    }
                  } finally {
                    setLoadingAction(null);
                  }
                }}
              >
                {loadingAction === 'backfill-modal' ? (
                  <>
                    <span className="spinner" /> Starting...
                  </>
                ) : (
                  'Start Backfill'
                )}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Cube Backfill Modal */}
      {cubeBackfillModal && (
        <CubeBackfillModal
          onClose={() => setCubeBackfillModal(false)}
          onSubmit={async (startDate, endDate) => {
            setLoadingAction('cube-backfill');
            try {
              const result = await onRunCubeBackfill(startDate, endDate);
              setCubeBackfillModal(false);
              if (result?.job_url) {
                setToastMessage(
                  <span>
                    Backfill started:{' '}
                    <a
                      href={result.job_url}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      View Job ↗
                    </a>
                  </span>,
                );
                setTimeout(() => setToastMessage(null), 10000);
              }
            } finally {
              setLoadingAction(null);
            }
          }}
          loading={loadingAction === 'cube-backfill'}
        />
      )}

      {/* Toast Message */}
      {toastMessage && (
        <div className="toast-message">
          {toastMessage}
          <button className="toast-close" onClick={() => setToastMessage(null)}>
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
export function PreAggDetailsPanel({
  preAgg,
  metricFormulas,
  onClose,
  highlightedComponent,
}) {
  const componentsSectionRef = useRef(null);

  // Scroll to and highlight component when highlightedComponent changes
  useEffect(() => {
    if (highlightedComponent && componentsSectionRef.current) {
      // Scroll the components section into view
      componentsSectionRef.current.scrollIntoView({
        behavior: 'smooth',
        block: 'start',
      });
    }
  }, [highlightedComponent]);

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

      {/* Components Table */}
      <div
        className="details-section details-section-full"
        ref={componentsSectionRef}
      >
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
                <tr
                  key={comp.name || i}
                  className={
                    highlightedComponent === comp.name
                      ? 'component-row-highlighted'
                      : ''
                  }
                >
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

      {/* Definition */}
      <div className="details-section">
        <h3 className="section-title">
          <span className="section-icon">⌘</span>
          Definition
        </h3>
        <div className="formula-display">
          <code>{metric.query}</code>
        </div>
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

// Export helper functions for testing
export {
  getDimensionNodeName,
  normalizeGrain,
  getScheduleSummary,
  getStatusInfo,
  inferGranularity,
  formatBytes,
  getScanWarningLevel,
  formatScanEstimate,
};

export default PreAggDetailsPanel;
