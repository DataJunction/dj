import { useContext, useEffect, useState, useCallback, useRef } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import DJClientContext from '../../providers/djclient';
import MetricFlowGraph from './MetricFlowGraph';
import SelectionPanel from './SelectionPanel';
import {
  PreAggDetailsPanel,
  MetricDetailsPanel,
  QueryOverviewPanel,
} from './PreAggDetailsPanel';
import './styles.css';

/**
 * Helper to normalize grain columns to short names for comparison
 * "default.date_dim.date_id" -> "date_id"
 */
function normalizeGrain(grainCols) {
  return (grainCols || [])
    .map(col => col.split('.').pop())
    .sort()
    .join(',');
}

export function QueryPlannerPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const location = useLocation();
  const navigate = useNavigate();

  // Available options
  const [metrics, setMetrics] = useState([]);
  const [commonDimensions, setCommonDimensions] = useState([]);
  const [cubes, setCubes] = useState([]);

  // Selection state - initialized from URL params
  const [selectedMetrics, setSelectedMetrics] = useState([]);
  const [selectedDimensions, setSelectedDimensions] = useState([]);
  const [loadedCubeName, setLoadedCubeName] = useState(null); // Track loaded cube preset

  // Track if we've initialized from URL (to avoid overwriting URL on first render)
  const initializedFromUrl = useRef(false);
  const pendingDimensionsFromUrl = useRef([]);
  const pendingCubeFromUrl = useRef(null);

  // Results state
  const [measuresResult, setMeasuresResult] = useState(null);
  const [metricsResult, setMetricsResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [dimensionsLoading, setDimensionsLoading] = useState(false);
  const [error, setError] = useState(null);

  // Node selection for details panel
  const [selectedNode, setSelectedNode] = useState(null);

  // Materialization state - map of grain_key -> pre-agg info
  const [plannedPreaggs, setPlannedPreaggs] = useState({});

  // Materialization error state
  const [materializationError, setMaterializationError] = useState(null);

  // Initialize selection from URL params on mount
  useEffect(() => {
    const params = new URLSearchParams(location.search);
    const urlMetrics = params.get('metrics')?.split(',').filter(Boolean) || [];
    const urlDimensions =
      params.get('dimensions')?.split(',').filter(Boolean) || [];
    const urlCube = params.get('cube');

    if (urlMetrics.length > 0) {
      setSelectedMetrics(urlMetrics);
      // Store dimensions to apply after commonDimensions are loaded
      if (urlDimensions.length > 0) {
        pendingDimensionsFromUrl.current = urlDimensions;
      }
      // Store cube name - will be set after cube data is loaded
      if (urlCube) {
        pendingCubeFromUrl.current = urlCube;
        // Don't set loadedCubeName here - wait until cube data loads
      }
      initializedFromUrl.current = true;
    } else if (urlCube) {
      // Cube specified without metrics - will load cube on mount
      pendingCubeFromUrl.current = urlCube;
      initializedFromUrl.current = true;
    }
  }, []); // Only run on mount

  // Update URL when selection changes
  useEffect(() => {
    // Skip the first render if we just initialized from URL
    if (!initializedFromUrl.current && selectedMetrics.length === 0 && !loadedCubeName) {
      return;
    }

    const params = new URLSearchParams();
    if (loadedCubeName) {
      params.set('cube', loadedCubeName);
    }
    if (selectedMetrics.length > 0) {
      params.set('metrics', selectedMetrics.join(','));
    }
    if (selectedDimensions.length > 0) {
      params.set('dimensions', selectedDimensions.join(','));
    }

    const newSearch = params.toString();
    const currentSearch = location.search.replace(/^\?/, '');

    // Only update if different (avoid unnecessary history entries)
    if (newSearch !== currentSearch) {
      navigate(
        {
          pathname: location.pathname,
          search: newSearch ? `?${newSearch}` : '',
        },
        { replace: true },
      );
    }
  }, [selectedMetrics, selectedDimensions, loadedCubeName, location.pathname, navigate]);

  // Get metrics list and cube names on mount
  // Uses GraphQL for lightweight cube listing with display names
  useEffect(() => {
    const fetchData = async () => {
      const [metricsList, cubesList] = await Promise.all([
        djClient.metrics(),
        djClient.listCubesForPreset().catch(() => []),
      ]);
      setMetrics(metricsList);
      // cubesList returns [{name, display_name}] from GraphQL
      setCubes(cubesList);

      // If there's a pending cube from URL, load it now
      if (pendingCubeFromUrl.current) {
        const cubeName = pendingCubeFromUrl.current;
        pendingCubeFromUrl.current = null; // Clear to prevent re-loading
        try {
          const cubeData = await djClient.cube(cubeName);
          // Validate cube data has expected fields
          if (cubeData && Array.isArray(cubeData.cube_node_metrics)) {
            const cubeMetrics = cubeData.cube_node_metrics || [];
            const cubeDimensions = cubeData.cube_node_dimensions || [];
            setLoadedCubeName(cubeName);
            setSelectedMetrics(cubeMetrics);
            pendingDimensionsFromUrl.current = cubeDimensions;
          } else {
            console.error('Invalid cube data from URL:', cubeData);
          }
        } catch (err) {
          console.error('Failed to load cube from URL:', err);
        }
      }
    };
    fetchData().catch(console.error);
  }, [djClient]);

  // Get common dimensions when metrics change
  useEffect(() => {
    const fetchData = async () => {
      if (selectedMetrics.length > 0) {
        setDimensionsLoading(true);
        try {
          const dims = await djClient.commonDimensions(selectedMetrics);
          setCommonDimensions(dims);

          // Apply pending dimensions from URL if we have them
          if (pendingDimensionsFromUrl.current.length > 0) {
            const validDimNames = dims.map(d => d.name);
            const validPending = pendingDimensionsFromUrl.current.filter(d =>
              validDimNames.includes(d),
            );
            if (validPending.length > 0) {
              setSelectedDimensions(validPending);
            }
            pendingDimensionsFromUrl.current = []; // Clear after applying
          }
        } catch (err) {
          console.error('Failed to fetch dimensions:', err);
          setCommonDimensions([]);
        }
        setDimensionsLoading(false);
      } else {
        setCommonDimensions([]);
        setSelectedDimensions([]);
      }
    };
    fetchData().catch(console.error);
  }, [selectedMetrics, djClient]);

  // Clear dimension selections that are no longer valid
  useEffect(() => {
    const validDimNames = commonDimensions.map(d => d.name);
    const validSelections = selectedDimensions.filter(d =>
      validDimNames.includes(d),
    );
    if (validSelections.length !== selectedDimensions.length) {
      setSelectedDimensions(validSelections);
    }
  }, [commonDimensions, selectedDimensions]);

  // Fetch V3 measures and metrics SQL when selection changes
  useEffect(() => {
    const fetchData = async () => {
      if (selectedMetrics.length > 0 && selectedDimensions.length > 0) {
        setLoading(true);
        setError(null);
        setSelectedNode(null);
        try {
          // Fetch both measures and metrics SQL in parallel
          const [measures, metrics] = await Promise.all([
            djClient.measuresV3(selectedMetrics, selectedDimensions),
            djClient.metricsV3(selectedMetrics, selectedDimensions),
          ]);
          setMeasuresResult(measures);
          setMetricsResult(metrics);
        } catch (err) {
          setError(err.message || 'Failed to fetch data');
          setMeasuresResult(null);
          setMetricsResult(null);
        }
        setLoading(false);
      } else {
        setMeasuresResult(null);
        setMetricsResult(null);
      }
    };
    fetchData().catch(console.error);
  }, [djClient, selectedMetrics, selectedDimensions]);

  // Fetch existing pre-aggregations for the grain groups
  useEffect(() => {
    const fetchExistingPreaggs = async () => {
      if (!measuresResult?.grain_groups?.length) {
        setPlannedPreaggs({});
        return;
      }

      // Get unique node names from grain groups
      try {
        // For each grain group, ask server to find matching pre-aggs
        const newPreaggs = {};

        await Promise.all(
          measuresResult.grain_groups.map(async gg => {
            const grainKey = `${gg.parent_name}|${normalizeGrain(gg.grain)}`;

            // Look up full dimension names (semantic_entity) from columns
            const grainCols = gg.grain
              .map(grainName => {
                const col = (gg.columns || []).find(c => c.name === grainName);
                return col?.semantic_entity || grainName;
              })
              .join(',');
            console.log('grainCols (from semantic_entity):', grainCols);

            // Extract measure names
            const measureNames = (gg.components || [])
              .map(c => c.name)
              .filter(Boolean)
              .join(',');

            // First try exact match
            let result = await djClient.listPreaggs({
              node_name: gg.parent_name,
              grain: grainCols,
              grain_mode: 'exact',
              measures: measureNames || undefined,
            });

            let preaggs = result.items || result.pre_aggregations || [];
            let match = preaggs[0];

            // If no exact match, try superset (finer grain)
            if (!match) {
              result = await djClient.listPreaggs({
                node_name: gg.parent_name,
                grain: grainCols,
                grain_mode: 'superset',
                measures: measureNames || undefined,
              });
              preaggs = result.items || result.pre_aggregations || [];
              match = preaggs[0];

              // Mark as compatible (not exact match)
              if (match) {
                match = { ...match, _isCompatible: true };
              }
            }

            if (match) {
              newPreaggs[grainKey] = match;
            }
          }),
        );

        setPlannedPreaggs(newPreaggs);
      } catch (err) {
        console.error('Failed to fetch existing pre-aggs:', err);
      }
    };

    fetchExistingPreaggs();
  }, [measuresResult, djClient]);

  const handleMetricsChange = useCallback(newMetrics => {
    setSelectedMetrics(newMetrics);
    setSelectedNode(null);
  }, []);

  // Load a cube preset - sets both metrics and dimensions from the cube definition
  const handleLoadCubePreset = useCallback(
    async cubeName => {
      if (!cubeName) return;

      try {
        const cubeData = await djClient.cube(cubeName);
        // Validate cube data has expected fields
        if (cubeData && Array.isArray(cubeData.cube_node_metrics)) {
          // Extract metrics and dimensions from the cube
          const cubeMetrics = cubeData.cube_node_metrics || [];
          const cubeDimensions = cubeData.cube_node_dimensions || [];

          // Set the cube name for URL and display
          setLoadedCubeName(cubeName);
          // Set the metrics first - dimensions will be loaded and filtered via the effect
          setSelectedMetrics(cubeMetrics);
          // Store dimensions to apply after common dimensions are loaded
          pendingDimensionsFromUrl.current = cubeDimensions;
          setSelectedNode(null);
        } else {
          console.error('Invalid cube data received:', cubeData);
        }
      } catch (err) {
        console.error('Failed to load cube preset:', err);
      }
    },
    [djClient],
  );

  // Clear cube preset when selection is manually cleared
  const handleClearSelection = useCallback(() => {
    setSelectedMetrics([]);
    setSelectedDimensions([]);
    setLoadedCubeName(null);
  }, []);

  const handleDimensionsChange = useCallback(newDimensions => {
    setSelectedDimensions(newDimensions);
    setSelectedNode(null);
  }, []);

  const handleNodeSelect = useCallback(node => {
    setSelectedNode(node);
  }, []);

  const handleClosePanel = useCallback(() => {
    setSelectedNode(null);
  }, []);

  // Handle planning/saving a new materialization configuration
  // Note: This creates pre-aggs for ALL grain groups with the same settings
  const handlePlanMaterialization = useCallback(
    async (grainGroup, config) => {
      setMaterializationError(null); // Clear any previous error

      try {
        // Step 1: Create the pre-agg records with config
        const result = await djClient.planPreaggs(
          selectedMetrics,
          selectedDimensions,
          config.strategy,
          config.schedule,
          config.lookbackWindow,
        );

        // Check for error in response
        if (result._error || result.message || result.detail) {
          const errorMsg =
            result.message ||
            result.detail ||
            'Failed to plan pre-aggregations';
          setMaterializationError(errorMsg);
          throw new Error(errorMsg);
        }

        // Get the created pre-aggs (API returns `preaggs`, list endpoint returns `items`)
        const preaggs =
          result.preaggs || result.items || result.pre_aggregations || [];

        // Update local state
        const newPreaggs = { ...plannedPreaggs };
        preaggs.forEach(preagg => {
          const grainKey = `${preagg.node_name}|${normalizeGrain(
            preagg.grain_columns,
          )}`;
          newPreaggs[grainKey] = preagg;
        });
        setPlannedPreaggs(newPreaggs);

        // Step 2: Create scheduled workflows and optionally run backfills
        if (preaggs.length > 0 && config.schedule) {
          const workflowPromises = [];
          const backfillPromises = [];

          for (const preagg of preaggs) {
            // Always create the scheduled workflow when a schedule is provided
            workflowPromises.push(
              djClient.materializePreagg(preagg.id).catch(err => {
                console.error(
                  `Failed to create workflow for preagg ${preagg.id}:`,
                  err,
                );
                return null;
              }),
            );
          }

          // First: Wait for all workflows to be created
          const workflowResults = await Promise.all(workflowPromises);

          // Second: Only after workflows are created, start backfills
          for (const preagg of preaggs) {
            if (
              config.runBackfill &&
              config.backfillFrom &&
              config.backfillTo
            ) {
              backfillPromises.push(
                djClient
                  .runPreaggBackfill(
                    preagg.id,
                    config.backfillFrom,
                    config.backfillTo,
                  )
                  .catch(err => {
                    console.error(
                      `Failed to run backfill for preagg ${preagg.id}:`,
                      err,
                    );
                    return null;
                  }),
              );
            }
          }

          // Wait for all backfills to complete
          const backfillResults = await Promise.all(backfillPromises);

          // Update state with workflow URLs
          const updatedPreaggs = { ...newPreaggs };
          preaggs.forEach((preagg, idx) => {
            const grainKey = `${preagg.node_name}|${normalizeGrain(
              preagg.grain_columns,
            )}`;
            const workflowResult = workflowResults[idx];
            if (workflowResult?.workflow_url) {
              updatedPreaggs[grainKey] = {
                ...updatedPreaggs[grainKey],
                scheduled_workflow_url: workflowResult.workflow_url,
                workflow_status: workflowResult.status,
              };
            }
          });
          setPlannedPreaggs(updatedPreaggs);

          // Show toast with backfill info
          const successfulBackfills = backfillResults.filter(r => r?.job_url);
          if (successfulBackfills.length > 0) {
            console.log('Backfills started:', successfulBackfills);
          }
        }

        return result;
      } catch (err) {
        console.error('Failed to plan materialization:', err);
        const errorMsg = err.message || 'Failed to plan materialization';
        setMaterializationError(errorMsg);
        throw err;
      }
    },
    [djClient, selectedMetrics, selectedDimensions, plannedPreaggs],
  );

  // Handle updating config for a single existing pre-agg
  const handleUpdateConfig = useCallback(
    async (preaggId, config) => {
      setMaterializationError(null);
      try {
        const result = await djClient.updatePreaggConfig(
          preaggId,
          config.strategy,
          config.schedule,
          config.lookbackWindow,
        );

        if (result._error || result.message || result.detail) {
          const errorMsg =
            result.message || result.detail || 'Failed to update config';
          setMaterializationError(errorMsg);
          throw new Error(errorMsg);
        }

        // Update the specific pre-agg in our state
        setPlannedPreaggs(prev => {
          const updated = { ...prev };
          for (const key in updated) {
            if (updated[key].id === preaggId) {
              updated[key] = { ...updated[key], ...result };
              break;
            }
          }
          return updated;
        });

        return result;
      } catch (err) {
        console.error('Failed to update config:', err);
        const errorMsg = err.message || 'Failed to update config';
        setMaterializationError(errorMsg);
        throw err;
      }
    },
    [djClient],
  );

  // Handle creating/refreshing a scheduled workflow for a pre-agg
  const handleCreateWorkflow = useCallback(
    async preaggId => {
      setMaterializationError(null);
      try {
        const result = await djClient.materializePreagg(preaggId);

        if (result._error || result.message || result.detail) {
          const errorMsg =
            result.message || result.detail || 'Failed to create workflow';
          setMaterializationError(errorMsg);
          return null;
        }

        // Update the pre-agg with workflow info
        setPlannedPreaggs(prev => {
          const updated = { ...prev };
          for (const key in updated) {
            if (updated[key].id === preaggId) {
              updated[key] = {
                ...updated[key],
                scheduled_workflow_url: result.scheduled_workflow_url,
                workflow_status: result.workflow_status,
              };
              break;
            }
          }
          return updated;
        });

        return result;
      } catch (err) {
        console.error('Failed to create workflow:', err);
        setMaterializationError(err.message || 'Failed to create workflow');
        return null;
      }
    },
    [djClient],
  );

  // Handle running a backfill for a pre-agg
  const handleRunBackfill = useCallback(
    async (preaggId, startDate, endDate) => {
      setMaterializationError(null);
      try {
        const result = await djClient.runPreaggBackfill(
          preaggId,
          startDate,
          endDate,
        );

        if (result._error || result.message || result.detail) {
          const errorMsg =
            result.message || result.detail || 'Failed to run backfill';
          setMaterializationError(errorMsg);
          return null;
        }

        // Return the job URL so the UI can display it
        return result;
      } catch (err) {
        console.error('Failed to run backfill:', err);
        setMaterializationError(err.message || 'Failed to run backfill');
        return null;
      }
    },
    [djClient],
  );

  // Handle running an ad-hoc job for a pre-agg (uses backfill with same start/end date)
  const handleRunAdhoc = useCallback(
    async (preaggId, partitionDate) => {
      setMaterializationError(null);
      try {
        // Use backfill endpoint with same start and end date for single-date runs
        const result = await djClient.runPreaggBackfill(
          preaggId,
          partitionDate,
          partitionDate,
        );

        if (result._error || result.message || result.detail) {
          const errorMsg =
            result.message || result.detail || 'Failed to run ad-hoc job';
          setMaterializationError(errorMsg);
          return null;
        }

        // Return the job URL so the UI can display it
        return result;
      } catch (err) {
        console.error('Failed to run ad-hoc job:', err);
        setMaterializationError(err.message || 'Failed to run ad-hoc job');
        return null;
      }
    },
    [djClient],
  );

  // Fetch raw SQL (without pre-aggregations)
  const handleFetchRawSql = useCallback(async () => {
    try {
      const result = await djClient.metricsV3(
        selectedMetrics,
        selectedDimensions,
        '',
        false, // use_materialized = false for raw SQL
      );
      return result.sql;
    } catch (err) {
      console.error('Failed to fetch raw SQL:', err);
      return null;
    }
  }, [djClient, selectedMetrics, selectedDimensions]);

  return (
    <div className="planner-page">
      {/* Header */}
      <header className="planner-header">
        <div className="planner-header-content">
          <h1>Query Planner</h1>
          {/* <p>Explore metrics and dimensions and plan materializations</p> */}
        </div>
        {error && <div className="header-error">{error}</div>}
      </header>

      {/* Three-column layout */}
      <div className="planner-layout">
        {/* Left: Selection Panel */}
        <aside className="planner-selection">
          <SelectionPanel
            metrics={metrics}
            selectedMetrics={selectedMetrics}
            onMetricsChange={handleMetricsChange}
            dimensions={commonDimensions}
            selectedDimensions={selectedDimensions}
            onDimensionsChange={handleDimensionsChange}
            loading={dimensionsLoading}
            cubes={cubes}
            onLoadCubePreset={handleLoadCubePreset}
            loadedCubeName={loadedCubeName}
            onClearSelection={handleClearSelection}
          />
        </aside>

        {/* Center: Graph */}
        <main className="planner-graph">
          {loading ? (
            <div className="graph-loading">
              <div className="loading-spinner" />
              <span>Building data flow...</span>
            </div>
          ) : measuresResult ? (
            <>
              <div className="graph-header">
                <span className="graph-stats">
                  {measuresResult.grain_groups?.length || 0} pre-aggregations →{' '}
                  {measuresResult.metric_formulas?.length || 0} metrics
                </span>
              </div>
              <MetricFlowGraph
                grainGroups={measuresResult.grain_groups}
                metricFormulas={measuresResult.metric_formulas}
                selectedNode={selectedNode}
                onNodeSelect={handleNodeSelect}
              />
            </>
          ) : (
            <div className="graph-empty">
              <div className="empty-icon">⊞</div>
              <h3>Select Metrics & Dimensions</h3>
              <p>
                Choose metrics from the left panel, then select dimensions to
                see how they decompose into pre-aggregations.
              </p>
            </div>
          )}
        </main>

        {/* Right: Details Panel */}
        <aside className="planner-details">
          {selectedNode?.type === 'preagg' ? (
            <PreAggDetailsPanel
              preAgg={selectedNode.data}
              metricFormulas={measuresResult?.metric_formulas}
              onClose={handleClosePanel}
            />
          ) : selectedNode?.type === 'metric' ? (
            <MetricDetailsPanel
              metric={selectedNode.data}
              grainGroups={measuresResult?.grain_groups}
              onClose={handleClosePanel}
            />
          ) : (
            <QueryOverviewPanel
              measuresResult={measuresResult}
              metricsResult={metricsResult}
              selectedMetrics={selectedMetrics}
              selectedDimensions={selectedDimensions}
              plannedPreaggs={plannedPreaggs}
              onPlanMaterialization={handlePlanMaterialization}
              onUpdateConfig={handleUpdateConfig}
              onCreateWorkflow={handleCreateWorkflow}
              onRunBackfill={handleRunBackfill}
              onRunAdhoc={handleRunAdhoc}
              onFetchRawSql={handleFetchRawSql}
              materializationError={materializationError}
              onClearError={() => setMaterializationError(null)}
            />
          )}
        </aside>
      </div>
    </div>
  );
}

export default QueryPlannerPage;
