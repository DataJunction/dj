import {
  useContext,
  useEffect,
  useState,
  useCallback,
  useRef,
  lazy,
  Suspense,
} from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import DJClientContext from '../../providers/djclient';
import SelectionPanel from './SelectionPanel';
import {
  PreAggDetailsPanel,
  MetricDetailsPanel,
  QueryOverviewPanel,
} from './PreAggDetailsPanel';
import ResultsView from './ResultsView';
import './styles.css';

// Lazy load the graph component - ReactFlow and dagre are heavy (~500KB)
// This allows API calls to start immediately while the graph loads in parallel
const MetricFlowGraph = lazy(() => import('./MetricFlowGraph'));

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

  // Query execution state
  const [showResults, setShowResults] = useState(false);
  const [queryResults, setQueryResults] = useState(null);
  const [queryLoading, setQueryLoading] = useState(false);
  const [queryError, setQueryError] = useState(null);
  const [queryStartTime, setQueryStartTime] = useState(null);
  const [queryElapsedTime, setQueryElapsedTime] = useState(null);

  // Filters state
  const [filters, setFilters] = useState([]);

  // Cube availability state (for displaying freshness info)
  const [cubeAvailability, setCubeAvailability] = useState(null);

  // Node selection for details panel
  const [selectedNode, setSelectedNode] = useState(null);

  // Materialization state - map of grain_key -> pre-agg info
  const [plannedPreaggs, setPlannedPreaggs] = useState({});

  // Materialization error state
  const [materializationError, setMaterializationError] = useState(null);

  // Workflow URLs from successful cube materialization
  const [workflowUrls, setWorkflowUrls] = useState([]);

  // Full cube materialization info (for edit/refresh/backfill)
  const [cubeMaterialization, setCubeMaterialization] = useState(null);

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
    if (
      !initializedFromUrl.current &&
      selectedMetrics.length === 0 &&
      !loadedCubeName
    ) {
      return;
    }

    const params = new URLSearchParams();
    if (loadedCubeName) {
      // Cube is loaded - only include cube name (metrics/dims come from cube definition)
      params.set('cube', loadedCubeName);
    } else {
      // No cube - include metrics and dimensions
      if (selectedMetrics.length > 0) {
        params.set('metrics', selectedMetrics.join(','));
      }
      if (selectedDimensions.length > 0) {
        params.set('dimensions', selectedDimensions.join(','));
      }
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
  }, [
    selectedMetrics,
    selectedDimensions,
    loadedCubeName,
    location.pathname,
    navigate,
  ]);

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
          // Use lightweight GraphQL query - much faster than REST endpoint
          const cubeData = await djClient.cubeForPlanner(cubeName);
          // Validate cube data has expected fields
          if (cubeData && Array.isArray(cubeData.cube_node_metrics)) {
            const cubeMetrics = cubeData.cube_node_metrics || [];
            const cubeDimensions = cubeData.cube_node_dimensions || [];
            setLoadedCubeName(cubeName);
            setSelectedMetrics(cubeMetrics);
            pendingDimensionsFromUrl.current = cubeDimensions;

            // Materialization info is included in the GraphQL response
            const cubeMat = cubeData.cubeMaterialization;
            setCubeMaterialization(cubeMat || null);
            setWorkflowUrls(cubeMat?.workflowUrls || []);
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

        // Use requested_dimensions from the measures result - these are the fully qualified
        // dimension references that pre-aggs are created with
        const requestedDims = measuresResult.requested_dimensions || [];
        const grainColsForLookup = requestedDims.join(',');
        console.log(
          'grainCols for pre-agg lookup (from requested_dimensions):',
          grainColsForLookup,
        );

        await Promise.all(
          measuresResult.grain_groups.map(async gg => {
            const grainKey = `${gg.parent_name}|${normalizeGrain(gg.grain)}`;

            // Extract measure names
            const measureNames = (gg.components || [])
              .map(c => c.name)
              .filter(Boolean)
              .join(',');

            console.log(
              `Looking for pre-agg: node=${gg.parent_name}, grain=${grainColsForLookup}, measures=${measureNames}`,
            );

            // First try exact match using requested_dimensions
            let result = await djClient.listPreaggs({
              node_name: gg.parent_name,
              grain: grainColsForLookup,
              grain_mode: 'exact',
              measures: measureNames || undefined,
            });

            console.log(`Exact match result for ${gg.parent_name}:`, result);

            let preaggs = result.items || result.pre_aggregations || [];
            let match = preaggs[0];

            // If no exact match, try superset (finer grain)
            if (!match) {
              result = await djClient.listPreaggs({
                node_name: gg.parent_name,
                grain: grainColsForLookup,
                grain_mode: 'superset',
                measures: measureNames || undefined,
              });
              console.log(
                `Superset match result for ${gg.parent_name}:`,
                result,
              );
              preaggs = result.items || result.pre_aggregations || [];
              match = preaggs[0];

              // Mark as compatible (not exact match)
              if (match) {
                match = { ...match, _isCompatible: true };
              }
            }

            if (match) {
              console.log(`Found pre-agg match for ${gg.parent_name}:`, match);
              newPreaggs[grainKey] = match;
            } else {
              console.log(`No pre-agg match found for ${gg.parent_name}`);
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

  // Auto-detect cube info when metricsResult has cube_name (backend found a matching cube)
  // This effect manages cubeAvailability, cubeMaterialization, and workflowUrls based on
  // either: 1) explicit user selection (loadedCubeName), or 2) auto-detected cube (metricsResult.cube_name)
  useEffect(() => {
    const fetchCubeInfo = async () => {
      // Determine which cube to use: explicit selection takes precedence over auto-detection
      const cubeName = loadedCubeName || metricsResult?.cube_name;

      if (!cubeName) {
        // No cube - clear state
        setCubeAvailability(null);
        setCubeMaterialization(null);
        setWorkflowUrls([]);
        return;
      }

      try {
        // Fetch full cube info including materialization
        const cubeData = await djClient.cubeForPlanner(cubeName);
        setCubeAvailability(cubeData?.availability || null);

        if (cubeData) {
          const cubeMat = cubeData.cubeMaterialization;
          setCubeMaterialization(cubeMat || null);
          setWorkflowUrls(cubeMat?.workflowUrls || []);
        } else {
          setCubeMaterialization(null);
          setWorkflowUrls([]);
        }
      } catch (err) {
        console.error('Failed to fetch cube info:', err);
        setCubeAvailability(null);
        setCubeMaterialization(null);
        setWorkflowUrls([]);
      }
    };

    fetchCubeInfo();
  }, [metricsResult?.cube_name, loadedCubeName, djClient]);

  const handleMetricsChange = useCallback(newMetrics => {
    setSelectedMetrics(newMetrics);
    setSelectedNode(null);
    // Clear loaded cube name to indicate user is manually changing selection
    // (workflowUrls and cubeMaterialization will be updated by the effect when metricsResult changes)
    setLoadedCubeName(null);
  }, []);

  // Load a cube preset - sets both metrics and dimensions from the cube definition
  const handleLoadCubePreset = useCallback(
    async cubeName => {
      if (!cubeName) return;

      try {
        // Use lightweight GraphQL query - much faster than REST endpoint
        const cubeData = await djClient.cubeForPlanner(cubeName);
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

          // Materialization and availability info is included in the GraphQL response
          setCubeAvailability(cubeData.availability || null);
          const cubeMat = cubeData.cubeMaterialization;
          setCubeMaterialization(cubeMat || null);
          setWorkflowUrls(cubeMat?.workflowUrls || []);
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
    setWorkflowUrls([]);
    setCubeMaterialization(null);
  }, []);

  const handleDimensionsChange = useCallback(newDimensions => {
    setSelectedDimensions(newDimensions);
    setSelectedNode(null);
    // Clear loaded cube name to indicate user is manually changing selection
    // (workflowUrls and cubeMaterialization will be updated by the effect when metricsResult changes)
    setLoadedCubeName(null);
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
            if (workflowResult?.workflow_urls?.length > 0) {
              updatedPreaggs[grainKey] = {
                ...updatedPreaggs[grainKey],
                workflow_urls: workflowResult.workflow_urls,
                workflow_status: workflowResult.workflow_status || 'active',
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

        // Step 3: If Druid cube enabled, schedule cube materialization
        console.log(
          'Scheduling Druid cube materialization for:',
          config.enableDruidCube,
        );
        if (config.enableDruidCube) {
          // Use existing cube if loaded, otherwise use the new cube name from config
          const cubeName = loadedCubeName || config.druidCubeName;

          if (!cubeName) {
            const errorMsg = 'Cube name is required for Druid materialization';
            setMaterializationError(errorMsg);
            throw new Error(errorMsg); // Throw so form doesn't close
          }

          // Continue with cube creation/materialization
          console.log('Scheduling Druid cube materialization for:', cubeName);

          // If no existing cube, create one first
          if (!loadedCubeName && config.druidCubeName) {
            const cubeResult = await djClient.createCube(
              config.druidCubeName, // name
              config.druidCubeName.split('.').pop(), // display_name (short name)
              `Druid cube for ${selectedMetrics.join(', ')}`, // description
              'published', // mode
              selectedMetrics, // metrics
              selectedDimensions, // dimensions
              [], // filters
            );

            if (cubeResult.status >= 400) {
              const errorMsg =
                cubeResult.json?.message ||
                cubeResult.json?.detail ||
                'Failed to create cube';
              // Check if it's a "cube already exists" error - that's okay
              if (!errorMsg.toLowerCase().includes('already exists')) {
                setMaterializationError(`Failed to create cube: ${errorMsg}`);
                return result; // Don't proceed with materialization
              }
              console.log(
                'Cube already exists, proceeding with materialization',
              );
            }
            // Set the loaded cube name so the banner shows it
            setLoadedCubeName(config.druidCubeName);
          }

          // Schedule cube materialization (waits on pre-aggs, ingests to Druid)
          const cubeMaterializeResult = await djClient.materializeCubeV2(
            cubeName,
            config.schedule || '0 6 * * *',
            config.strategy,
            config.lookbackWindow,
            config.runBackfill ?? true,
          );

          if (cubeMaterializeResult.status >= 400) {
            const errorMsg =
              cubeMaterializeResult.json?.message ||
              cubeMaterializeResult.json?.detail ||
              'Failed to schedule cube materialization';
            console.error('Cube materialization failed:', errorMsg);
            // Don't throw - pre-aggs were still created successfully
            setMaterializationError(
              `Pre-aggs created but cube materialization failed: ${errorMsg}`,
            );
          } else {
            console.log(
              'Cube materialization scheduled:',
              cubeMaterializeResult.json,
            );
            console.log(
              'Workflow URLs from response:',
              cubeMaterializeResult.json?.workflow_urls,
            );
            // Store workflow URLs for display
            const urls = cubeMaterializeResult.json?.workflow_urls || [];
            console.log('Setting workflowUrls to:', urls);
            setWorkflowUrls(urls);

            // If backfill is enabled, also kick off cube backfill
            if (
              config.runBackfill &&
              config.backfillFrom &&
              config.backfillTo
            ) {
              console.log(
                'Running cube backfill from',
                config.backfillFrom,
                'to',
                config.backfillTo,
              );
              try {
                const cubeBackfillResult = await djClient.runCubeBackfill(
                  cubeName,
                  config.backfillFrom,
                  config.backfillTo,
                );
                if (cubeBackfillResult._error) {
                  console.error(
                    'Cube backfill failed:',
                    cubeBackfillResult.message,
                  );
                  // Don't throw - cube materialization was still created
                } else {
                  console.log(
                    'Cube backfill started:',
                    cubeBackfillResult.job_url,
                  );
                }
              } catch (cubeBackfillErr) {
                console.error('Failed to run cube backfill:', cubeBackfillErr);
                // Don't throw - cube materialization was still created
              }
            }
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
    [
      djClient,
      selectedMetrics,
      selectedDimensions,
      plannedPreaggs,
      loadedCubeName,
    ],
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
                workflow_urls: result.workflow_urls,
                workflow_status: result.workflow_status || 'active',
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

  // Handle deactivating (pausing) a pre-agg workflow
  const handleDeactivatePreaggWorkflow = useCallback(
    async preaggId => {
      setMaterializationError(null);
      try {
        const result = await djClient.deactivatePreaggWorkflow(preaggId);

        if (result._error || result.message?.includes('Failed')) {
          const errorMsg =
            result.message || result.detail || 'Failed to deactivate workflow';
          setMaterializationError(errorMsg);
          return null;
        }

        // Update the pre-agg in our state to reflect deactivation
        // Backend clears all config, so we clear it here too for clean slate
        setPlannedPreaggs(prev => {
          const updated = { ...prev };
          for (const key in updated) {
            if (updated[key].id === preaggId) {
              updated[key] = {
                ...updated[key],
                strategy: null,
                schedule: null,
                lookback_window: null,
                workflow_status: null,
                workflow_urls: null,
              };
              break;
            }
          }
          return updated;
        });

        return result;
      } catch (err) {
        console.error('Failed to deactivate workflow:', err);
        setMaterializationError(err.message || 'Failed to deactivate workflow');
        return null;
      }
    },
    [djClient],
  );

  // Handle deactivating cube workflow
  const handleDeactivateCubeWorkflow = useCallback(async () => {
    if (!loadedCubeName) {
      setMaterializationError('No cube loaded');
      return null;
    }
    setMaterializationError(null);
    try {
      const result = await djClient.deactivateCubeWorkflow(loadedCubeName);

      if (result.status >= 400) {
        const errorMsg =
          result.json?.message ||
          result.json?.detail ||
          'Failed to deactivate cube workflow';
        setMaterializationError(errorMsg);
        return null;
      }

      // Clear cube materialization state
      setWorkflowUrls([]);
      setCubeMaterialization(null);

      return result.json;
    } catch (err) {
      console.error('Failed to deactivate cube workflow:', err);
      setMaterializationError(
        err.message || 'Failed to deactivate cube workflow',
      );
      return null;
    }
  }, [djClient, loadedCubeName]);

  // Handle updating cube config
  const handleUpdateCubeConfig = useCallback(
    async config => {
      if (!loadedCubeName) {
        setMaterializationError('No cube loaded');
        return null;
      }
      setMaterializationError(null);
      try {
        // Re-call materialize with new config (run_backfill=false to just update)
        const result = await djClient.refreshCubeWorkflow(
          loadedCubeName,
          config.schedule,
          config.strategy,
          config.lookbackWindow,
        );

        if (result.status >= 400) {
          const errorMsg =
            result.json?.message ||
            result.json?.detail ||
            'Failed to update cube config';
          setMaterializationError(errorMsg);
          return null;
        }

        // Update local state
        const urls = result.json?.workflow_urls || [];
        setWorkflowUrls(urls);
        setCubeMaterialization(prev => ({
          ...prev,
          schedule: config.schedule,
          strategy: config.strategy,
          lookbackWindow: config.lookbackWindow,
          workflowUrls: urls,
        }));

        return result.json;
      } catch (err) {
        console.error('Failed to update cube config:', err);
        setMaterializationError(err.message || 'Failed to update cube config');
        return null;
      }
    },
    [djClient, loadedCubeName],
  );

  // Handle refreshing cube workflow
  const handleRefreshCubeWorkflow = useCallback(async () => {
    if (!loadedCubeName || !cubeMaterialization) {
      setMaterializationError('No cube materialization to refresh');
      return null;
    }
    setMaterializationError(null);
    try {
      const result = await djClient.refreshCubeWorkflow(
        loadedCubeName,
        cubeMaterialization.schedule,
        cubeMaterialization.strategy,
        cubeMaterialization.lookbackWindow,
      );

      if (result.status >= 400) {
        const errorMsg =
          result.json?.message ||
          result.json?.detail ||
          'Failed to refresh cube workflow';
        setMaterializationError(errorMsg);
        return null;
      }

      // Update workflow URLs
      const urls = result.json?.workflow_urls || [];
      setWorkflowUrls(urls);
      setCubeMaterialization(prev => ({
        ...prev,
        workflowUrls: urls,
      }));

      return result.json;
    } catch (err) {
      console.error('Failed to refresh cube workflow:', err);
      setMaterializationError(err.message || 'Failed to refresh cube workflow');
      return null;
    }
  }, [djClient, loadedCubeName, cubeMaterialization]);

  // Handle running cube backfill with date range
  const handleRunCubeBackfill = useCallback(
    async (startDate, endDate) => {
      if (!loadedCubeName) {
        setMaterializationError('No cube to backfill');
        return null;
      }
      if (!startDate) {
        setMaterializationError('Start date is required');
        return null;
      }
      setMaterializationError(null);
      try {
        const result = await djClient.runCubeBackfill(
          loadedCubeName,
          startDate,
          endDate,
        );

        if (result._error) {
          setMaterializationError(
            result.message || 'Failed to run cube backfill',
          );
          return null;
        }

        return result;
      } catch (err) {
        console.error('Failed to run cube backfill:', err);
        setMaterializationError(err.message || 'Failed to run cube backfill');
        return null;
      }
    },
    [djClient, loadedCubeName],
  );

  // Fetch raw SQL (without pre-aggregations)
  const handleFetchRawSql = useCallback(async () => {
    try {
      const result = await djClient.metricsV3(
        selectedMetrics,
        selectedDimensions,
        '',
        false, // useMaterialized = false for raw SQL
      );
      return result; // Return full result including scan_estimate
    } catch (err) {
      console.error('Failed to fetch raw SQL:', err);
      return null;
    }
  }, [djClient, selectedMetrics, selectedDimensions]);

  // Set partition on a column (for enabling incremental materialization)
  const handleSetPartition = useCallback(
    async (nodeName, columnName, partitionType, format, granularity) => {
      try {
        return await djClient.setPartition(
          nodeName,
          columnName,
          partitionType,
          format,
          granularity,
        );
      } catch (err) {
        console.error('Failed to set partition:', err);
        throw err;
      }
    },
    [djClient],
  );

  // Refresh measures result (after setting partition)
  const handleRefreshMeasures = useCallback(async () => {
    if (selectedMetrics.length > 0 && selectedDimensions.length > 0) {
      try {
        const [measures, metrics] = await Promise.all([
          djClient.measuresV3(selectedMetrics, selectedDimensions),
          djClient.metricsV3(selectedMetrics, selectedDimensions),
        ]);
        setMeasuresResult(measures);
        setMetricsResult(metrics);
      } catch (err) {
        console.error('Failed to refresh measures:', err);
      }
    }
  }, [djClient, selectedMetrics, selectedDimensions]);

  // Fetch node columns with partition info (for checking if temporal partitions exist)
  const handleFetchNodePartitions = useCallback(
    async nodeName => {
      return await djClient.getNodeColumnsWithPartitions(nodeName);
    },
    [djClient],
  );

  // Run query and fetch results
  const handleRunQuery = useCallback(async () => {
    if (selectedMetrics.length === 0 || selectedDimensions.length === 0) {
      setQueryError('Select at least one metric and one dimension');
      return;
    }

    setQueryLoading(true);
    setQueryError(null);
    setQueryResults(null);
    setShowResults(true);
    const startTime = Date.now();
    setQueryStartTime(startTime);
    setQueryElapsedTime(null);

    try {
      // Fetch data from /data/ endpoint
      const results = await djClient.data(
        selectedMetrics,
        selectedDimensions,
        filters,
      );
      const elapsed = (Date.now() - startTime) / 1000;
      setQueryElapsedTime(elapsed);
      setQueryResults(results);
    } catch (err) {
      console.error('Query failed:', err);
      setQueryError(err.message || 'Failed to execute query');
    } finally {
      setQueryLoading(false);
    }
  }, [djClient, selectedMetrics, selectedDimensions, filters]);

  // Handle back to plan view
  const handleBackToPlan = useCallback(() => {
    setShowResults(false);
    setQueryResults(null);
    setQueryError(null);
  }, []);

  // Handle filter changes
  const handleFiltersChange = useCallback(newFilters => {
    setFilters(newFilters);
  }, []);

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
            filters={filters}
            onFiltersChange={handleFiltersChange}
            onRunQuery={handleRunQuery}
            canRunQuery={
              selectedMetrics.length > 0 && selectedDimensions.length > 0
            }
            queryLoading={queryLoading}
          />
        </aside>

        {/* Main Content Area - Either Results or Graph+Details */}
        {showResults ? (
          <ResultsView
            sql={metricsResult?.sql}
            results={queryResults}
            loading={queryLoading}
            error={queryError}
            elapsedTime={queryElapsedTime}
            onBackToPlan={handleBackToPlan}
            selectedMetrics={selectedMetrics}
            selectedDimensions={selectedDimensions}
            filters={filters}
            dialect={metricsResult?.dialect}
            cubeName={metricsResult?.cube_name}
            availability={cubeAvailability}
          />
        ) : (
          <>
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
                      {measuresResult.grain_groups?.length || 0}{' '}
                      pre-aggregations →{' '}
                      {measuresResult.metric_formulas?.length || 0} metrics
                    </span>
                  </div>
                  {/* Suspense boundary for lazy-loaded ReactFlow graph */}
                  <Suspense
                    fallback={
                      <div className="graph-loading">
                        <div className="loading-spinner" />
                        <span>Loading graph...</span>
                      </div>
                    }
                  >
                    <MetricFlowGraph
                      grainGroups={measuresResult.grain_groups}
                      metricFormulas={measuresResult.metric_formulas}
                      selectedNode={selectedNode}
                      onNodeSelect={handleNodeSelect}
                    />
                  </Suspense>
                </>
              ) : (
                <div className="graph-empty">
                  <div className="empty-icon">⊞</div>
                  <h3>Select Metrics & Dimensions</h3>
                  <p>
                    Choose metrics from the left panel, then select dimensions
                    to see how they decompose into pre-aggregations.
                  </p>
                </div>
              )}
            </main>

            {/* Right: Details Panel */}
            <aside className="planner-details">
              {selectedNode?.type === 'preagg' ||
              selectedNode?.type === 'component' ? (
                <PreAggDetailsPanel
                  preAgg={
                    selectedNode?.type === 'component'
                      ? measuresResult?.grain_groups?.[
                          selectedNode.data?.grainGroupIndex
                        ]
                      : selectedNode.data
                  }
                  metricFormulas={measuresResult?.metric_formulas}
                  onClose={handleClosePanel}
                  highlightedComponent={
                    selectedNode?.type === 'component'
                      ? selectedNode.data?.name
                      : null
                  }
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
                  onSetPartition={handleSetPartition}
                  onRefreshMeasures={handleRefreshMeasures}
                  onFetchNodePartitions={handleFetchNodePartitions}
                  materializationError={materializationError}
                  onClearError={() => setMaterializationError(null)}
                  workflowUrls={workflowUrls}
                  onClearWorkflowUrls={() => setWorkflowUrls([])}
                  loadedCubeName={loadedCubeName}
                  cubeMaterialization={cubeMaterialization}
                  cubeAvailability={cubeAvailability}
                  onUpdateCubeConfig={handleUpdateCubeConfig}
                  onRefreshCubeWorkflow={handleRefreshCubeWorkflow}
                  onRunCubeBackfill={handleRunCubeBackfill}
                  onDeactivatePreaggWorkflow={handleDeactivatePreaggWorkflow}
                  onDeactivateCubeWorkflow={handleDeactivateCubeWorkflow}
                />
              )}
            </aside>
          </>
        )}
      </div>
    </div>
  );
}

export default QueryPlannerPage;
