import { useContext, useEffect, useState, useCallback } from 'react';
import DJClientContext from '../../providers/djclient';
import MetricFlowGraph from './MetricFlowGraph';
import SelectionPanel from './SelectionPanel';
import {
  PreAggDetailsPanel,
  MetricDetailsPanel,
  QueryOverviewPanel,
} from './PreAggDetailsPanel';
import './styles.css';

export function MaterializationPlannerPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  // Available options
  const [metrics, setMetrics] = useState([]);
  const [commonDimensions, setCommonDimensions] = useState([]);

  // Selection state
  const [selectedMetrics, setSelectedMetrics] = useState([]);
  const [selectedDimensions, setSelectedDimensions] = useState([]);

  // Results state
  const [measuresResult, setMeasuresResult] = useState(null);
  const [metricsResult, setMetricsResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [dimensionsLoading, setDimensionsLoading] = useState(false);
  const [error, setError] = useState(null);

  // Node selection for details panel
  const [selectedNode, setSelectedNode] = useState(null);

  // Get metrics list on mount
  useEffect(() => {
    const fetchData = async () => {
      const metricsList = await djClient.metrics();
      setMetrics(metricsList);
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

  const handleMetricsChange = useCallback(newMetrics => {
    setSelectedMetrics(newMetrics);
    setSelectedNode(null);
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

  return (
    <div className="planner-page">
      {/* Header */}
      <header className="planner-header">
        <div className="planner-header-content">
          <h1>Query Planner</h1>
          <p>Explore metrics and dimensions and plan materializations</p>
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
            />
          )}
        </aside>
      </div>
    </div>
  );
}

export default MaterializationPlannerPage;
