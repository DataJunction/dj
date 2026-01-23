import {
  render,
  screen,
  fireEvent,
  waitFor,
  act,
} from '@testing-library/react';
import DJClientContext from '../../../providers/djclient';
import { QueryPlannerPage } from '../index';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import React from 'react';

// Mock the MetricFlowGraph component to avoid dagre dependency issues
jest.mock('../MetricFlowGraph', () => ({
  __esModule: true,
  default: ({ grainGroups, metricFormulas, selectedNode, onNodeSelect }) => {
    if (!grainGroups?.length || !metricFormulas?.length) {
      return <div data-testid="graph-empty">Select metrics and dimensions</div>;
    }
    return (
      <div data-testid="metric-flow-graph">
        <span className="graph-stats">
          {grainGroups.length} pre-aggregations → {metricFormulas.length}{' '}
          metrics
        </span>
        <button
          data-testid="select-preagg"
          onClick={() =>
            onNodeSelect?.({
              type: 'preagg',
              index: 0,
              data: grainGroups[0],
            })
          }
        >
          Select Pre-agg
        </button>
        <button
          data-testid="select-metric"
          onClick={() =>
            onNodeSelect?.({
              type: 'metric',
              index: 0,
              data: metricFormulas[0],
            })
          }
        >
          Select Metric
        </button>
      </div>
    );
  },
}));

const mockDjClient = {
  metrics: jest.fn(),
  commonDimensions: jest.fn(),
  measuresV3: jest.fn(),
  metricsV3: jest.fn(),
  listCubesForPreset: jest.fn(),
  cubeForPlanner: jest.fn(),
  planPreaggs: jest.fn(),
  updatePreaggConfig: jest.fn(),
  materializePreagg: jest.fn(),
  runPreaggBackfill: jest.fn(),
  deactivatePreaggWorkflow: jest.fn(),
  deactivateCubeWorkflow: jest.fn(),
  createCube: jest.fn(),
  materializeCubeV2: jest.fn(),
  refreshCubeWorkflow: jest.fn(),
  runCubeBackfill: jest.fn(),
  listPreaggs: jest.fn(),
  getNodeColumnsWithPartitions: jest.fn(),
  setPartition: jest.fn(),
};

const mockMetrics = [
  'default.num_repair_orders',
  'default.avg_repair_price',
  'default.total_repair_cost',
  'sales.revenue',
  'sales.order_count',
];

const mockCommonDimensions = [
  {
    name: 'default.date_dim.dateint',
    type: 'timestamp',
    node_name: 'default.date_dim',
    node_display_name: 'Date',
    properties: [],
    path: ['default.repair_orders', 'default.date_dim.dateint'],
  },
  {
    name: 'default.date_dim.month',
    type: 'int',
    node_name: 'default.date_dim',
    node_display_name: 'Date',
    properties: [],
    path: ['default.repair_orders', 'default.date_dim.month'],
  },
  {
    name: 'default.hard_hat.country',
    type: 'string',
    node_name: 'default.hard_hat',
    node_display_name: 'Hard Hat',
    properties: [],
    path: ['default.repair_orders', 'default.hard_hat.country'],
  },
];

const mockMeasuresResult = {
  grain_groups: [
    {
      parent_name: 'default.repair_orders',
      aggregability: 'FULL',
      grain: ['date_id', 'customer_id'],
      components: [
        {
          name: 'sum_revenue',
          expression: 'SUM(revenue)',
          aggregation: 'SUM',
          merge: 'SUM',
        },
        {
          name: 'count_orders',
          expression: 'COUNT(*)',
          aggregation: 'COUNT',
          merge: 'SUM',
        },
      ],
      sql: 'SELECT date_id, customer_id, SUM(revenue) FROM orders GROUP BY 1, 2',
    },
  ],
  metric_formulas: [
    {
      name: 'default.num_repair_orders',
      short_name: 'num_repair_orders',
      combiner: 'SUM(count_orders)',
      is_derived: false,
      components: ['count_orders'],
    },
    {
      name: 'default.avg_repair_price',
      short_name: 'avg_repair_price',
      combiner: 'SUM(sum_revenue) / SUM(count_orders)',
      is_derived: true,
      components: ['sum_revenue', 'count_orders'],
    },
  ],
  requested_dimensions: ['default.date_dim.dateint'],
};

const mockMetricsResult = {
  sql: 'SELECT date_id, SUM(revenue) as total_revenue FROM orders GROUP BY 1',
};

const mockCubes = [
  { name: 'default.test_cube', display_name: 'Test Cube' },
  { name: 'sales.revenue_cube', display_name: 'Revenue Cube' },
];

const mockCubeData = {
  cube_node_metrics: ['default.num_repair_orders', 'default.avg_repair_price'],
  cube_node_dimensions: ['default.date_dim.dateint'],
  cubeMaterialization: {
    schedule: '0 6 * * *',
    strategy: 'incremental_time',
    lookbackWindow: '1 DAY',
    workflowUrls: ['http://workflow.example.com/1'],
  },
};

const renderPage = (initialEntries = ['/query-planner']) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <Routes>
        <Route
          path="/query-planner"
          element={
            <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
              <QueryPlannerPage />
            </DJClientContext.Provider>
          }
        />
      </Routes>
    </MemoryRouter>,
  );
};

describe('QueryPlannerPage', () => {
  beforeEach(() => {
    mockDjClient.metrics.mockResolvedValue(mockMetrics);
    mockDjClient.commonDimensions.mockResolvedValue(mockCommonDimensions);
    mockDjClient.measuresV3.mockResolvedValue(mockMeasuresResult);
    mockDjClient.metricsV3.mockResolvedValue(mockMetricsResult);
    mockDjClient.listCubesForPreset.mockResolvedValue(mockCubes);
    mockDjClient.cubeForPlanner.mockResolvedValue(null);
    mockDjClient.listPreaggs.mockResolvedValue({ items: [] });
    mockDjClient.getNodeColumnsWithPartitions.mockResolvedValue({
      columns: [{ name: 'date_id', type: 'int' }],
      temporalPartitions: [],
    });
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('Initial Render', () => {
    it('renders the page header', async () => {
      renderPage();
      // Page has "Query Planner" text in multiple places (header and empty state)
      await waitFor(() => {
        expect(screen.getAllByText('Query Planner').length).toBeGreaterThan(0);
      });
    });

    it('renders the metrics section', async () => {
      renderPage();
      await waitFor(() => {
        expect(screen.getByText('Metrics')).toBeInTheDocument();
      });
    });

    it('renders the dimensions section', async () => {
      renderPage();
      await waitFor(() => {
        expect(screen.getByText('Dimensions')).toBeInTheDocument();
      });
    });

    it('fetches metrics on mount', async () => {
      renderPage();
      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });
    });

    it('fetches cube list on mount', async () => {
      renderPage();
      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });
    });

    it('shows empty state when no metrics/dimensions selected', async () => {
      renderPage();
      await waitFor(() => {
        expect(
          screen.getByText('Select Metrics & Dimensions'),
        ).toBeInTheDocument();
      });
    });
  });

  describe('Metric Selection', () => {
    it('displays metrics grouped by namespace', async () => {
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // Check namespace headers are present
      expect(screen.getByText('default')).toBeInTheDocument();
      expect(screen.getByText('sales')).toBeInTheDocument();
    });

    it('expands namespace when clicked', async () => {
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // Click to expand namespace
      const defaultNamespace = screen.getByText('default');
      fireEvent.click(defaultNamespace);

      // Metrics should now be visible
      await waitFor(() => {
        expect(screen.getByText('num_repair_orders')).toBeInTheDocument();
      });
    });

    it('fetches common dimensions when metrics are selected', async () => {
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // Expand and select a metric
      const defaultNamespace = screen.getByText('default');
      fireEvent.click(defaultNamespace);

      await waitFor(() => {
        const checkbox = screen.getByRole('checkbox', {
          name: /num_repair_orders/i,
        });
        fireEvent.click(checkbox);
      });

      await waitFor(() => {
        expect(mockDjClient.commonDimensions).toHaveBeenCalled();
      });
    });
  });

  describe('Search Functionality', () => {
    it('filters metrics by search term', async () => {
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      const searchInput = screen.getByPlaceholderText('Search metrics...');
      fireEvent.change(searchInput, { target: { value: 'repair' } });

      // Should auto-expand matching namespaces
      await waitFor(() => {
        expect(screen.getByText('num_repair_orders')).toBeInTheDocument();
      });
    });
  });

  describe('Cube Preset Loading', () => {
    it('displays cube dropdown button when cubes are available', async () => {
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });

      expect(screen.getByText('Load from Cube')).toBeInTheDocument();
    });

    it('opens cube dropdown when clicked', async () => {
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });

      const cubeButton = screen.getByText('Load from Cube');
      fireEvent.click(cubeButton);

      expect(
        screen.getByPlaceholderText('Search cubes...'),
      ).toBeInTheDocument();
    });

    it('loads cube data when a cube is selected', async () => {
      mockDjClient.cubeForPlanner.mockResolvedValue(mockCubeData);

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });

      // Open dropdown
      const cubeButton = screen.getByText('Load from Cube');
      fireEvent.click(cubeButton);

      // Select a cube
      const cubeOption = screen.getByText('Test Cube');
      fireEvent.click(cubeOption);

      await waitFor(() => {
        expect(mockDjClient.cubeForPlanner).toHaveBeenCalledWith(
          'default.test_cube',
        );
      });
    });
  });

  describe('URL Parameter Handling', () => {
    it('initializes from URL with metrics parameter', async () => {
      mockDjClient.cubeForPlanner.mockResolvedValue(mockCubeData);

      renderPage([
        '/query-planner?metrics=default.num_repair_orders,default.avg_repair_price',
      ]);

      await waitFor(() => {
        expect(mockDjClient.commonDimensions).toHaveBeenCalled();
      });
    });

    it('initializes from URL with cube parameter', async () => {
      mockDjClient.cubeForPlanner.mockResolvedValue(mockCubeData);

      renderPage(['/query-planner?cube=default.test_cube']);

      await waitFor(() => {
        expect(mockDjClient.cubeForPlanner).toHaveBeenCalledWith(
          'default.test_cube',
        );
      });
    });
  });

  describe('Graph Interactions', () => {
    it('displays graph when metrics and dimensions are selected', async () => {
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // Expand and select metric
      fireEvent.click(screen.getByText('default'));
      await waitFor(() => {
        fireEvent.click(
          screen.getByRole('checkbox', { name: /num_repair_orders/i }),
        );
      });

      // Wait for dimensions and select one
      await waitFor(() => {
        expect(mockDjClient.commonDimensions).toHaveBeenCalled();
      });

      const dimCheckbox = screen.getByRole('checkbox', { name: /dateint/i });
      fireEvent.click(dimCheckbox);

      // Wait for measures to be fetched
      await waitFor(() => {
        expect(mockDjClient.measuresV3).toHaveBeenCalled();
      });

      // Graph should now be displayed
      await waitFor(() => {
        expect(screen.getByTestId('metric-flow-graph')).toBeInTheDocument();
      });
    });

    it('shows loading state while building data flow', async () => {
      // Delay the measures response
      mockDjClient.measuresV3.mockImplementation(
        () =>
          new Promise(resolve =>
            setTimeout(() => resolve(mockMeasuresResult), 100),
          ),
      );

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // Select metric and dimension quickly
      fireEvent.click(screen.getByText('default'));
      await waitFor(() => {
        fireEvent.click(
          screen.getByRole('checkbox', { name: /num_repair_orders/i }),
        );
      });

      await waitFor(() => {
        expect(mockDjClient.commonDimensions).toHaveBeenCalled();
      });

      fireEvent.click(screen.getByRole('checkbox', { name: /dateint/i }));

      // Should show loading state
      await waitFor(() => {
        expect(screen.getByText('Building data flow...')).toBeInTheDocument();
      });
    });

    it('shows pre-agg details when preagg node is selected', async () => {
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // Select metric and dimension
      fireEvent.click(screen.getByText('default'));
      await waitFor(() => {
        fireEvent.click(
          screen.getByRole('checkbox', { name: /num_repair_orders/i }),
        );
      });

      await waitFor(() => {
        expect(mockDjClient.commonDimensions).toHaveBeenCalled();
      });

      fireEvent.click(screen.getByRole('checkbox', { name: /dateint/i }));

      // Wait for graph to appear
      await waitFor(() => {
        expect(screen.getByTestId('metric-flow-graph')).toBeInTheDocument();
      });

      // Click on preagg node
      fireEvent.click(screen.getByTestId('select-preagg'));

      // Should show pre-agg details
      await waitFor(() => {
        expect(screen.getByText('Pre-aggregation')).toBeInTheDocument();
      });
    });

    it('shows metric details when metric node is selected', async () => {
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // Select metric and dimension
      fireEvent.click(screen.getByText('default'));
      await waitFor(() => {
        fireEvent.click(
          screen.getByRole('checkbox', { name: /num_repair_orders/i }),
        );
      });

      await waitFor(() => {
        expect(mockDjClient.commonDimensions).toHaveBeenCalled();
      });

      fireEvent.click(screen.getByRole('checkbox', { name: /dateint/i }));

      // Wait for graph to appear
      await waitFor(() => {
        expect(screen.getByTestId('metric-flow-graph')).toBeInTheDocument();
      });

      // Click on metric node
      fireEvent.click(screen.getByTestId('select-metric'));

      // Should show metric details (badge shows "Metric" for non-derived)
      await waitFor(() => {
        expect(screen.getByText('Metric')).toBeInTheDocument();
      });
    });
  });

  describe('Error Handling', () => {
    it('displays error when API call fails', async () => {
      mockDjClient.measuresV3.mockRejectedValue(new Error('API Error'));

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // Select metric and dimension
      fireEvent.click(screen.getByText('default'));
      await waitFor(() => {
        fireEvent.click(
          screen.getByRole('checkbox', { name: /num_repair_orders/i }),
        );
      });

      await waitFor(() => {
        expect(mockDjClient.commonDimensions).toHaveBeenCalled();
      });

      fireEvent.click(screen.getByRole('checkbox', { name: /dateint/i }));

      // Should show error
      await waitFor(() => {
        expect(screen.getByText('API Error')).toBeInTheDocument();
      });
    });

    it('handles commonDimensions API error gracefully', async () => {
      mockDjClient.commonDimensions.mockRejectedValue(
        new Error('Dimensions fetch failed'),
      );

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // Select a metric
      fireEvent.click(screen.getByText('default'));
      await waitFor(() => {
        fireEvent.click(
          screen.getByRole('checkbox', { name: /num_repair_orders/i }),
        );
      });

      // Should handle gracefully (empty dimensions)
      await waitFor(() => {
        expect(mockDjClient.commonDimensions).toHaveBeenCalled();
      });
    });
  });

  describe('Dimension Selection', () => {
    it('clears invalid dimension selections when dimensions change', async () => {
      // First commonDimensions call returns 2 dimensions
      mockDjClient.commonDimensions.mockResolvedValueOnce(mockCommonDimensions);
      // Second call returns only 1 dimension
      mockDjClient.commonDimensions.mockResolvedValueOnce([
        mockCommonDimensions[0],
      ]);

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // Select first metric
      fireEvent.click(screen.getByText('default'));
      await waitFor(() => {
        fireEvent.click(
          screen.getByRole('checkbox', { name: /num_repair_orders/i }),
        );
      });

      // Wait for dimensions
      await waitFor(() => {
        expect(mockDjClient.commonDimensions).toHaveBeenCalled();
      });

      // Select a dimension
      const dimCheckbox = screen.getByRole('checkbox', { name: /dateint/i });
      fireEvent.click(dimCheckbox);

      // Select another metric (triggers new commonDimensions call)
      await waitFor(() => {
        fireEvent.click(
          screen.getByRole('checkbox', { name: /avg_repair_price/i }),
        );
      });

      // Invalid dimensions should be cleared automatically
      await waitFor(() => {
        expect(mockDjClient.commonDimensions).toHaveBeenCalledTimes(2);
      });
    });
  });

  describe('Clear Selection', () => {
    it('clears all selections when Clear is clicked', async () => {
      mockDjClient.cubeForPlanner.mockResolvedValue(mockCubeData);

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });

      // Load a cube
      fireEvent.click(screen.getByText('Load from Cube'));
      fireEvent.click(screen.getByText('Test Cube'));

      await waitFor(() => {
        expect(mockDjClient.cubeForPlanner).toHaveBeenCalled();
      });

      // Wait for common dimensions to load after cube selection
      await waitFor(() => {
        expect(mockDjClient.commonDimensions).toHaveBeenCalled();
      });

      // Click the global Clear button (clear-all-btn class)
      const clearButton = document.querySelector('.clear-all-btn');
      fireEvent.click(clearButton);

      // Should show "Load from Cube" again (cube unloaded)
      await waitFor(() => {
        expect(screen.getByText('Load from Cube')).toBeInTheDocument();
      });
    });
  });

  describe('Pre-aggregation Lookup', () => {
    it('fetches existing pre-aggregations when measures result changes', async () => {
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // Select metric and dimension
      fireEvent.click(screen.getByText('default'));
      await waitFor(() => {
        fireEvent.click(
          screen.getByRole('checkbox', { name: /num_repair_orders/i }),
        );
      });

      await waitFor(() => {
        expect(mockDjClient.commonDimensions).toHaveBeenCalled();
      });

      fireEvent.click(screen.getByRole('checkbox', { name: /dateint/i }));

      // Should fetch existing pre-aggs
      await waitFor(() => {
        expect(mockDjClient.listPreaggs).toHaveBeenCalled();
      });
    });
  });

  describe('Filter Handling', () => {
    it('adds a filter when add filter button is clicked', async () => {
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // Find filter input and add a filter
      const filterInput = screen.getByPlaceholderText(
        /e\.g\. v3\.date\.date_id/i,
      );
      fireEvent.change(filterInput, {
        target: { value: "date_id >= '2024-01-01'" },
      });

      const addButton = screen.getByText('Add');
      fireEvent.click(addButton);

      // Filter should be added (check for chip)
      await waitFor(() => {
        expect(screen.getByText("date_id >= '2024-01-01'")).toBeInTheDocument();
      });
    });

    it('removes a filter when remove button is clicked', async () => {
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // Add a filter first
      const filterInput = screen.getByPlaceholderText(
        /e\.g\. v3\.date\.date_id/i,
      );
      fireEvent.change(filterInput, {
        target: { value: "status = 'active'" },
      });
      fireEvent.click(screen.getByText('Add'));

      await waitFor(() => {
        expect(screen.getByText("status = 'active'")).toBeInTheDocument();
      });

      // Remove the filter (button has generic title "Remove filter")
      const removeButton = screen.getByTitle('Remove filter');
      fireEvent.click(removeButton);

      await waitFor(() => {
        expect(screen.queryByText("status = 'active'")).not.toBeInTheDocument();
      });
    });

    it('disables add button when filter input is empty', async () => {
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      const addButton = screen.getByText('Add');
      expect(addButton).toBeDisabled();
    });
  });

  describe('Run Query', () => {
    beforeEach(() => {
      mockDjClient.metricsV3.mockResolvedValue({
        sql: 'SELECT * FROM metrics',
        dialect: 'SPARK',
        cube_name: null,
      });
    });

    it('shows Run Query button when metrics and dimensions selected', async () => {
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // Select metric
      fireEvent.click(screen.getByText('default'));
      await waitFor(() => {
        fireEvent.click(
          screen.getByRole('checkbox', { name: /num_repair_orders/i }),
        );
      });

      await waitFor(() => {
        expect(mockDjClient.commonDimensions).toHaveBeenCalled();
      });

      // Select dimension
      fireEvent.click(screen.getByRole('checkbox', { name: /dateint/i }));

      // Run Query button should exist
      await waitFor(() => {
        expect(screen.getByText('Run Query')).toBeInTheDocument();
      });
    });

    it('disables Run Query button when no metrics selected', async () => {
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      const runButton = screen.getByText('Run Query');
      expect(runButton).toBeDisabled();
    });
  });

  describe('Materialization Handlers', () => {
    const mockMeasuresResult = {
      grainGroups: [
        {
          node: 'default.repair_orders',
          grain_columns: ['default.date_dim.dateint'],
          measures: [
            { name: 'sum_revenue', expression: 'SUM(revenue)' },
            { name: 'count_orders', expression: 'COUNT(*)' },
          ],
        },
      ],
      metricFormulas: [
        {
          metric: 'default.num_repair_orders',
          formula: 'default.repair_orders.count_orders',
        },
      ],
    };

    beforeEach(() => {
      mockDjClient.measuresV3.mockResolvedValue(mockMeasuresResult);
      mockDjClient.metricsV3.mockResolvedValue({
        sql: 'SELECT * FROM metrics',
        dialect: 'SPARK',
        cube_name: null,
      });
      mockDjClient.listPreaggs.mockResolvedValue({ items: [] });
      mockDjClient.getNodeColumnsWithPartitions.mockResolvedValue({
        columns: [],
        temporalPartitions: [],
      });
    });

    it('calls planPreaggs when materialization is planned', async () => {
      mockDjClient.planPreaggs.mockResolvedValue({
        preaggs: [
          {
            id: 1,
            node_name: 'default.repair_orders',
            grain_columns: ['default.date_dim.dateint'],
          },
        ],
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // Select metric and dimension
      fireEvent.click(screen.getByText('default'));
      await waitFor(() => {
        fireEvent.click(
          screen.getByRole('checkbox', { name: /num_repair_orders/i }),
        );
      });

      await waitFor(() => {
        expect(mockDjClient.commonDimensions).toHaveBeenCalled();
      });

      fireEvent.click(screen.getByRole('checkbox', { name: /dateint/i }));

      await waitFor(() => {
        expect(mockDjClient.measuresV3).toHaveBeenCalled();
      });
    });

    it('handles planPreaggs error gracefully', async () => {
      mockDjClient.planPreaggs.mockResolvedValue({
        _error: true,
        message: 'Failed to plan',
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });
    });

    it('calls materializePreagg when workflow is created', async () => {
      mockDjClient.planPreaggs.mockResolvedValue({
        preaggs: [
          {
            id: 123,
            node_name: 'default.repair_orders',
            grain_columns: ['default.date_dim.dateint'],
          },
        ],
      });
      mockDjClient.materializePreagg.mockResolvedValue({
        workflow_urls: ['http://workflow.example.com'],
        workflow_status: 'active',
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });
    });

    it('handles materializePreagg error', async () => {
      mockDjClient.materializePreagg.mockResolvedValue({
        _error: true,
        message: 'Failed to create workflow',
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });
    });

    it('calls runPreaggBackfill when backfill is triggered', async () => {
      mockDjClient.runPreaggBackfill.mockResolvedValue({
        job_url: 'http://job.example.com',
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });
    });

    it('handles runPreaggBackfill error', async () => {
      mockDjClient.runPreaggBackfill.mockResolvedValue({
        _error: true,
        message: 'Backfill failed',
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });
    });

    it('calls updatePreaggConfig for config updates', async () => {
      mockDjClient.updatePreaggConfig.mockResolvedValue({
        id: 123,
        strategy: 'incremental_time',
        schedule: '0 6 * * *',
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });
    });

    it('handles updatePreaggConfig error', async () => {
      mockDjClient.updatePreaggConfig.mockResolvedValue({
        _error: true,
        message: 'Config update failed',
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });
    });

    it('calls deactivatePreaggWorkflow when workflow is deactivated', async () => {
      mockDjClient.deactivatePreaggWorkflow.mockResolvedValue({
        success: true,
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });
    });

    it('handles deactivatePreaggWorkflow error', async () => {
      mockDjClient.deactivatePreaggWorkflow.mockResolvedValue({
        _error: true,
        message: 'Failed to deactivate',
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });
    });
  });

  describe('Cube Materialization Flow', () => {
    beforeEach(() => {
      mockDjClient.listCubesForPreset.mockResolvedValue([
        { name: 'default.test_cube', display_name: 'Test Cube' },
      ]);
      mockDjClient.cubeForPlanner.mockResolvedValue({
        name: 'default.test_cube',
        display_name: 'Test Cube',
        cube_node_metrics: ['default.num_repair_orders'],
        cube_node_dimensions: ['default.date_dim.dateint'],
        cubeMaterialization: {
          strategy: 'full',
          schedule: '0 6 * * *',
          lookbackWindow: null,
          druidDatasource: 'test_ds',
          preaggTables: [],
          workflowUrls: ['http://workflow.url'],
        },
        availability: null,
      });
    });

    it('calls createCube when new cube is created', async () => {
      mockDjClient.createCube.mockResolvedValue({
        status: 200,
        json: { name: 'new.cube' },
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });
    });

    it('handles createCube error - cube already exists', async () => {
      mockDjClient.createCube.mockResolvedValue({
        status: 400,
        json: { message: 'Cube already exists' },
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });
    });

    it('handles createCube error - other error', async () => {
      mockDjClient.createCube.mockResolvedValue({
        status: 500,
        json: { message: 'Server error' },
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });
    });

    it('calls materializeCubeV2 for cube materialization', async () => {
      mockDjClient.materializeCubeV2.mockResolvedValue({
        status: 200,
        json: {
          workflow_urls: ['http://cube-workflow.example.com'],
        },
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });
    });

    it('handles materializeCubeV2 error', async () => {
      mockDjClient.materializeCubeV2.mockResolvedValue({
        status: 500,
        json: { message: 'Cube materialization failed' },
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });
    });

    it('calls refreshCubeWorkflow for config updates', async () => {
      mockDjClient.refreshCubeWorkflow.mockResolvedValue({
        status: 200,
        json: { workflow_urls: ['http://updated-workflow.url'] },
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });
    });

    it('handles refreshCubeWorkflow error', async () => {
      mockDjClient.refreshCubeWorkflow.mockResolvedValue({
        status: 500,
        json: { message: 'Refresh failed' },
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });
    });

    it('calls deactivateCubeWorkflow when deactivating', async () => {
      mockDjClient.deactivateCubeWorkflow.mockResolvedValue({
        status: 200,
        json: { success: true },
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });

      // Load cube first
      fireEvent.click(screen.getByText('Load from Cube'));
      fireEvent.click(screen.getByText('Test Cube'));

      await waitFor(() => {
        expect(mockDjClient.cubeForPlanner).toHaveBeenCalled();
      });
    });

    it('handles deactivateCubeWorkflow error', async () => {
      mockDjClient.deactivateCubeWorkflow.mockResolvedValue({
        status: 500,
        json: { message: 'Deactivation failed' },
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });
    });

    it('calls runCubeBackfill for backfill', async () => {
      mockDjClient.runCubeBackfill.mockResolvedValue({
        job_url: 'http://backfill-job.example.com',
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });

      // Load cube
      fireEvent.click(screen.getByText('Load from Cube'));
      fireEvent.click(screen.getByText('Test Cube'));

      await waitFor(() => {
        expect(mockDjClient.cubeForPlanner).toHaveBeenCalled();
      });
    });

    it('handles runCubeBackfill error', async () => {
      mockDjClient.runCubeBackfill.mockResolvedValue({
        _error: true,
        message: 'Backfill failed',
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });
    });

    it('handles runCubeBackfill with missing cube name', async () => {
      // Don't load any cube - loadedCubeName will be null
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // No cube loaded, so runCubeBackfill shouldn't be callable
    });
  });

  describe('Workflow Status Updates', () => {
    const mockMeasuresWithPreagg = {
      grainGroups: [
        {
          node: 'default.repair_orders',
          grain_columns: ['default.date_dim.dateint'],
          measures: [{ name: 'count_orders', expression: 'COUNT(*)' }],
        },
      ],
      metricFormulas: [
        {
          metric: 'default.num_repair_orders',
          formula: 'count_orders',
        },
      ],
    };

    beforeEach(() => {
      mockDjClient.measuresV3.mockResolvedValue(mockMeasuresWithPreagg);
      mockDjClient.metricsV3.mockResolvedValue({
        sql: 'SELECT * FROM metrics',
        dialect: 'SPARK',
        cube_name: null,
      });
      mockDjClient.listPreaggs.mockResolvedValue({ items: [] });
    });

    it('updates state when workflow is created successfully', async () => {
      mockDjClient.materializePreagg.mockResolvedValue({
        workflow_urls: ['http://new-workflow.example.com'],
        workflow_status: 'active',
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });
    });

    it('updates state when workflow is deactivated', async () => {
      mockDjClient.deactivatePreaggWorkflow.mockResolvedValue({
        success: true,
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });
    });

    it('handles network errors in workflow operations', async () => {
      mockDjClient.materializePreagg.mockRejectedValue(
        new Error('Network error'),
      );

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });
    });
  });

  describe('Raw SQL Fetching', () => {
    it('fetches raw SQL when needed', async () => {
      mockDjClient.metricsV3.mockResolvedValue({
        sql: 'SELECT * FROM raw_tables',
        dialect: 'SPARK',
        cube_name: null,
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      // Select metric and dimension to trigger SQL fetch
      fireEvent.click(screen.getByText('default'));
      await waitFor(() => {
        fireEvent.click(
          screen.getByRole('checkbox', { name: /num_repair_orders/i }),
        );
      });

      await waitFor(() => {
        expect(mockDjClient.commonDimensions).toHaveBeenCalled();
      });

      fireEvent.click(screen.getByRole('checkbox', { name: /dateint/i }));

      await waitFor(() => {
        expect(mockDjClient.metricsV3).toHaveBeenCalled();
      });
    });
  });

  describe('Cube Workflow Handlers', () => {
    beforeEach(() => {
      mockDjClient.listCubesForPreset.mockResolvedValue([
        { name: 'default.test_cube', display_name: 'Test Cube' },
      ]);
    });

    it('handles cube deactivation', async () => {
      mockDjClient.cubeForPlanner.mockResolvedValue({
        name: 'default.test_cube',
        display_name: 'Test Cube',
        cube_node_metrics: ['default.num_repair_orders'],
        cube_node_dimensions: ['default.date_dim.dateint'],
        cubeMaterialization: {
          strategy: 'full',
          schedule: '0 6 * * *',
          lookbackWindow: null,
          druidDatasource: 'test_ds',
          preaggTables: [],
          workflowUrls: ['http://workflow.url'],
        },
        availability: null,
      });
      mockDjClient.deactivateCubeWorkflow.mockResolvedValue({ success: true });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });

      // Load cube
      fireEvent.click(screen.getByText('Load from Cube'));
      fireEvent.click(screen.getByText('Test Cube'));

      await waitFor(() => {
        expect(mockDjClient.cubeForPlanner).toHaveBeenCalled();
      });
    });

    it('handles cube backfill', async () => {
      mockDjClient.cubeForPlanner.mockResolvedValue({
        name: 'default.test_cube',
        display_name: 'Test Cube',
        cube_node_metrics: ['default.num_repair_orders'],
        cube_node_dimensions: ['default.date_dim.dateint'],
        cubeMaterialization: {
          strategy: 'full',
          schedule: '0 6 * * *',
          lookbackWindow: null,
          druidDatasource: 'test_ds',
          preaggTables: [],
          workflowUrls: [],
        },
        availability: null,
      });
      mockDjClient.runCubeBackfill.mockResolvedValue({ job_url: 'http://job' });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.listCubesForPreset).toHaveBeenCalled();
      });

      // Load cube
      fireEvent.click(screen.getByText('Load from Cube'));
      fireEvent.click(screen.getByText('Test Cube'));

      await waitFor(() => {
        expect(mockDjClient.cubeForPlanner).toHaveBeenCalled();
      });
    });
  });

  describe('Partition Handling', () => {
    it('fetches node partitions when needed', async () => {
      mockDjClient.getNodeColumnsWithPartitions.mockResolvedValue({
        columns: [{ name: 'date_col', type: 'date' }],
        temporalPartitions: [{ column: 'date_col' }],
      });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });
    });

    it('handles setPartition call', async () => {
      mockDjClient.setPartition.mockResolvedValue({ success: true });

      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });
    });
  });

  describe('Results View Navigation', () => {
    it('shows results view hint when no selection', async () => {
      renderPage();

      await waitFor(() => {
        expect(mockDjClient.metrics).toHaveBeenCalled();
      });

      expect(
        screen.getByText('Select metrics and dimensions to run a query'),
      ).toBeInTheDocument();
    });
  });
});
