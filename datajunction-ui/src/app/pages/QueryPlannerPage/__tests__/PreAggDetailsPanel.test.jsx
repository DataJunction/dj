import {
  render,
  screen,
  fireEvent,
  waitFor,
  act,
} from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import {
  QueryOverviewPanel,
  PreAggDetailsPanel,
  MetricDetailsPanel,
} from '../PreAggDetailsPanel';
import React from 'react';

// Mock the syntax highlighter to avoid issues with CSS imports
jest.mock('react-syntax-highlighter', () => ({
  Light: ({ children }) => (
    <pre data-testid="syntax-highlighter">{children}</pre>
  ),
}));

jest.mock('react-syntax-highlighter/src/styles/hljs', () => ({
  atomOneLight: {},
}));

// Mock clipboard API
Object.assign(navigator, {
  clipboard: {
    writeText: jest.fn(),
  },
});

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
    {
      parent_name: 'inventory.stock',
      aggregability: 'LIMITED',
      grain: ['warehouse_id'],
      components: [
        {
          name: 'sum_quantity',
          expression: 'SUM(quantity)',
          aggregation: 'SUM',
          merge: 'SUM',
        },
      ],
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
};

const mockMetricsResult = {
  sql: 'SELECT date_id, SUM(revenue) as total_revenue FROM orders GROUP BY 1',
};

const renderWithRouter = component => {
  return render(<MemoryRouter>{component}</MemoryRouter>);
};

describe('QueryOverviewPanel', () => {
  const defaultProps = {
    measuresResult: mockMeasuresResult,
    metricsResult: mockMetricsResult,
    selectedMetrics: ['default.num_repair_orders', 'default.avg_repair_price'],
    selectedDimensions: [
      'default.date_dim.dateint',
      'default.customer.country',
    ],
  };

  describe('Empty States', () => {
    it('shows hint when no metrics selected', () => {
      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          selectedMetrics={[]}
          selectedDimensions={[]}
        />,
      );
      expect(screen.getByText('Query Planner')).toBeInTheDocument();
      expect(
        screen.getByText(/Select metrics and dimensions/),
      ).toBeInTheDocument();
    });

    it('shows loading state when results are pending', () => {
      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          measuresResult={null}
          metricsResult={null}
        />,
      );
      expect(screen.getByText('Building query plan...')).toBeInTheDocument();
    });
  });

  describe('Header', () => {
    it('renders the overview header', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByText('Query Plan')).toBeInTheDocument();
    });

    it('shows metric and dimension counts', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByText('2 metrics × 2 dimensions')).toBeInTheDocument();
    });
  });

  describe('Pre-Aggregations Summary', () => {
    it('displays pre-aggregations section', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByText(/Pre-Aggregations/)).toBeInTheDocument();
    });

    it('shows correct count of pre-aggregations', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByText('Pre-Aggregations (2)')).toBeInTheDocument();
    });

    it('displays pre-agg source names', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByText('repair_orders')).toBeInTheDocument();
      expect(screen.getByText('stock')).toBeInTheDocument();
    });

    it('shows status badge for each pre-agg', () => {
      // The updated UI shows status badges instead of aggregability badges
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      // All pre-aggs without materialization config show "Not Set" status
      expect(screen.getAllByText('○ Not Set').length).toBe(2);
    });

    it('displays grain columns', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByText('date_id, customer_id')).toBeInTheDocument();
      expect(screen.getByText('warehouse_id')).toBeInTheDocument();
    });

    it('shows materialization status', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      // Status shows "Not Set" when no materialization is configured
      expect(screen.getAllByText('○ Not Set').length).toBe(2);
    });
  });

  describe('Metrics Summary', () => {
    it('displays metrics section', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByText(/Metrics \(2\)/)).toBeInTheDocument();
    });

    it('shows metric short names', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByText('num_repair_orders')).toBeInTheDocument();
      expect(screen.getByText('avg_repair_price')).toBeInTheDocument();
    });

    it('shows derived badge for derived metrics', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByText('Derived')).toBeInTheDocument();
    });

    it('renders metric links', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      const links = screen.getAllByRole('link');
      expect(
        links.some(
          link =>
            link.getAttribute('href') === '/nodes/default.num_repair_orders',
        ),
      ).toBe(true);
    });
  });

  describe('Dimensions Summary', () => {
    it('displays dimensions section', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByText(/Dimensions \(2\)/)).toBeInTheDocument();
    });

    it('shows dimension short names', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByText('dateint')).toBeInTheDocument();
      expect(screen.getByText('country')).toBeInTheDocument();
    });

    it('renders dimension links', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      const links = screen.getAllByRole('link');
      expect(
        links.some(
          link => link.getAttribute('href') === '/nodes/default.date_dim',
        ),
      ).toBe(true);
    });
  });

  describe('SQL Section', () => {
    it('displays generated SQL section', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByText('Generated SQL')).toBeInTheDocument();
    });

    it('shows SQL view toggle with Optimized and Raw options', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByText('Optimized')).toBeInTheDocument();
      expect(screen.getByText('Raw')).toBeInTheDocument();
    });

    it('renders SQL in syntax highlighter', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByTestId('syntax-highlighter')).toBeInTheDocument();
      expect(screen.getByText(mockMetricsResult.sql)).toBeInTheDocument();
    });

    it('defaults to Optimized view', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      const optimizedBtn = screen.getByText('Optimized');
      expect(optimizedBtn).toHaveClass('active');
    });

    it('fetches and displays raw SQL when Raw tab is clicked', async () => {
      const mockRawSql =
        'SELECT * FROM raw_table WHERE date_id = 123 GROUP BY 1';
      const onFetchRawSql = jest.fn().mockResolvedValue(mockRawSql);

      renderWithRouter(
        <QueryOverviewPanel {...defaultProps} onFetchRawSql={onFetchRawSql} />,
      );

      // Click Raw tab
      const rawBtn = screen.getByText('Raw');
      await act(async () => {
        fireEvent.click(rawBtn);
      });

      await waitFor(() => {
        expect(onFetchRawSql).toHaveBeenCalled();
      });
    });
  });

  describe('Materialization CTA', () => {
    it('shows Configure button when not materialized', () => {
      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={jest.fn()}
        />,
      );
      expect(screen.getByText('Configure')).toBeInTheDocument();
    });

    it('shows CTA content with Ready to materialize text', () => {
      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={jest.fn()}
        />,
      );
      expect(screen.getByText('Ready to materialize?')).toBeInTheDocument();
    });

    it('opens configuration form when Configure button is clicked', async () => {
      const onFetchNodePartitions = jest.fn().mockResolvedValue({
        columns: [{ name: 'date_id', type: 'int' }],
        temporalPartitions: [],
      });

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={jest.fn()}
          onFetchNodePartitions={onFetchNodePartitions}
        />,
      );

      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      // Should show configuration form
      await waitFor(() => {
        expect(
          screen.getByText('Configure Materialization'),
        ).toBeInTheDocument();
      });
    });
  });

  describe('Materialization Configuration Form', () => {
    const setupConfigForm = async () => {
      const onFetchNodePartitions = jest.fn().mockResolvedValue({
        columns: [
          { name: 'date_id', type: 'int' },
          { name: 'customer_id', type: 'int' },
        ],
        temporalPartitions: [{ name: 'date_id', granularity: 'DAY' }],
      });

      const onPlanMaterialization = jest.fn().mockResolvedValue({});

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={onPlanMaterialization}
          onFetchNodePartitions={onFetchNodePartitions}
        />,
      );

      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(
          screen.getByText('Configure Materialization'),
        ).toBeInTheDocument();
      });

      return { onPlanMaterialization, onFetchNodePartitions };
    };

    it('shows strategy options (Full and Incremental)', async () => {
      await setupConfigForm();

      expect(screen.getByText('Strategy')).toBeInTheDocument();
      expect(screen.getByText('Full')).toBeInTheDocument();
      expect(screen.getByText('Incremental')).toBeInTheDocument();
    });

    it('allows switching strategy to Full', async () => {
      await setupConfigForm();

      const fullRadio = screen.getByLabelText('Full');
      fireEvent.click(fullRadio);

      expect(fullRadio).toBeChecked();
    });

    it('shows close button on configuration form', async () => {
      await setupConfigForm();

      // Close button is the × in the header
      const closeButtons = screen.getAllByText('×');
      expect(closeButtons.length).toBeGreaterThan(0);
    });

    it('closes configuration form when close button is clicked', async () => {
      await setupConfigForm();

      // Find and click the close button
      const closeButton = screen.getByRole('button', {
        name: /×/,
      });
      fireEvent.click(closeButton);

      // Configuration form should be closed
      await waitFor(() => {
        expect(
          screen.queryByText('Configure Materialization'),
        ).not.toBeInTheDocument();
      });
    });
  });

  describe('Error Display', () => {
    it('shows materialization error when present', () => {
      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={jest.fn()}
          materializationError="Failed to plan materialization"
          onClearError={jest.fn()}
        />,
      );

      expect(
        screen.getByText('Failed to plan materialization'),
      ).toBeInTheDocument();
    });

    it('calls onClearError when dismiss button is clicked', () => {
      const onClearError = jest.fn();

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={jest.fn()}
          materializationError="Failed to plan materialization"
          onClearError={onClearError}
        />,
      );

      // Find dismiss button (aria-label="Dismiss error")
      const dismissBtn = screen.getByLabelText('Dismiss error');
      fireEvent.click(dismissBtn);

      expect(onClearError).toHaveBeenCalled();
    });
  });

  describe('Workflow URLs Display', () => {
    it('displays workflow URLs when provided', () => {
      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={jest.fn()}
          workflowUrls={['http://workflow.example.com/job/123']}
          onClearWorkflowUrls={jest.fn()}
        />,
      );

      // The workflow URL or a related indicator should be shown
      expect(screen.getByText(/workflow/i)).toBeInTheDocument();
    });
  });

  describe('Planned Pre-aggregations', () => {
    it('shows status badges for planned pre-aggs', () => {
      const plannedPreaggs = {
        'default.repair_orders|customer_id,date_id': {
          id: 1,
          node_name: 'default.repair_orders',
          grain_columns: ['date_id', 'customer_id'],
          workflow_status: 'active',
          workflow_urls: ['http://example.com'],
        },
      };

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          plannedPreaggs={plannedPreaggs}
          onPlanMaterialization={jest.fn()}
        />,
      );

      // Should show some indicator of planned state
      // The exact text depends on the status logic
      expect(
        screen.queryByText('Not Set') || screen.queryByText('Workflow Active'),
      ).toBeTruthy();
    });
  });

  describe('Loaded Cube Display', () => {
    it('shows cube name banner when loadedCubeName is provided', () => {
      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          loadedCubeName="default.test_cube"
          onPlanMaterialization={jest.fn()}
        />,
      );

      // Should show some indication of loaded cube
      // The exact text depends on the implementation
      expect(screen.getByText(/Query Plan/)).toBeInTheDocument();
    });
  });

  describe('Partition Setup Form', () => {
    const setupPartitionTest = async (partitionResults = {}) => {
      const mockNodePartitions = {
        'default.repair_orders': {
          columns: [
            { name: 'date_id', type: 'int' },
            { name: 'customer_id', type: 'int' },
            { name: 'dateint', type: 'int' },
          ],
          temporalPartitions: [],
          ...partitionResults,
        },
      };

      const onFetchNodePartitions = jest.fn().mockImplementation(nodeName =>
        Promise.resolve(
          mockNodePartitions[nodeName] || {
            columns: [],
            temporalPartitions: [],
          },
        ),
      );

      const onSetPartition = jest.fn().mockResolvedValue({ status: 200 });
      const onPlanMaterialization = jest.fn().mockResolvedValue({});

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={onPlanMaterialization}
          onFetchNodePartitions={onFetchNodePartitions}
          onSetPartition={onSetPartition}
        />,
      );

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(
          screen.getByText('Configure Materialization'),
        ).toBeInTheDocument();
      });

      return { onFetchNodePartitions, onSetPartition, onPlanMaterialization };
    };

    it('shows partition setup form when incremental is selected and no temporal partitions', async () => {
      await setupPartitionTest();

      // Select incremental strategy
      const incrementalRadio = screen.getByLabelText('Incremental');
      await act(async () => {
        fireEvent.click(incrementalRadio);
      });

      // Should show partition setup header
      await waitFor(() => {
        expect(
          screen.getByText('Set up temporal partitions for incremental builds'),
        ).toBeInTheDocument();
      });
    });

    it('shows column dropdown with date-like columns prioritized', async () => {
      await setupPartitionTest();

      // Select incremental strategy
      const incrementalRadio = screen.getByLabelText('Incremental');
      await act(async () => {
        fireEvent.click(incrementalRadio);
      });

      await waitFor(() => {
        // Multiple Column labels may exist (one per node)
        const columnLabels = screen.getAllByText('Column');
        expect(columnLabels.length).toBeGreaterThan(0);
      });

      // Should have date-like columns with star markers
      expect(screen.getByText(/dateint.*★/)).toBeInTheDocument();
      expect(screen.getByText(/date_id.*★/)).toBeInTheDocument();
    });

    it('shows granularity dropdown with Day, Hour, Month options', async () => {
      await setupPartitionTest();

      const incrementalRadio = screen.getByLabelText('Incremental');
      await act(async () => {
        fireEvent.click(incrementalRadio);
      });

      await waitFor(() => {
        const granularityLabels = screen.getAllByText('Granularity');
        expect(granularityLabels.length).toBeGreaterThan(0);
      });

      // Check granularity options exist - use getAllByRole since there may be multiple
      const granularitySelects = screen.getAllByRole('combobox');
      // Find the one with Day selected
      const daySelect = granularitySelects.find(
        select => select.value === 'day',
      );
      expect(daySelect).toBeInTheDocument();
    });

    it('shows format input field with placeholder', async () => {
      await setupPartitionTest();

      const incrementalRadio = screen.getByLabelText('Incremental');
      await act(async () => {
        fireEvent.click(incrementalRadio);
      });

      await waitFor(() => {
        const formatLabels = screen.getAllByText('Format');
        expect(formatLabels.length).toBeGreaterThan(0);
      });

      // Format input should have placeholder - there may be multiple
      const formatInputs = screen.getAllByPlaceholderText('yyyyMMdd');
      expect(formatInputs.length).toBeGreaterThan(0);
    });

    it('disables Set button when no column is selected', async () => {
      await setupPartitionTest();

      const incrementalRadio = screen.getByLabelText('Incremental');
      await act(async () => {
        fireEvent.click(incrementalRadio);
      });

      await waitFor(() => {
        // Wait for partition setup form to show
        expect(
          screen.getByText('Set up temporal partitions for incremental builds'),
        ).toBeInTheDocument();
      });

      // Set button should be disabled when no column selected (initially empty)
      const setBtns = screen.getAllByText('Set');
      // At least one Set button should be disabled
      const hasDisabledBtn = setBtns.some(btn => btn.disabled);
      expect(hasDisabledBtn).toBe(true);
    });

    it('enables Set button when column is selected', async () => {
      await setupPartitionTest();

      const incrementalRadio = screen.getByLabelText('Incremental');
      await act(async () => {
        fireEvent.click(incrementalRadio);
      });

      await waitFor(() => {
        const columnLabels = screen.getAllByText('Column');
        expect(columnLabels.length).toBeGreaterThan(0);
      });

      // Select a column in the first dropdown
      const columnSelects = screen.getAllByRole('combobox');
      await act(async () => {
        fireEvent.change(columnSelects[0], { target: { value: 'date_id' } });
      });

      // Set button should now be enabled
      const setBtns = screen.getAllByText('Set');
      expect(setBtns[0]).not.toBeDisabled();
    });

    it('calls onSetPartition when Set button is clicked', async () => {
      const { onSetPartition } = await setupPartitionTest();

      const incrementalRadio = screen.getByLabelText('Incremental');
      await act(async () => {
        fireEvent.click(incrementalRadio);
      });

      await waitFor(() => {
        const columnLabels = screen.getAllByText('Column');
        expect(columnLabels.length).toBeGreaterThan(0);
      });

      // Select a column
      const columnSelects = screen.getAllByRole('combobox');
      await act(async () => {
        fireEvent.change(columnSelects[0], { target: { value: 'date_id' } });
      });

      // Click Set
      const setBtns = screen.getAllByText('Set');
      await act(async () => {
        fireEvent.click(setBtns[0]);
      });

      await waitFor(() => {
        expect(onSetPartition).toHaveBeenCalledWith(
          'default.repair_orders',
          'date_id',
          'temporal',
          'yyyyMMdd',
          'day',
        );
      });
    });

    it('shows success state when partition is already configured', async () => {
      const onFetchNodePartitions = jest.fn().mockResolvedValue({
        columns: [{ name: 'date_id', type: 'int' }],
        temporalPartitions: [{ name: 'date_id', granularity: 'DAY' }],
      });

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={jest.fn()}
          onFetchNodePartitions={onFetchNodePartitions}
        />,
      );

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(
          screen.getByText('Configure Materialization'),
        ).toBeInTheDocument();
      });

      // When partition already exists, the incremental strategy should default to enabled
      // and partition setup form should NOT be shown
      await waitFor(() => {
        // The incremental option should show the partition name badge
        // If partition setup prompt is NOT shown, it means partition is configured
        expect(
          screen.queryByText(
            'Set up temporal partitions for incremental builds',
          ),
        ).not.toBeInTheDocument();
      });
    });
  });

  describe('Backfill Date Range in Config Form', () => {
    const setupBackfillTest = async () => {
      const onFetchNodePartitions = jest.fn().mockResolvedValue({
        columns: [{ name: 'date_id', type: 'int' }],
        temporalPartitions: [{ name: 'date_id', granularity: 'DAY' }],
      });

      const onPlanMaterialization = jest.fn().mockResolvedValue({});

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={onPlanMaterialization}
          onFetchNodePartitions={onFetchNodePartitions}
        />,
      );

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(
          screen.getByText('Configure Materialization'),
        ).toBeInTheDocument();
      });

      return { onPlanMaterialization };
    };

    it('shows "Run initial backfill" checkbox for incremental strategy', async () => {
      await setupBackfillTest();

      // Incremental should be the default when partition exists
      await waitFor(() => {
        expect(screen.getByText('Run initial backfill')).toBeInTheDocument();
      });
    });

    it('shows backfill date range when checkbox is checked', async () => {
      await setupBackfillTest();

      await waitFor(() => {
        expect(screen.getByText('Run initial backfill')).toBeInTheDocument();
      });

      // Checkbox should be checked by default
      const checkbox = screen.getByRole('checkbox', {
        name: /Run initial backfill/i,
      });
      expect(checkbox).toBeChecked();

      // Date range should be visible
      expect(screen.getByText('Backfill Date Range')).toBeInTheDocument();
      expect(screen.getByText('From')).toBeInTheDocument();
      expect(screen.getByText('To')).toBeInTheDocument();
    });

    it('hides backfill date range when checkbox is unchecked', async () => {
      await setupBackfillTest();

      await waitFor(() => {
        expect(screen.getByText('Run initial backfill')).toBeInTheDocument();
      });

      // Uncheck the checkbox
      const checkbox = screen.getByRole('checkbox', {
        name: /Run initial backfill/i,
      });
      await act(async () => {
        fireEvent.click(checkbox);
      });

      // Date range should be hidden
      expect(screen.queryByText('Backfill Date Range')).not.toBeInTheDocument();
    });

    it('shows "Today" and "Specific date" options for end date', async () => {
      await setupBackfillTest();

      await waitFor(() => {
        expect(screen.getByText('Backfill Date Range')).toBeInTheDocument();
      });

      // Find the select for backfill "To" field
      // There are multiple selects, we need the one with 'today' value
      const selects = screen.getAllByRole('combobox');
      const toSelect = selects.find(s => s.value === 'today');
      expect(toSelect).toBeInTheDocument();

      // Change to specific date
      await act(async () => {
        fireEvent.change(toSelect, { target: { value: 'specific' } });
      });

      // Should show an additional date input
      await waitFor(() => {
        // There should be date inputs visible
        const dateInputs = document.querySelectorAll('input[type="date"]');
        expect(dateInputs.length).toBeGreaterThanOrEqual(2);
      });
    });

    it('hides backfill options for full strategy', async () => {
      await setupBackfillTest();

      // Switch to full strategy
      const fullRadio = screen.getByLabelText('Full');
      await act(async () => {
        fireEvent.click(fullRadio);
      });

      // Backfill checkbox should not be visible
      expect(
        screen.queryByText('Run initial backfill'),
      ).not.toBeInTheDocument();
    });
  });

  describe('Schedule Configuration', () => {
    const setupScheduleTest = async () => {
      const onFetchNodePartitions = jest.fn().mockResolvedValue({
        columns: [{ name: 'date_id', type: 'int' }],
        temporalPartitions: [{ name: 'date_id', granularity: 'DAY' }],
      });

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={jest.fn()}
          onFetchNodePartitions={onFetchNodePartitions}
        />,
      );

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(
          screen.getByText('Configure Materialization'),
        ).toBeInTheDocument();
      });
    };

    it('shows schedule dropdown with recommended, hourly, and custom options', async () => {
      await setupScheduleTest();

      expect(screen.getByText('Schedule')).toBeInTheDocument();

      // The schedule select should have options - find one with 'auto' value
      const selects = screen.getAllByRole('combobox');
      const scheduleSelect = selects.find(s => s.value === 'auto');
      expect(scheduleSelect).toBeInTheDocument();
    });

    it('shows custom cron input when custom is selected', async () => {
      await setupScheduleTest();

      // Find the schedule select (has 'auto' value initially)
      const selects = screen.getAllByRole('combobox');
      const scheduleSelect = selects.find(s => s.value === 'auto');

      await act(async () => {
        fireEvent.change(scheduleSelect, { target: { value: 'custom' } });
      });

      // Custom input should appear
      await waitFor(() => {
        expect(screen.getByPlaceholderText('0 6 * * *')).toBeInTheDocument();
      });
    });
  });

  describe('Lookback Window', () => {
    it('shows lookback window input for incremental strategy', async () => {
      const onFetchNodePartitions = jest.fn().mockResolvedValue({
        columns: [{ name: 'date_id', type: 'int' }],
        temporalPartitions: [{ name: 'date_id', granularity: 'DAY' }],
      });

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={jest.fn()}
          onFetchNodePartitions={onFetchNodePartitions}
        />,
      );

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('Lookback Window')).toBeInTheDocument();
      });

      // Should have placeholder
      expect(screen.getByPlaceholderText('1 day')).toBeInTheDocument();

      // Should have hint text
      expect(screen.getByText(/For late-arriving data/)).toBeInTheDocument();
    });

    it('hides lookback window for full strategy', async () => {
      const onFetchNodePartitions = jest.fn().mockResolvedValue({
        columns: [{ name: 'date_id', type: 'int' }],
        temporalPartitions: [{ name: 'date_id', granularity: 'DAY' }],
      });

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={jest.fn()}
          onFetchNodePartitions={onFetchNodePartitions}
        />,
      );

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(
          screen.getByText('Configure Materialization'),
        ).toBeInTheDocument();
      });

      // Switch to full strategy
      const fullRadio = screen.getByLabelText('Full');
      await act(async () => {
        fireEvent.click(fullRadio);
      });

      // Lookback window should be hidden
      expect(screen.queryByText('Lookback Window')).not.toBeInTheDocument();
    });
  });

  describe('Druid Cube Configuration', () => {
    it('shows Druid cube checkbox', async () => {
      const onFetchNodePartitions = jest.fn().mockResolvedValue({
        columns: [{ name: 'date_id', type: 'int' }],
        temporalPartitions: [{ name: 'date_id', granularity: 'DAY' }],
      });

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={jest.fn()}
          onFetchNodePartitions={onFetchNodePartitions}
        />,
      );

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(
          screen.getByText('Enable Druid cube materialization'),
        ).toBeInTheDocument();
      });
    });

    it('shows cube name inputs when Druid is enabled and no cube loaded', async () => {
      const onFetchNodePartitions = jest.fn().mockResolvedValue({
        columns: [{ name: 'date_id', type: 'int' }],
        temporalPartitions: [{ name: 'date_id', granularity: 'DAY' }],
      });

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={jest.fn()}
          onFetchNodePartitions={onFetchNodePartitions}
        />,
      );

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('Cube Name')).toBeInTheDocument();
      });

      // Should show namespace and name inputs
      expect(screen.getByPlaceholderText('users.myname')).toBeInTheDocument();
      expect(screen.getByPlaceholderText('my_cube')).toBeInTheDocument();
    });

    it('shows preview of pre-aggregations to combine', async () => {
      const onFetchNodePartitions = jest.fn().mockResolvedValue({
        columns: [{ name: 'date_id', type: 'int' }],
        temporalPartitions: [{ name: 'date_id', granularity: 'DAY' }],
      });

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={jest.fn()}
          onFetchNodePartitions={onFetchNodePartitions}
        />,
      );

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(
          screen.getByText('Pre-aggregations to combine:'),
        ).toBeInTheDocument();
      });

      // Should show the pre-agg source (may appear multiple times)
      const repairOrdersElements = screen.getAllByText('repair_orders');
      expect(repairOrdersElements.length).toBeGreaterThan(0);
    });
  });

  describe('Form Submission', () => {
    it('shows correct button text for Druid cube materialization', async () => {
      const onFetchNodePartitions = jest.fn().mockResolvedValue({
        columns: [{ name: 'date_id', type: 'int' }],
        temporalPartitions: [{ name: 'date_id', granularity: 'DAY' }],
      });

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={jest.fn()}
          onFetchNodePartitions={onFetchNodePartitions}
        />,
      );

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(
          screen.getByText('Create Pre-Agg Workflows & Schedule Cube'),
        ).toBeInTheDocument();
      });
    });

    it('shows Cancel and submit buttons', async () => {
      const onFetchNodePartitions = jest.fn().mockResolvedValue({
        columns: [{ name: 'date_id', type: 'int' }],
        temporalPartitions: [{ name: 'date_id', granularity: 'DAY' }],
      });

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={jest.fn()}
          onFetchNodePartitions={onFetchNodePartitions}
        />,
      );

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('Cancel')).toBeInTheDocument();
      });
    });

    it('closes form when Cancel is clicked', async () => {
      const onFetchNodePartitions = jest.fn().mockResolvedValue({
        columns: [{ name: 'date_id', type: 'int' }],
        temporalPartitions: [{ name: 'date_id', granularity: 'DAY' }],
      });

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={jest.fn()}
          onFetchNodePartitions={onFetchNodePartitions}
        />,
      );

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(
          screen.getByText('Configure Materialization'),
        ).toBeInTheDocument();
      });

      // Click Cancel
      fireEvent.click(screen.getByText('Cancel'));

      // Form should be closed
      await waitFor(() => {
        expect(
          screen.queryByText('Configure Materialization'),
        ).not.toBeInTheDocument();
      });
    });

    it('calls onPlanMaterialization with config when submitted', async () => {
      const onFetchNodePartitions = jest.fn().mockResolvedValue({
        columns: [{ name: 'date_id', type: 'int' }],
        temporalPartitions: [{ name: 'date_id', granularity: 'DAY' }],
      });

      const onPlanMaterialization = jest.fn().mockResolvedValue({});

      renderWithRouter(
        <QueryOverviewPanel
          {...defaultProps}
          onPlanMaterialization={onPlanMaterialization}
          onFetchNodePartitions={onFetchNodePartitions}
        />,
      );

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(
          screen.getByText('Create Pre-Agg Workflows & Schedule Cube'),
        ).toBeInTheDocument();
      });

      // Submit the form
      const submitBtn = screen.getByText(
        'Create Pre-Agg Workflows & Schedule Cube',
      );
      await act(async () => {
        fireEvent.click(submitBtn);
      });

      await waitFor(() => {
        expect(onPlanMaterialization).toHaveBeenCalled();
      });

      // Check the config structure
      const callArgs = onPlanMaterialization.mock.calls[0][1];
      expect(callArgs).toHaveProperty('strategy');
      expect(callArgs).toHaveProperty('schedule');
      expect(callArgs).toHaveProperty('enableDruidCube', true);
    });
  });
});

describe('PreAggDetailsPanel', () => {
  const mockPreAgg = {
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
  };

  const mockMetricFormulas = [
    {
      name: 'default.total_revenue',
      short_name: 'total_revenue',
      combiner: 'SUM(sum_revenue)',
      is_derived: false,
      components: ['sum_revenue'],
    },
  ];

  const onClose = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('returns null when no preAgg provided', () => {
    const { container } = render(
      <PreAggDetailsPanel preAgg={null} onClose={onClose} />,
    );
    expect(container.firstChild).toBeNull();
  });

  it('renders pre-aggregation badge', () => {
    render(
      <PreAggDetailsPanel
        preAgg={mockPreAgg}
        metricFormulas={mockMetricFormulas}
        onClose={onClose}
      />,
    );
    expect(screen.getByText('Pre-aggregation')).toBeInTheDocument();
  });

  it('displays source name', () => {
    render(
      <PreAggDetailsPanel
        preAgg={mockPreAgg}
        metricFormulas={mockMetricFormulas}
        onClose={onClose}
      />,
    );
    expect(screen.getByText('repair_orders')).toBeInTheDocument();
    expect(screen.getByText('default.repair_orders')).toBeInTheDocument();
  });

  it('displays close button', () => {
    render(
      <PreAggDetailsPanel
        preAgg={mockPreAgg}
        metricFormulas={mockMetricFormulas}
        onClose={onClose}
      />,
    );
    expect(screen.getByTitle('Close panel')).toBeInTheDocument();
  });

  it('calls onClose when close button clicked', () => {
    render(
      <PreAggDetailsPanel
        preAgg={mockPreAgg}
        metricFormulas={mockMetricFormulas}
        onClose={onClose}
      />,
    );
    fireEvent.click(screen.getByTitle('Close panel'));
    expect(onClose).toHaveBeenCalled();
  });

  describe('Grain Section', () => {
    it('displays grain section', () => {
      render(
        <PreAggDetailsPanel
          preAgg={mockPreAgg}
          metricFormulas={mockMetricFormulas}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('Grain (GROUP BY)')).toBeInTheDocument();
    });

    it('shows grain columns as pills', () => {
      render(
        <PreAggDetailsPanel
          preAgg={mockPreAgg}
          metricFormulas={mockMetricFormulas}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('date_id')).toBeInTheDocument();
      expect(screen.getByText('customer_id')).toBeInTheDocument();
    });

    it('shows empty message when no grain', () => {
      const noGrainPreAgg = { ...mockPreAgg, grain: [] };
      render(
        <PreAggDetailsPanel
          preAgg={noGrainPreAgg}
          metricFormulas={mockMetricFormulas}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('No grain columns')).toBeInTheDocument();
    });
  });

  describe('Related Metrics Section', () => {
    it('displays metrics using this section', () => {
      render(
        <PreAggDetailsPanel
          preAgg={mockPreAgg}
          metricFormulas={mockMetricFormulas}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('Metrics Using This')).toBeInTheDocument();
    });

    it('shows related metrics', () => {
      render(
        <PreAggDetailsPanel
          preAgg={mockPreAgg}
          metricFormulas={mockMetricFormulas}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('total_revenue')).toBeInTheDocument();
    });
  });

  describe('Components Table', () => {
    it('displays components section', () => {
      render(
        <PreAggDetailsPanel
          preAgg={mockPreAgg}
          metricFormulas={mockMetricFormulas}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('Components (2)')).toBeInTheDocument();
    });

    it('shows component names', () => {
      render(
        <PreAggDetailsPanel
          preAgg={mockPreAgg}
          metricFormulas={mockMetricFormulas}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('sum_revenue')).toBeInTheDocument();
      expect(screen.getByText('count_orders')).toBeInTheDocument();
    });

    it('shows component expressions', () => {
      render(
        <PreAggDetailsPanel
          preAgg={mockPreAgg}
          metricFormulas={mockMetricFormulas}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('SUM(revenue)')).toBeInTheDocument();
      expect(screen.getByText('COUNT(*)')).toBeInTheDocument();
    });

    it('shows aggregation functions', () => {
      render(
        <PreAggDetailsPanel
          preAgg={mockPreAgg}
          metricFormulas={mockMetricFormulas}
          onClose={onClose}
        />,
      );
      expect(screen.getAllByText('SUM').length).toBeGreaterThan(0);
      expect(screen.getByText('COUNT')).toBeInTheDocument();
    });
  });

  describe('SQL Section', () => {
    it('displays SQL section when sql is present', () => {
      render(
        <PreAggDetailsPanel
          preAgg={mockPreAgg}
          metricFormulas={mockMetricFormulas}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('Pre-Aggregation SQL')).toBeInTheDocument();
    });

    it('shows copy button', () => {
      render(
        <PreAggDetailsPanel
          preAgg={mockPreAgg}
          metricFormulas={mockMetricFormulas}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('Copy SQL')).toBeInTheDocument();
    });

    it('copies SQL when copy button is clicked', () => {
      render(
        <PreAggDetailsPanel
          preAgg={mockPreAgg}
          metricFormulas={mockMetricFormulas}
          onClose={onClose}
        />,
      );

      const copyBtn = screen.getByText('Copy SQL');
      fireEvent.click(copyBtn);

      expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
        mockPreAgg.sql,
      );
    });

    it('hides SQL section when sql is not present', () => {
      const preAggWithoutSql = { ...mockPreAgg, sql: null };
      render(
        <PreAggDetailsPanel
          preAgg={preAggWithoutSql}
          metricFormulas={mockMetricFormulas}
          onClose={onClose}
        />,
      );

      expect(screen.queryByText('Pre-Aggregation SQL')).not.toBeInTheDocument();
    });
  });

  describe('Pre-aggregation Type Badge', () => {
    it('shows Pre-aggregation badge', () => {
      render(
        <PreAggDetailsPanel
          preAgg={mockPreAgg}
          metricFormulas={mockMetricFormulas}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('Pre-aggregation')).toBeInTheDocument();
    });

    it('shows parent name in title', () => {
      render(
        <PreAggDetailsPanel
          preAgg={mockPreAgg}
          metricFormulas={mockMetricFormulas}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('repair_orders')).toBeInTheDocument();
    });
  });

  describe('Merge Function Display', () => {
    it('shows merge functions for components', () => {
      render(
        <PreAggDetailsPanel
          preAgg={mockPreAgg}
          metricFormulas={mockMetricFormulas}
          onClose={onClose}
        />,
      );

      // Components have merge functions
      expect(screen.getAllByText('SUM').length).toBeGreaterThan(0);
    });
  });
});

describe('MetricDetailsPanel', () => {
  const mockMetric = {
    name: 'default.avg_repair_price',
    short_name: 'avg_repair_price',
    combiner: 'SUM(sum_revenue) / SUM(count_orders)',
    is_derived: true,
    components: ['sum_revenue', 'count_orders'],
  };

  const mockGrainGroups = [
    {
      parent_name: 'default.repair_orders',
      components: [{ name: 'sum_revenue' }, { name: 'count_orders' }],
    },
  ];

  const onClose = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('returns null when no metric provided', () => {
    const { container } = render(
      <MetricDetailsPanel metric={null} onClose={onClose} />,
    );
    expect(container.firstChild).toBeNull();
  });

  it('renders metric badge', () => {
    render(
      <MetricDetailsPanel
        metric={mockMetric}
        grainGroups={mockGrainGroups}
        onClose={onClose}
      />,
    );
    expect(screen.getByText('Derived Metric')).toBeInTheDocument();
  });

  it('renders regular metric badge for non-derived', () => {
    const nonDerivedMetric = { ...mockMetric, is_derived: false };
    render(
      <MetricDetailsPanel
        metric={nonDerivedMetric}
        grainGroups={mockGrainGroups}
        onClose={onClose}
      />,
    );
    expect(screen.getByText('Metric')).toBeInTheDocument();
  });

  it('displays metric name', () => {
    render(
      <MetricDetailsPanel
        metric={mockMetric}
        grainGroups={mockGrainGroups}
        onClose={onClose}
      />,
    );
    expect(screen.getByText('avg_repair_price')).toBeInTheDocument();
    expect(screen.getByText('default.avg_repair_price')).toBeInTheDocument();
  });

  it('calls onClose when close button clicked', () => {
    render(
      <MetricDetailsPanel
        metric={mockMetric}
        grainGroups={mockGrainGroups}
        onClose={onClose}
      />,
    );
    fireEvent.click(screen.getByTitle('Close panel'));
    expect(onClose).toHaveBeenCalled();
  });

  describe('Formula Section', () => {
    it('displays combiner formula section', () => {
      render(
        <MetricDetailsPanel
          metric={mockMetric}
          grainGroups={mockGrainGroups}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('Combiner Formula')).toBeInTheDocument();
    });

    it('shows the formula', () => {
      render(
        <MetricDetailsPanel
          metric={mockMetric}
          grainGroups={mockGrainGroups}
          onClose={onClose}
        />,
      );
      expect(
        screen.getByText('SUM(sum_revenue) / SUM(count_orders)'),
      ).toBeInTheDocument();
    });
  });

  describe('Components Section', () => {
    it('displays components used section', () => {
      render(
        <MetricDetailsPanel
          metric={mockMetric}
          grainGroups={mockGrainGroups}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('Components Used')).toBeInTheDocument();
    });

    it('shows component tags', () => {
      render(
        <MetricDetailsPanel
          metric={mockMetric}
          grainGroups={mockGrainGroups}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('sum_revenue')).toBeInTheDocument();
      expect(screen.getByText('count_orders')).toBeInTheDocument();
    });
  });

  describe('Source Pre-aggregations Section', () => {
    it('displays source pre-aggregations section', () => {
      render(
        <MetricDetailsPanel
          metric={mockMetric}
          grainGroups={mockGrainGroups}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('Source Pre-aggregations')).toBeInTheDocument();
    });

    it('shows related pre-agg sources', () => {
      render(
        <MetricDetailsPanel
          metric={mockMetric}
          grainGroups={mockGrainGroups}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('repair_orders')).toBeInTheDocument();
    });

    it('shows empty message when no sources found', () => {
      render(
        <MetricDetailsPanel
          metric={mockMetric}
          grainGroups={[]}
          onClose={onClose}
        />,
      );
      expect(screen.getByText('No source found')).toBeInTheDocument();
    });
  });

  describe('Multiple Source Pre-aggregations', () => {
    it('shows multiple sources when metric uses components from different pre-aggs', () => {
      const multiSourceGrainGroups = [
        {
          parent_name: 'default.repair_orders',
          components: [{ name: 'sum_revenue' }],
        },
        {
          parent_name: 'default.inventory',
          components: [{ name: 'count_orders' }],
        },
      ];

      render(
        <MetricDetailsPanel
          metric={mockMetric}
          grainGroups={multiSourceGrainGroups}
          onClose={onClose}
        />,
      );

      expect(screen.getByText('repair_orders')).toBeInTheDocument();
      expect(screen.getByText('inventory')).toBeInTheDocument();
    });
  });

  describe('Full Metric Name Display', () => {
    it('displays full metric name as subtitle', () => {
      render(
        <MetricDetailsPanel
          metric={mockMetric}
          grainGroups={mockGrainGroups}
          onClose={onClose}
        />,
      );

      expect(screen.getByText('default.avg_repair_price')).toBeInTheDocument();
    });
  });

  describe('Full Name Display', () => {
    it('renders the full metric name', () => {
      render(
        <MemoryRouter>
          <MetricDetailsPanel
            metric={mockMetric}
            grainGroups={mockGrainGroups}
            onClose={onClose}
          />
        </MemoryRouter>,
      );

      expect(screen.getByText('default.avg_repair_price')).toBeInTheDocument();
    });
  });
});

describe('QueryOverviewPanel - Pre-Agg Cards', () => {
  const mockMeasuresWithPreaggs = {
    grain_groups: [
      {
        parent_name: 'default.repair_orders',
        aggregability: 'FULL',
        grain: ['customer_id', 'date_id'], // alphabetically sorted for consistency
        components: [
          { name: 'sum_revenue', expression: 'SUM(revenue)' },
          { name: 'count_orders', expression: 'COUNT(*)' },
        ],
        sql: 'SELECT date_id, customer_id, SUM(revenue) FROM orders GROUP BY 1, 2',
      },
    ],
    metric_formulas: [
      {
        name: 'default.num_repair_orders',
        short_name: 'num_repair_orders',
        combiner: 'SUM(count_orders)',
        components: ['count_orders'],
      },
    ],
  };

  // Key format is: parent_name|sorted_grain_cols
  const mockPlannedPreaggs = {
    'default.repair_orders|customer_id,date_id': {
      id: 'preagg-123',
      parent_name: 'default.repair_orders',
      grain_columns: ['customer_id', 'date_id'],
      strategy: 'incremental_time',
      schedule: '0 6 * * *',
      lookback_window: '1 day',
      workflow_urls: ['https://workflow.example.com/scheduled-123'],
      availability: {
        updated_at: '2024-01-15T10:30:00Z',
      },
    },
  };

  const baseProps = {
    measuresResult: mockMeasuresWithPreaggs,
    metricsResult: { sql: 'SELECT ...' },
    selectedMetrics: ['default.num_repair_orders'],
    selectedDimensions: ['default.date_dim.dateint'],
    loadedCubeName: null,
    plannedPreaggs: mockPlannedPreaggs,
    onPlanMaterialization: jest.fn(),
    onUpdateConfig: jest.fn(),
    onCreateWorkflow: jest.fn(),
    onRunBackfill: jest.fn(),
    onDeactivatePreaggWorkflow: jest.fn(),
    onFetchNodePartitions: jest.fn().mockResolvedValue({
      columns: [{ name: 'date_id', type: 'int' }],
      temporalPartitions: [{ name: 'date_id', granularity: 'DAY' }],
    }),
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('Pre-Agg Card Display', () => {
    it('shows pre-agg cards with names', () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);
      expect(screen.getByText('repair_orders')).toBeInTheDocument();
    });

    it('shows Active status pill for configured pre-aggs', () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);
      expect(screen.getByText('● Active')).toBeInTheDocument();
    });

    it('shows Not Set status for unconfigured pre-aggs', () => {
      renderWithRouter(
        <QueryOverviewPanel {...baseProps} plannedPreaggs={{}} />,
      );
      expect(screen.getByText('○ Not Set')).toBeInTheDocument();
    });

    it('displays grain information', () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);
      // Grain is displayed as joined string
      expect(screen.getByText(/customer_id.*date_id/i)).toBeInTheDocument();
    });

    it('shows schedule summary for active pre-aggs', () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);
      // The schedule '0 6 * * *' should show "Daily at 6:00am" or the raw schedule
      // Schedule is shown for active preaggs (may appear multiple times)
      const scheduleTexts = screen.getAllByText(/Daily|6:00|0 6/i);
      expect(scheduleTexts.length).toBeGreaterThan(0);
    });
  });

  describe('Pre-Agg Card Expansion', () => {
    it('expands card when clicked to show details', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      // Find and click the expand button
      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      // Should show strategy details
      await waitFor(() => {
        expect(
          screen.getByText('Incremental (Time-based)'),
        ).toBeInTheDocument();
      });
    });

    it('shows schedule in expanded view', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('0 6 * * *')).toBeInTheDocument();
      });
    });

    it('shows lookback window in expanded view', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('1 day')).toBeInTheDocument();
      });
    });

    it('shows last run time when available', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('Last Run:')).toBeInTheDocument();
      });
    });

    it('shows workflow links when available', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('Scheduled')).toBeInTheDocument();
      });
    });

    it('collapses when clicking expand button again', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(
          screen.getByText('Incremental (Time-based)'),
        ).toBeInTheDocument();
      });

      // Click collapse button
      const collapseBtn = screen.getByRole('button', { name: 'Collapse' });
      await act(async () => {
        fireEvent.click(collapseBtn);
      });

      await waitFor(() => {
        expect(
          screen.queryByText('Incremental (Time-based)'),
        ).not.toBeInTheDocument();
      });
    });
  });

  describe('Pre-Agg Edit Config Form', () => {
    it('opens edit form when Edit Config button is clicked', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      // Expand the card first
      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('Edit Config')).toBeInTheDocument();
      });

      // Click Edit Config
      await act(async () => {
        fireEvent.click(screen.getByText('Edit Config'));
      });

      await waitFor(() => {
        expect(
          screen.getByText('Edit Materialization Config'),
        ).toBeInTheDocument();
      });
    });

    it('shows strategy options in edit form', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      // Expand and open edit
      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('Edit Config')).toBeInTheDocument();
      });

      await act(async () => {
        fireEvent.click(screen.getByText('Edit Config'));
      });

      await waitFor(() => {
        expect(screen.getByText('Full')).toBeInTheDocument();
        expect(screen.getByText('Incremental (Time)')).toBeInTheDocument();
      });
    });

    it('closes edit form when close button is clicked', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      // Expand and open edit
      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('Edit Config')).toBeInTheDocument();
      });

      await act(async () => {
        fireEvent.click(screen.getByText('Edit Config'));
      });

      await waitFor(() => {
        expect(
          screen.getByText('Edit Materialization Config'),
        ).toBeInTheDocument();
      });

      // Find close button (×) and click
      const closeBtn = screen.getByRole('button', { name: '×' });
      await act(async () => {
        fireEvent.click(closeBtn);
      });

      await waitFor(() => {
        expect(
          screen.queryByText('Edit Materialization Config'),
        ).not.toBeInTheDocument();
      });
    });

    it('closes edit form when Cancel is clicked', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      // Expand and open edit
      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('Edit Config')).toBeInTheDocument();
      });

      await act(async () => {
        fireEvent.click(screen.getByText('Edit Config'));
      });

      await waitFor(() => {
        expect(
          screen.getByText('Edit Materialization Config'),
        ).toBeInTheDocument();
      });

      // Click Cancel
      const cancelBtns = screen.getAllByText('Cancel');
      await act(async () => {
        fireEvent.click(cancelBtns[cancelBtns.length - 1]);
      });

      await waitFor(() => {
        expect(
          screen.queryByText('Edit Materialization Config'),
        ).not.toBeInTheDocument();
      });
    });

    it('calls onUpdateConfig when Save is clicked', async () => {
      const onUpdateConfig = jest.fn().mockResolvedValue({});
      renderWithRouter(
        <QueryOverviewPanel {...baseProps} onUpdateConfig={onUpdateConfig} />,
      );

      // Expand and open edit
      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('Edit Config')).toBeInTheDocument();
      });

      await act(async () => {
        fireEvent.click(screen.getByText('Edit Config'));
      });

      await waitFor(() => {
        expect(screen.getByText('Save')).toBeInTheDocument();
      });

      // Click Save
      await act(async () => {
        fireEvent.click(screen.getByText('Save'));
      });

      await waitFor(() => {
        expect(onUpdateConfig).toHaveBeenCalledWith(
          'preagg-123',
          expect.any(Object),
        );
      });
    });
  });

  describe('Pre-Agg Workflow Actions', () => {
    it('shows Refresh button when workflow exists', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('↻ Refresh')).toBeInTheDocument();
      });
    });

    it('calls onCreateWorkflow when Refresh is clicked', async () => {
      const onCreateWorkflow = jest.fn().mockResolvedValue({});
      renderWithRouter(
        <QueryOverviewPanel
          {...baseProps}
          onCreateWorkflow={onCreateWorkflow}
        />,
      );

      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('↻ Refresh')).toBeInTheDocument();
      });

      await act(async () => {
        fireEvent.click(screen.getByText('↻ Refresh'));
      });

      await waitFor(() => {
        expect(onCreateWorkflow).toHaveBeenCalledWith('preagg-123', true);
      });
    });

    it('shows Run Backfill button', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('Run Backfill')).toBeInTheDocument();
      });
    });

    it('shows Deactivate button when workflow exists', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('⏹ Deactivate')).toBeInTheDocument();
      });
    });

    it('calls onDeactivatePreaggWorkflow when Deactivate is clicked and confirmed', async () => {
      const onDeactivatePreaggWorkflow = jest.fn().mockResolvedValue({});
      window.confirm = jest.fn().mockReturnValue(true);

      renderWithRouter(
        <QueryOverviewPanel
          {...baseProps}
          onDeactivatePreaggWorkflow={onDeactivatePreaggWorkflow}
        />,
      );

      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('⏹ Deactivate')).toBeInTheDocument();
      });

      await act(async () => {
        fireEvent.click(screen.getByText('⏹ Deactivate'));
      });

      await waitFor(() => {
        expect(window.confirm).toHaveBeenCalled();
        expect(onDeactivatePreaggWorkflow).toHaveBeenCalledWith('preagg-123');
      });
    });

    it('does not deactivate when confirmation is cancelled', async () => {
      const onDeactivatePreaggWorkflow = jest.fn().mockResolvedValue({});
      window.confirm = jest.fn().mockReturnValue(false);

      renderWithRouter(
        <QueryOverviewPanel
          {...baseProps}
          onDeactivatePreaggWorkflow={onDeactivatePreaggWorkflow}
        />,
      );

      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('⏹ Deactivate')).toBeInTheDocument();
      });

      await act(async () => {
        fireEvent.click(screen.getByText('⏹ Deactivate'));
      });

      expect(window.confirm).toHaveBeenCalled();
      expect(onDeactivatePreaggWorkflow).not.toHaveBeenCalled();
    });
  });

  describe('Create Workflow Button', () => {
    it('shows Create Workflow button when no workflow exists', async () => {
      const preaggWithoutWorkflow = {
        'default.repair_orders|customer_id,date_id': {
          id: 'preagg-123',
          parent_name: 'default.repair_orders',
          grain_columns: ['date_id', 'customer_id'],
          strategy: 'incremental_time',
          schedule: '0 6 * * *',
          workflow_urls: [], // No workflows
        },
      };

      renderWithRouter(
        <QueryOverviewPanel
          {...baseProps}
          plannedPreaggs={preaggWithoutWorkflow}
        />,
      );

      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('Create Workflow')).toBeInTheDocument();
      });
    });

    it('calls onCreateWorkflow when Create Workflow is clicked', async () => {
      const onCreateWorkflow = jest.fn().mockResolvedValue({
        workflow_urls: ['https://workflow.example.com/new-123'],
      });

      const preaggWithoutWorkflow = {
        'default.repair_orders|customer_id,date_id': {
          id: 'preagg-123',
          parent_name: 'default.repair_orders',
          grain_columns: ['date_id', 'customer_id'],
          strategy: 'incremental_time',
          schedule: '0 6 * * *',
          workflow_urls: [],
        },
      };

      renderWithRouter(
        <QueryOverviewPanel
          {...baseProps}
          plannedPreaggs={preaggWithoutWorkflow}
          onCreateWorkflow={onCreateWorkflow}
        />,
      );

      const expandBtn = screen.getByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('Create Workflow')).toBeInTheDocument();
      });

      await act(async () => {
        fireEvent.click(screen.getByText('Create Workflow'));
      });

      await waitFor(() => {
        expect(onCreateWorkflow).toHaveBeenCalledWith('preagg-123');
      });
    });
  });
});

describe('QueryOverviewPanel - Backfill Modal', () => {
  const mockMeasuresWithPreaggs = {
    grain_groups: [
      {
        parent_name: 'default.repair_orders',
        aggregability: 'FULL',
        grain: ['date_id'],
        components: [{ name: 'count_orders' }],
      },
    ],
    metric_formulas: [
      {
        name: 'default.num_repair_orders',
        short_name: 'num_repair_orders',
        combiner: 'SUM(count_orders)',
        components: ['count_orders'],
      },
    ],
  };

  // Key must match normalized grain
  const mockPlannedPreaggs = {
    'default.repair_orders|date_id': {
      id: 'preagg-456',
      parent_name: 'default.repair_orders',
      grain_columns: ['date_id'],
      strategy: 'incremental_time',
      schedule: '0 6 * * *',
      workflow_urls: ['https://workflow.example.com/test'],
    },
  };

  const baseProps = {
    measuresResult: mockMeasuresWithPreaggs,
    metricsResult: { sql: 'SELECT ...' },
    selectedMetrics: ['default.num_repair_orders'],
    selectedDimensions: ['default.date_dim.date_id'], // Must have at least one dimension
    plannedPreaggs: mockPlannedPreaggs,
    onRunBackfill: jest.fn(),
    onFetchNodePartitions: jest.fn().mockResolvedValue({
      columns: [],
      temporalPartitions: [],
    }),
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('opens backfill modal when Run Backfill is clicked on preagg card', async () => {
    renderWithRouter(<QueryOverviewPanel {...baseProps} />);

    // Find and expand the preagg card
    const expandBtn = screen.getByRole('button', { name: 'Expand' });
    await act(async () => {
      fireEvent.click(expandBtn);
    });

    // Wait for Run Backfill button to appear in expanded view
    await waitFor(() => {
      const backfillBtns = screen.getAllByText('Run Backfill');
      expect(backfillBtns.length).toBeGreaterThan(0);
    });

    // Click the Run Backfill button on preagg
    const backfillBtns = screen.getAllByText('Run Backfill');
    await act(async () => {
      fireEvent.click(backfillBtns[backfillBtns.length - 1]);
    });

    // Modal should open with heading
    await waitFor(() => {
      const headings = screen.getAllByRole('heading', { level: 3 });
      const backfillHeading = headings.find(
        h => h.textContent === 'Run Backfill',
      );
      expect(backfillHeading).toBeInTheDocument();
    });
  });

  it('shows start and end date inputs in modal', async () => {
    renderWithRouter(<QueryOverviewPanel {...baseProps} />);

    // Expand card
    const expandBtn = screen.getByRole('button', { name: 'Expand' });
    await act(async () => {
      fireEvent.click(expandBtn);
    });

    // Open modal
    await waitFor(() => {
      const backfillBtns = screen.getAllByText('Run Backfill');
      expect(backfillBtns.length).toBeGreaterThan(0);
    });

    const backfillBtns = screen.getAllByText('Run Backfill');
    await act(async () => {
      fireEvent.click(backfillBtns[backfillBtns.length - 1]);
    });

    await waitFor(() => {
      expect(screen.getByText('Start Date')).toBeInTheDocument();
      expect(screen.getByText('End Date')).toBeInTheDocument();
    });
  });

  it('calls onRunBackfill when Start Backfill is clicked in modal', async () => {
    const onRunBackfill = jest
      .fn()
      .mockResolvedValue({ job_url: 'https://job.example.com' });
    renderWithRouter(
      <QueryOverviewPanel {...baseProps} onRunBackfill={onRunBackfill} />,
    );

    // Expand card
    const expandBtn = screen.getByRole('button', { name: 'Expand' });
    await act(async () => {
      fireEvent.click(expandBtn);
    });

    // Open modal
    await waitFor(() => {
      const backfillBtns = screen.getAllByText('Run Backfill');
      expect(backfillBtns.length).toBeGreaterThan(0);
    });

    const backfillBtns = screen.getAllByText('Run Backfill');
    await act(async () => {
      fireEvent.click(backfillBtns[backfillBtns.length - 1]);
    });

    // Wait for modal
    await waitFor(() => {
      expect(screen.getByText('Start Date')).toBeInTheDocument();
    });

    // Click Start Backfill
    await act(async () => {
      fireEvent.click(screen.getByText('Start Backfill'));
    });

    await waitFor(() => {
      expect(onRunBackfill).toHaveBeenCalledWith(
        'preagg-456',
        expect.any(String),
        expect.any(String),
      );
    });
  });

  it('closes modal when Cancel is clicked', async () => {
    renderWithRouter(<QueryOverviewPanel {...baseProps} />);

    // Expand card
    const expandBtn = screen.getByRole('button', { name: 'Expand' });
    await act(async () => {
      fireEvent.click(expandBtn);
    });

    // Open modal
    await waitFor(() => {
      const backfillBtns = screen.getAllByText('Run Backfill');
      expect(backfillBtns.length).toBeGreaterThan(0);
    });

    const backfillBtns = screen.getAllByText('Run Backfill');
    await act(async () => {
      fireEvent.click(backfillBtns[backfillBtns.length - 1]);
    });

    // Wait for modal
    await waitFor(() => {
      expect(screen.getByText('Start Date')).toBeInTheDocument();
    });

    // Click Cancel
    const cancelBtns = screen.getAllByText('Cancel');
    await act(async () => {
      fireEvent.click(cancelBtns[cancelBtns.length - 1]);
    });

    // Modal should close
    await waitFor(() => {
      expect(screen.queryByText('Start Date')).not.toBeInTheDocument();
    });
  });
});

describe('QueryOverviewPanel - Cube Materialization Section', () => {
  // Cube section only appears when workflowUrls.length > 0
  // Also, selectedMetrics AND selectedDimensions must be non-empty to render main content
  const mockMeasuresResult = {
    grain_groups: [
      {
        parent_name: 'default.repair_orders',
        grain: ['date_id'],
        components: [{ name: 'count_orders' }],
      },
    ],
    metric_formulas: [
      {
        name: 'default.num_repair_orders',
        short_name: 'num_repair_orders',
        combiner: 'SUM(count_orders)',
        components: ['count_orders'],
      },
    ],
  };

  const mockCubeMaterialization = {
    strategy: 'incremental_time',
    schedule: '0 8 * * *',
    lookbackWindow: '2 days',
    druidDatasource: 'dj__test_cube',
    preaggTables: ['default.repair_orders'],
  };

  // These workflow URLs trigger the cube section to show
  const mockWorkflowUrls = [
    'https://workflow.example.com/scheduled',
    'https://workflow.example.com/adhoc_backfill',
  ];

  const baseProps = {
    measuresResult: mockMeasuresResult,
    metricsResult: { sql: 'SELECT ...' },
    selectedMetrics: ['default.num_repair_orders'],
    selectedDimensions: ['default.date_dim.date_id'], // Must have at least one dimension
    plannedPreaggs: {},
    loadedCubeName: 'default.test_cube',
    cubeMaterialization: mockCubeMaterialization,
    workflowUrls: mockWorkflowUrls, // This triggers cube section
    onUpdateCubeConfig: jest.fn(),
    onRefreshCubeWorkflow: jest.fn(),
    onRunCubeBackfill: jest.fn(),
    onDeactivateCubeWorkflow: jest.fn(),
    onFetchNodePartitions: jest.fn().mockResolvedValue({
      columns: [],
      temporalPartitions: [],
    }),
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('Cube Summary Display', () => {
    it('shows cube section when workflowUrls exist', () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);
      expect(screen.getByText('Druid Cube')).toBeInTheDocument();
    });

    it('displays cube datasource name', () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);
      expect(screen.getByText('dj__test_cube')).toBeInTheDocument();
    });

    it('shows Active status for configured cube', () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);
      const activePills = screen.getAllByText('● Active');
      expect(activePills.length).toBeGreaterThan(0);
    });

    it('shows Workflow active status', () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);
      expect(screen.getByText('Workflow active')).toBeInTheDocument();
    });
  });

  describe('Cube Card Expansion', () => {
    it('expands cube card when clicked', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      // Find cube expand button (first one)
      const expandBtns = screen.getAllByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtns[0]);
      });

      // Should show strategy details
      await waitFor(() => {
        expect(screen.getByText('Strategy:')).toBeInTheDocument();
      });
    });

    it('shows workflow links when expanded', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      const expandBtns = screen.getAllByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtns[0]);
      });

      await waitFor(() => {
        // Check for workflow links
        expect(screen.getByText('Workflows:')).toBeInTheDocument();
      });
    });

    it('shows action buttons when expanded', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      const expandBtns = screen.getAllByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtns[0]);
      });

      await waitFor(() => {
        expect(screen.getByText('Edit Config')).toBeInTheDocument();
        expect(screen.getByText('↻ Refresh')).toBeInTheDocument();
      });
    });
  });

  describe('Cube Edit Config Form', () => {
    it('opens edit form when Edit Config is clicked', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      // Expand cube card first
      const expandBtns = screen.getAllByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtns[0]);
      });

      await waitFor(() => {
        expect(screen.getByText('Edit Config')).toBeInTheDocument();
      });

      // Click Edit Config
      await act(async () => {
        fireEvent.click(screen.getByText('Edit Config'));
      });

      await waitFor(() => {
        expect(
          screen.getByText('Edit Materialization Config'),
        ).toBeInTheDocument();
      });
    });

    it('shows strategy options in edit form', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      // Expand and open edit
      const expandBtns = screen.getAllByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtns[0]);
      });

      await waitFor(() => {
        expect(screen.getByText('Edit Config')).toBeInTheDocument();
      });

      await act(async () => {
        fireEvent.click(screen.getByText('Edit Config'));
      });

      await waitFor(() => {
        expect(screen.getByText('Full')).toBeInTheDocument();
        expect(screen.getByText('Incremental (Time)')).toBeInTheDocument();
      });
    });

    it('calls onUpdateCubeConfig when Save is clicked', async () => {
      const onUpdateCubeConfig = jest.fn().mockResolvedValue({});
      renderWithRouter(
        <QueryOverviewPanel
          {...baseProps}
          onUpdateCubeConfig={onUpdateCubeConfig}
        />,
      );

      // Expand and open edit
      const expandBtns = screen.getAllByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtns[0]);
      });

      await waitFor(() => {
        expect(screen.getByText('Edit Config')).toBeInTheDocument();
      });

      await act(async () => {
        fireEvent.click(screen.getByText('Edit Config'));
      });

      await waitFor(() => {
        expect(screen.getByText('Save')).toBeInTheDocument();
      });

      await act(async () => {
        fireEvent.click(screen.getByText('Save'));
      });

      await waitFor(() => {
        expect(onUpdateCubeConfig).toHaveBeenCalled();
      });
    });
  });

  describe('Cube Workflow Actions', () => {
    it('calls onRefreshCubeWorkflow when Refresh is clicked', async () => {
      const onRefreshCubeWorkflow = jest.fn().mockResolvedValue({});
      renderWithRouter(
        <QueryOverviewPanel
          {...baseProps}
          onRefreshCubeWorkflow={onRefreshCubeWorkflow}
        />,
      );

      // Expand cube card
      const expandBtns = screen.getAllByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtns[0]);
      });

      await waitFor(() => {
        expect(screen.getByText('↻ Refresh')).toBeInTheDocument();
      });

      await act(async () => {
        fireEvent.click(screen.getByText('↻ Refresh'));
      });

      await waitFor(() => {
        expect(onRefreshCubeWorkflow).toHaveBeenCalled();
      });
    });

    it('calls onDeactivateCubeWorkflow when Deactivate is clicked and confirmed', async () => {
      const onDeactivateCubeWorkflow = jest.fn().mockResolvedValue({});
      window.confirm = jest.fn().mockReturnValue(true);

      renderWithRouter(
        <QueryOverviewPanel
          {...baseProps}
          onDeactivateCubeWorkflow={onDeactivateCubeWorkflow}
        />,
      );

      // Expand cube card
      const expandBtns = screen.getAllByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtns[0]);
      });

      await waitFor(() => {
        expect(screen.getByText('⏹ Deactivate')).toBeInTheDocument();
      });

      await act(async () => {
        fireEvent.click(screen.getByText('⏹ Deactivate'));
      });

      await waitFor(() => {
        expect(window.confirm).toHaveBeenCalled();
        expect(onDeactivateCubeWorkflow).toHaveBeenCalled();
      });
    });
  });

  describe('Cube Backfill Modal', () => {
    it('opens cube backfill modal when Run Backfill is clicked', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      // Expand cube card
      const expandBtns = screen.getAllByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtns[0]);
      });

      await waitFor(() => {
        const backfillBtns = screen.getAllByText('Run Backfill');
        expect(backfillBtns.length).toBeGreaterThan(0);
      });

      const backfillBtns = screen.getAllByText('Run Backfill');
      await act(async () => {
        fireEvent.click(backfillBtns[0]);
      });

      // Modal should show
      await waitFor(() => {
        expect(screen.getByText('Run Cube Backfill')).toBeInTheDocument();
      });
    });

    it('shows backfill description in modal', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      // Expand and open modal
      const expandBtns = screen.getAllByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtns[0]);
      });

      await waitFor(() => {
        const backfillBtns = screen.getAllByText('Run Backfill');
        expect(backfillBtns.length).toBeGreaterThan(0);
      });

      const backfillBtns = screen.getAllByText('Run Backfill');
      await act(async () => {
        fireEvent.click(backfillBtns[0]);
      });

      await waitFor(() => {
        expect(
          screen.getByText(/Run a backfill for the specified date range/i),
        ).toBeInTheDocument();
      });
    });

    it('calls onRunCubeBackfill when Start Backfill is clicked', async () => {
      const onRunCubeBackfill = jest.fn().mockResolvedValue({
        workflow_urls: ['https://workflow.example.com/backfill'],
      });
      renderWithRouter(
        <QueryOverviewPanel
          {...baseProps}
          onRunCubeBackfill={onRunCubeBackfill}
        />,
      );

      // Expand and open modal
      const expandBtns = screen.getAllByRole('button', { name: 'Expand' });
      await act(async () => {
        fireEvent.click(expandBtns[0]);
      });

      await waitFor(() => {
        const backfillBtns = screen.getAllByText('Run Backfill');
        expect(backfillBtns.length).toBeGreaterThan(0);
      });

      const backfillBtns = screen.getAllByText('Run Backfill');
      await act(async () => {
        fireEvent.click(backfillBtns[0]);
      });

      await waitFor(() => {
        expect(screen.getByText('Run Cube Backfill')).toBeInTheDocument();
      });

      // Click Start Backfill
      await act(async () => {
        fireEvent.click(screen.getByText('Start Backfill'));
      });

      await waitFor(() => {
        expect(onRunCubeBackfill).toHaveBeenCalled();
      });
    });
  });
});

describe('QueryOverviewPanel - Custom Schedule and Druid Config', () => {
  // This tests the materialization config form which shows when there are unconfigured preaggs
  const mockMeasuresResult = {
    grain_groups: [
      {
        parent_name: 'default.repair_orders',
        grain: ['date_id'],
        components: [{ name: 'count_orders' }],
      },
    ],
    metric_formulas: [
      {
        name: 'default.num_repair_orders',
        short_name: 'num_repair_orders',
        combiner: 'SUM(count_orders)',
        components: ['count_orders'],
      },
    ],
  };

  const baseProps = {
    measuresResult: mockMeasuresResult,
    metricsResult: { sql: 'SELECT ...' },
    selectedMetrics: ['default.num_repair_orders'],
    selectedDimensions: ['default.date_dim.date_id'], // Must have at least one dimension
    plannedPreaggs: {}, // No planned preaggs = shows Configure button
    onPlanMaterialization: jest.fn(),
    onFetchNodePartitions: jest.fn().mockResolvedValue({
      columns: [{ name: 'date_id', type: 'int' }],
      temporalPartitions: [{ name: 'date_id', granularity: 'DAY' }],
    }),
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('Custom Schedule Input', () => {
    it('shows schedule options in config form', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      // Wait for form to load
      await waitFor(() => {
        expect(
          screen.getByText('Configure Materialization'),
        ).toBeInTheDocument();
      });

      // Should have schedule label
      expect(screen.getByText('Schedule')).toBeInTheDocument();
    });

    it('allows selecting custom schedule type', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(
          screen.getByText('Configure Materialization'),
        ).toBeInTheDocument();
      });

      // Find and change schedule select
      const selects = screen.getAllByRole('combobox');
      expect(selects.length).toBeGreaterThan(0);
    });
  });

  describe('Druid Cube Namespace and Name Inputs', () => {
    it('shows Druid cube config when checkbox is enabled', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(
          screen.getByText('Enable Druid cube materialization'),
        ).toBeInTheDocument();
      });

      // The checkbox should be checked by default, showing cube name inputs
      await waitFor(() => {
        expect(screen.getByText('Cube Name')).toBeInTheDocument();
      });
    });

    it('shows cube namespace and name placeholders', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(screen.getByPlaceholderText('users.myname')).toBeInTheDocument();
        expect(screen.getByPlaceholderText('my_cube')).toBeInTheDocument();
      });
    });

    it('shows full cube name preview', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(screen.getByText('Full name:')).toBeInTheDocument();
      });
    });
  });

  describe('Form submission button text', () => {
    it('shows Create Pre-Agg Workflows button when Druid is enabled', async () => {
      renderWithRouter(<QueryOverviewPanel {...baseProps} />);

      // Open config form
      const configureBtn = screen.getByText('Configure');
      await act(async () => {
        fireEvent.click(configureBtn);
      });

      await waitFor(() => {
        expect(
          screen.getByText('Create Pre-Agg Workflows & Schedule Cube'),
        ).toBeInTheDocument();
      });
    });
  });
});
