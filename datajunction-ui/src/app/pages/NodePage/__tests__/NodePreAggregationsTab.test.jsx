import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import NodePreAggregationsTab from '../NodePreAggregationsTab';
import DJClientContext from '../../../providers/djclient';

// Mock the CSS import
jest.mock('../../../../styles/preaggregations.css', () => ({}));

// Mock cronstrue - it's imported via require() in the component
jest.mock('cronstrue', () => ({
  toString: cron => {
    if (cron === '0 0 * * *') return 'At 12:00 AM';
    if (cron === '0 * * * *') return 'Every hour';
    return cron || 'Not scheduled';
  },
}));

// Mock labelize from utils/form
jest.mock('../../../../utils/form', () => ({
  labelize: str => {
    if (!str) return '';
    // Convert snake_case/SCREAMING_SNAKE to Title Case
    return str
      .toLowerCase()
      .replace(/_/g, ' ')
      .replace(/\b\w/g, c => c.toUpperCase());
  },
}));

const mockNode = {
  name: 'default.orders_fact',
  version: 'v1.0',
  type: 'transform',
};

const mockPreaggs = {
  items: [
    {
      id: 1,
      node_revision_id: 1,
      node_name: 'default.orders_fact',
      node_version: 'v1.0',
      grain_columns: [
        'default.date_dim.date_id',
        'default.customer_dim.customer_id',
      ],
      measures: [
        {
          name: 'total_revenue',
          expression: 'revenue',
          aggregation: 'SUM',
          merge: 'SUM',
          rule: { type: 'full' },
          used_by_metrics: [
            { name: 'default.revenue_metric', display_name: 'Revenue' },
          ],
        },
        {
          name: 'order_count',
          expression: '*',
          aggregation: 'COUNT',
          merge: 'SUM',
          rule: { type: 'full' },
          used_by_metrics: [
            { name: 'default.order_count_metric', display_name: 'Order Count' },
          ],
        },
      ],
      sql: 'SELECT date_id, customer_id, SUM(revenue), COUNT(*) FROM orders GROUP BY 1, 2',
      grain_group_hash: 'abc123',
      strategy: 'full',
      schedule: '0 0 * * *',
      lookback_window: null,
      workflow_urls: [
        { label: 'scheduled', url: 'http://scheduler/workflow/123.main' },
      ],
      workflow_status: 'active',
      status: 'active',
      materialized_table_ref: 'analytics.preaggs.orders_fact_abc123',
      max_partition: ['2024', '01', '15'],
      related_metrics: ['default.revenue_metric', 'default.order_count_metric'],
      created_at: '2024-01-01T00:00:00Z',
      updated_at: '2024-01-15T00:00:00Z',
    },
    {
      id: 2,
      node_revision_id: 1,
      node_name: 'default.orders_fact',
      node_version: 'v1.0',
      grain_columns: ['default.product_dim.category'],
      measures: [
        {
          name: 'total_quantity',
          expression: 'quantity',
          aggregation: 'SUM',
          merge: 'SUM',
          rule: { type: 'full' },
          used_by_metrics: null,
        },
      ],
      sql: 'SELECT category, SUM(quantity) FROM orders GROUP BY 1',
      grain_group_hash: 'def456',
      strategy: 'incremental_time',
      schedule: '0 * * * *',
      lookback_window: '3 days',
      workflow_urls: null,
      workflow_status: null,
      status: 'pending',
      materialized_table_ref: null,
      max_partition: null,
      related_metrics: null,
      created_at: '2024-01-10T00:00:00Z',
      updated_at: null,
    },
  ],
  total: 2,
  limit: 50,
  offset: 0,
};

const mockPreaggsWithStale = {
  items: [
    ...mockPreaggs.items,
    {
      id: 3,
      node_revision_id: 0,
      node_name: 'default.orders_fact',
      node_version: 'v0.9', // Stale version
      grain_columns: ['default.date_dim.date_id'],
      measures: [
        {
          name: 'old_revenue',
          expression: 'revenue',
          aggregation: 'SUM',
          merge: 'SUM',
          rule: { type: 'full' },
          used_by_metrics: null,
        },
      ],
      sql: 'SELECT date_id, SUM(revenue) FROM orders GROUP BY 1',
      grain_group_hash: 'old123',
      strategy: 'full',
      schedule: '0 0 * * *',
      workflow_urls: [
        { label: 'scheduled', url: 'http://scheduler/workflow/old.main' },
      ],
      workflow_status: 'active',
      status: 'active',
      materialized_table_ref: 'analytics.preaggs.orders_fact_old',
      max_partition: ['2024', '01', '10'],
      related_metrics: null,
      created_at: '2023-12-01T00:00:00Z',
      updated_at: '2024-01-10T00:00:00Z',
    },
  ],
  total: 3,
  limit: 50,
  offset: 0,
};

const createMockDjClient = (preaggs = mockPreaggs) => ({
  DataJunctionAPI: {
    listPreaggs: jest.fn().mockResolvedValue(preaggs),
    deactivatePreaggWorkflow: jest.fn().mockResolvedValue({ status: 'none' }),
    bulkDeactivatePreaggWorkflows: jest.fn().mockResolvedValue({
      deactivated_count: 1,
      deactivated: [{ id: 3 }],
    }),
  },
});

const renderWithContext = (component, djClient) => {
  return render(
    <MemoryRouter>
      <DJClientContext.Provider value={djClient}>
        {component}
      </DJClientContext.Provider>
    </MemoryRouter>,
  );
};

describe('<NodePreAggregationsTab />', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('Loading and Empty States', () => {
    it('shows loading state initially', () => {
      const djClient = createMockDjClient();
      // Make the promise never resolve to keep loading state
      djClient.DataJunctionAPI.listPreaggs.mockReturnValue(
        new Promise(() => {}),
      );

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      expect(
        screen.getByText('Loading pre-aggregations...'),
      ).toBeInTheDocument();
    });

    it('shows empty state when no pre-aggregations exist', async () => {
      const djClient = createMockDjClient({
        items: [],
        total: 0,
        limit: 50,
        offset: 0,
      });

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        expect(
          screen.getByText('No pre-aggregations found for this node.'),
        ).toBeInTheDocument();
      });
    });

    it('shows error state when API fails', async () => {
      const djClient = createMockDjClient();
      djClient.DataJunctionAPI.listPreaggs.mockResolvedValue({
        _error: true,
        message: 'Failed to fetch',
      });

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        expect(
          screen.getByText(/Error loading pre-aggregations/),
        ).toBeInTheDocument();
      });
    });
  });

  describe('Section Headers', () => {
    it('renders "Current Pre-Aggregations" section header with version', async () => {
      const djClient = createMockDjClient();

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        expect(
          screen.getByText('Current Pre-Aggregations (v1.0)'),
        ).toBeInTheDocument();
      });
    });

    it('renders "Stale Pre-Aggregations" section when stale preaggs exist', async () => {
      const djClient = createMockDjClient(mockPreaggsWithStale);

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        expect(
          screen.getByText('Stale Pre-Aggregations (1)'),
        ).toBeInTheDocument();
      });
    });

    it('does not render stale section when no stale preaggs exist', async () => {
      const djClient = createMockDjClient();

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        expect(
          screen.getByText('Current Pre-Aggregations (v1.0)'),
        ).toBeInTheDocument();
      });

      expect(
        screen.queryByText(/Stale Pre-Aggregations/),
      ).not.toBeInTheDocument();
    });
  });

  describe('Pre-agg Row Header', () => {
    it('renders grain columns as chips', async () => {
      const djClient = createMockDjClient();

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        // Should show short names from grain columns
        expect(screen.getByText('date_id')).toBeInTheDocument();
        expect(screen.getByText('customer_id')).toBeInTheDocument();
      });
    });

    it('renders measure count', async () => {
      const djClient = createMockDjClient();

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        expect(screen.getByText('2 measures')).toBeInTheDocument();
        expect(screen.getByText('1 measure')).toBeInTheDocument();
      });
    });

    it('renders metric count badge when related metrics exist', async () => {
      const djClient = createMockDjClient();

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        expect(screen.getByText('2 metrics')).toBeInTheDocument();
      });
    });

    it('renders status badges', async () => {
      const djClient = createMockDjClient();

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        expect(screen.getByText('Active')).toBeInTheDocument();
        expect(screen.getByText('Pending')).toBeInTheDocument();
      });
    });

    it('renders schedule in human-readable format', async () => {
      const djClient = createMockDjClient();

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        // Schedule appears in both header and config section, so use getAllByText
        const scheduleElements = screen.getAllByText(/at 12:00 am/i);
        expect(scheduleElements.length).toBeGreaterThan(0);
      });
    });
  });

  describe('Expanded Details', () => {
    it('expands first pre-agg by default', async () => {
      const djClient = createMockDjClient();

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        // Config section should be visible for the first expanded preagg
        expect(screen.getByText('Config')).toBeInTheDocument();
        expect(screen.getByText('Grain')).toBeInTheDocument();
        expect(screen.getByText('Measures')).toBeInTheDocument();
      });
    });

    it('shows Config section with strategy and schedule', async () => {
      const djClient = createMockDjClient();

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        expect(screen.getByText('Strategy')).toBeInTheDocument();
        expect(screen.getByText('Full')).toBeInTheDocument();
        expect(screen.getByText('Schedule')).toBeInTheDocument();
      });
    });

    it('shows Grain section with dimension links', async () => {
      const djClient = createMockDjClient();

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        // Full grain column names should appear in expanded section
        const grainBadges = screen.getAllByText(
          /default\.(date_dim|customer_dim)\./,
        );
        expect(grainBadges.length).toBeGreaterThan(0);
      });
    });

    it('shows Measures table with aggregation and merge info', async () => {
      const djClient = createMockDjClient();

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        expect(screen.getByText('total_revenue')).toBeInTheDocument();
        expect(screen.getByText('SUM(revenue)')).toBeInTheDocument();
      });
    });

    it('shows metrics that use each measure', async () => {
      const djClient = createMockDjClient();

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        // Display names should appear
        expect(screen.getByText('Revenue')).toBeInTheDocument();
        expect(screen.getByText('Order Count')).toBeInTheDocument();
      });
    });

    it('toggles expansion when clicking row header', async () => {
      // Use single preagg to simplify test
      const singlePreagg = {
        items: [mockPreaggs.items[0]],
        total: 1,
        limit: 50,
        offset: 0,
      };
      const djClient = createMockDjClient(singlePreagg);

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      // Wait for initial render with expanded state
      await waitFor(() => {
        expect(screen.getByText('Config')).toBeInTheDocument();
      });

      // Find and click the row header to collapse it
      const measureText = screen.getByText('2 measures');
      fireEvent.click(measureText.closest('.preagg-row-header'));

      // After collapse, Config should no longer be visible
      await waitFor(
        () => {
          expect(screen.queryByText('Config')).not.toBeInTheDocument();
        },
        { timeout: 2000 },
      );
    });
  });

  describe('Workflow Actions', () => {
    it('renders workflow button with capitalized label', async () => {
      const djClient = createMockDjClient();

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        const scheduledBtn = screen.getByText('Scheduled');
        expect(scheduledBtn).toBeInTheDocument();
        expect(scheduledBtn.closest('a')).toHaveAttribute(
          'href',
          'http://scheduler/workflow/123.main',
        );
      });
    });

    it('renders deactivate button for active workflows', async () => {
      const djClient = createMockDjClient();

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        expect(screen.getByText('Deactivate')).toBeInTheDocument();
      });
    });

    it('calls deactivatePreaggWorkflow when deactivate is clicked', async () => {
      const djClient = createMockDjClient();
      window.confirm = jest.fn(() => true);

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(
        () => {
          expect(screen.getByText('Deactivate')).toBeInTheDocument();
        },
        { timeout: 2000 },
      );

      fireEvent.click(screen.getByText('Deactivate'));

      await waitFor(
        () => {
          expect(
            djClient.DataJunctionAPI.deactivatePreaggWorkflow,
          ).toHaveBeenCalledWith(1);
        },
        { timeout: 2000 },
      );
    });
  });

  describe('Stale Pre-aggregations', () => {
    it('shows stale version warning in row header', async () => {
      const djClient = createMockDjClient(mockPreaggsWithStale);

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        expect(screen.getByText('was v0.9')).toBeInTheDocument();
      });
    });

    it('shows "Deactivate All Stale" button when active stale workflows exist', async () => {
      const djClient = createMockDjClient(mockPreaggsWithStale);

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        expect(screen.getByText('Deactivate All Stale')).toBeInTheDocument();
      });
    });

    it('calls bulkDeactivatePreaggWorkflows when "Deactivate All Stale" is clicked', async () => {
      const djClient = createMockDjClient(mockPreaggsWithStale);
      window.confirm = jest.fn(() => true);

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(
        () => {
          expect(screen.getByText('Deactivate All Stale')).toBeInTheDocument();
        },
        { timeout: 2000 },
      );

      fireEvent.click(screen.getByText('Deactivate All Stale'));

      await waitFor(
        () => {
          expect(
            djClient.DataJunctionAPI.bulkDeactivatePreaggWorkflows,
          ).toHaveBeenCalledWith('default.orders_fact', true);
        },
        { timeout: 2000 },
      );
    });
  });

  describe('Grain Truncation', () => {
    it('shows "+N more" button when grain has more than MAX_VISIBLE_GRAIN columns', async () => {
      const manyGrainPreaggs = {
        items: [
          {
            ...mockPreaggs.items[0],
            grain_columns: [
              'default.dim1.col1',
              'default.dim2.col2',
              'default.dim3.col3',
              'default.dim4.col4',
              'default.dim5.col5',
              'default.dim6.col6',
              'default.dim7.col7',
              'default.dim8.col8',
              'default.dim9.col9',
              'default.dim10.col10',
              'default.dim11.col11',
              'default.dim12.col12',
            ],
          },
        ],
        total: 1,
        limit: 50,
        offset: 0,
      };
      const djClient = createMockDjClient(manyGrainPreaggs);

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        // Should show "+N more" since there are 12 columns and MAX_VISIBLE_GRAIN is 10
        expect(screen.getByText('+2 more')).toBeInTheDocument();
      });
    });

    it('shows "Show less" after expanding grain list', async () => {
      const manyGrainPreaggs = {
        items: [
          {
            ...mockPreaggs.items[0],
            grain_columns: [
              'default.dim1.col1',
              'default.dim2.col2',
              'default.dim3.col3',
              'default.dim4.col4',
              'default.dim5.col5',
              'default.dim6.col6',
              'default.dim7.col7',
              'default.dim8.col8',
              'default.dim9.col9',
              'default.dim10.col10',
              'default.dim11.col11',
              'default.dim12.col12',
            ],
          },
        ],
        total: 1,
        limit: 50,
        offset: 0,
      };
      const djClient = createMockDjClient(manyGrainPreaggs);

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(
        () => {
          expect(screen.getByText('+2 more')).toBeInTheDocument();
        },
        { timeout: 2000 },
      );

      // Click to expand
      fireEvent.click(screen.getByText('+2 more'));

      await waitFor(
        () => {
          expect(screen.getByText('Show less')).toBeInTheDocument();
          // All columns should now be visible
          expect(screen.getByText('default.dim12.col12')).toBeInTheDocument();
        },
        { timeout: 2000 },
      );
    });
  });

  describe('API Integration', () => {
    it('calls listPreaggs with include_stale=true', async () => {
      const djClient = createMockDjClient();

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(() => {
        expect(djClient.DataJunctionAPI.listPreaggs).toHaveBeenCalledWith({
          node_name: 'default.orders_fact',
          include_stale: true,
        });
      });
    });

    it('refreshes list after deactivate action', async () => {
      const djClient = createMockDjClient();
      window.confirm = jest.fn(() => true);

      renderWithContext(<NodePreAggregationsTab node={mockNode} />, djClient);

      await waitFor(
        () => {
          expect(screen.getByText('Deactivate')).toBeInTheDocument();
        },
        { timeout: 2000 },
      );

      fireEvent.click(screen.getByText('Deactivate'));

      await waitFor(
        () => {
          // Should be called twice: initial load + refresh after deactivate
          expect(djClient.DataJunctionAPI.listPreaggs).toHaveBeenCalledTimes(2);
        },
        { timeout: 2000 },
      );
    });
  });
});
