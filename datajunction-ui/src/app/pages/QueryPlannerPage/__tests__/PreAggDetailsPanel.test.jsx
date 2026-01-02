import { render, screen, fireEvent } from '@testing-library/react';
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
      expect(screen.getByText('Generated Query Overview')).toBeInTheDocument();
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

    it('shows aggregability badge', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByText('FULL')).toBeInTheDocument();
      expect(screen.getByText('LIMITED')).toBeInTheDocument();
    });

    it('displays grain columns', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByText('date_id, customer_id')).toBeInTheDocument();
      expect(screen.getByText('warehouse_id')).toBeInTheDocument();
    });

    it('shows materialization status', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getAllByText('Not materialized').length).toBe(2);
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

    it('shows copy SQL button', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByText('Copy SQL')).toBeInTheDocument();
    });

    it('renders SQL in syntax highlighter', () => {
      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);
      expect(screen.getByTestId('syntax-highlighter')).toBeInTheDocument();
      expect(screen.getByText(mockMetricsResult.sql)).toBeInTheDocument();
    });

    it('copies SQL to clipboard when copy button clicked', () => {
      const mockClipboard = { writeText: jest.fn() };
      Object.assign(navigator, { clipboard: mockClipboard });

      renderWithRouter(<QueryOverviewPanel {...defaultProps} />);

      fireEvent.click(screen.getByText('Copy SQL'));
      expect(mockClipboard.writeText).toHaveBeenCalledWith(
        mockMetricsResult.sql,
      );
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
});
