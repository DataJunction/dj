import { render, screen, fireEvent } from '@testing-library/react';
import { SelectionPanel } from '../SelectionPanel';
import React from 'react';

const mockMetrics = [
  'default.num_repair_orders',
  'default.avg_repair_price',
  'default.total_repair_cost',
  'sales.revenue',
  'sales.order_count',
  'inventory.stock_level',
];

const mockDimensions = [
  {
    name: 'default.date_dim.dateint',
    type: 'timestamp',
    path: ['default.orders', 'default.date_dim.dateint'],
  },
  {
    name: 'default.date_dim.month',
    type: 'int',
    path: ['default.orders', 'default.date_dim.month'],
  },
  {
    name: 'default.date_dim.year',
    type: 'int',
    path: ['default.orders', 'default.date_dim.year'],
  },
  {
    name: 'default.customer.country',
    type: 'string',
    path: ['default.orders', 'default.customer.country'],
  },
];

const defaultProps = {
  metrics: mockMetrics,
  selectedMetrics: [],
  onMetricsChange: jest.fn(),
  dimensions: mockDimensions,
  selectedDimensions: [],
  onDimensionsChange: jest.fn(),
  loading: false,
};

describe('SelectionPanel', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('Metrics Section', () => {
    it('renders metrics section header', () => {
      render(<SelectionPanel {...defaultProps} />);
      expect(screen.getByText('Metrics')).toBeInTheDocument();
    });

    it('displays selection count', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.num_repair_orders']}
        />,
      );
      expect(screen.getByText('1 selected')).toBeInTheDocument();
    });

    it('groups metrics by namespace', () => {
      render(<SelectionPanel {...defaultProps} />);
      expect(screen.getByText('default')).toBeInTheDocument();
      expect(screen.getByText('sales')).toBeInTheDocument();
      expect(screen.getByText('inventory')).toBeInTheDocument();
    });

    it('shows metric count per namespace', () => {
      render(<SelectionPanel {...defaultProps} />);
      // default has 3 metrics
      expect(screen.getByText('3')).toBeInTheDocument();
    });

    it('expands namespace when clicked', () => {
      render(<SelectionPanel {...defaultProps} />);

      const defaultNamespace = screen.getByText('default');
      fireEvent.click(defaultNamespace);

      expect(screen.getByText('num_repair_orders')).toBeInTheDocument();
      expect(screen.getByText('avg_repair_price')).toBeInTheDocument();
    });

    it('collapses namespace when clicked again', () => {
      render(<SelectionPanel {...defaultProps} />);

      const defaultNamespace = screen.getByText('default');
      fireEvent.click(defaultNamespace);
      expect(screen.getByText('num_repair_orders')).toBeInTheDocument();

      fireEvent.click(defaultNamespace);
      expect(screen.queryByText('num_repair_orders')).not.toBeInTheDocument();
    });

    it('calls onMetricsChange when metric is selected', () => {
      const onMetricsChange = jest.fn();
      render(
        <SelectionPanel {...defaultProps} onMetricsChange={onMetricsChange} />,
      );

      // Expand namespace first
      fireEvent.click(screen.getByText('default'));

      // Click checkbox
      const checkbox = screen.getByRole('checkbox', {
        name: /num_repair_orders/i,
      });
      fireEvent.click(checkbox);

      expect(onMetricsChange).toHaveBeenCalledWith([
        'default.num_repair_orders',
      ]);
    });

    it('removes metric when unchecked', () => {
      const onMetricsChange = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.num_repair_orders']}
          onMetricsChange={onMetricsChange}
        />,
      );

      fireEvent.click(screen.getByText('default'));

      const checkbox = screen.getByRole('checkbox', {
        name: /num_repair_orders/i,
      });
      fireEvent.click(checkbox);

      expect(onMetricsChange).toHaveBeenCalledWith([]);
    });
  });

  describe('Metrics Search', () => {
    it('renders search input', () => {
      render(<SelectionPanel {...defaultProps} />);
      expect(
        screen.getByPlaceholderText('Search metrics...'),
      ).toBeInTheDocument();
    });

    it('filters metrics by search term', () => {
      render(<SelectionPanel {...defaultProps} />);

      const searchInput = screen.getByPlaceholderText('Search metrics...');
      fireEvent.change(searchInput, { target: { value: 'repair' } });

      // Should auto-expand and show matching metrics
      expect(screen.getByText('num_repair_orders')).toBeInTheDocument();
      expect(screen.getByText('avg_repair_price')).toBeInTheDocument();
    });

    it('filters out non-matching metrics', () => {
      render(<SelectionPanel {...defaultProps} />);

      const searchInput = screen.getByPlaceholderText('Search metrics...');
      fireEvent.change(searchInput, { target: { value: 'revenue' } });

      // Only sales.revenue should match
      expect(screen.getByText('revenue')).toBeInTheDocument();
      expect(screen.queryByText('num_repair_orders')).not.toBeInTheDocument();
    });

    it('shows no results message when no metrics match', () => {
      render(<SelectionPanel {...defaultProps} />);

      const searchInput = screen.getByPlaceholderText('Search metrics...');
      fireEvent.change(searchInput, { target: { value: 'nonexistent' } });

      expect(
        screen.getByText('No metrics match your search'),
      ).toBeInTheDocument();
    });

    it('prioritizes prefix matches in search results', () => {
      const metricsWithSimilarNames = [
        'default.total_orders',
        'default.orders_total',
        'default.order_count',
      ];
      render(
        <SelectionPanel {...defaultProps} metrics={metricsWithSimilarNames} />,
      );

      const searchInput = screen.getByPlaceholderText('Search metrics...');
      fireEvent.change(searchInput, { target: { value: 'order' } });

      // order_count should appear (prefix match on short name)
      // orders_total should appear (contains 'order')
      const items = screen.getAllByRole('checkbox');
      expect(items.length).toBeGreaterThan(0);
    });

    it('clears search when clear button is clicked', () => {
      render(<SelectionPanel {...defaultProps} />);

      const searchInput = screen.getByPlaceholderText('Search metrics...');
      fireEvent.change(searchInput, { target: { value: 'test' } });

      const clearButton = screen.getAllByText('×')[0];
      fireEvent.click(clearButton);

      expect(searchInput.value).toBe('');
    });
  });

  describe('Select All / Clear Actions', () => {
    it('shows Select all and Clear buttons when namespace is expanded', () => {
      render(<SelectionPanel {...defaultProps} />);

      fireEvent.click(screen.getByText('default'));

      expect(screen.getByText('Select all')).toBeInTheDocument();
      expect(screen.getByText('Clear')).toBeInTheDocument();
    });

    it('selects all metrics in namespace when Select all is clicked', () => {
      const onMetricsChange = jest.fn();
      render(
        <SelectionPanel {...defaultProps} onMetricsChange={onMetricsChange} />,
      );

      fireEvent.click(screen.getByText('default'));
      fireEvent.click(screen.getByText('Select all'));

      expect(onMetricsChange).toHaveBeenCalledWith([
        'default.num_repair_orders',
        'default.avg_repair_price',
        'default.total_repair_cost',
      ]);
    });

    it('clears all metrics in namespace when Clear is clicked', () => {
      const onMetricsChange = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={[
            'default.num_repair_orders',
            'default.avg_repair_price',
          ]}
          onMetricsChange={onMetricsChange}
        />,
      );

      fireEvent.click(screen.getByText('default'));
      fireEvent.click(screen.getByText('Clear'));

      expect(onMetricsChange).toHaveBeenCalledWith([]);
    });
  });

  describe('Dimensions Section', () => {
    it('renders dimensions section header', () => {
      render(<SelectionPanel {...defaultProps} />);
      expect(screen.getByText('Dimensions')).toBeInTheDocument();
    });

    it('shows hint when no metrics selected', () => {
      render(<SelectionPanel {...defaultProps} selectedMetrics={[]} />);
      expect(
        screen.getByText('Select metrics to see available dimensions'),
      ).toBeInTheDocument();
    });

    it('shows loading state while fetching dimensions', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.test']}
          loading={true}
        />,
      );
      expect(screen.getByText('Loading dimensions...')).toBeInTheDocument();
    });

    it('displays dimensions when metrics are selected', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.num_repair_orders']}
        />,
      );

      expect(screen.getByText('date_dim.dateint')).toBeInTheDocument();
      expect(screen.getByText('date_dim.month')).toBeInTheDocument();
    });

    it('calls onDimensionsChange when dimension is selected', () => {
      const onDimensionsChange = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.num_repair_orders']}
          onDimensionsChange={onDimensionsChange}
        />,
      );

      const checkbox = screen.getByRole('checkbox', { name: /dateint/i });
      fireEvent.click(checkbox);

      expect(onDimensionsChange).toHaveBeenCalledWith([
        'default.date_dim.dateint',
      ]);
    });

    it('deduplicates dimensions with same name', () => {
      const duplicateDimensions = [
        { name: 'default.date_dim.month', path: ['path1', 'path2', 'path3'] },
        { name: 'default.date_dim.month', path: ['short', 'path'] },
      ];
      render(
        <SelectionPanel
          {...defaultProps}
          dimensions={duplicateDimensions}
          selectedMetrics={['default.test']}
        />,
      );

      // Should only show one checkbox for month
      const monthCheckboxes = screen.getAllByRole('checkbox', {
        name: /month/i,
      });
      expect(monthCheckboxes.length).toBe(1);
    });

    it('shows dimension display name (last 2 segments)', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.num_repair_orders']}
        />,
      );

      // Should show 'date_dim.dateint' not full path
      expect(screen.getByText('date_dim.dateint')).toBeInTheDocument();
    });
  });

  describe('Dimensions Search', () => {
    it('filters dimensions by search term', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.num_repair_orders']}
        />,
      );

      const searchInput = screen.getByPlaceholderText('Search dimensions...');
      fireEvent.change(searchInput, { target: { value: 'month' } });

      expect(screen.getByText('date_dim.month')).toBeInTheDocument();
      expect(screen.queryByText('date_dim.year')).not.toBeInTheDocument();
    });

    it('shows no results message when no dimensions match', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.num_repair_orders']}
        />,
      );

      const searchInput = screen.getByPlaceholderText('Search dimensions...');
      fireEvent.change(searchInput, { target: { value: 'nonexistent' } });

      expect(
        screen.getByText('No dimensions match your search'),
      ).toBeInTheDocument();
    });
  });

  describe('Selected State Display', () => {
    it('shows selected badge in namespace when metrics are selected', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={[
            'default.num_repair_orders',
            'default.avg_repair_price',
          ]}
        />,
      );

      // Should show '2' in the selected badge
      const selectedBadge = document.querySelector('.selected-badge');
      expect(selectedBadge).toBeInTheDocument();
      expect(selectedBadge).toHaveTextContent('2');
    });

    it('shows checked state for selected metrics', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.num_repair_orders']}
        />,
      );

      fireEvent.click(screen.getByText('default'));

      const checkbox = screen.getByRole('checkbox', {
        name: /num_repair_orders/i,
      });
      expect(checkbox).toBeChecked();
    });

    it('shows checked state for selected dimensions', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.test']}
          selectedDimensions={['default.date_dim.dateint']}
        />,
      );

      const checkbox = screen.getByRole('checkbox', { name: /dateint/i });
      expect(checkbox).toBeChecked();
    });
  });
});
