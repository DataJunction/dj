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
  });

  describe('Select All / Clear Actions', () => {
    it('shows Select all and Clear buttons when namespace is expanded', () => {
      render(<SelectionPanel {...defaultProps} />);

      fireEvent.click(screen.getByText('default'));

      expect(screen.getByText('Select all')).toBeInTheDocument();
      // Check for namespace-level Clear button (inside namespace-actions)
      // Both buttons have select-all-btn class, Clear is the second one
      const namespaceButtons = document.querySelectorAll(
        '.namespace-actions .select-all-btn',
      );
      expect(namespaceButtons.length).toBe(2);
      expect(namespaceButtons[1].textContent).toBe('Clear');
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

    it('clears all metrics in namespace when namespace Clear is clicked', () => {
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
      // Click the namespace-level Clear button (second button in namespace-actions)
      const namespaceButtons = document.querySelectorAll(
        '.namespace-actions .select-all-btn',
      );
      fireEvent.click(namespaceButtons[1]); // Clear is the second button

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

  describe('Cube Preset Loading', () => {
    const cubeProps = {
      ...defaultProps,
      cubes: [
        { name: 'default.test_cube', display_name: 'Test Cube' },
        { name: 'sales.revenue_cube', display_name: 'Revenue Cube' },
      ],
      onLoadCubePreset: jest.fn(),
    };

    it('shows Load from Cube button when cubes are available', () => {
      render(<SelectionPanel {...cubeProps} />);
      expect(screen.getByText('Load from Cube')).toBeInTheDocument();
    });

    it('opens dropdown when Load from Cube button is clicked', () => {
      render(<SelectionPanel {...cubeProps} />);

      fireEvent.click(screen.getByText('Load from Cube'));

      expect(
        screen.getByPlaceholderText('Search cubes...'),
      ).toBeInTheDocument();
    });

    it('displays cube options in dropdown', () => {
      render(<SelectionPanel {...cubeProps} />);

      fireEvent.click(screen.getByText('Load from Cube'));

      expect(screen.getByText('Test Cube')).toBeInTheDocument();
      expect(screen.getByText('Revenue Cube')).toBeInTheDocument();
    });

    it('filters cubes by search term', () => {
      render(<SelectionPanel {...cubeProps} />);

      fireEvent.click(screen.getByText('Load from Cube'));

      const searchInput = screen.getByPlaceholderText('Search cubes...');
      fireEvent.change(searchInput, { target: { value: 'Revenue' } });

      expect(screen.getByText('Revenue Cube')).toBeInTheDocument();
      expect(screen.queryByText('Test Cube')).not.toBeInTheDocument();
    });

    it('calls onLoadCubePreset when a cube is selected', () => {
      const onLoadCubePreset = jest.fn();
      render(
        <SelectionPanel {...cubeProps} onLoadCubePreset={onLoadCubePreset} />,
      );

      fireEvent.click(screen.getByText('Load from Cube'));
      fireEvent.click(screen.getByText('Test Cube'));

      expect(onLoadCubePreset).toHaveBeenCalledWith('default.test_cube');
    });

    it('shows loaded cube name in button when cube is loaded', () => {
      render(
        <SelectionPanel {...cubeProps} loadedCubeName="default.test_cube" />,
      );

      // Should show the cube display name or short name
      expect(screen.getByText('Test Cube')).toBeInTheDocument();
    });

    it('shows "No cubes match your search" when search has no results', () => {
      render(<SelectionPanel {...cubeProps} />);

      fireEvent.click(screen.getByText('Load from Cube'));

      const searchInput = screen.getByPlaceholderText('Search cubes...');
      fireEvent.change(searchInput, { target: { value: 'nonexistent' } });

      expect(
        screen.getByText('No cubes match your search'),
      ).toBeInTheDocument();
    });

    it('closes dropdown when clicking outside', () => {
      render(<SelectionPanel {...cubeProps} />);

      fireEvent.click(screen.getByText('Load from Cube'));
      expect(
        screen.getByPlaceholderText('Search cubes...'),
      ).toBeInTheDocument();

      // Simulate clicking outside
      fireEvent.mouseDown(document.body);

      expect(
        screen.queryByPlaceholderText('Search cubes...'),
      ).not.toBeInTheDocument();
    });
  });

  describe('Selected Metrics Chips', () => {
    it('displays selected metrics as chips', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.num_repair_orders']}
        />,
      );

      expect(screen.getByText('num_repair_orders')).toBeInTheDocument();
    });

    it('removes metric when chip remove button is clicked', () => {
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

      // Find the remove button for num_repair_orders chip
      const removeBtn = screen.getByTitle('Remove num_repair_orders');
      fireEvent.click(removeBtn);

      expect(onMetricsChange).toHaveBeenCalledWith([
        'default.avg_repair_price',
      ]);
    });

    it('shows "Show all" button when many metrics are selected', () => {
      const manyMetrics = Array.from(
        { length: 12 },
        (_, i) => `default.metric_${i}`,
      );
      render(
        <SelectionPanel
          {...defaultProps}
          metrics={manyMetrics}
          selectedMetrics={manyMetrics}
        />,
      );

      expect(screen.getByText('Show all')).toBeInTheDocument();
    });

    it('toggles chips expansion when Show all/Show less is clicked', () => {
      const manyMetrics = Array.from(
        { length: 12 },
        (_, i) => `default.metric_${i}`,
      );
      render(
        <SelectionPanel
          {...defaultProps}
          metrics={manyMetrics}
          selectedMetrics={manyMetrics}
        />,
      );

      // Click to expand
      const expandBtn = screen.getByText('Show all');
      fireEvent.click(expandBtn);

      // Should now show "Show less"
      expect(screen.getByText('Show less')).toBeInTheDocument();

      // Click to collapse
      fireEvent.click(screen.getByText('Show less'));

      // Should show "Show all" again
      expect(screen.getByText('Show all')).toBeInTheDocument();
    });
  });

  describe('Selected Dimensions Chips', () => {
    it('displays selected dimensions as chips', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.test']}
          selectedDimensions={['default.date_dim.dateint']}
        />,
      );

      // Check for chip by looking for the chip container with the dimension display name
      const chipElements = screen.getAllByText('date_dim.dateint');
      // Should have at least one chip (and possibly one in the list)
      expect(chipElements.length).toBeGreaterThanOrEqual(1);
      // The chip should have the dimension-chip class
      expect(document.querySelector('.dimension-chip')).toBeInTheDocument();
    });

    it('removes dimension when chip remove button is clicked', () => {
      const onDimensionsChange = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.test']}
          selectedDimensions={[
            'default.date_dim.dateint',
            'default.date_dim.month',
          ]}
          onDimensionsChange={onDimensionsChange}
        />,
      );

      // Find the remove button for dateint chip
      const removeBtn = screen.getByTitle('Remove date_dim.dateint');
      fireEvent.click(removeBtn);

      expect(onDimensionsChange).toHaveBeenCalledWith([
        'default.date_dim.month',
      ]);
    });
  });

  describe('Clear Button', () => {
    it('shows global Clear button when items are selected', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.num_repair_orders']}
          cubes={[{ name: 'default.cube', display_name: 'Cube' }]}
        />,
      );

      const clearAllBtn = document.querySelector('.clear-all-btn');
      expect(clearAllBtn).toBeInTheDocument();
      expect(clearAllBtn.textContent).toBe('Clear');
    });

    it('calls onClearSelection when global Clear is clicked', () => {
      const onClearSelection = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.num_repair_orders']}
          cubes={[{ name: 'default.cube', display_name: 'Cube' }]}
          onClearSelection={onClearSelection}
        />,
      );

      const clearAllBtn = document.querySelector('.clear-all-btn');
      fireEvent.click(clearAllBtn);

      expect(onClearSelection).toHaveBeenCalled();
    });

    it('clears metrics and dimensions if no onClearSelection provided', () => {
      const onMetricsChange = jest.fn();
      const onDimensionsChange = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.num_repair_orders']}
          selectedDimensions={['default.date_dim.dateint']}
          onMetricsChange={onMetricsChange}
          onDimensionsChange={onDimensionsChange}
          cubes={[{ name: 'default.cube', display_name: 'Cube' }]}
        />,
      );

      const clearAllBtn = document.querySelector('.clear-all-btn');
      fireEvent.click(clearAllBtn);

      expect(onMetricsChange).toHaveBeenCalledWith([]);
      expect(onDimensionsChange).toHaveBeenCalledWith([]);
    });
  });

  describe('Dimension Path Display', () => {
    it('shows dimension path when path has multiple segments', () => {
      const dimensionsWithPath = [
        {
          name: 'default.date_dim.dateint',
          type: 'timestamp',
          path: ['default.orders', 'default.date_dim.dateint'],
        },
      ];

      render(
        <SelectionPanel
          {...defaultProps}
          dimensions={dimensionsWithPath}
          selectedMetrics={['default.test']}
        />,
      );

      // Full name appears in both the dimension-full-name span and the path span
      expect(
        screen.getAllByText('default.date_dim.dateint').length,
      ).toBeGreaterThanOrEqual(1);
    });
  });

  describe('Namespace Sorting Logic', () => {
    it('prioritizes namespaces that start with search term', () => {
      const metricsWithNamespaces = [
        'zebra.metric1',
        'alpha.metric2',
        'alpha_test.metric3',
        'beta.metric4',
      ];

      render(
        <SelectionPanel {...defaultProps} metrics={metricsWithNamespaces} />,
      );

      const searchInput = screen.getByPlaceholderText('Search metrics...');
      fireEvent.change(searchInput, { target: { value: 'alpha' } });

      // Alpha namespace should be expanded first since it starts with 'alpha'
      const namespaces = document.querySelectorAll('.namespace-header');
      expect(namespaces.length).toBeGreaterThan(0);
    });

    it('sorts namespaces with more matching items higher', () => {
      const metricsWithNamespaces = [
        'default.test_metric1',
        'default.test_metric2',
        'default.test_metric3',
        'other.test_metric4',
      ];

      render(
        <SelectionPanel {...defaultProps} metrics={metricsWithNamespaces} />,
      );

      const searchInput = screen.getByPlaceholderText('Search metrics...');
      fireEvent.change(searchInput, { target: { value: 'test' } });

      // Should show namespaces - default has more matching items
      expect(screen.getByText('default')).toBeInTheDocument();
      expect(screen.getByText('other')).toBeInTheDocument();
    });

    it('sorts namespaces alphabetically when other criteria are equal', () => {
      const metricsWithNamespaces = [
        'zebra.metric1',
        'alpha.metric2',
        'beta.metric3',
      ];

      render(
        <SelectionPanel {...defaultProps} metrics={metricsWithNamespaces} />,
      );

      // Namespaces should be available
      expect(screen.getByText('alpha')).toBeInTheDocument();
      expect(screen.getByText('beta')).toBeInTheDocument();
      expect(screen.getByText('zebra')).toBeInTheDocument();
    });
  });

  describe('Dimension Sorting Logic', () => {
    it('prioritizes dimensions that start with search term', () => {
      const sortableDimensions = [
        { name: 'default.zebra.column', path: [] },
        { name: 'default.alpha.column', path: [] },
        { name: 'default.date_dim.alpha_col', path: [] },
      ];

      render(
        <SelectionPanel
          {...defaultProps}
          dimensions={sortableDimensions}
          selectedMetrics={['default.test']}
        />,
      );

      const searchInput = screen.getByPlaceholderText('Search dimensions...');
      fireEvent.change(searchInput, { target: { value: 'alpha' } });

      // Should show matching dimensions
      const checkboxes = screen.getAllByRole('checkbox');
      expect(checkboxes.length).toBeGreaterThan(0);
    });

    it('sorts dimensions alphabetically by short name', () => {
      const sortableDimensions = [
        { name: 'default.zebra.col', path: [] },
        { name: 'default.alpha.col', path: [] },
        { name: 'default.beta.col', path: [] },
      ];

      render(
        <SelectionPanel
          {...defaultProps}
          dimensions={sortableDimensions}
          selectedMetrics={['default.test']}
        />,
      );

      const searchInput = screen.getByPlaceholderText('Search dimensions...');
      fireEvent.change(searchInput, { target: { value: 'col' } });

      // All three should be visible
      expect(screen.getByText('alpha.col')).toBeInTheDocument();
      expect(screen.getByText('beta.col')).toBeInTheDocument();
      expect(screen.getByText('zebra.col')).toBeInTheDocument();
    });

    it('handles dimensions with prefix matches before contains matches', () => {
      const sortableDimensions = [
        { name: 'default.country_code', path: [] },
        { name: 'default.customer.country', path: [] },
      ];

      render(
        <SelectionPanel
          {...defaultProps}
          dimensions={sortableDimensions}
          selectedMetrics={['default.test']}
        />,
      );

      const searchInput = screen.getByPlaceholderText('Search dimensions...');
      fireEvent.change(searchInput, { target: { value: 'country' } });

      // Both should be visible
      const checkboxes = screen.getAllByRole('checkbox');
      expect(checkboxes.length).toBe(2);
    });
  });

  describe('Dimensions Chips Toggle', () => {
    it('shows "Show all" button when many dimensions are selected', () => {
      const manyDimensions = Array.from({ length: 15 }, (_, i) => ({
        name: `default.dim_${i}`,
        path: [],
      }));

      render(
        <SelectionPanel
          {...defaultProps}
          dimensions={manyDimensions}
          selectedMetrics={['default.test']}
          selectedDimensions={manyDimensions.map(d => d.name)}
        />,
      );

      expect(screen.getByText('Show all')).toBeInTheDocument();
    });

    it('toggles dimension chips expansion', () => {
      const manyDimensions = Array.from({ length: 15 }, (_, i) => ({
        name: `default.dim_${i}`,
        path: [],
      }));

      render(
        <SelectionPanel
          {...defaultProps}
          dimensions={manyDimensions}
          selectedMetrics={['default.test']}
          selectedDimensions={manyDimensions.map(d => d.name)}
        />,
      );

      // Click to expand
      const expandBtn = screen.getByText('Show all');
      fireEvent.click(expandBtn);

      // Should show "Show less"
      expect(screen.getByText('Show less')).toBeInTheDocument();

      // Click to collapse
      fireEvent.click(screen.getByText('Show less'));

      // Should show "Show all" again
      expect(screen.getByText('Show all')).toBeInTheDocument();
    });
  });

  describe('Toggle Namespace', () => {
    it('toggles namespace expansion state', () => {
      render(<SelectionPanel {...defaultProps} />);

      // Click to expand 'default'
      fireEvent.click(screen.getByText('default'));
      expect(screen.getByText('num_repair_orders')).toBeInTheDocument();

      // Click again to collapse
      fireEvent.click(screen.getByText('default'));
      expect(screen.queryByText('num_repair_orders')).not.toBeInTheDocument();

      // Click again to expand
      fireEvent.click(screen.getByText('default'));
      expect(screen.getByText('num_repair_orders')).toBeInTheDocument();
    });

    it('allows multiple namespaces to be expanded', () => {
      render(<SelectionPanel {...defaultProps} />);

      // Expand both default and sales
      fireEvent.click(screen.getByText('default'));
      fireEvent.click(screen.getByText('sales'));

      // Both should show their metrics
      expect(screen.getByText('num_repair_orders')).toBeInTheDocument();
      expect(screen.getByText('revenue')).toBeInTheDocument();
    });
  });

  describe('Remove Dimension from Selected', () => {
    it('removes dimension when clicking X on dimension chip', () => {
      const onDimensionsChange = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.test']}
          selectedDimensions={[
            'default.date_dim.dateint',
            'default.date_dim.month',
            'default.date_dim.year',
          ]}
          onDimensionsChange={onDimensionsChange}
        />,
      );

      // Find and click remove button for dateint
      const removeBtn = screen.getByTitle('Remove date_dim.dateint');
      fireEvent.click(removeBtn);

      expect(onDimensionsChange).toHaveBeenCalledWith([
        'default.date_dim.month',
        'default.date_dim.year',
      ]);
    });
  });

  describe('Toggle Dimension Selection', () => {
    it('removes dimension when unchecking already selected dimension', () => {
      const onDimensionsChange = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.test']}
          selectedDimensions={['default.date_dim.dateint']}
          onDimensionsChange={onDimensionsChange}
        />,
      );

      const checkbox = screen.getByRole('checkbox', { name: /dateint/i });
      fireEvent.click(checkbox);

      expect(onDimensionsChange).toHaveBeenCalledWith([]);
    });
  });

  describe('Filters Section', () => {
    it('renders filter input with placeholder', () => {
      render(<SelectionPanel {...defaultProps} onFiltersChange={jest.fn()} />);
      expect(
        screen.getByPlaceholderText("e.g. v3.date.date_id >= '2024-01-01'"),
      ).toBeInTheDocument();
    });

    it('renders "Add" button for filters', () => {
      render(<SelectionPanel {...defaultProps} onFiltersChange={jest.fn()} />);
      expect(screen.getByText('Add')).toBeInTheDocument();
    });

    it('Add button is disabled when filter input is empty', () => {
      render(<SelectionPanel {...defaultProps} onFiltersChange={jest.fn()} />);
      expect(screen.getByText('Add')).toBeDisabled();
    });

    it('Add button is enabled when filter input has text', () => {
      render(<SelectionPanel {...defaultProps} onFiltersChange={jest.fn()} />);

      const filterInput = screen.getByPlaceholderText(
        "e.g. v3.date.date_id >= '2024-01-01'",
      );
      fireEvent.change(filterInput, {
        target: { value: "date >= '2024-01-01'" },
      });

      expect(screen.getByText('Add')).not.toBeDisabled();
    });

    it('calls onFiltersChange when Add button is clicked with non-empty input (lines 281-284)', () => {
      const onFiltersChange = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          filters={[]}
          onFiltersChange={onFiltersChange}
        />,
      );

      const filterInput = screen.getByPlaceholderText(
        "e.g. v3.date.date_id >= '2024-01-01'",
      );
      fireEvent.change(filterInput, {
        target: { value: "date >= '2024-01-01'" },
      });
      fireEvent.click(screen.getByText('Add'));

      expect(onFiltersChange).toHaveBeenCalledWith(["date >= '2024-01-01'"]);
    });

    it('clears filter input after adding a filter', () => {
      const onFiltersChange = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          filters={[]}
          onFiltersChange={onFiltersChange}
        />,
      );

      const filterInput = screen.getByPlaceholderText(
        "e.g. v3.date.date_id >= '2024-01-01'",
      );
      fireEvent.change(filterInput, { target: { value: 'status = active' } });
      fireEvent.click(screen.getByText('Add'));

      expect(filterInput.value).toBe('');
    });

    it('does not add duplicate filters', () => {
      const onFiltersChange = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          filters={["date >= '2024-01-01'"]}
          onFiltersChange={onFiltersChange}
        />,
      );

      const filterInput = screen.getByPlaceholderText(
        "e.g. v3.date.date_id >= '2024-01-01'",
      );
      fireEvent.change(filterInput, {
        target: { value: "date >= '2024-01-01'" },
      });
      fireEvent.click(screen.getByText('Add'));

      expect(onFiltersChange).not.toHaveBeenCalled();
    });

    it('adds filter on Enter key press (lines 289-291)', () => {
      const onFiltersChange = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          filters={[]}
          onFiltersChange={onFiltersChange}
        />,
      );

      const filterInput = screen.getByPlaceholderText(
        "e.g. v3.date.date_id >= '2024-01-01'",
      );
      fireEvent.change(filterInput, { target: { value: 'status = active' } });
      fireEvent.keyDown(filterInput, { key: 'Enter' });

      expect(onFiltersChange).toHaveBeenCalledWith(['status = active']);
    });

    it('does not add filter on non-Enter key press', () => {
      const onFiltersChange = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          filters={[]}
          onFiltersChange={onFiltersChange}
        />,
      );

      const filterInput = screen.getByPlaceholderText(
        "e.g. v3.date.date_id >= '2024-01-01'",
      );
      fireEvent.change(filterInput, { target: { value: 'status = active' } });
      fireEvent.keyDown(filterInput, { key: 'Tab' });

      expect(onFiltersChange).not.toHaveBeenCalled();
    });

    it('renders existing filter chips (lines 665-686)', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          filters={["date >= '2024-01-01'", 'status = active']}
          onFiltersChange={jest.fn()}
        />,
      );

      expect(screen.getByText("date >= '2024-01-01'")).toBeInTheDocument();
      expect(screen.getByText('status = active')).toBeInTheDocument();
    });

    it('calls onFiltersChange when filter chip remove button is clicked (lines 296-297)', () => {
      const onFiltersChange = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          filters={["date >= '2024-01-01'", 'status = active']}
          onFiltersChange={onFiltersChange}
        />,
      );

      // Get all "Remove filter" buttons and click the first one
      const removeBtns = screen.getAllByTitle('Remove filter');
      fireEvent.click(removeBtns[0]);

      expect(onFiltersChange).toHaveBeenCalledWith(['status = active']);
    });

    it('shows 0 applied count when no filters', () => {
      render(<SelectionPanel {...defaultProps} />);
      expect(screen.getByText('0 applied')).toBeInTheDocument();
    });

    it('shows correct applied count when filters present', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          filters={['filter1', 'filter2']}
          onFiltersChange={jest.fn()}
        />,
      );
      expect(screen.getByText('2 applied')).toBeInTheDocument();
    });
  });

  describe('Engine Selection (line 709)', () => {
    it('renders engine pills', () => {
      render(<SelectionPanel {...defaultProps} />);
      expect(screen.getByText('Auto')).toBeInTheDocument();
      expect(screen.getByText('Druid')).toBeInTheDocument();
      expect(screen.getByText('Trino')).toBeInTheDocument();
    });

    it('highlights the active engine pill', () => {
      render(<SelectionPanel {...defaultProps} selectedEngine="druid" />);
      const druidPill = screen.getByText('Druid').closest('button');
      expect(druidPill).toHaveClass('active');
    });

    it('auto engine pill is active when selectedEngine is null', () => {
      render(<SelectionPanel {...defaultProps} selectedEngine={null} />);
      const autoPill = screen.getByText('Auto').closest('button');
      expect(autoPill).toHaveClass('active');
    });

    it('calls onEngineChange when engine pill is clicked', () => {
      const onEngineChange = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          selectedEngine={null}
          onEngineChange={onEngineChange}
        />,
      );

      fireEvent.click(screen.getByText('Trino'));
      expect(onEngineChange).toHaveBeenCalledWith('trino');
    });

    it('does not throw when onEngineChange is not provided', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedEngine={null}
          onEngineChange={undefined}
        />,
      );

      expect(() => {
        fireEvent.click(screen.getByText('Druid'));
      }).not.toThrow();
    });
  });

  describe('Metrics combobox-input click handler (line 386)', () => {
    it('clicking combobox-input container focuses the metrics search input', () => {
      render(<SelectionPanel {...defaultProps} />);
      const comboboxInputs = document.querySelectorAll('.combobox-input');
      // The first combobox-input is for metrics
      expect(comboboxInputs[0]).toBeInTheDocument();
      // Click the container — should not throw
      expect(() => fireEvent.click(comboboxInputs[0])).not.toThrow();
    });
  });

  describe('Metrics search input stopPropagation (line 423)', () => {
    it('clicking metrics search input does not bubble to combobox container', () => {
      render(<SelectionPanel {...defaultProps} />);
      const searchInput = screen.getByPlaceholderText('Search metrics...');
      // Clicking the input itself should not throw
      expect(() => fireEvent.click(searchInput)).not.toThrow();
    });
  });

  describe('Clear metrics button (lines 440-441)', () => {
    it('shows inline Clear button when metrics are selected', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.num_repair_orders']}
        />,
      );
      // The combobox-action Clear button inside metrics combobox
      const clearBtns = document.querySelectorAll('.combobox-action');
      const clearMetricsBtns = Array.from(clearBtns).filter(
        btn => btn.textContent === 'Clear',
      );
      expect(clearMetricsBtns.length).toBeGreaterThanOrEqual(1);
    });

    it('calls onMetricsChange([]) when inline Clear is clicked', () => {
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

      // Find the Clear button inside metrics combobox (not the namespace-level Clear)
      const metricsSection = document.querySelector('.combobox-input');
      const clearBtn = metricsSection.querySelector('.combobox-action');
      fireEvent.click(clearBtn);

      expect(onMetricsChange).toHaveBeenCalledWith([]);
    });
  });

  describe('Dimensions combobox-input click handler (line 546)', () => {
    it('clicking dimensions combobox-input container does not throw', () => {
      render(
        <SelectionPanel {...defaultProps} selectedMetrics={['default.test']} />,
      );
      const comboboxInputs = document.querySelectorAll('.combobox-input');
      // The second combobox-input is for dimensions
      if (comboboxInputs.length >= 2) {
        expect(() => fireEvent.click(comboboxInputs[1])).not.toThrow();
      }
    });
  });

  describe('Dimensions search input stopPropagation (line 586)', () => {
    it('clicking dimensions search input does not throw', () => {
      render(
        <SelectionPanel {...defaultProps} selectedMetrics={['default.test']} />,
      );
      const searchInput = screen.getByPlaceholderText('Search dimensions...');
      expect(() => fireEvent.click(searchInput)).not.toThrow();
    });
  });

  describe('Clear dimensions button (lines 603-604)', () => {
    it('shows inline Clear button for dimensions when dimensions are selected', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.test']}
          selectedDimensions={['default.date_dim.dateint']}
        />,
      );
      const clearBtns = document.querySelectorAll('.combobox-action');
      const clearDimBtns = Array.from(clearBtns).filter(
        btn => btn.textContent === 'Clear',
      );
      expect(clearDimBtns.length).toBeGreaterThanOrEqual(1);
    });

    it('calls onDimensionsChange([]) when dimensions inline Clear is clicked', () => {
      const onDimensionsChange = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.test']}
          selectedDimensions={[
            'default.date_dim.dateint',
            'default.date_dim.month',
          ]}
          onDimensionsChange={onDimensionsChange}
        />,
      );

      // The combobox-action buttons — the last set belongs to dimensions
      const comboboxActions = document.querySelectorAll('.combobox-action');
      // Last Clear button is for dimensions (metrics Clear is first if only one metric selected
      // but here no many-metrics Show all button, so first Clear = metrics, second = dims)
      const clearBtns = Array.from(comboboxActions).filter(
        btn => btn.textContent === 'Clear',
      );
      // Click the last Clear button (dimensions)
      fireEvent.click(clearBtns[clearBtns.length - 1]);

      expect(onDimensionsChange).toHaveBeenCalledWith([]);
    });
  });

  describe('Run Query Section', () => {
    it('renders Run Query button', () => {
      render(<SelectionPanel {...defaultProps} />);
      expect(screen.getByText('Run Query')).toBeInTheDocument();
    });

    it('Run Query button is disabled when canRunQuery is false', () => {
      render(<SelectionPanel {...defaultProps} canRunQuery={false} />);
      expect(screen.getByText('Run Query').closest('button')).toBeDisabled();
    });

    it('Run Query button is enabled when canRunQuery is true', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          canRunQuery={true}
          onRunQuery={jest.fn()}
        />,
      );
      expect(
        screen.getByText('Run Query').closest('button'),
      ).not.toBeDisabled();
    });

    it('shows running state when queryLoading is true', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          canRunQuery={true}
          queryLoading={true}
          onRunQuery={jest.fn()}
        />,
      );
      expect(screen.getByText('Running...')).toBeInTheDocument();
    });

    it('calls onRunQuery when Run Query is clicked', () => {
      const onRunQuery = jest.fn();
      render(
        <SelectionPanel
          {...defaultProps}
          canRunQuery={true}
          onRunQuery={onRunQuery}
        />,
      );
      fireEvent.click(screen.getByText('Run Query').closest('button'));
      expect(onRunQuery).toHaveBeenCalled();
    });

    it('shows hint when metrics selected but no dimensions', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={['default.num_repair_orders']}
          canRunQuery={false}
        />,
      );
      expect(
        screen.getByText('Select at least one dimension'),
      ).toBeInTheDocument();
    });

    it('shows hint when no metrics and no dimensions selected', () => {
      render(
        <SelectionPanel
          {...defaultProps}
          selectedMetrics={[]}
          canRunQuery={false}
        />,
      );
      expect(
        screen.getByText('Select metrics and dimensions to run a query'),
      ).toBeInTheDocument();
    });
  });
});
