import { render, screen, fireEvent } from '@testing-library/react';
import React from 'react';

// Mock dagre module with inline class definition
jest.mock('dagre', () => {
  // Define mock graph class inside the mock factory
  function MockGraph() {
    this.setDefaultEdgeLabel = jest.fn().mockReturnValue(this);
    this.setGraph = jest.fn().mockReturnValue(this);
    this.setNode = jest.fn().mockReturnValue(this);
    this.setEdge = jest.fn().mockReturnValue(this);
    this.node = jest.fn().mockReturnValue({ x: 100, y: 100 });
  }

  return {
    graphlib: {
      Graph: MockGraph,
    },
    layout: jest.fn(),
  };
});

// Mock ReactFlow and related components
jest.mock('reactflow', () => {
  return {
    __esModule: true,
    default: ({ children, nodes, edges, onNodeClick, onPaneClick }) => (
      <div data-testid="react-flow" onClick={onPaneClick}>
        {nodes?.map(node => (
          <div
            key={node.id}
            data-testid={`node-${node.id}`}
            className={`flow-node ${node.type}`}
            onClick={e => {
              e.stopPropagation();
              onNodeClick?.(e, node);
            }}
          >
            {/* For metrics, use shortName; for preaggs, use name */}
            {node.data.shortName || node.data.name}
          </div>
        ))}
        {children}
      </div>
    ),
    Background: () => <div data-testid="background" />,
    Controls: () => <div data-testid="controls" />,
    MarkerType: { ArrowClosed: 'arrowclosed' },
    useNodesState: nodes => [nodes, jest.fn(), jest.fn()],
    useEdgesState: edges => [edges, jest.fn(), jest.fn()],
    Handle: ({ type, position }) => (
      <div data-testid={`handle-${type}-${position}`} />
    ),
    Position: { Left: 'left', Right: 'right' },
  };
});

// Import the component after mocks are set up
import { MetricFlowGraph } from '../MetricFlowGraph';

const mockGrainGroups = [
  {
    parent_name: 'default.repair_orders',
    grain: ['date_id', 'customer_id'],
    components: [
      { name: 'sum_revenue', expression: 'SUM(revenue)' },
      { name: 'count_orders', expression: 'COUNT(*)' },
    ],
  },
  {
    parent_name: 'inventory.stock',
    grain: ['warehouse_id'],
    components: [{ name: 'sum_quantity', expression: 'SUM(quantity)' }],
  },
];

const mockMetricFormulas = [
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
  {
    name: 'inventory.total_stock',
    short_name: 'total_stock',
    combiner: 'SUM(sum_quantity)',
    is_derived: false,
    components: ['sum_quantity'],
  },
];

describe('MetricFlowGraph', () => {
  describe('Empty State', () => {
    it('shows empty state when no grain groups provided', () => {
      render(
        <MetricFlowGraph
          grainGroups={[]}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={jest.fn()}
        />,
      );

      expect(
        screen.getByText(
          /Select metrics and dimensions above to visualize the data flow/i,
        ),
      ).toBeInTheDocument();
    });

    it('shows empty state when no metric formulas provided', () => {
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={[]}
          onNodeSelect={jest.fn()}
        />,
      );

      expect(
        screen.getByText(
          /Select metrics and dimensions above to visualize the data flow/i,
        ),
      ).toBeInTheDocument();
    });

    it('shows empty state when both are null', () => {
      render(
        <MetricFlowGraph
          grainGroups={null}
          metricFormulas={null}
          onNodeSelect={jest.fn()}
        />,
      );

      expect(
        screen.getByText(
          /Select metrics and dimensions above to visualize the data flow/i,
        ),
      ).toBeInTheDocument();
    });

    it('shows empty icon in empty state', () => {
      render(
        <MetricFlowGraph
          grainGroups={[]}
          metricFormulas={[]}
          onNodeSelect={jest.fn()}
        />,
      );

      expect(screen.getByText('◎')).toBeInTheDocument();
    });
  });

  describe('Graph Rendering', () => {
    it('renders ReactFlow when data is provided', () => {
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={jest.fn()}
        />,
      );

      expect(screen.getByTestId('react-flow')).toBeInTheDocument();
    });

    it('renders background and controls', () => {
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={jest.fn()}
        />,
      );

      expect(screen.getByTestId('background')).toBeInTheDocument();
      expect(screen.getByTestId('controls')).toBeInTheDocument();
    });

    it('renders pre-aggregation nodes', () => {
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={jest.fn()}
        />,
      );

      expect(screen.getByText('repair_orders')).toBeInTheDocument();
      expect(screen.getByText('stock')).toBeInTheDocument();
    });

    it('renders metric nodes', () => {
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={jest.fn()}
        />,
      );

      expect(screen.getByText('num_repair_orders')).toBeInTheDocument();
      expect(screen.getByText('avg_repair_price')).toBeInTheDocument();
      expect(screen.getByText('total_stock')).toBeInTheDocument();
    });
  });

  describe('Node Selection', () => {
    it('calls onNodeSelect with preagg data when preagg node is clicked', () => {
      const onNodeSelect = jest.fn();
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={onNodeSelect}
        />,
      );

      const preaggNode = screen.getByText('repair_orders');
      fireEvent.click(preaggNode);

      expect(onNodeSelect).toHaveBeenCalledWith(
        expect.objectContaining({
          type: 'preagg',
          index: 0,
          data: mockGrainGroups[0],
        }),
      );
    });

    it('calls onNodeSelect with metric data when metric node is clicked', () => {
      const onNodeSelect = jest.fn();
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={onNodeSelect}
        />,
      );

      const metricNode = screen.getByText('num_repair_orders');
      fireEvent.click(metricNode);

      expect(onNodeSelect).toHaveBeenCalledWith(
        expect.objectContaining({
          type: 'metric',
          index: 0,
          data: mockMetricFormulas[0],
        }),
      );
    });

    it('calls onNodeSelect with null when pane is clicked', () => {
      const onNodeSelect = jest.fn();
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={onNodeSelect}
        />,
      );

      const flowPane = screen.getByTestId('react-flow');
      fireEvent.click(flowPane);

      expect(onNodeSelect).toHaveBeenCalledWith(null);
    });
  });

  describe('Legend', () => {
    it('renders legend with Pre-agg, Metric, and Derived labels', () => {
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={jest.fn()}
        />,
      );

      expect(screen.getByText('Pre-agg')).toBeInTheDocument();
      expect(screen.getByText('Metric')).toBeInTheDocument();
      expect(screen.getByText('Derived')).toBeInTheDocument();
    });
  });

  describe('Node Types', () => {
    it('creates preagg nodes for each grain group', () => {
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={jest.fn()}
        />,
      );

      // 2 grain groups = 2 preagg nodes
      const preaggNodes = document.querySelectorAll('.flow-node.preagg');
      expect(preaggNodes.length).toBe(2);
    });

    it('creates metric nodes for each metric formula', () => {
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={jest.fn()}
        />,
      );

      // 3 metric formulas = 3 metric nodes
      const metricNodes = document.querySelectorAll('.flow-node.metric');
      expect(metricNodes.length).toBe(3);
    });
  });

  describe('Node Selection by Index', () => {
    it('selects correct preagg when second preagg is clicked', () => {
      const onNodeSelect = jest.fn();
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={onNodeSelect}
        />,
      );

      const stockNode = screen.getByText('stock');
      fireEvent.click(stockNode);

      expect(onNodeSelect).toHaveBeenCalledWith(
        expect.objectContaining({
          type: 'preagg',
          index: 1,
          data: mockGrainGroups[1],
        }),
      );
    });

    it('selects correct metric when derived metric is clicked', () => {
      const onNodeSelect = jest.fn();
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={onNodeSelect}
        />,
      );

      const derivedMetric = screen.getByText('avg_repair_price');
      fireEvent.click(derivedMetric);

      expect(onNodeSelect).toHaveBeenCalledWith(
        expect.objectContaining({
          type: 'metric',
          index: 1,
          data: expect.objectContaining({
            is_derived: true,
            short_name: 'avg_repair_price',
          }),
        }),
      );
    });
  });

  describe('Graph Layout', () => {
    it('uses dagre for layout computation', () => {
      const dagre = require('dagre');

      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={jest.fn()}
        />,
      );

      // dagre.layout should have been called
      expect(dagre.layout).toHaveBeenCalled();
    });
  });
});

describe('MetricFlowGraph Node Display', () => {
  it('shows parent name without namespace prefix', () => {
    render(
      <MetricFlowGraph
        grainGroups={[
          {
            parent_name: 'default.namespace.my_table',
            grain: ['id'],
            components: [{ name: 'comp1' }],
          },
        ]}
        metricFormulas={[
          {
            name: 'default.metric',
            short_name: 'metric',
            components: ['comp1'],
          },
        ]}
        onNodeSelect={jest.fn()}
      />,
    );

    // Should show just 'my_table', not the full path
    expect(screen.getByText('my_table')).toBeInTheDocument();
  });

  it('shows short name for metric', () => {
    render(
      <MetricFlowGraph
        grainGroups={mockGrainGroups}
        metricFormulas={mockMetricFormulas}
        onNodeSelect={jest.fn()}
      />,
    );

    // Should show short_name
    expect(screen.getByText('num_repair_orders')).toBeInTheDocument();
    expect(screen.getByText('avg_repair_price')).toBeInTheDocument();
  });

  describe('Branch Coverage - Edge Cases', () => {
    it('handles grain group with empty grain array', () => {
      const grainGroupsEmptyGrain = [
        {
          parent_name: 'default.orders',
          grain: [], // Empty grain array
          components: [{ name: 'count_orders', expression: 'COUNT(*)' }],
        },
      ];

      render(
        <MetricFlowGraph
          grainGroups={grainGroupsEmptyGrain}
          metricFormulas={[
            {
              name: 'default.metric',
              short_name: 'metric',
              components: ['count_orders'],
              is_derived: false,
            },
          ]}
          onNodeSelect={jest.fn()}
        />,
      );

      expect(screen.getByTestId('react-flow')).toBeInTheDocument();
    });

    it('handles grain group with no components', () => {
      const grainGroupsNoComponents = [
        {
          parent_name: 'default.orders',
          grain: ['date_id'],
          components: [], // Empty components
        },
      ];

      render(
        <MetricFlowGraph
          grainGroups={grainGroupsNoComponents}
          metricFormulas={[
            {
              name: 'default.metric',
              short_name: 'metric',
              components: [],
              is_derived: false,
            },
          ]}
          onNodeSelect={jest.fn()}
        />,
      );

      expect(screen.getByTestId('react-flow')).toBeInTheDocument();
    });

    it('handles grain group with undefined components', () => {
      const grainGroupsUndefinedComponents = [
        {
          parent_name: 'default.orders',
          grain: ['date_id'],
          // components is undefined
        },
      ];

      render(
        <MetricFlowGraph
          grainGroups={grainGroupsUndefinedComponents}
          metricFormulas={[
            {
              name: 'default.metric',
              short_name: 'metric',
              components: [],
              is_derived: false,
            },
          ]}
          onNodeSelect={jest.fn()}
        />,
      );

      expect(screen.getByTestId('react-flow')).toBeInTheDocument();
    });

    it('handles metric with is_derived false', () => {
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={[
            {
              name: 'default.simple_metric',
              short_name: 'simple_metric',
              combiner: 'SUM(count)',
              is_derived: false,
              components: ['count_orders'],
            },
          ]}
          onNodeSelect={jest.fn()}
        />,
      );

      expect(screen.getByText('simple_metric')).toBeInTheDocument();
    });

    it('handles metric with is_derived true', () => {
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={[
            {
              name: 'default.derived_metric',
              short_name: 'derived_metric',
              combiner: 'SUM(a) / SUM(b)',
              is_derived: true,
              components: ['sum_revenue', 'count_orders'],
            },
          ]}
          onNodeSelect={jest.fn()}
        />,
      );

      expect(screen.getByText('derived_metric')).toBeInTheDocument();
    });

    it('handles selectedNode prop for preagg', () => {
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={jest.fn()}
          selectedNode={{ type: 'preagg', index: 0, data: mockGrainGroups[0] }}
        />,
      );

      expect(screen.getByTestId('react-flow')).toBeInTheDocument();
    });

    it('handles selectedNode prop for metric', () => {
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={jest.fn()}
          selectedNode={{
            type: 'metric',
            index: 0,
            data: mockMetricFormulas[0],
          }}
        />,
      );

      expect(screen.getByTestId('react-flow')).toBeInTheDocument();
    });

    it('handles no selectedNode', () => {
      render(
        <MetricFlowGraph
          grainGroups={mockGrainGroups}
          metricFormulas={mockMetricFormulas}
          onNodeSelect={jest.fn()}
          selectedNode={null}
        />,
      );

      expect(screen.getByTestId('react-flow')).toBeInTheDocument();
    });

    it('handles grain group with undefined grain', () => {
      const grainGroupsUndefinedGrain = [
        {
          parent_name: 'default.orders',
          // grain is undefined
          components: [{ name: 'count_orders', expression: 'COUNT(*)' }],
        },
      ];

      render(
        <MetricFlowGraph
          grainGroups={grainGroupsUndefinedGrain}
          metricFormulas={[
            {
              name: 'default.metric',
              short_name: 'metric',
              components: ['count_orders'],
              is_derived: false,
            },
          ]}
          onNodeSelect={jest.fn()}
        />,
      );

      expect(screen.getByTestId('react-flow')).toBeInTheDocument();
    });
  });
});
