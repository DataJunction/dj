import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import DJClientContext from '../../../providers/djclient';
import NodeDimensionsTab from '../NodeDimensionsTab';

// Mock reactflow — renders DimNode components directly to cover lines 42-136
jest.mock('reactflow', () => {
  const { useState } = require('react');
  const React = require('react');
  return {
    __esModule: true,
    default: ({ nodes, nodeTypes }) => {
      return (
        <div data-testid="reactflow">
          {(nodes || []).map(n => {
            const NodeComp = nodeTypes?.[n.type];
            return NodeComp ? (
              <NodeComp key={n.id} data={n.data} />
            ) : (
              <div key={n.id}>{n.id}</div>
            );
          })}
        </div>
      );
    },
    useNodesState: initial => {
      const [n, s] = useState(initial || []);
      return [n, s, () => {}];
    },
    useEdgesState: initial => {
      const [e, s] = useState(initial || []);
      return [e, s, () => {}];
    },
    Handle: () => null,
    MarkerType: { ArrowClosed: 'arrowclosed' },
    Position: { Left: 'left', Right: 'right' },
    addEdge: (params, eds) => [...(eds || []), params],
  };
});

jest.mock('reactflow/dist/style.css', () => ({}));

jest.mock('dagre', () => {
  function MockGraph() {
    this.setDefaultEdgeLabel = function () {};
    this.setGraph = function () {};
    this.setNode = function () {};
    this.setEdge = function () {};
    this.node = function () {
      return { x: 100, y: 100 };
    };
  }
  return {
    graphlib: { Graph: MockGraph },
    layout: function () {},
  };
});

describe('<NodeDimensionsTab />', () => {
  const mockDjClient = {
    dimensionDag: jest.fn(),
  };

  const renderWithContext = djNode =>
    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <MemoryRouter>
          <NodeDimensionsTab djNode={djNode} />
        </MemoryRouter>
      </DJClientContext.Provider>,
    );

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('shows "No dimension links found" when dimensionDag returns empty', async () => {
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [],
      dimension_links: [],
    };
    mockDjClient.dimensionDag.mockResolvedValue({
      inbound: [],
      inbound_edges: [],
      outbound: [],
      outbound_edges: [],
    });

    renderWithContext(djNode);

    await waitFor(() => {
      expect(
        screen.getByText('No dimension links found for this node.'),
      ).toBeInTheDocument();
    });
  });

  it('does not fetch when djNode has no name (early return)', () => {
    renderWithContext({ type: 'metric', parents: [], dimension_links: [] });
    expect(mockDjClient.dimensionDag).not.toHaveBeenCalled();
  });

  it('renders ReactFlow with DimNode components when outbound dimensions exist', async () => {
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [],
      dimension_links: [],
    };
    const dimNode = {
      name: 'default.dim1',
      type: 'dimension',
      display_name: 'Default: Dim 1',
    };

    mockDjClient.dimensionDag.mockResolvedValue({
      inbound: [],
      inbound_edges: [],
      outbound: [dimNode],
      outbound_edges: [{ source: 'default.metric1', target: 'default.dim1' }],
    });

    renderWithContext(djNode);

    await waitFor(() => {
      expect(screen.getByTestId('reactflow')).toBeInTheDocument();
    });
    expect(screen.getByText('dimension')).toBeInTheDocument();
  });

  it('handles error from dimensionDag gracefully', async () => {
    const consoleSpy = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [],
      dimension_links: [],
    };
    mockDjClient.dimensionDag.mockRejectedValue(new Error('fetch failed'));

    renderWithContext(djNode);

    await waitFor(() => {
      expect(consoleSpy).toHaveBeenCalledWith(expect.any(Error));
    });
    await waitFor(() => {
      expect(
        screen.getByText('No dimension links found for this node.'),
      ).toBeInTheDocument();
    });
    consoleSpy.mockRestore();
  });

  it('covers DimNode isCurrent border style (current node renders correctly)', async () => {
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [],
      dimension_links: [],
    };

    mockDjClient.dimensionDag.mockResolvedValue({
      inbound: [],
      inbound_edges: [],
      outbound: [
        { name: 'default.dim1', type: 'dimension', display_name: 'Dim 1' },
      ],
      outbound_edges: [{ source: 'default.metric1', target: 'default.dim1' }],
    });

    renderWithContext(djNode);

    await waitFor(() => {
      expect(screen.getByTestId('reactflow')).toBeInTheDocument();
    });
    // metric1 is the current node (isCurrent=true)
    expect(screen.getByText('metric')).toBeInTheDocument();
  });

  it('covers DimNode with no namespace (node name has no dots)', async () => {
    const djNode = {
      name: 'metric1',
      type: 'metric',
      parents: [],
      dimension_links: [],
    };

    mockDjClient.dimensionDag.mockResolvedValue({
      inbound: [],
      inbound_edges: [],
      outbound: [{ name: 'dim1', type: 'dimension', display_name: 'Dim 1' }],
      outbound_edges: [{ source: 'metric1', target: 'dim1' }],
    });

    renderWithContext(djNode);

    await waitFor(() => {
      expect(screen.getByTestId('reactflow')).toBeInTheDocument();
    });
  });

  it('covers DimNode with unknown type (fallback color)', async () => {
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [],
      dimension_links: [],
    };

    mockDjClient.dimensionDag.mockResolvedValue({
      inbound: [],
      inbound_edges: [],
      outbound: [
        {
          name: 'default.custom_node',
          type: 'unknown_type',
          display_name: 'Custom Node',
        },
      ],
      outbound_edges: [
        { source: 'default.metric1', target: 'default.custom_node' },
      ],
    });

    renderWithContext(djNode);

    await waitFor(() => {
      expect(screen.getByTestId('reactflow')).toBeInTheDocument();
    });
    expect(screen.getByText('unknown_type')).toBeInTheDocument();
  });

  it('renders dimension DAG for a dimension node (inbound + outbound)', async () => {
    const djNode = {
      name: 'default.hard_hat',
      type: 'dimension',
      display_name: 'Hard Hat',
      parents: [],
      dimension_links: [],
    };
    const inboundNode = {
      name: 'default.repair_orders',
      display_name: 'Repair Orders',
      type: 'source',
    };
    const outboundNode = {
      name: 'default.us_state',
      display_name: 'US State',
      type: 'dimension',
    };

    mockDjClient.dimensionDag.mockResolvedValue({
      inbound: [inboundNode],
      inbound_edges: [{ source: inboundNode.name, target: djNode.name }],
      outbound: [outboundNode],
      outbound_edges: [{ source: djNode.name, target: outboundNode.name }],
    });

    renderWithContext(djNode);

    await waitFor(() => {
      expect(mockDjClient.dimensionDag).toHaveBeenCalledWith(
        'default.hard_hat',
      );
    });
    await waitFor(() => {
      expect(screen.getByTestId('reactflow')).toBeInTheDocument();
    });
  });

  it('shows "No dimension links found" when dimension DAG returns empty inbound + outbound', async () => {
    const djNode = {
      name: 'default.payment_type',
      type: 'dimension',
      display_name: 'Payment Type',
      parents: [],
      dimension_links: [],
    };
    mockDjClient.dimensionDag.mockResolvedValue({
      inbound: [],
      inbound_edges: [],
      outbound: [],
      outbound_edges: [],
    });

    renderWithContext(djNode);

    await waitFor(() => {
      expect(
        screen.getByText('No dimension links found for this node.'),
      ).toBeInTheDocument();
    });
  });

  it('handles dimensionDag error gracefully', async () => {
    const consoleSpy = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    const djNode = {
      name: 'default.hard_hat',
      type: 'dimension',
      parents: [],
      dimension_links: [],
    };
    mockDjClient.dimensionDag.mockRejectedValue(new Error('network error'));

    renderWithContext(djNode);

    await waitFor(() => {
      expect(consoleSpy).toHaveBeenCalledWith(expect.any(Error));
    });
    await waitFor(() => {
      expect(
        screen.getByText('No dimension links found for this node.'),
      ).toBeInTheDocument();
    });
    consoleSpy.mockRestore();
  });

  it('renders cube dimension graph via dimensionDag (backend handles cube expansion)', async () => {
    const djNode = {
      name: 'default.repairs_cube',
      type: 'cube',
      parents: [],
      dimension_links: [],
    };

    // The backend now seeds the BFS from the cube's metric upstreams, so the
    // frontend just calls dimensionDag normally and gets back a populated result.
    mockDjClient.dimensionDag.mockResolvedValue({
      inbound: [
        {
          name: 'default.repair_orders',
          type: 'source',
          display_name: 'Repair Orders',
        },
      ],
      inbound_edges: [
        { source: 'default.repair_orders', target: 'default.hard_hat' },
      ],
      outbound: [
        {
          name: 'default.hard_hat',
          type: 'dimension',
          display_name: 'Hard Hat',
        },
        {
          name: 'default.dispatcher',
          type: 'dimension',
          display_name: 'Dispatcher',
        },
      ],
      outbound_edges: [
        { source: 'default.repair_orders', target: 'default.hard_hat' },
        { source: 'default.repair_orders', target: 'default.dispatcher' },
      ],
    });

    renderWithContext(djNode);

    await waitFor(() => {
      expect(screen.getByTestId('reactflow')).toBeInTheDocument();
    });
    expect(mockDjClient.dimensionDag).toHaveBeenCalledWith(
      'default.repairs_cube',
    );
    expect(screen.getAllByText('dimension').length).toBeGreaterThan(0);
  });

  it('renders multi-level inbound chain (non-flat graph)', async () => {
    const djNode = {
      name: 'default.us_state',
      type: 'dimension',
      display_name: 'US State',
      parents: [],
      dimension_links: [],
    };

    mockDjClient.dimensionDag.mockResolvedValue({
      inbound: [
        {
          name: 'default.hard_hat',
          type: 'dimension',
          display_name: 'Hard Hat',
        },
        {
          name: 'default.repair_order',
          type: 'source',
          display_name: 'Repair Order',
        },
      ],
      inbound_edges: [
        { source: 'default.hard_hat', target: 'default.us_state' },
        { source: 'default.repair_order', target: 'default.hard_hat' },
      ],
      outbound: [],
      outbound_edges: [],
    });

    renderWithContext(djNode);

    await waitFor(() => {
      expect(screen.getByTestId('reactflow')).toBeInTheDocument();
    });
    expect(screen.getAllByText('dimension').length).toBeGreaterThan(0);
  });
});
