import React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import DJClientContext from '../../../providers/djclient';
import NodeDimensionsTab from '../NodeDimensionsTab';

// Mock reactflow — renders DimNode components directly to cover lines 42-136
jest.mock('reactflow', () => {
  const { useState } = require('react');
  const React = require('react');
  return {
    __esModule: true,
    default: ({
      nodes,
      nodeTypes,
      onNodeMouseEnter,
      onNodeMouseLeave,
      onConnect,
    }) => {
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
    Position: { Left: 'left', Right: 'right' },
    addEdge: (params, eds) => [...(eds || []), params],
  };
});

jest.mock('reactflow/dist/style.css', () => ({}));

jest.mock('dagre', () => ({
  graphlib: {
    Graph: jest.fn().mockImplementation(() => ({
      setDefaultEdgeLabel: jest.fn(),
      setGraph: jest.fn(),
      setNode: jest.fn(),
      setEdge: jest.fn(),
      node: jest.fn().mockReturnValue({ x: 100, y: 100 }),
    })),
  },
  layout: jest.fn(),
}));

describe('<NodeDimensionsTab />', () => {
  const mockDjClient = {
    node_dag: jest.fn(),
    node: jest.fn(),
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

  it('shows "No dimension links found" when node has no dimension links (line 317)', async () => {
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [],
      dimension_links: [],
    };
    mockDjClient.node_dag.mockResolvedValue([]);

    renderWithContext(djNode);

    await waitFor(() => {
      expect(
        screen.getByText('No dimension links found for this node.'),
      ).toBeInTheDocument();
    });
  });

  it('does not fetch when djNode has no name (line 181 early return)', () => {
    renderWithContext({ type: 'metric', parents: [], dimension_links: [] });
    expect(mockDjClient.node_dag).not.toHaveBeenCalled();
  });

  it('renders ReactFlow with DimNode components when dimension links exist (lines 43-136, 139-158)', async () => {
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [],
      dimension_links: [{ dimension: { name: 'default.dim1' } }],
    };
    const dimNode = {
      name: 'default.dim1',
      type: 'dimension',
      display_name: 'Default: Dim 1',
      dimension_links: [],
      parents: [],
    };

    mockDjClient.node_dag.mockResolvedValue([]);
    mockDjClient.node.mockResolvedValue(dimNode);

    renderWithContext(djNode);

    await waitFor(() => {
      expect(screen.getByTestId('reactflow')).toBeInTheDocument();
    });
    // DimNode renders the type badge and namespace
    expect(screen.getByText('dimension')).toBeInTheDocument();
  });

  it('covers cube type with dimensions array (lines 195-200)', async () => {
    const djNode = {
      name: 'default.cube1',
      type: 'cube',
      parents: [],
      dimension_links: [],
      dimensions: [{ value: 'default.dim1.some_attribute' }],
    };
    const dimNode = {
      name: 'default.dim1',
      type: 'dimension',
      display_name: 'Dim 1',
      dimension_links: [],
      parents: [],
    };

    mockDjClient.node_dag.mockResolvedValue([]);
    mockDjClient.node.mockResolvedValue(dimNode);

    renderWithContext(djNode);

    await waitFor(() => {
      expect(mockDjClient.node).toHaveBeenCalledWith('default.dim1');
    });
    await waitFor(() => {
      expect(screen.getByTestId('reactflow')).toBeInTheDocument();
    });
  });

  it('handles error from node_dag gracefully (lines 270-273)', async () => {
    const consoleSpy = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [],
      dimension_links: [{ dimension: { name: 'default.dim1' } }],
    };
    mockDjClient.node_dag.mockRejectedValue(new Error('DAG fetch failed'));

    renderWithContext(djNode);

    await waitFor(() => {
      expect(consoleSpy).toHaveBeenCalledWith(expect.any(Error));
    });
    // After error, loaded=true and nodes=[] → "No dimension links found"
    await waitFor(() => {
      expect(
        screen.getByText('No dimension links found for this node.'),
      ).toBeInTheDocument();
    });
    consoleSpy.mockRestore();
  });

  it('covers DimNode isCurrent border style (line 52)', async () => {
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [],
      dimension_links: [{ dimension: { name: 'default.dim1' } }],
    };
    const dimNode = {
      name: 'default.dim1',
      type: 'dimension',
      display_name: 'Dim 1',
      dimension_links: [],
      parents: [],
    };

    mockDjClient.node_dag.mockResolvedValue([]);
    mockDjClient.node.mockResolvedValue(dimNode);

    renderWithContext(djNode);

    await waitFor(() => {
      expect(screen.getByTestId('reactflow')).toBeInTheDocument();
    });
    // metric1 is the current node (isCurrent=true) and dim1 is not (isCurrent=false)
    // Both DimNode variants render correctly
    expect(screen.getByText('metric')).toBeInTheDocument();
  });

  it('covers DimNode with no namespace (node name has no dots)', async () => {
    // node name with no dots → namespace = '' → namespace falsy → no namespace div rendered
    const djNode = {
      name: 'metric1',
      type: 'metric',
      parents: [],
      dimension_links: [{ dimension: { name: 'dim1' } }],
    };
    const dimNode = {
      name: 'dim1',
      type: 'dimension',
      display_name: 'Dim 1',
      dimension_links: [],
      parents: [],
    };

    mockDjClient.node_dag.mockResolvedValue([]);
    mockDjClient.node.mockResolvedValue(dimNode);

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
      dimension_links: [{ dimension: { name: 'default.custom_node' } }],
    };
    const customNode = {
      name: 'default.custom_node',
      type: 'unknown_type',
      display_name: 'Custom Node',
      dimension_links: [],
      parents: [],
    };

    mockDjClient.node_dag.mockResolvedValue([]);
    mockDjClient.node.mockResolvedValue(customNode);

    renderWithContext(djNode);

    await waitFor(() => {
      expect(screen.getByTestId('reactflow')).toBeInTheDocument();
    });
    // DimNode uses fallback color for unknown type
    expect(screen.getByText('unknown_type')).toBeInTheDocument();
  });

  it('handles nodes already in byName (no extra fetch needed)', async () => {
    // DAG returns the dimension node already → missing array is empty → no djClient.node call
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [],
      dimension_links: [{ dimension: { name: 'default.dim1' } }],
    };
    const dimNode = {
      name: 'default.dim1',
      type: 'dimension',
      display_name: 'Dim 1',
      dimension_links: [],
      parents: [],
    };

    // node_dag returns the dim node — it's already in byName
    mockDjClient.node_dag.mockResolvedValue([dimNode]);

    renderWithContext(djNode);

    await waitFor(() => {
      expect(screen.getByTestId('reactflow')).toBeInTheDocument();
    });
    // node() should NOT have been called since dim1 was already fetched via node_dag
    expect(mockDjClient.node).not.toHaveBeenCalled();
  });
});
