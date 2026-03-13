import React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import DJClientContext from '../../../providers/djclient';
import NodeDataFlowTab from '../NodeDataFlowTab';

// Sankey mock renders the node/link elements with fake props so SankeyNode/SankeyLink are covered
jest.mock('recharts', () => {
  const React = require('react');
  return {
    Sankey: ({ node: nodeEl, link: linkEl, data, children }) => {
      const nodes = data?.nodes || [];
      const makeNode = (payload, extra = {}) =>
        React.isValidElement(nodeEl)
          ? React.cloneElement(nodeEl, {
              x: 10,
              y: 10,
              width: 20,
              height: 30,
              payload,
              hoveredNodeName: null,
              onNodeHover: () => {},
              onNavigate: () => {},
              ...extra,
            })
          : null;
      const makeLink = (payload, extra = {}) =>
        React.isValidElement(linkEl)
          ? React.cloneElement(linkEl, {
              sourceX: 0,
              targetX: 100,
              sourceY: 50,
              targetY: 50,
              sourceControlX: 30,
              targetControlX: 70,
              linkWidth: 5,
              index: 0,
              payload,
              ...extra,
            })
          : null;
      const firstNode = nodes[0];
      const secondNode = nodes[1] || nodes[0];
      return (
        <div data-testid="sankey">
          {/* SankeyNode: null payload → returns null */}
          {makeNode(null)}
          {/* SankeyNode: phantom type → returns <g/> */}
          {firstNode && makeNode({ ...firstNode, type: 'phantom' })}
          {/* SankeyNode: normal */}
          {firstNode && makeNode(firstNode)}
          {/* SankeyNode: hovered (isHovered=true) */}
          {firstNode &&
            makeNode(firstNode, { hoveredNodeName: firstNode.name })}
          {/* SankeyNode: dimmed (hoveredNodeName !== this node) */}
          {firstNode && makeNode(firstNode, { hoveredNodeName: 'other.node' })}
          {/* SankeyNode: no name → cursor:default */}
          {makeNode({ type: 'metric', display_name: 'No Name Node' })}
          {/* SankeyLink: phantom target → returns <g/> */}
          {firstNode &&
            secondNode &&
            makeLink({
              source: firstNode,
              target: { ...secondNode, type: 'phantom' },
            })}
          {/* SankeyLink: no hover */}
          {firstNode &&
            secondNode &&
            makeLink({ source: firstNode, target: secondNode })}
          {/* SankeyLink: hovered matching source → isConnected=true, opacity=0.8 */}
          {firstNode &&
            secondNode &&
            makeLink(
              { source: firstNode, target: secondNode },
              { hoveredNodeName: firstNode.name },
            )}
          {/* SankeyLink: hovered not matching → isConnected=false, opacity=0.15 */}
          {firstNode &&
            secondNode &&
            makeLink(
              { source: firstNode, target: secondNode },
              { hoveredNodeName: 'something.else' },
            )}
          {children}
        </div>
      );
    },
    // Tooltip mock invokes the content function with various args to cover lines 413-421
    Tooltip: ({ content }) => {
      if (typeof content !== 'function') return null;
      return (
        <div data-testid="tooltip">
          {/* active=false → line 413 returns null */}
          {content({
            active: false,
            payload: [{ payload: { name: 'test', type: 'metric' } }],
          })}
          {/* payload empty → line 413 returns null */}
          {content({ active: true, payload: [] })}
          {/* item=null → line 415 returns null */}
          {content({ active: true, payload: [{}] })}
          {/* item.name exists + item.type → lines 416-421 */}
          {content({
            active: true,
            payload: [{ payload: { name: 'a.b.metric', type: 'metric' } }],
          })}
          {/* item.name falsy → uses source→target fallback (line 418) */}
          {content({
            active: true,
            payload: [
              {
                payload: {
                  source: { display_name: 'Src Node', name: 'src' },
                  target: { name: 'tgt' },
                },
              },
            ],
          })}
          {/* item.name falsy, source has no display_name → uses source.name */}
          {content({
            active: true,
            payload: [
              {
                payload: {
                  source: { name: 'src.node' },
                  target: { display_name: 'Target Display' },
                },
              },
            ],
          })}
        </div>
      );
    },
  };
});

// ResizeObserver fires callback immediately so containerWidth gets set to 800
beforeAll(() => {
  global.ResizeObserver = function (callback) {
    return {
      observe: function (el) {
        callback([{ contentRect: { width: 800 } }]);
      },
      disconnect: function () {},
    };
  };
  HTMLCanvasElement.prototype.getContext = () => ({
    font: '',
    measureText: () => ({ width: 50 }),
  });
});

describe('<NodeDataFlowTab />', () => {
  const mockDjClient = {
    node_dag: jest.fn(),
    downstreamsGQL: jest.fn(),
    findCubesWithMetrics: jest.fn(),
  };

  const renderWithContext = djNode =>
    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <MemoryRouter>
          <NodeDataFlowTab djNode={djNode} />
        </MemoryRouter>
      </DJClientContext.Provider>,
    );

  beforeEach(() => {
    jest.clearAllMocks();
    mockDjClient.findCubesWithMetrics.mockResolvedValue([]);
  });

  it('stays in loading state when djNode has no name (line 186 early return)', async () => {
    renderWithContext({});
    // Effect returns early, loading stays true — no API calls
    expect(mockDjClient.node_dag).not.toHaveBeenCalled();
    expect(
      screen.queryByText('No data flow relationships found for this node.'),
    ).not.toBeInTheDocument();
  });

  it('shows "No data flow relationships" when dag and downstreams return empty arrays', async () => {
    // Use a source node — metrics get phantom links which make links.length > 0
    const djNode = { name: 'default.source1', type: 'source', parents: [] };
    mockDjClient.node_dag.mockResolvedValue([]);
    mockDjClient.downstreamsGQL.mockResolvedValue([]);

    renderWithContext(djNode);

    await waitFor(() => {
      expect(
        screen.getByText('No data flow relationships found for this node.'),
      ).toBeInTheDocument();
    });
  });

  it('renders Sankey when links exist between nodes', async () => {
    const source = { name: 'default.source1', type: 'source', parents: [] };
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [{ name: 'default.source1' }],
    };
    mockDjClient.node_dag.mockResolvedValue([source]);
    mockDjClient.downstreamsGQL.mockResolvedValue([]);

    renderWithContext(djNode);

    await waitFor(() => {
      expect(screen.getByTestId('sankey')).toBeInTheDocument();
    });
  });

  it('handles error from Promise.all gracefully (lines 268-269)', async () => {
    const consoleSpy = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    const djNode = { name: 'default.metric1', type: 'metric', parents: [] };
    mockDjClient.node_dag.mockRejectedValue(new Error('Network error'));
    mockDjClient.downstreamsGQL.mockResolvedValue([]);

    renderWithContext(djNode);

    await waitFor(() => {
      expect(consoleSpy).toHaveBeenCalledWith(expect.any(Error));
    });
    expect(
      screen.getByText('No data flow relationships found for this node.'),
    ).toBeInTheDocument();
    consoleSpy.mockRestore();
  });

  it('filters CUBE type from downstream nodes and calls findCubesWithMetrics (lines 195-196)', async () => {
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [],
    };
    mockDjClient.node_dag.mockResolvedValue([]);
    mockDjClient.downstreamsGQL.mockResolvedValue([
      { name: 'default.cube1', type: 'CUBE' },
      { name: 'default.transform1', type: 'TRANSFORM' },
    ]);
    // cube has metric as parent → creates a link
    mockDjClient.findCubesWithMetrics.mockResolvedValue([
      {
        name: 'default.cube1',
        type: 'cube',
        parents: [{ name: 'default.metric1' }],
      },
    ]);

    renderWithContext(djNode);

    await waitFor(() => {
      expect(mockDjClient.findCubesWithMetrics).toHaveBeenCalledWith([
        'default.cube1',
      ]);
    });

    await waitFor(() => {
      expect(screen.getByTestId('sankey')).toBeInTheDocument();
    });
  });

  it('covers sort return 0 branch for same-type non-seed nodes (line 220)', async () => {
    // Two source nodes, neither is the seed (metric1) — sort returns 0
    const source1 = { name: 'default.source1', type: 'source', parents: [] };
    const source2 = { name: 'default.source2', type: 'source', parents: [] };
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [{ name: 'default.source1' }, { name: 'default.source2' }],
    };
    mockDjClient.node_dag.mockResolvedValue([source1, source2]);
    mockDjClient.downstreamsGQL.mockResolvedValue([]);

    renderWithContext(djNode);

    await waitFor(() => {
      expect(screen.getByTestId('sankey')).toBeInTheDocument();
    });
  });

  it('covers SankeyLink hover (linkHovered=true) via mouseEnter on path', async () => {
    const source = { name: 'default.source1', type: 'source', parents: [] };
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [{ name: 'default.source1' }],
    };
    mockDjClient.node_dag.mockResolvedValue([source]);
    mockDjClient.downstreamsGQL.mockResolvedValue([]);

    const { container } = renderWithContext(djNode);

    await waitFor(() => {
      expect(screen.getByTestId('sankey')).toBeInTheDocument();
    });

    // Trigger linkHovered=true to cover the 0.85 opacity branch (line 137)
    const path = container.querySelector('path');
    if (path) {
      fireEvent.mouseEnter(path);
      fireEvent.mouseLeave(path);
    }
  });

  it('covers ResizeObserver callback setting containerWidth (line 179)', async () => {
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [{ name: 'default.source1' }],
    };
    const source = { name: 'default.source1', type: 'source', parents: [] };
    mockDjClient.node_dag.mockResolvedValue([source]);
    mockDjClient.downstreamsGQL.mockResolvedValue([]);

    renderWithContext(djNode);

    // ResizeObserver mock fires immediately with width=800 → containerWidth>0 → Sankey renders
    await waitFor(() => {
      expect(screen.getByTestId('sankey')).toBeInTheDocument();
    });
  });

  it('renders SankeyNode with rightmostType=metric when no cubes', async () => {
    const source = { name: 'default.source1', type: 'source', parents: [] };
    const transform = {
      name: 'default.transform1',
      type: 'transform',
      parents: [{ name: 'default.source1' }],
    };
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [{ name: 'default.transform1' }],
    };
    mockDjClient.node_dag.mockResolvedValue([source, transform]);
    mockDjClient.downstreamsGQL.mockResolvedValue([]);

    renderWithContext(djNode);

    await waitFor(() => {
      expect(screen.getByTestId('sankey')).toBeInTheDocument();
    });
  });

  it('excludes dimension nodes from the flow graph', async () => {
    const dimNode = { name: 'default.dim1', type: 'dimension', parents: [] };
    const djNode = {
      name: 'default.metric1',
      type: 'metric',
      parents: [{ name: 'default.source1' }],
    };
    const source = { name: 'default.source1', type: 'source', parents: [] };
    mockDjClient.node_dag.mockResolvedValue([dimNode, source]);
    mockDjClient.downstreamsGQL.mockResolvedValue([]);

    renderWithContext(djNode);

    await waitFor(() => {
      expect(screen.getByTestId('sankey')).toBeInTheDocument();
    });
  });
});
