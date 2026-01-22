import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { mocks } from '../../../../mocks/mockNodes';
import DJClientContext from '../../../providers/djclient';
import { NodePage } from '../Loadable';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import userEvent from '@testing-library/user-event';

// Mock cronstrue for NodePreAggregationsTab
jest.mock('cronstrue', () => ({
  toString: () => 'Every day at midnight',
}));

// Mock CSS imports
jest.mock('../../../../styles/preaggregations.css', () => ({}));

describe('<NodePage />', () => {
  const domTestingLib = require('@testing-library/dom');
  const { queryHelpers } = domTestingLib;

  const queryByAttribute = attribute =>
    queryHelpers.queryAllByAttribute.bind(null, attribute);

  function getByAttribute(container, id, attribute, ...rest) {
    const result = queryByAttribute(attribute)(container, id, ...rest);
    return result[0];
  }

  const mockDJClient = () => {
    return {
      DataJunctionAPI: {
        node: jest.fn(),
        metric: jest.fn(),
        getMetric: jest.fn(),
        revalidate: jest.fn().mockReturnValue({ status: 'valid' }),
        node_dag: jest.fn().mockReturnValue(mocks.mockNodeDAG),
        clientCode: jest.fn().mockResolvedValue('dj_client = DJClient()'),
        columns: jest.fn(),
        history: jest.fn(),
        revisions: jest.fn(),
        materializations: jest.fn(),
        availabilityStates: jest.fn(),
        refreshLatestMaterialization: jest.fn(),
        materializationInfo: jest.fn(),
        sql: jest.fn(),
        cube: jest.fn(),
        compiledSql: jest.fn(),
        node_lineage: jest.fn(),
        nodesWithDimension: jest.fn(),
        attributes: jest.fn(),
        dimensions: jest.fn(),
        setPartition: jest.fn(),
        engines: jest.fn(),
        streamNodeData: jest.fn(),
        nodeDimensions: jest.fn(),
        getNotificationPreferences: jest.fn().mockResolvedValue([]),
        subscribeToNotifications: jest.fn().mockResolvedValue({ status: 200 }),
        unsubscribeFromNotifications: jest
          .fn()
          .mockResolvedValue({ status: 200 }),
        setAttributes: jest.fn().mockResolvedValue({ status: 200 }),
        linkDimension: jest.fn().mockResolvedValue({ status: 200 }),
        unlinkDimension: jest.fn().mockResolvedValue({ status: 200 }),
        addReferenceDimensionLink: jest.fn().mockResolvedValue({ status: 200 }),
        removeReferenceDimensionLink: jest
          .fn()
          .mockResolvedValue({ status: 200 }),
        addComplexDimensionLink: jest.fn().mockResolvedValue({ status: 200 }),
        removeComplexDimensionLink: jest
          .fn()
          .mockResolvedValue({ status: 200 }),
        listPreaggs: jest.fn().mockResolvedValue({ items: [] }),
        deactivatePreaggWorkflow: jest.fn().mockResolvedValue({ status: 200 }),
      },
    };
  };

  const defaultProps = {
    name: 'default.avg_repair_price',
    djNode: {
      namespace: 'default',
      node_revision_id: 24,
      node_id: 24,
      type: 'metric',
      name: 'default.avg_repair_price',
      display_name: 'Default: Avg Repair Price',
      version: 'v1.0',
      status: 'valid',
      mode: 'published',
      catalog: {
        id: 1,
        uuid: '0fc18295-e1a2-4c3c-b72a-894725c12488',
        created_at: '2023-08-21T16:48:51.146121+00:00',
        updated_at: '2023-08-21T16:48:51.146122+00:00',
        extra_params: {},
        name: 'warehouse',
      },
      schema_: null,
      table: null,
      description: 'Average repair price',
      query:
        'SELECT  avg(price) default_DOT_avg_repair_price \n FROM default.repair_order_details\n\n',
      availability: null,
      dimension_links: [],
      columns: [
        {
          name: 'default_DOT_avg_repair_price',
          type: 'double',
          display_name: 'Default DOT avg repair price',
          attributes: [],
          dimension: null,
        },
      ],
      updated_at: '2023-08-21T16:48:56.932231+00:00',
      materializations: [],
      parents: [
        {
          name: 'default.repair_order_details',
        },
      ],
      created_at: '2023-08-21T16:48:56.932162+00:00',
      tags: [{ name: 'purpose', display_name: 'Purpose' }],
      primary_key: [],
      incompatible_druid_functions: ['IF'],
      createNodeClientCode:
        'dj = DJBuilder(DJ_URL)\n\navg_repair_price = dj.create_metric(\n    description="Average repair price",\n    display_name="Default: Avg Repair Price",\n    name="default.avg_repair_price",\n    primary_key=[],\n    query="""SELECT  avg(price) default_DOT_avg_repair_price \n FROM default.repair_order_details\n\n"""\n)',
      dimensions: [
        {
          name: 'default.date_dim.dateint',
          type: 'timestamp',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
            'default.hard_hat.birth_date',
          ],
        },
        {
          name: 'default.date_dim.dateint',
          type: 'timestamp',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
            'default.hard_hat.hire_date',
          ],
        },
        {
          name: 'default.date_dim.day',
          type: 'int',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
            'default.hard_hat.birth_date',
          ],
        },
        {
          name: 'default.date_dim.day',
          type: 'int',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
            'default.hard_hat.hire_date',
          ],
        },
        {
          name: 'default.date_dim.month',
          type: 'int',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
            'default.hard_hat.birth_date',
          ],
        },
        {
          name: 'default.date_dim.month',
          type: 'int',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
            'default.hard_hat.hire_date',
          ],
        },
        {
          name: 'default.date_dim.year',
          type: 'int',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
            'default.hard_hat.birth_date',
          ],
        },
        {
          name: 'default.date_dim.year',
          type: 'int',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
            'default.hard_hat.hire_date',
          ],
        },
        {
          name: 'default.hard_hat.address',
          type: 'string',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
          ],
        },
        {
          name: 'default.hard_hat.birth_date',
          type: 'date',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
          ],
        },
        {
          name: 'default.hard_hat.city',
          type: 'string',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
          ],
        },
        {
          name: 'default.hard_hat.contractor_id',
          type: 'int',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
          ],
        },
        {
          name: 'default.hard_hat.country',
          type: 'string',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
          ],
        },
        {
          name: 'default.hard_hat.first_name',
          type: 'string',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
          ],
        },
        {
          name: 'default.hard_hat.hard_hat_id',
          type: 'int',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
          ],
        },
        {
          name: 'default.hard_hat.hire_date',
          type: 'date',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
          ],
        },
        {
          name: 'default.hard_hat.last_name',
          type: 'string',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
          ],
        },
        {
          name: 'default.hard_hat.manager',
          type: 'int',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
          ],
        },
        {
          name: 'default.hard_hat.postal_code',
          type: 'string',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
          ],
        },
        {
          name: 'default.hard_hat.state',
          type: 'string',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
          ],
        },
        {
          name: 'default.hard_hat.title',
          type: 'string',
          path: [
            'default.repair_order_details.repair_order_id',
            'default.repair_order.hard_hat_id',
          ],
        },
      ],
    },
  };

  it('renders the NodeInfo tab correctly for a metric node', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.getMetric.mockResolvedValue(
      mocks.mockMetricNodeJson,
    );
    const element = (
      <DJClientContext.Provider value={djClient}>
        <NodePage {...defaultProps} />
      </DJClientContext.Provider>
    );
    const { container } = render(
      <MemoryRouter initialEntries={['/nodes/default.num_repair_orders/info']}>
        <Routes>
          <Route path="nodes/:name/:tab" element={element} />
        </Routes>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(djClient.DataJunctionAPI.node).toHaveBeenCalledWith(
        'default.num_repair_orders',
      );

      expect(
        screen.getByRole('dialog', { name: 'NodeName' }),
      ).toHaveTextContent('default.num_repair_orders');

      expect(screen.getByRole('button', { name: 'Info' })).toBeInTheDocument();
      expect(
        screen.getByRole('dialog', { name: 'Description' }),
      ).toHaveTextContent('Number of repair orders');

      expect(screen.getByRole('dialog', { name: 'Version' })).toHaveTextContent(
        'v1.0',
      );

      expect(
        screen.getByRole('dialog', { name: 'NodeStatus' }),
      ).toBeInTheDocument();

      expect(screen.getByRole('dialog', { name: 'Tags' })).toHaveTextContent(
        'Purpose',
      );

      expect(
        screen.getByRole('dialog', { name: 'RequiredDimensions' }),
      ).toHaveTextContent('');

      expect(
        screen.getByRole('dialog', { name: 'DisplayName' }),
      ).toHaveTextContent('Default: Num Repair Orders');

      expect(
        screen.getByRole('dialog', { name: 'NodeType' }),
      ).toHaveTextContent('metric');
    });

    // Wait separately for getMetric to be called and data to render
    await waitFor(() => {
      expect(djClient.DataJunctionAPI.getMetric).toHaveBeenCalledWith(
        'default.num_repair_orders',
      );
    });

    // Wait for metric expression to appear (SyntaxHighlighter may split text)
    await waitFor(() => {
      expect(screen.getByText(/count/)).toBeInTheDocument();
    });

    expect(container.getElementsByClassName('language-sql')).toMatchSnapshot();
  }, 60000);

  it('renders the NodeInfo tab correctly for cube nodes', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockReturnValue(mocks.mockCubeNode);
    djClient.DataJunctionAPI.cube.mockResolvedValue(mocks.mockCubesCube);
    const element = (
      <DJClientContext.Provider value={djClient}>
        <NodePage {...defaultProps} />
      </DJClientContext.Provider>
    );
    const { container } = render(
      <MemoryRouter initialEntries={['/nodes/default.repair_orders_cube']}>
        <Routes>
          <Route path="nodes/:name" element={element} />
        </Routes>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(djClient.DataJunctionAPI.node).toHaveBeenCalledWith(
        'default.repair_orders_cube',
      );
      userEvent.click(screen.getByRole('button', { name: 'Info' }));

      expect(
        screen.getByRole('dialog', { name: 'NodeName' }),
      ).toHaveTextContent('default.repair_orders_cube');

      expect(screen.getByRole('button', { name: 'Info' })).toBeInTheDocument();
      expect(
        screen.getByRole('dialog', { name: 'Description' }),
      ).toHaveTextContent('Repair Orders');

      expect(screen.getByRole('dialog', { name: 'Version' })).toHaveTextContent(
        'v1.0',
      );

      expect(
        screen.getByRole('dialog', { name: 'PrimaryKey' }),
      ).toHaveTextContent('');

      expect(
        screen.getByRole('dialog', { name: 'DisplayName' }),
      ).toHaveTextContent('Default: Repair Orders Cube');

      expect(
        screen.getByRole('dialog', { name: 'NodeType' }),
      ).toHaveTextContent('cube');

      expect(
        screen.getByRole('dialog', { name: 'NodeType' }),
      ).toHaveTextContent('cube');

      expect(screen.getByText('Cube Elements')).toBeInTheDocument();
      screen
        .getAllByRole('cell', { name: 'CubeElement' })
        .map(cube => cube.hasAttribute('a'));
    });
  }, 60000);

  it('renders the NodeColumns tab correctly', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockResolvedValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.getMetric.mockResolvedValue(
      mocks.mockMetricNodeJson,
    );
    djClient.DataJunctionAPI.columns.mockResolvedValue(mocks.metricNodeColumns);
    djClient.DataJunctionAPI.attributes.mockResolvedValue(mocks.attributes);
    djClient.DataJunctionAPI.dimensions.mockResolvedValue(mocks.dimensions);
    djClient.DataJunctionAPI.engines.mockReturnValue([]);
    djClient.DataJunctionAPI.setPartition.mockResolvedValue({
      status: 200,
      json: { message: '' },
    });

    const element = (
      <DJClientContext.Provider value={djClient}>
        <NodePage />
      </DJClientContext.Provider>
    );
    render(
      <MemoryRouter
        initialEntries={['/nodes/default.num_repair_orders/columns']}
      >
        <Routes>
          <Route path="nodes/:name/:tab" element={element} />
        </Routes>
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(djClient.DataJunctionAPI.columns).toHaveBeenCalledWith(
        expect.objectContaining({ name: mocks.mockMetricNode.name }),
      );
      expect(
        screen.getByRole('columnheader', { name: 'ColumnName' }),
      ).toHaveTextContent('default_DOT_avg_repair_price');
      expect(
        screen.getByRole('columnheader', { name: 'ColumnDisplayName' }),
      ).toHaveTextContent('Default DOT avg repair price');
      expect(
        screen.getByRole('columnheader', { name: 'ColumnType' }),
      ).toHaveTextContent('double');

      // check that the edit column popover can be clicked
      const editColumnPopover = screen.getByRole('button', {
        name: 'EditColumn',
      });
      expect(editColumnPopover).toBeInTheDocument();
      fireEvent.click(editColumnPopover);
      expect(
        screen.getByRole('button', { name: 'SaveEditColumn' }),
      ).toBeInTheDocument();

      // check that the manage dimension links dialog can be opened
      const manageDimensionLinksButton = screen.getByRole('button', {
        name: 'ManageDimensionLinksToggle',
      });
      expect(manageDimensionLinksButton).toBeInTheDocument();
      fireEvent.click(manageDimensionLinksButton);
      expect(
        screen.getByRole('dialog', { name: 'ManageDimensionLinksDialog' }),
      ).toBeInTheDocument();

      // check that the set column partition popover can be clicked
      const partitionColumnPopover = screen.getByRole('button', {
        name: 'PartitionColumn',
      });
      expect(partitionColumnPopover).toBeInTheDocument();
      fireEvent.click(partitionColumnPopover);
      const savePartition = screen.getByRole('button', {
        name: 'SaveEditColumn',
      });
      expect(savePartition).toBeInTheDocument();
      fireEvent.click(savePartition);
      expect(screen.getByText('Saved!'));
    });
  }, 60000);

  it('renders the NodeHistory tab correctly', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.getMetric.mockResolvedValue(
      mocks.mockMetricNodeJson,
    );
    djClient.DataJunctionAPI.columns.mockReturnValue(mocks.metricNodeColumns);
    djClient.DataJunctionAPI.history.mockReturnValue(mocks.metricNodeHistory);
    djClient.DataJunctionAPI.revisions.mockReturnValue(
      mocks.metricNodeRevisions,
    );

    const element = (
      <DJClientContext.Provider value={djClient}>
        <NodePage />
      </DJClientContext.Provider>
    );
    const { container } = render(
      <MemoryRouter
        initialEntries={['/nodes/default.num_repair_orders/history']}
      >
        <Routes>
          <Route path="nodes/:name/:tab" element={element} />
        </Routes>
      </MemoryRouter>,
    );
    await waitFor(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'History' }));
      expect(djClient.DataJunctionAPI.node).toHaveBeenCalledWith(
        mocks.mockMetricNode.name,
      );
      expect(djClient.DataJunctionAPI.history).toHaveBeenCalledWith(
        'node',
        mocks.mockMetricNode.name,
      );
      expect(
        screen.getByRole('list', { name: 'Activity' }),
      ).toBeInTheDocument();
      screen
        .queryAllByRole('cell', {
          name: 'HistoryAttribute',
        })
        .forEach(cell => expect(cell).toHaveTextContent(/Set col1 as /));

      screen
        .queryAllByRole('cell', {
          name: 'HistoryCreateLink',
        })
        .forEach(cell =>
          expect(cell).toHaveTextContent(
            'Linked col1 todefault.hard_hat viahard_hat_id',
          ),
        );

      screen
        .queryAllByRole('cell', {
          name: 'HistoryCreateMaterialization',
        })
        .forEach(cell =>
          expect(cell).toHaveTextContent(
            'Initialized materialization some_random_materialization',
          ),
        );

      screen
        .queryAllByRole('cell', {
          name: 'HistoryNodeStatusChange',
        })
        .forEach(cell =>
          expect(cell).toHaveTextContent(
            'Status changed from valid to invalid Caused by a change in upstream default.repair_order_details',
          ),
        );
    });
  });

  it('renders an empty NodeMaterialization tab correctly', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockResolvedValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.getMetric.mockResolvedValue(
      mocks.mockMetricNodeJson,
    );
    djClient.DataJunctionAPI.columns.mockResolvedValue(mocks.metricNodeColumns);
    // For metric nodes, listPreaggs is called, not materializations
    djClient.DataJunctionAPI.listPreaggs.mockResolvedValue({ items: [] });

    const element = (
      <DJClientContext.Provider value={djClient}>
        <NodePage />
      </DJClientContext.Provider>
    );
    render(
      <MemoryRouter
        initialEntries={['/nodes/default.num_repair_orders/materializations']}
      >
        <Routes>
          <Route path="nodes/:name/:tab" element={element} />
        </Routes>
      </MemoryRouter>,
    );

    // For metric nodes, NodePreAggregationsTab is used, which calls listPreaggs
    await waitFor(() => {
      expect(djClient.DataJunctionAPI.listPreaggs).toHaveBeenCalledWith({
        node_name: mocks.mockMetricNode.name,
        include_stale: true,
      });
    });

    // Check for the empty state text (for NodePreAggregationsTab)
    expect(
      screen.getByText('No pre-aggregations found for this node.'),
    ).toBeInTheDocument();
  });

  it('renders the NodeMaterialization tab with materializations correctly', async () => {
    const djClient = mockDJClient();
    // Use cube node - only cubes use NodeMaterializationTab
    // Override columns with explicit partition: null to avoid undefined.type_ error
    const cubeNodeWithPartitions = {
      ...mocks.mockCubeNode,
      columns: mocks.mockCubeNode.columns.map(col => ({
        ...col,
        partition: null,
      })),
    };
    djClient.DataJunctionAPI.node.mockResolvedValue(cubeNodeWithPartitions);
    djClient.DataJunctionAPI.cube.mockResolvedValue(mocks.mockCubesCube);
    djClient.DataJunctionAPI.columns.mockResolvedValue([]);
    djClient.DataJunctionAPI.materializations.mockResolvedValue(
      mocks.nodeMaterializations,
    );
    djClient.DataJunctionAPI.availabilityStates.mockResolvedValue([]);
    djClient.DataJunctionAPI.materializationInfo.mockResolvedValue(
      mocks.materializationInfo,
    );

    const element = (
      <DJClientContext.Provider value={djClient}>
        <NodePage />
      </DJClientContext.Provider>
    );
    render(
      <MemoryRouter
        initialEntries={['/nodes/default.repair_orders_cube/materializations']}
      >
        <Routes>
          <Route path="nodes/:name/:tab" element={element} />
        </Routes>
      </MemoryRouter>,
    );

    // Wait for the node to load first
    await waitFor(() => {
      expect(djClient.DataJunctionAPI.node).toHaveBeenCalledWith(
        'default.repair_orders_cube',
      );
    });

    // Then wait for materializations to be fetched
    await waitFor(() => {
      expect(djClient.DataJunctionAPI.materializations).toHaveBeenCalledWith(
        mocks.mockCubeNode.name,
      );
    });
  });

  it('renders a NodeColumnLineage tab correctly', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.getMetric.mockResolvedValue(
      mocks.mockMetricNodeJson,
    );
    djClient.DataJunctionAPI.columns.mockReturnValue(mocks.metricNodeColumns);
    djClient.DataJunctionAPI.node_lineage.mockReturnValue(
      mocks.mockNodeLineage,
    );

    const element = (
      <DJClientContext.Provider value={djClient}>
        <NodePage />
      </DJClientContext.Provider>
    );
    render(
      <MemoryRouter
        initialEntries={['/nodes/default.num_repair_orders/lineage']}
      >
        <Routes>
          <Route path="nodes/:name/:tab" element={element} />
        </Routes>
      </MemoryRouter>,
    );
    await waitFor(() => {
      fireEvent.click(screen.getByRole('button', { name: 'Lineage' }));
      expect(djClient.DataJunctionAPI.node_lineage).toHaveBeenCalledWith(
        mocks.mockMetricNode.name,
      );
    });
  });

  it('renders a NodeGraph tab correctly', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.getMetric.mockResolvedValue(
      mocks.mockMetricNodeJson,
    );
    djClient.DataJunctionAPI.columns.mockReturnValue(mocks.metricNodeColumns);

    const element = (
      <DJClientContext.Provider value={djClient}>
        <NodePage />
      </DJClientContext.Provider>
    );
    render(
      <MemoryRouter initialEntries={['/nodes/default.num_repair_orders/graph']}>
        <Routes>
          <Route path="nodes/:name/:tab" element={element} />
        </Routes>
      </MemoryRouter>,
    );
    await waitFor(() => {
      fireEvent.click(screen.getByRole('button', { name: 'Graph' }));
      expect(djClient.DataJunctionAPI.node_dag).toHaveBeenCalledWith(
        mocks.mockMetricNode.name,
      );
    });
  });

  it('renders Linked Nodes tab correctly', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockReturnValue(mocks.mockDimensionNode);
    djClient.DataJunctionAPI.nodesWithDimension.mockReturnValue(
      mocks.mockLinkedNodes,
    );

    const element = (
      <DJClientContext.Provider value={djClient}>
        <NodePage />
      </DJClientContext.Provider>
    );
    render(
      <MemoryRouter initialEntries={['/nodes/default.dispatcher/linked']}>
        <Routes>
          <Route path="nodes/:name/:tab" element={element} />
        </Routes>
      </MemoryRouter>,
    );
    await waitFor(() => {
      fireEvent.click(screen.getByRole('button', { name: 'Linked Nodes' }));
      expect(djClient.DataJunctionAPI.nodesWithDimension).toHaveBeenCalledWith(
        mocks.mockDimensionNode.name,
      );
    });
  });
});
