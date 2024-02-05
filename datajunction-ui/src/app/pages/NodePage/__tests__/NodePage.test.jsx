import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { mocks } from '../../../../mocks/mockNodes';
import DJClientContext from '../../../providers/djclient';
import { NodePage } from '../Loadable';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import userEvent from '@testing-library/user-event';

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
        node_dag: jest.fn().mockReturnValue(mocks.mockNodeDAG),
        clientCode: jest.fn().mockReturnValue('dj_client = DJClient()'),
        columns: jest.fn(),
        history: jest.fn(),
        revisions: jest.fn(),
        materializations: jest.fn(),
        sql: jest.fn(),
        cube: jest.fn(),
        compiledSql: jest.fn(),
        node_lineage: jest.fn(),
        nodesWithDimension: jest.fn(),
        attributes: jest.fn(),
        dimensions: jest.fn(),
        setPartition: jest.fn(),
        engines: jest.fn(),
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
    djClient.DataJunctionAPI.metric.mockReturnValue(mocks.mockMetricNode);
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

      expect(
        container.getElementsByClassName('language-sql'),
      ).toMatchSnapshot();
    });
  }, 60000);

  it('renders the NodeInfo tab correctly for cube nodes', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockReturnValue(mocks.mockCubeNode);
    djClient.DataJunctionAPI.cube.mockReturnValue(mocks.mockCubesCube);
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
    djClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.metric.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.columns.mockReturnValue(mocks.metricNodeColumns);
    djClient.DataJunctionAPI.attributes.mockReturnValue(mocks.attributes);
    djClient.DataJunctionAPI.dimensions.mockReturnValue(mocks.dimensions);
    djClient.DataJunctionAPI.engines.mockReturnValue([]);
    djClient.DataJunctionAPI.setPartition.mockReturnValue({
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
        mocks.mockMetricNode,
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

      // check that the link dimension popover can be clicked
      const linkDimensionPopover = screen.getByRole('button', {
        name: 'LinkDimension',
      });
      expect(linkDimensionPopover).toBeInTheDocument();
      fireEvent.click(linkDimensionPopover);
      expect(
        screen.getByRole('button', { name: 'SaveLinkDimension' }),
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
  // check compiled SQL on nodeInfo page

  it('renders the NodeHistory tab correctly', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.metric.mockReturnValue(mocks.mockMetricNode);
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
      expect(djClient.DataJunctionAPI.revisions).toHaveBeenCalledWith(
        mocks.mockMetricNode.name,
      );
      expect(
        screen.getByRole('table', { name: 'Revisions' }),
      ).toMatchSnapshot();
      expect(screen.getByRole('table', { name: 'Activity' })).toHaveTextContent(
        'ActivityTypeNameUserTimestampDetailscreatenodedefault.avg_repair_priceunknown2023-08-21T16:48:56.950482+00:00',
      );
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

  it('renders compiled sql correctly', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockReturnValue(mocks.mockTransformNode);
    djClient.DataJunctionAPI.columns.mockReturnValue(mocks.metricNodeColumns);
    djClient.DataJunctionAPI.compiledSql.mockReturnValue('select 1');

    const element = (
      <DJClientContext.Provider value={djClient}>
        <NodePage />
      </DJClientContext.Provider>
    );
    render(
      <MemoryRouter initialEntries={[`/nodes/${mocks.mockTransformNode.name}`]}>
        <Routes>
          <Route path="nodes/:name" element={element} />
        </Routes>
      </MemoryRouter>,
    );
    await waitFor(() => {
      fireEvent.click(screen.getByRole('checkbox', { name: 'ToggleSwitch' }));
      expect(djClient.DataJunctionAPI.compiledSql).toHaveBeenCalledWith(
        mocks.mockTransformNode.name,
      );
    });
  });

  it('renders an empty NodeMaterialization tab correctly', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.metric.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.columns.mockReturnValue(mocks.metricNodeColumns);
    djClient.DataJunctionAPI.materializations.mockReturnValue([]);

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
    await waitFor(() => {
      fireEvent.click(screen.getByRole('button', { name: 'Materializations' }));
      expect(djClient.DataJunctionAPI.materializations).toHaveBeenCalledWith(
        mocks.mockMetricNode.name,
      );
      screen.getByText(
        'No materialization workflows configured for this node.',
      );
      screen.getByText('No materialized datasets available for this node.');
    });
  });

  it('renders the NodeMaterialization tab with materializations correctly', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.metric.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.columns.mockReturnValue(mocks.metricNodeColumns);
    djClient.DataJunctionAPI.materializations.mockReturnValue(
      mocks.nodeMaterializations,
    );

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
    await waitFor(
      () => {
        fireEvent.click(
          screen.getByRole('button', { name: 'Materializations' }),
        );
        expect(djClient.DataJunctionAPI.node).toHaveBeenCalledWith(
          mocks.mockMetricNode.name,
        );
        expect(djClient.DataJunctionAPI.materializations).toHaveBeenCalledWith(
          mocks.mockMetricNode.name,
        );

        expect(
          screen.getByRole('table', { name: 'Materializations' }),
        ).toMatchSnapshot();
      },
      { timeout: 3000 },
    );
  }, 60000);

  it('renders the NodeSQL tab', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.metric.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.columns.mockReturnValue(mocks.metricNodeColumns);
    djClient.DataJunctionAPI.sql.mockReturnValue(mocks.metricNodeColumns);
    const element = (
      <DJClientContext.Provider value={djClient}>
        <NodePage />
      </DJClientContext.Provider>
    );
    render(
      <MemoryRouter initialEntries={['/nodes/default.num_repair_orders/sql']}>
        <Routes>
          <Route path="nodes/:name/:tab" element={element} />
        </Routes>
      </MemoryRouter>,
    );
    await waitFor(() => {
      const sqlButton = screen.getByRole('button', { name: 'SQL' });
      sqlButton.click();
      expect(djClient.DataJunctionAPI.sql).toHaveBeenCalledWith(
        'default.num_repair_orders',
        { dimensions: [], filters: [] },
      );
    });
  });

  it('renders a NodeColumnLineage tab correctly', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.metric.mockReturnValue(mocks.mockMetricNodeJson);
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
    djClient.DataJunctionAPI.metric.mockReturnValue(mocks.mockMetricNodeJson);
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
