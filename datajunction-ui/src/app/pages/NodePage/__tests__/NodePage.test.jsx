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

  const mockNodeDAG = [
    {
      namespace: 'default',
      node_revision_id: 2,
      node_id: 2,
      type: 'source',
      name: 'default.repair_order_details',
      display_name: 'Default: Repair Order Details',
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
      schema_: 'roads',
      table: 'repair_order_details',
      description: 'Details on repair orders',
      query: null,
      availability: null,
      columns: [
        {
          name: 'repair_order_id',
          type: 'int',
          attributes: [],
          dimension: {
            name: 'default.repair_order',
          },
        },
        {
          name: 'repair_type_id',
          type: 'int',
          attributes: [],
          dimension: null,
        },
        {
          name: 'price',
          type: 'float',
          attributes: [],
          dimension: null,
        },
        {
          name: 'quantity',
          type: 'int',
          attributes: [],
          dimension: null,
        },
        {
          name: 'discount',
          type: 'float',
          attributes: [],
          dimension: null,
        },
      ],
      updated_at: '2023-08-21T16:48:52.981201+00:00',
      materializations: [],
      parents: [],
      created_at: '2023-08-21T16:48:52.970554+00:00',
      tags: [],
    },
    {
      namespace: 'default',
      node_revision_id: 14,
      node_id: 14,
      type: 'dimension',
      name: 'default.date_dim',
      display_name: 'Default: Date Dim',
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
      description: 'Date dimension',
      query:
        '\n            SELECT\n              dateint,\n              month,\n              year,\n              day\n            FROM default.date\n        ',
      availability: null,
      columns: [
        {
          name: 'dateint',
          type: 'timestamp',
          attributes: [
            {
              attribute_type: {
                namespace: 'system',
                name: 'primary_key',
              },
            },
          ],
          dimension: null,
        },
        {
          name: 'month',
          type: 'int',
          attributes: [],
          dimension: null,
        },
        {
          name: 'year',
          type: 'int',
          attributes: [],
          dimension: null,
        },
        {
          name: 'day',
          type: 'int',
          attributes: [],
          dimension: null,
        },
      ],
      updated_at: '2023-08-21T16:48:54.726980+00:00',
      materializations: [],
      parents: [
        {
          name: 'default.date',
        },
      ],
      created_at: '2023-08-21T16:48:54.726871+00:00',
      tags: [],
    },
    {
      namespace: 'default',
      node_revision_id: 18,
      node_id: 18,
      type: 'dimension',
      name: 'default.hard_hat',
      display_name: 'Default: Hard Hat',
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
      description: 'Hard hat dimension',
      query:
        '\n            SELECT\n            hard_hat_id,\n            last_name,\n            first_name,\n            title,\n            birth_date,\n            hire_date,\n            address,\n            city,\n            state,\n            postal_code,\n            country,\n            manager,\n            contractor_id\n            FROM default.hard_hats\n        ',
      availability: null,
      columns: [
        {
          name: 'hard_hat_id',
          type: 'int',
          attributes: [
            {
              attribute_type: {
                namespace: 'system',
                name: 'primary_key',
              },
            },
          ],
          dimension: null,
        },
        {
          name: 'last_name',
          type: 'string',
          attributes: [],
          dimension: null,
        },
        {
          name: 'first_name',
          type: 'string',
          attributes: [],
          dimension: null,
        },
        {
          name: 'title',
          type: 'string',
          attributes: [],
          dimension: null,
        },
        {
          name: 'birth_date',
          type: 'date',
          attributes: [],
          dimension: {
            name: 'default.date_dim',
          },
        },
        {
          name: 'hire_date',
          type: 'date',
          attributes: [],
          dimension: {
            name: 'default.date_dim',
          },
        },
        {
          name: 'address',
          type: 'string',
          attributes: [],
          dimension: null,
        },
        {
          name: 'city',
          type: 'string',
          attributes: [],
          dimension: null,
        },
        {
          name: 'state',
          type: 'string',
          attributes: [],
          dimension: {
            name: 'default.us_state',
          },
        },
        {
          name: 'postal_code',
          type: 'string',
          attributes: [],
          dimension: null,
        },
        {
          name: 'country',
          type: 'string',
          attributes: [],
          dimension: null,
        },
        {
          name: 'manager',
          type: 'int',
          attributes: [],
          dimension: null,
        },
        {
          name: 'contractor_id',
          type: 'int',
          attributes: [],
          dimension: null,
        },
      ],
      updated_at: '2023-08-21T16:48:55.594603+00:00',
      materializations: [],
      parents: [
        {
          name: 'default.hard_hats',
        },
      ],
      created_at: '2023-08-21T16:48:55.594537+00:00',
      tags: [],
    },
    {
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
      columns: [
        {
          name: 'default_DOT_avg_repair_price',
          type: 'double',
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
      tags: [],
    },
  ];

  function getByAttribute(container, id, attribute, ...rest) {
    const result = queryByAttribute(attribute)(container, id, ...rest);
    return result[0];
  }

  const mockDJClient = () => {
    return {
      DataJunctionAPI: {
        node: jest.fn(),
        metric: jest.fn(),
        node_dag: jest.fn().mockReturnValue(mockNodeDAG),
        clientCode: jest.fn().mockReturnValue('dj_client = DJClient()'),
        columns: jest.fn(),
        history: jest.fn(),
        revisions: jest.fn(),
        materializations: jest.fn(),
        sql: jest.fn(),
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
      columns: [
        {
          name: 'default_DOT_avg_repair_price',
          type: 'double',
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
      tags: [],
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

  it('renders the NodeInfo tab correctly', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.metric.mockReturnValue(mocks.mockMetricNode);
    const element = (
      <DJClientContext.Provider value={djClient}>
        <NodePage {...defaultProps} />
      </DJClientContext.Provider>
    );
    const { container } = render(
      <MemoryRouter initialEntries={['/nodes/default.num_repair_orders']}>
        <Routes>
          <Route path="nodes/:name" element={element} />
        </Routes>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(djClient.DataJunctionAPI.node).toHaveBeenCalledWith(
        'default.num_repair_orders',
      );
      userEvent.click(screen.getByRole('button', { name: 'Info' }));

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

      // expect(screen.getByRole('dialog', { name: 'Table' })).not.toBeInTheDocument();
      expect(
        screen.getByRole('dialog', { name: 'NodeStatus' }),
      ).toBeInTheDocument();

      expect(screen.getByRole('dialog', { name: 'Tags' })).toHaveTextContent(
        '',
      );

      expect(
        screen.getByRole('dialog', { name: 'PrimaryKey' }),
      ).toHaveTextContent('repair_order_id, country');

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
  });

  it('renders the NodeColumns tab correctly', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.metric.mockReturnValue(mocks.mockMetricNode);
    djClient.DataJunctionAPI.columns.mockReturnValue(mocks.metricNodeColumns);
    const element = (
      <DJClientContext.Provider value={djClient}>
        <NodePage />
      </DJClientContext.Provider>
    );
    render(
      <MemoryRouter initialEntries={['/nodes/default.num_repair_orders']}>
        <Routes>
          <Route path="nodes/:name" element={element} />
        </Routes>
      </MemoryRouter>,
    );
    await waitFor(() => {
      fireEvent.click(screen.getByRole('button', { name: 'Columns' }));
      expect(djClient.DataJunctionAPI.columns).toHaveBeenCalledWith(
        mocks.mockMetricNode,
      );
      expect(
        screen.getByRole('columnheader', { name: 'ColumnName' }),
      ).toHaveTextContent('default_DOT_avg_repair_price');
      expect(
        screen.getByRole('columnheader', { name: 'ColumnType' }),
      ).toHaveTextContent('double');
    });
  });
  // check compiled SQL on nodeInfo page
  // check cube nodes on nodeinfo page
  // check history

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
    render(
      <MemoryRouter initialEntries={['/nodes/default.num_repair_orders']}>
        <Routes>
          <Route path="nodes/:name" element={element} />
        </Routes>
      </MemoryRouter>,
    );
    await waitFor(() => {
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
      <MemoryRouter initialEntries={['/nodes/default.num_repair_orders']}>
        <Routes>
          <Route path="nodes/:name" element={element} />
        </Routes>
      </MemoryRouter>,
    );
    await waitFor(() => {
      fireEvent.click(screen.getByRole('button', { name: 'Materializations' }));
      expect(djClient.DataJunctionAPI.materializations).toHaveBeenCalledWith(
        mocks.mockMetricNode.name,
      );
      screen.getByText('No materializations available for this node');
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
      <MemoryRouter initialEntries={['/nodes/default.num_repair_orders']}>
        <Routes>
          <Route path="nodes/:name" element={element} />
        </Routes>
      </MemoryRouter>,
    );
    await waitFor(() => {
      fireEvent.click(screen.getByRole('button', { name: 'Materializations' }));
      expect(djClient.DataJunctionAPI.node).toHaveBeenCalledWith(
        mocks.mockMetricNode.name,
      );
      expect(djClient.DataJunctionAPI.materializations).toHaveBeenCalledWith(
        mocks.mockMetricNode.name,
      );

      expect(
        screen.getByRole('table', { name: 'Materializations' }),
      ).toMatchSnapshot();
    });
  });

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
      <MemoryRouter initialEntries={['/nodes/default.num_repair_orders']}>
        <Routes>
          <Route path="nodes/:name" element={element} />
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
});
