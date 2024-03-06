import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import NodeLineage from '../NodeGraphTab';
import DJClientContext from '../../../providers/djclient';

describe('<NodeLineage />', () => {
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
      dimension_links: [],
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
      dimension_links: [],
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
      dimension_links: [],
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
      dimension_links: [],
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
        metric: jest.fn(),
        node_dag: jest.fn(name => {
          return mockNodeDAG;
        }),
      },
    };
  };

  const defaultProps = {
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

  it('renders and calls node_dag with the correct node name', async () => {
    const djClient = mockDJClient();
    djClient.DataJunctionAPI.metric = name => defaultProps.djNode;
    // const layoutFlowMock = jest.spyOn(LayoutFlow);
    const { container } = render(
      <DJClientContext.Provider value={djClient}>
        <NodeLineage {...defaultProps} />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(djClient.DataJunctionAPI.node_dag).toHaveBeenCalledWith(
        defaultProps.djNode.name,
      );

      // The origin node should be displayed
      expect(screen.getByText('Default: Avg Repair Price')).toBeInTheDocument();

      // Every node in the DAG should be displayed on the screen
      mockNodeDAG.forEach(node =>
        expect(
          getByAttribute(container, node.name, 'data-id'),
        ).toBeInTheDocument(),
      );

      const metricNode = getByAttribute(
        container,
        'default.avg_repair_price',
        'data-id',
      );
      expect(screen.getByText('▶ Show dimensions')).toBeInTheDocument();
      expect(screen.getByText('▶ More columns')).toBeInTheDocument();
    });
  });
});
