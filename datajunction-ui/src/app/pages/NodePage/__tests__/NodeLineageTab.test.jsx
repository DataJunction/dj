import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import NodeColumnLineage from '../NodeLineageTab';
import DJClientContext from '../../../providers/djclient';

describe('<NodeColumnLineage />', () => {
  const domTestingLib = require('@testing-library/dom');
  const { queryHelpers } = domTestingLib;

  const queryByAttribute = attribute =>
    queryHelpers.queryAllByAttribute.bind(null, attribute);

  const mockNodeLineage = [
    {
      column_name: 'default_DOT_avg_repair_price',
      node_name: 'default.avg_repair_price',
      node_type: 'metric',
      display_name: 'Default: Avg Repair Price',
      lineage: [
        {
          column_name: 'price',
          node_name: 'default.repair_order_details',
          node_type: 'source',
          display_name: 'Default: Repair Order Details',
          lineage: [],
        },
      ],
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
        node_lineage: jest.fn(name => {
          return mockNodeLineage;
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
    const { container } = render(
      <DJClientContext.Provider value={djClient}>
        <NodeColumnLineage {...defaultProps} />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(djClient.DataJunctionAPI.node_lineage).toHaveBeenCalledWith(
        defaultProps.djNode.name,
      );

      // The origin node should be displayed
      expect(screen.getByText('Default: Avg Repair Price')).toBeInTheDocument();

      // Every node in the DAG should be displayed on the screen
      // mockNodeLineage.forEach(node =>
      //   expect(
      //     getByAttribute(container, node.name, 'data-id'),
      //   ).toBeInTheDocument(),
      // );

      const metricNode = getByAttribute(
        container,
        'default.avg_repair_price',
        'data-id',
      );
    });
  });
});
