import React from 'react';
import { render, waitFor, screen } from '@testing-library/react';
import NodeDependenciesTab from '../NodeDependenciesTab';

describe('<NodeDependenciesTab />', () => {
  const mockDjClient = {
    node: jest.fn(),
    nodeDimensions: jest.fn(),
    upstreams: jest.fn(),
    downstreams: jest.fn(),
  };

  const mockNode = {
    node_revision_id: 1,
    node_id: 1,
    type: 'source',
    name: 'default.repair_orders',
    display_name: 'Default: Repair Orders',
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
    table: 'repair_orders',
    description: 'Repair orders',
    query: null,
    availability: null,
    columns: [
      {
        name: 'repair_order_id',
        type: 'int',
        attributes: [],
        dimension: null,
      },
      {
        name: 'municipality_id',
        type: 'string',
        attributes: [],
        dimension: null,
      },
      {
        name: 'hard_hat_id',
        type: 'int',
        attributes: [],
        dimension: null,
      },
      {
        name: 'order_date',
        type: 'date',
        attributes: [],
        dimension: null,
      },
      {
        name: 'required_date',
        type: 'date',
        attributes: [],
        dimension: null,
      },
      {
        name: 'dispatched_date',
        type: 'date',
        attributes: [],
        dimension: null,
      },
      {
        name: 'dispatcher_id',
        type: 'int',
        attributes: [],
        dimension: null,
      },
    ],
    updated_at: '2023-08-21T16:48:52.880498+00:00',
    materializations: [],
    parents: [],
    dimension_links: [
      {
        dimension: {
          name: 'default.contractor',
        },
        join_type: 'left',
        join_sql:
          'default.contractor.contractor_id = default.repair_orders.contractor_id',
        join_cardinality: 'one_to_one',
        role: 'contractor',
      },
    ],
  };

  const mockDimensions = [
    {
      properties: [],
      name: 'default.dispatcher.company_name',
      node_display_name: 'Default: Dispatcher',
      node_name: 'default.dispatcher',
      path: ['default.repair_orders_fact.dispatcher_id'],
      type: 'string',
    },
    {
      properties: ['primary_key'],
      name: 'default.dispatcher.dispatcher_id',
      node_display_name: 'Default: Dispatcher',
      node_name: 'default.dispatcher',
      path: ['default.repair_orders_fact.dispatcher_id'],
      type: 'int',
    },
    {
      properties: [],
      name: 'default.hard_hat.city',
      node_display_name: 'Default: Hard Hat',
      node_name: 'default.hard_hat',
      path: ['default.repair_orders_fact.hard_hat_id'],
      type: 'string',
    },
    {
      properties: ['primary_key'],
      name: 'default.hard_hat.hard_hat_id',
      node_display_name: 'Default: Hard Hat',
      node_name: 'default.hard_hat',
      path: ['default.repair_orders_fact.hard_hat_id'],
      type: 'int',
    },
  ];

  beforeEach(() => {
    // Reset the mocks before each test
    mockDjClient.nodeDimensions.mockReset();
    mockDjClient.upstreams.mockReset();
    mockDjClient.downstreams.mockReset();
  });

  it('renders nodes with dimensions', async () => {
    mockDjClient.nodeDimensions.mockReturnValue(mockDimensions);
    mockDjClient.upstreams.mockReturnValue([mockNode]);
    mockDjClient.downstreams.mockReturnValue([mockNode]);
    render(<NodeDependenciesTab node={mockNode} djClient={mockDjClient} />);
    await waitFor(() => {
      for (const dimension of mockDimensions) {
        const link = screen.getByText(dimension.node_display_name).closest('a');
        expect(link).toHaveAttribute('href', `/nodes/${dimension.node_name}`);
        expect(screen.getByText(dimension.name)).toBeInTheDocument();
      }
    });
  });
});
