import React from 'react';
import { render, waitFor, screen } from '@testing-library/react';
import NodeColumnTab from '../NodeColumnTab';

describe('<NodeColumnTab />', () => {
  const mockDjClient = {
    node: jest.fn(),
    columns: jest.fn(),
    attributes: jest.fn(),
    dimensions: jest.fn(),
  };

  const mockNodeColumns = [
    {
      name: 'repair_order_id',
      display_name: 'Repair Order Id',
      type: 'int',
      attributes: [],
      dimension: null,
    },
    {
      name: 'municipality_id',
      display_name: 'Municipality Id',
      type: 'string',
      attributes: [],
      dimension: null,
    },
    {
      name: 'hard_hat_id',
      display_name: 'Hard Hat Id',
      type: 'int',
      attributes: [],
      dimension: null,
    },
    {
      name: 'order_date',
      display_name: 'Order Date',
      type: 'date',
      attributes: [],
      dimension: null,
    },
    {
      name: 'required_date',
      display_name: 'Required Date',
      type: 'date',
      attributes: [],
      dimension: null,
    },
    {
      name: 'dispatched_date',
      display_name: 'Dispatched Date',
      type: 'date',
      attributes: [],
      dimension: null,
    },
    {
      name: 'dispatcher_id',
      display_name: 'Dispatcher Id',
      type: 'int',
      attributes: [],
      dimension: null,
    },
  ];

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
    columns: mockNodeColumns,
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
        foreign_keys: {
          'default.repair_orders.contractor_id':
            'default.contractor.contractor_id',
        },
      },
    ],
  };

  const mockAttributes = [
    {
      uniqueness_scope: [],
      namespace: 'system',
      name: 'primary_key',
      description:
        'Points to a column which is part of the primary key of the node',
      allowed_node_types: ['source', 'transform', 'dimension'],
      id: 1,
    },
    {
      uniqueness_scope: [],
      namespace: 'system',
      name: 'dimension',
      description: 'Points to a dimension attribute column',
      allowed_node_types: ['source', 'transform'],
      id: 2,
    },
  ];

  const mockDimensions = ['default.contractor', 'default.hard_hat'];

  beforeEach(() => {
    // Reset the mocks before each test
    mockDjClient.node.mockReset();
    mockDjClient.columns.mockReset();
    mockDjClient.attributes.mockReset();
    mockDjClient.dimensions.mockReset();
  });

  it('renders node columns and dimension links', async () => {
    mockDjClient.node.mockReturnValue(mockNode);
    mockDjClient.columns.mockReturnValue(mockNodeColumns);
    mockDjClient.attributes.mockReturnValue(mockAttributes);
    mockDjClient.dimensions.mockReturnValue(mockDimensions);

    render(<NodeColumnTab node={mockNode} djClient={mockDjClient} />);

    await waitFor(() => {
      // Displays the columns
      for (const column of mockNode.columns) {
        expect(screen.getByText(column.name)).toBeInTheDocument();
        expect(screen.getByText(column.display_name)).toBeInTheDocument();
      }

      // Displays the dimension links
      for (const dimensionLink of mockNode.dimension_links) {
        const link = screen
          .getByText(dimensionLink.dimension.name)
          .closest('a');
        expect(link).toHaveAttribute(
          'href',
          `/nodes/${dimensionLink.dimension.name}`,
        );
      }
    });
  });
});
