import React from 'react';
import { render, waitFor, screen } from '@testing-library/react';
import NodeMaterializationTab from '../NodeMaterializationTab';

describe('<NodeMaterializationTab />', () => {
  const mockDjClient = {
    node: jest.fn(),
    materializations: jest.fn(),
  };

  const mockMaterializations = [
    {
      name: 'mat_one',
      config: {},
      schedule: '@daily',
      job: 'SparkSqlMaterializationJob',
      backfills: [
        {
          spec: [
            {
              column_name: 'date',
              values: ['20200101'],
              range: ['20201010'],
            },
          ],
          urls: ['https://example.com/'],
        },
      ],
      strategy: 'full',
      output_tables: ['table1'],
      urls: ['https://example.com/'],
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
    availability: {
      catalog: 'default',
      categorical_partitions: [],
      max_temporal_partition: ['2023', '01', '25'],
      min_temporal_partition: ['2022', '01', '01'],
      partitions: [],
      schema_: 'foo',
      table: 'bar',
      temporal_partitions: [],
      valid_through_ts: 1729667463,
      url: 'https://www.table.com',
      links: { dashboard: 'https://www.foobar.com/dashboard' },
    },
    columns: [
      {
        name: 'repair_order_id',
        type: 'int',
        attributes: [],
        dimension: null,
        partition: {
          type_: 'temporal',
          format: 'YYYYMMDD',
          granularity: 'day',
        },
      },
      {
        name: 'municipality_id',
        type: 'string',
        attributes: [],
        dimension: null,
        partition: null,
      },
      {
        name: 'hard_hat_id',
        type: 'int',
        attributes: [],
        dimension: null,
        partition: null,
      },
    ],
    updated_at: '2023-08-21T16:48:52.880498+00:00',
    materializations: [
      {
        name: 'mat1',
        config: {},
        schedule: 'string',
        job: 'string',
        backfills: [
          {
            spec: [
              {
                column_name: 'string',
                values: ['string'],
                range: ['string'],
              },
            ],
            urls: ['string'],
          },
        ],
        strategy: 'string',
        output_tables: ['string'],
        urls: ['https://example.com/'],
      },
    ],
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

  beforeEach(() => {
    mockDjClient.materializations.mockReset();
  });

  it('renders NodeMaterializationTab tab correctly', async () => {
    mockDjClient.materializations.mockReturnValue(mockMaterializations);

    render(<NodeMaterializationTab node={mockNode} djClient={mockDjClient} />);
    await waitFor(() => {
      const link = screen.getByText('dashboard').closest('a');
      expect(link).toHaveAttribute('href', `https://www.foobar.com/dashboard`);
    });
  });
});
