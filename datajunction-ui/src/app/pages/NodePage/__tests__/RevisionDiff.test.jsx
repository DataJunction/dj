import React from 'react';
import { render, waitFor, screen } from '@testing-library/react';
import RevisionDiff from '../RevisionDiff';
import DJClientContext from '../../../providers/djclient';
import { NodePage } from '../Loadable';
import { MemoryRouter, Route, Routes } from 'react-router-dom';

describe('<RevisionDiff />', () => {
  const mockDjClient = {
    DataJunctionAPI: {
      revisions: jest.fn(),
    },
  };

  const mockNodesWithDimension = [
    {
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
    },
    {
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
          dimension: {
            name: 'default.repair_order',
          },
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
    },
  ];

  beforeEach(() => {
    // Reset the mocks before each test
    mockDjClient.DataJunctionAPI.revisions.mockReset();
  });

  it('renders revision diff', async () => {
    mockDjClient.DataJunctionAPI.revisions.mockReturnValue(
      mockNodesWithDimension,
    );
    const element = (
      <DJClientContext.Provider value={mockDjClient}>
        <RevisionDiff />
      </DJClientContext.Provider>
    );
    const { container } = render(
      <MemoryRouter
        initialEntries={['/nodes/default.repair_orders_cube/revisions/v1.0']}
      >
        <Routes>
          <Route path="nodes/:name/revisions/:revision" element={element} />
        </Routes>
      </MemoryRouter>,
    );
    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.revisions).toHaveBeenCalledWith(
        'default.repair_orders_cube',
      );
    });
  });
});
