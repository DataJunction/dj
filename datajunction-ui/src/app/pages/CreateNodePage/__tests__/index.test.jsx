// CreateNodePage.test.js
import React from 'react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { render, screen, waitFor } from '@testing-library/react';
import fetchMock from 'jest-fetch-mock';
import { CreateNodePage } from '../index.jsx';
import DJClientContext from '../../../providers/djclient';
import userEvent from '@testing-library/user-event';

fetchMock.enableMocks();

describe('CreateNodePage', () => {
  let mockDjClient;

  beforeEach(() => {
    fetchMock.resetMocks();

    // Mocking the djClient context
    mockDjClient = {
      DataJunctionAPI: {
        namespace: _ => {
          return [
            {
              name: 'default.repair_orders',
              display_name: 'Default: Repair Orders',
              version: 'v1.0',
              type: 'source',
              status: 'valid',
              mode: 'published',
              updated_at: '2023-08-21T16:48:52.880498+00:00',
            },
          ];
        },
        metrics: {},
        namespaces: () => {
          return [
            {
              namespace: 'default',
              num_nodes: 33,
            },
          ];
        },
        node: _ => {
          return {
            namespace: 'default',
            node_revision_id: 1,
            node_id: 1,
            type: 'source',
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
            ],
          };
        },
      },
    };
  });

  const renderElement = element => {
    render(
      <MemoryRouter initialEntries={['/create/metric/default']}>
        <Routes>
          <Route path="create/:nodeType/:initialNamespace" element={element} />
        </Routes>
      </MemoryRouter>,
    );
  };

  it('Renders with the selected nodeType and namespace', () => {
    const element = (
      <DJClientContext.Provider value={mockDjClient}>
        <CreateNodePage />
      </DJClientContext.Provider>
    );
    renderElement(element);

    // The node type should be included in the page title
    expect(screen.getByText('Create metric')).toBeInTheDocument();
    // The namespace should be set to the one provided in parmas
    expect(screen.getByText('default')).toBeInTheDocument();
  });

  it('Form submission works with right values', async () => {
    const customCreateNode = jest.fn();
    const element = (
      <DJClientContext.Provider value={mockDjClient}>
        <CreateNodePage customCreateNode={customCreateNode} />
      </DJClientContext.Provider>
    );
    renderElement(element);

    // Fill in display name
    await userEvent.type(
      screen.getByLabelText('Display Name'),
      'Some Test Metric',
    );

    // After typing in a display name, the full name should be updated based on the display name
    expect(
      screen.getByDisplayValue('default.some_test_metric'),
    ).toBeInTheDocument();

    // Fill in the rest of the fields and submit
    await userEvent.type(screen.getByLabelText('Query'), 'SELECT * FROM test');
    await userEvent.click(screen.getByText('Create'));

    await waitFor(() => {
      expect(customCreateNode).toBeCalledTimes(1);
      expect(customCreateNode).toBeCalledWith(
        expect.objectContaining({
          description: '',
          display_name: 'Some Test Metric',
          mode: 'draft',
          name: 'default.some_test_metric',
          namespace: 'default',
          node_type: '',
          query: 'SELECT * FROM test',
        }),
        expect.any(Function),
      );
    });
  });
});
