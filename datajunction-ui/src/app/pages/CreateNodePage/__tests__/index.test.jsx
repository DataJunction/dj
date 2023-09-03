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
    window.scrollTo = jest.fn();
    mockDjClient = {
      DataJunctionAPI: {
        namespace: _ => {
          return [
            {
              name: 'default.contractors',
              display_name: 'Default: Contractors',
              version: 'v1.0',
              type: 'source',
              status: 'valid',
              mode: 'published',
              updated_at: '2023-08-21T16:48:53.246914+00:00',
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
        createNode: jest.fn(),
        node: _ => {
          return {
            namespace: 'default',
            node_revision_id: 1,
            node_id: 1,
            type: 'source',
            columns: [
              {
                name: 'contractor_id',
                type: 'int',
                attributes: [],
                dimension: {
                  name: 'default.repair_order',
                },
              },
              {
                name: 'contact_title',
                type: 'string',
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
    return render(
      <MemoryRouter initialEntries={['/create/dimension/default']}>
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
    const { container } = renderElement(element);

    // The node type should be included in the page title
    expect(
      container.getElementsByClassName('node_type__metric'),
    ).toMatchSnapshot();

    // The namespace should be set to the one provided in params
    expect(screen.getByText('default')).toBeInTheDocument();
  });

  it('Verify form user interaction and successful submission', async () => {
    const element = (
      <DJClientContext.Provider value={mockDjClient}>
        <CreateNodePage />
      </DJClientContext.Provider>
    );
    mockDjClient.DataJunctionAPI.createNode.mockReturnValue({
      status: 200,
      json: { name: 'default.special_forces_contractors', type: 'dimension' },
    });
    const query =
      'select \n' +
      '  C.contractor_id,\n' +
      '  C.address, C.contact_title, C.contact_name, C.city from default.contractors C \n' +
      "where C.contact_title = 'special forces agent'";

    const { container } = renderElement(element);

    // Fill in display name
    await userEvent.type(
      screen.getByLabelText('Display Name'),
      'Special Forces Contractors',
    );

    // After typing in a display name, the full name should be updated based on the display name
    expect(
      screen.getByDisplayValue('default.special_forces_contractors'),
    ).toBeInTheDocument();

    // Fill in the rest of the fields and submit
    await userEvent.type(screen.getByLabelText('Query'), query);
    await userEvent.type(screen.getByLabelText('Primary Key'), 'contractor_id');
    await userEvent.click(screen.getByText('Create dimension'));

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.createNode).toBeCalledTimes(1);
      expect(mockDjClient.DataJunctionAPI.createNode).toBeCalledWith(
        'dimension',
        'default.special_forces_contractors',
        'Special Forces Contractors',
        '',
        query,
        'draft',
        'default',
        ['contractor_id'],
      );
    });

    // After successful creation, it should return a success message
    expect(container.getElementsByClassName('success')).toMatchSnapshot();
  }, 60000);

  it('Verify form failed submission', async () => {
    const element = (
      <DJClientContext.Provider value={mockDjClient}>
        <CreateNodePage />
      </DJClientContext.Provider>
    );
    mockDjClient.DataJunctionAPI.createNode.mockReturnValue({
      status: 500,
      json: { message: 'Some columns in the primary key [] were not found' },
    });

    const { container } = renderElement(element);

    await userEvent.type(
      screen.getByLabelText('Display Name'),
      'Some Test Metric',
    );
    await userEvent.type(screen.getByLabelText('Query'), 'SELECT * FROM test');
    await userEvent.click(screen.getByText('Create dimension'));

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.createNode).toBeCalledTimes(1);
      expect(mockDjClient.DataJunctionAPI.createNode).toBeCalledWith(
        'dimension',
        'default.some_test_metric',
        'Some Test Metric',
        '',
        'SELECT * FROM test',
        'draft',
        'default',
        [''],
      );
    });

    // After failed creation, it should return a failure message
    expect(container.getElementsByClassName('alert')).toMatchSnapshot();
  }, 60000);
});
