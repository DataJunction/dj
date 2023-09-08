import React from 'react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { screen, waitFor } from '@testing-library/react';
import { render } from '../../../../setupTests';
import fetchMock from 'jest-fetch-mock';
import { AddEditNodePage } from '../index.jsx';
import { mocks } from '../../../../mocks/mockNodes';
import DJClientContext from '../../../providers/djclient';
import userEvent from '@testing-library/user-event';

fetchMock.enableMocks();

describe('AddEditNodePage', () => {
  const initializeMockDJClient = () => {
    return {
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
            {
              name: 'default.num_repair_orders',
              display_name: 'Default: Num Repair Orders',
              version: 'v1.0',
              type: 'metric',
              status: 'valid',
              mode: 'published',
              updated_at: '2023-08-21T16:48:56.841704+00:00',
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
            {
              namespace: 'default123',
              num_nodes: 0,
            },
          ];
        },
        createNode: jest.fn(),
        patchNode: jest.fn(),
        node: jest.fn(),
      },
    };
  };

  const testElement = djClient => {
    return (
      <DJClientContext.Provider value={djClient}>
        <AddEditNodePage />
      </DJClientContext.Provider>
    );
  };

  beforeEach(() => {
    fetchMock.resetMocks();
    jest.clearAllMocks();
    window.scrollTo = jest.fn();
  });

  const renderCreateNode = element => {
    return render(
      <MemoryRouter initialEntries={['/create/dimension/default']}>
        <Routes>
          <Route path="create/:nodeType/:initialNamespace" element={element} />
        </Routes>
      </MemoryRouter>,
    );
  };

  const renderEditNode = element => {
    return render(
      <MemoryRouter initialEntries={['/nodes/default.num_repair_orders/edit']}>
        <Routes>
          <Route path="nodes/:name/edit" element={element} />
        </Routes>
      </MemoryRouter>,
    );
  };

  it('Create node page renders with the selected nodeType and namespace', async () => {
    const mockDjClient = initializeMockDJClient();
    const element = testElement(mockDjClient);
    const { container } = renderCreateNode(element);

    // The node type should be included in the page title
    await waitFor(() => {
      expect(
        container.getElementsByClassName('node_type__metric'),
      ).toMatchSnapshot();

      // The namespace should be set to the one provided in params
      expect(screen.getByText('default')).toBeInTheDocument();
    });
  });

  it('Edit node page renders with the selected node', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);

    const element = testElement(mockDjClient);
    renderEditNode(element);

    await waitFor(() => {
      // Should be an edit node page
      expect(screen.getByText('Edit')).toBeInTheDocument();

      // The node name should be loaded onto the page
      expect(screen.getByText('default.num_repair_orders')).toBeInTheDocument();

      // The node type should be loaded onto the page
      expect(screen.getByText('metric')).toBeInTheDocument();

      // The description should be populated
      expect(screen.getByText('Number of repair orders')).toBeInTheDocument();

      // The query should be populated
      expect(
        screen.getByText(
          'SELECT count(repair_order_id) default_DOT_num_repair_orders FROM default.repair_orders',
        ),
      ).toBeInTheDocument();
    });
  });

  it('Verify create node page user interaction and successful form submission', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.createNode.mockReturnValue({
      status: 200,
      json: { name: 'default.special_forces_contractors', type: 'dimension' },
    });

    const element = testElement(mockDjClient);
    const { container } = renderCreateNode(element);

    const query =
      'select \n' +
      '  C.contractor_id,\n' +
      '  C.address, C.contact_title, C.contact_name, C.city from default.contractors C \n' +
      "where C.contact_title = 'special forces agent'";

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
    await userEvent.type(
      screen.getByLabelText('Description'),
      'A curated list of special forces contractors',
    );
    await userEvent.type(screen.getByLabelText('Primary Key'), 'contractor_id');
    await userEvent.click(screen.getByText('Create dimension'));

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.createNode).toBeCalled();
      expect(mockDjClient.DataJunctionAPI.createNode).toBeCalledWith(
        'dimension',
        'default.special_forces_contractors',
        'Special Forces Contractors',
        'A curated list of special forces contractors',
        query,
        'draft',
        'default',
        ['contractor_id'],
      );
      expect(
        screen.getByText(/Successfully created dimension node/),
      ).toBeInTheDocument();
    });

    // After successful creation, it should return a success message
    expect(container.getElementsByClassName('success')).toMatchSnapshot();
  }, 60000);

  it('Verify create node page form failed submission', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.createNode.mockReturnValue({
      status: 500,
      json: { message: 'Some columns in the primary key [] were not found' },
    });

    const element = testElement(mockDjClient);
    const { container } = renderCreateNode(element);

    await userEvent.type(
      screen.getByLabelText('Display Name'),
      'Some Test Metric',
    );
    await userEvent.type(screen.getByLabelText('Query'), 'SELECT * FROM test');
    await userEvent.click(screen.getByText('Create dimension'));

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.createNode).toBeCalled();
      expect(mockDjClient.DataJunctionAPI.createNode).toBeCalledWith(
        'dimension',
        'default.some_test_metric',
        'Some Test Metric',
        '',
        'SELECT * FROM test',
        'draft',
        'default',
        null,
      );
      expect(
        screen.getByText(/Some columns in the primary key \[] were not found/),
      ).toBeInTheDocument();
    });

    // After failed creation, it should return a failure message
    expect(container.getElementsByClassName('alert')).toMatchSnapshot();
  }, 60000);

  it('Verify edit node page form submission success', async () => {
    const mockDjClient = initializeMockDJClient();

    mockDjClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);
    mockDjClient.DataJunctionAPI.patchNode = jest.fn();
    mockDjClient.DataJunctionAPI.patchNode.mockReturnValue({
      status: 201,
      json: { name: 'default.num_repair_orders', type: 'metric' },
    });

    const element = testElement(mockDjClient);
    renderEditNode(element);

    await userEvent.type(screen.getByLabelText('Display Name'), '!!!');
    await userEvent.type(screen.getByLabelText('Description'), '!!!');
    await userEvent.click(screen.getByText('Save'));

    await waitFor(async () => {
      expect(mockDjClient.DataJunctionAPI.patchNode).toBeCalledTimes(1);
      expect(mockDjClient.DataJunctionAPI.patchNode).toBeCalledWith(
        'default.num_repair_orders',
        'Default: Num Repair Orders!!!',
        'Number of repair orders!!!',
        'SELECT count(repair_order_id) default_DOT_num_repair_orders FROM default.repair_orders',
        'published',
        ['repair_order_id', 'country'],
      );
      expect(
        await screen.getByDisplayValue('repair_order_id, country'),
      ).toBeInTheDocument();
      expect(
        await screen.getByText(/Successfully updated metric node/),
      ).toBeInTheDocument();
    });
  }, 1000000);

  it('Verify edit node page form submission failure displays alert', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.node.mockReturnValue(mocks.mockMetricNode);
    mockDjClient.DataJunctionAPI.patchNode.mockReturnValue({
      status: 500,
      json: { message: 'Update failed' },
    });

    const element = testElement(mockDjClient);
    renderEditNode(element);

    await userEvent.type(screen.getByLabelText('Display Name'), '!!!');
    await userEvent.type(screen.getByLabelText('Description'), '!!!');
    await userEvent.click(screen.getByText('Save'));
    await waitFor(async () => {
      expect(mockDjClient.DataJunctionAPI.patchNode).toBeCalledTimes(1);
      expect(await screen.getByText('Update failed')).toBeInTheDocument();
    });
  }, 60000);

  it('Verify edit page node not found', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.node = jest.fn();
    mockDjClient.DataJunctionAPI.node.mockReturnValue({
      message: 'A node with name `default.num_repair_orders` does not exist.',
      errors: [],
      warnings: [],
    });
    const element = testElement(mockDjClient);
    renderEditNode(element);

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.node).toBeCalledTimes(1);
      expect(
        screen.getByText('Node default.num_repair_orders does not exist!'),
      ).toBeInTheDocument();
    });
  }, 60000);

  it('Verify only transforms, metrics, and dimensions can be edited', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.node = jest.fn();
    mockDjClient.DataJunctionAPI.node.mockReturnValue({
      namespace: 'default',
      type: 'source',
      name: 'default.repair_orders',
      display_name: 'Default: Repair Orders',
    });
    const element = testElement(mockDjClient);
    renderEditNode(element);

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.node).toBeCalledTimes(1);
      expect(
        screen.getByText(
          'Node default.num_repair_orders is of type source and cannot be edited',
        ),
      ).toBeInTheDocument();
    });
  }, 60000);
});
