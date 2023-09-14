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

export const initializeMockDJClient = () => {
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

export const testElement = djClient => {
  return (
    <DJClientContext.Provider value={djClient}>
      <AddEditNodePage />
    </DJClientContext.Provider>
  );
};

export const renderCreateNode = element => {
  return render(
    <MemoryRouter initialEntries={['/create/dimension/default']}>
      <Routes>
        <Route path="create/:nodeType/:initialNamespace" element={element} />
      </Routes>
    </MemoryRouter>,
  );
};

export const renderEditNode = element => {
  return render(
    <MemoryRouter initialEntries={['/nodes/default.num_repair_orders/edit']}>
      <Routes>
        <Route path="nodes/:name/edit" element={element} />
      </Routes>
    </MemoryRouter>,
  );
};

describe('AddEditNodePage', () => {
  beforeEach(() => {
    fetchMock.resetMocks();
    jest.clearAllMocks();
    window.scrollTo = jest.fn();
  });

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
