import React from 'react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { screen, waitFor } from '@testing-library/react';
import { render } from '../../../../setupTests';
import fetchMock from 'jest-fetch-mock';
import { AddEditNodePage } from '../index.jsx';
import { mocks } from '../../../../mocks/mockNodes';
import DJClientContext from '../../../providers/djclient';

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
      metrics: jest
        .fn()
        .mockReturnValue([
          'default.num_repair_orders',
          'default.some_other_metric',
        ]),
      getNodeForEditing: jest.fn(),
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
      tagsNode: jest.fn(),
      listTags: jest.fn().mockReturnValue([]),
      metric: jest.fn().mockReturnValue(mocks.mockMetricNode),
      nodesWithType: jest
        .fn()
        .mockReturnValueOnce(['a'])
        .mockReturnValueOnce(['b'])
        .mockReturnValueOnce(['default.repair_orders']),
      listMetricMetadata: jest.fn().mockReturnValue({
        directions: ['higher_is_better', 'lower_is_better', 'neutral'],
        units: [
          { name: 'dollar', label: 'Dollar' },
          { name: 'second', label: 'Second' },
        ],
      }),
      users: jest.fn().mockReturnValue([
        {
          id: 123,
          username: 'test_user',
        },
        {
          id: 1111,
          username: 'dj',
        },
      ]),
      whoami: jest.fn().mockReturnValue({
        id: 123,
        username: 'test_user',
      }),
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

export const renderCreateMetric = element => {
  return render(
    <MemoryRouter initialEntries={['/create/metric/default']}>
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

export const renderEditTransformNode = element => {
  return render(
    <MemoryRouter
      initialEntries={['/nodes/default.repair_order_transform/edit']}
    >
      <Routes>
        <Route path="nodes/:name/edit" element={element} />
      </Routes>
    </MemoryRouter>,
  );
};

export const renderEditDerivedMetricNode = element => {
  return render(
    <MemoryRouter initialEntries={['/nodes/default.revenue_per_order/edit']}>
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
      screen
        .getAllByText('default')
        .forEach(element => expect(element).toBeInTheDocument());
    });
  });

  it('Edit node page renders with the selected node', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.getNodeForEditing.mockReturnValue(
      mocks.mockGetMetricNode,
    );

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

      // The upstream node should be populated
      expect(screen.getByText('default.repair_orders')).toBeInTheDocument();

      // The aggregate expression should be populated
      expect(screen.getByText('count')).toBeInTheDocument();
      expect(screen.getByText('(repair_order_id)')).toBeInTheDocument();
    });
  });

  it('Verify edit page node not found', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.getNodeForEditing.mockReturnValue(null);
    const element = testElement(mockDjClient);
    renderEditNode(element);

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.getNodeForEditing).toBeCalledTimes(1);
      expect(
        screen.getByText('Node default.num_repair_orders does not exist!'),
      ).toBeInTheDocument();
    });
  }, 60000);

  it('Verify only transforms, metrics, and dimensions can be edited', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.getNodeForEditing.mockReturnValue(
      mocks.mockGetSourceNode,
    );
    const element = testElement(mockDjClient);
    renderEditNode(element);

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.getNodeForEditing).toBeCalledTimes(1);
      expect(
        screen.getByText(
          'Node default.num_repair_orders is of type source and cannot be edited',
        ),
      ).toBeInTheDocument();
    });
  }, 60000);

  it('Edit page renders correctly for derived metric (metric parent)', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.getNodeForEditing.mockReturnValue(
      mocks.mockGetDerivedMetricNode,
    );

    const element = testElement(mockDjClient);
    renderEditDerivedMetricNode(element);

    await waitFor(() => {
      // Should be an edit node page
      expect(screen.getByText('Edit')).toBeInTheDocument();

      // The node name should be loaded onto the page
      expect(screen.getByText('default.revenue_per_order')).toBeInTheDocument();

      // The node type should be loaded onto the page
      expect(screen.getByText('metric')).toBeInTheDocument();

      // The description should be populated
      expect(
        screen.getByText('Average revenue per order (derived metric)'),
      ).toBeInTheDocument();

      // For derived metrics, the upstream node select should show the placeholder
      // (indicating no upstream node is selected - derived metrics have metric parents)
      expect(
        screen.getByText('Select Upstream Node (optional for derived metrics)'),
      ).toBeInTheDocument();
    });
  });

  it('Create metric page renders correctly', async () => {
    const mockDjClient = initializeMockDJClient();
    const element = testElement(mockDjClient);
    renderCreateMetric(element);

    await waitFor(() => {
      // Should be a create metric page
      expect(screen.getByText('Create')).toBeInTheDocument();

      // The metric form should show the derived metric expression label
      // (when no upstream is selected, we're in derived metric mode)
      expect(
        screen.getByText('Derived Metric Expression *'),
      ).toBeInTheDocument();

      // The help text for derived metrics should be visible
      expect(
        screen.getByText(/Reference other metrics using their full names/),
      ).toBeInTheDocument();
    });
  });

  it('Metric page handles error loading metrics gracefully', async () => {
    const mockDjClient = initializeMockDJClient();
    // Make metrics() throw an error
    mockDjClient.DataJunctionAPI.metrics.mockRejectedValue(
      new Error('Network error'),
    );

    const consoleSpy = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});

    const element = testElement(mockDjClient);
    renderCreateMetric(element);

    await waitFor(() => {
      // The page should still render despite the error
      expect(screen.getByText('Create')).toBeInTheDocument();
      // The error should be logged
      expect(consoleSpy).toHaveBeenCalledWith(
        'Failed to load metrics for autocomplete:',
        expect.any(Error),
      );
    });

    consoleSpy.mockRestore();
  });
});
