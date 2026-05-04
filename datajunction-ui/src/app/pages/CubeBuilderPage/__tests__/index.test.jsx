import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import DJClientContext from '../../../providers/djclient';
import { CubeBuilderPage } from '../index';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import React from 'react';

const mockDjClient = {
  metrics: jest.fn(),
  searchMetrics: jest.fn(),
  getMetricsInfo: jest.fn(),
  commonDimensions: jest.fn(),
  createCube: jest.fn(),
  namespaces: jest.fn(),
  cube: jest.fn(),
  getCubeForEditing: jest.fn(),
  node: jest.fn(),
  listTags: jest.fn(),
  tagsNode: jest.fn(),
  patchCube: jest.fn(),
  users: jest.fn(),
  whoami: jest.fn(),
  metricsV3: jest.fn(),
};

const mockMetrics = [
  'default.num_repair_orders',
  'default.avg_repair_price',
  'default.total_repair_cost',
];

const mockCube = {
  name: 'default.repair_orders_cube',
  type: 'CUBE',
  owners: [
    {
      username: 'someone@example.com',
    },
  ],
  current: {
    displayName: 'Default: Repair Orders Cube',
    description: 'Repairs cube',
    mode: 'DRAFT',
    cubeMetrics: [
      {
        name: 'default.total_repair_cost',
      },
      {
        name: 'default.num_repair_orders',
      },
    ],
    cubeDimensions: [
      {
        name: 'default.hard_hat.country',
        attribute: 'country',
        properties: ['dimension'],
      },
      {
        name: 'default.hard_hat.state',
        attribute: 'state',
        properties: ['dimension'],
      },
    ],
  },
  tags: [
    {
      name: 'repairs',
      displayName: 'Repairs Domain',
    },
  ],
};

const mockCommonDimensions = [
  {
    name: 'default.date_dim.dateint',
    type: 'timestamp',
    node_name: 'default.date_dim',
    node_display_name: 'Date',
    properties: [],
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.birth_date',
    ],
  },
  {
    name: 'default.date_dim.dateint',
    type: 'timestamp',
    node_name: 'default.date_dim',
    node_display_name: 'Date',
    properties: [],
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.hire_date',
    ],
  },
  {
    name: 'default.date_dim.day',
    type: 'int',
    node_name: 'default.date_dim',
    node_display_name: 'Date',
    properties: [],
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.birth_date',
    ],
  },
  {
    name: 'default.date_dim.day',
    type: 'int',
    node_name: 'default.date_dim',
    node_display_name: 'Date',
    properties: [],
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.hire_date',
    ],
  },
  {
    name: 'default.date_dim.month',
    type: 'int',
    node_name: 'default.date_dim',
    node_display_name: 'Date',
    properties: [],
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.birth_date',
    ],
  },
  {
    name: 'default.date_dim.month',
    type: 'int',
    node_name: 'default.date_dim',
    node_display_name: 'Date',
    properties: [],
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.hire_date',
    ],
  },
  {
    name: 'default.date_dim.year',
    type: 'int',
    node_name: 'default.date_dim',
    node_display_name: 'Date',
    properties: [],
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.birth_date',
    ],
  },
  {
    name: 'default.date_dim.year',
    type: 'int',
    node_name: 'default.date_dim',
    node_display_name: 'Date',
    properties: [],
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.hire_date',
    ],
  },
];

const mockSearchMetricsResults = mockMetrics.map(m => ({
  value: m,
  label: m,
}));

describe('CubeBuilderPage', () => {
  beforeEach(() => {
    mockDjClient.metrics.mockResolvedValue(mockMetrics);
    mockDjClient.searchMetrics.mockResolvedValue(mockSearchMetricsResults);
    mockDjClient.commonDimensions.mockResolvedValue(mockCommonDimensions);
    mockDjClient.createCube.mockResolvedValue({ status: 201, json: {} });
    mockDjClient.namespaces.mockResolvedValue(['default']);
    mockDjClient.getCubeForEditing.mockResolvedValue(mockCube);
    mockDjClient.listTags.mockResolvedValue([]);
    mockDjClient.tagsNode.mockResolvedValue([]);
    mockDjClient.patchCube.mockResolvedValue({ status: 201, json: {} });
    mockDjClient.users.mockResolvedValue([{ username: 'dj' }]);
    mockDjClient.whoami.mockResolvedValue({ username: 'dj' });
    mockDjClient.getMetricsInfo.mockResolvedValue([]);
    mockDjClient.metricsV3.mockResolvedValue({ sql: '', errors: [] });

    window.scrollTo = jest.fn();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('renders without crashing', async () => {
    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <CubeBuilderPage />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    expect(screen.getByText('Cube')).toBeInTheDocument();
  });

  it('renders the Metrics section', async () => {
    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <CubeBuilderPage />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    expect(
      screen.getByRole('heading', { name: 'Metrics' }),
    ).toBeInTheDocument();
  });

  it('renders the Dimensions section', async () => {
    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <CubeBuilderPage />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    expect(
      screen.getByRole('heading', { name: 'Dimensions' }),
    ).toBeInTheDocument();
  });

  it('shows the Create Cube submit button in Add mode', async () => {
    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <CubeBuilderPage />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    expect(
      await screen.findByRole('button', { name: 'CreateCube' }),
    ).toHaveTextContent('Create Cube');
  });

  it('renders the Edit page with prefilled fields and saves via patchCube', async () => {
    render(
      <MemoryRouter
        initialEntries={['/nodes/default.repair_orders_cube/edit-cube']}
      >
        <Routes>
          <Route
            path="nodes/:name/edit-cube"
            element={
              <DJClientContext.Provider
                value={{ DataJunctionAPI: mockDjClient }}
              >
                <CubeBuilderPage />
              </DJClientContext.Provider>
            }
          />
        </Routes>
      </MemoryRouter>,
    );

    // The Edit branch fetches the cube and renders its name + display name.
    await waitFor(() =>
      expect(mockDjClient.getCubeForEditing).toHaveBeenCalledWith(
        'default.repair_orders_cube',
      ),
    );
    expect(
      await screen.findByText('default.repair_orders_cube'),
    ).toBeInTheDocument();
    // Heading reads "Edit" in edit mode.
    expect(screen.getByRole('heading', { name: /Edit/ })).toBeInTheDocument();

    // Save button reads "Save" in edit mode.
    const saveButton = screen.getByRole('button', { name: 'CreateCube' });
    expect(saveButton).toHaveTextContent('Save');
    fireEvent.click(saveButton);

    await waitFor(() => expect(mockDjClient.patchCube).toHaveBeenCalled(), {
      timeout: 1500,
    });
  });

  it('shows a save error when patchCube fails', async () => {
    mockDjClient.patchCube.mockResolvedValueOnce({
      status: 500,
      json: { message: 'something exploded' },
    });

    render(
      <MemoryRouter
        initialEntries={['/nodes/default.repair_orders_cube/edit-cube']}
      >
        <Routes>
          <Route
            path="nodes/:name/edit-cube"
            element={
              <DJClientContext.Provider
                value={{ DataJunctionAPI: mockDjClient }}
              >
                <CubeBuilderPage />
              </DJClientContext.Provider>
            }
          />
        </Routes>
      </MemoryRouter>,
    );

    await waitFor(() =>
      expect(mockDjClient.getCubeForEditing).toHaveBeenCalled(),
    );
    fireEvent.click(screen.getByRole('button', { name: 'CreateCube' }));
    expect(
      await screen.findByText(/something exploded/, {}, { timeout: 1500 }),
    ).toBeInTheDocument();
  });

  // TODO: Update test for async metrics search
  it.skip('creates a new cube', async () => {
    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <CubeBuilderPage />
      </DJClientContext.Provider>,
    );

    const selectMetrics = screen.getAllByTestId('select-metrics')[0];
    expect(selectMetrics).toBeDefined();
    expect(selectMetrics).not.toBeNull();

    // Type to search for metrics (async select requires typing)
    const metricsInput = selectMetrics.querySelector('input');
    fireEvent.change(metricsInput, { target: { value: 'default' } });

    // Advance timers to flush the 300ms debounce
    jest.advanceTimersByTime(400);

    // Wait for search results
    await waitFor(() => {
      expect(mockDjClient.searchMetrics).toHaveBeenCalled();
    });

    // Wait for options to appear and click each one
    for (const metric of mockMetrics) {
      await waitFor(() => {
        expect(screen.getByText(metric)).toBeInTheDocument();
      });
      fireEvent.click(screen.getByText(metric));
    }

    fireEvent.click(screen.getAllByText('Dimensions *')[0]);

    // Wait for commonDimensions to be called and state to update
    await waitFor(() => {
      expect(mockDjClient.commonDimensions).toHaveBeenCalled();
    });

    const selectDimensions = screen.getAllByTestId('select-dimensions')[0];
    expect(selectDimensions).toBeDefined();
    expect(selectDimensions).not.toBeNull();

    await waitFor(() => {
      expect(
        screen.getByText(
          'default.repair_order_details.repair_order_id → default.repair_order.hard_hat_id → default.hard_hat.birth_date',
        ),
      ).toBeInTheDocument();
    });

    const selectDimensionsDate = screen.getAllByTestId(
      'dimensions-default.date_dim',
    )[0];

    fireEvent.keyDown(selectDimensionsDate.firstChild, { key: 'ArrowDown' });
    await waitFor(() => {
      expect(screen.getByText('Day')).toBeInTheDocument();
    });
    fireEvent.click(screen.getByText('Day'));
    fireEvent.click(screen.getByText('Month'));
    fireEvent.click(screen.getByText('Year'));
    fireEvent.click(screen.getByText('Dateint'));

    // Save
    const createCube = screen.getAllByRole('button', {
      name: 'CreateCube',
    })[0];
    expect(createCube).toBeInTheDocument();

    fireEvent.click(createCube);

    await waitFor(() => {
      expect(mockDjClient.createCube).toHaveBeenCalledWith(
        '',
        '',
        '',
        'published',
        [
          'default.num_repair_orders',
          'default.avg_repair_price',
          'default.total_repair_cost',
        ],
        [
          'default.date_dim.day',
          'default.date_dim.month',
          'default.date_dim.year',
          'default.date_dim.dateint',
        ],
        [],
      );
    });
  });

  const renderEditNode = element => {
    return render(
      <MemoryRouter
        initialEntries={['/nodes/default.repair_orders_cube/edit-cube']}
      >
        <Routes>
          <Route path="nodes/:name/edit-cube" element={element} />
        </Routes>
      </MemoryRouter>,
    );
  };

  // TODO: Update test for async metrics search
  it.skip('updates an existing cube', async () => {
    renderEditNode(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <CubeBuilderPage />
      </DJClientContext.Provider>,
    );
    expect(screen.getAllByText('Edit')[0]).toBeInTheDocument();
    await waitFor(() => {
      expect(mockDjClient.getCubeForEditing).toHaveBeenCalled();
    });

    // In edit mode, existing metrics are pre-populated from cube data
    const selectMetrics = screen.getAllByTestId('select-metrics')[0];
    expect(selectMetrics).toBeDefined();
    expect(selectMetrics).not.toBeNull();

    // Wait for cube metrics to be loaded
    await waitFor(() => {
      expect(screen.getByText('default.num_repair_orders')).toBeInTheDocument();
    });

    fireEvent.click(screen.getAllByText('Dimensions *')[0]);

    // Wait for commonDimensions to be called and state to update
    await waitFor(() => {
      expect(mockDjClient.commonDimensions).toHaveBeenCalled();
    });

    const selectDimensions = screen.getAllByTestId('select-dimensions')[0];
    expect(selectDimensions).toBeDefined();
    expect(selectDimensions).not.toBeNull();

    await waitFor(() => {
      expect(
        screen.getByText(
          'default.repair_order_details.repair_order_id → default.repair_order.hard_hat_id → default.hard_hat.birth_date',
        ),
      ).toBeInTheDocument();
    });

    const selectDimensionsDate = screen.getAllByTestId(
      'dimensions-default.date_dim',
    )[0];

    fireEvent.keyDown(selectDimensionsDate.firstChild, { key: 'ArrowDown' });
    await waitFor(() => {
      expect(screen.getByText('Day')).toBeInTheDocument();
    });
    fireEvent.click(screen.getByText('Day'));
    fireEvent.click(screen.getByText('Month'));
    fireEvent.click(screen.getByText('Year'));
    fireEvent.click(screen.getByText('Dateint'));

    // Save
    const createCube = screen.getAllByRole('button', {
      name: 'CreateCube',
    })[0];
    expect(createCube).toBeInTheDocument();

    fireEvent.click(createCube);

    await waitFor(() => {
      expect(mockDjClient.patchCube).toHaveBeenCalledWith(
        'default.repair_orders_cube',
        'Default: Repair Orders Cube',
        'Repairs cube',
        'draft',
        ['default.total_repair_cost', 'default.num_repair_orders'],
        [
          'default.date_dim.day',
          'default.date_dim.month',
          'default.date_dim.year',
          'default.date_dim.dateint',
        ],
        [],
        [],
      );
    });
  });
});
