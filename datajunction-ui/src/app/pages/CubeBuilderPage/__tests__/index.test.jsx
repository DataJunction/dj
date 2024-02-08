import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import DJClientContext from '../../../providers/djclient';
import { CubeBuilderPage } from '../index';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import React from 'react';

const mockDjClient = {
  metrics: jest.fn(),
  commonDimensions: jest.fn(),
  createCube: jest.fn(),
  namespaces: jest.fn(),
  cube: jest.fn(),
  node: jest.fn(),
  listTags: jest.fn(),
  tagsNode: jest.fn(),
  patchCube: jest.fn(),
};

const mockMetrics = [
  'default.num_repair_orders',
  'default.avg_repair_price',
  'default.total_repair_cost',
];

const mockCube = {
  node_revision_id: 102,
  node_id: 33,
  type: 'cube',
  name: 'default.repair_orders_cube',
  display_name: 'Default: Repair Orders Cube',
  version: 'v4.0',
  description: 'Repairs cube',
  availability: null,
  cube_elements: [
    {
      name: 'default_DOT_total_repair_cost',
      display_name: 'Total Repair Cost',
      node_name: 'default.total_repair_cost',
      type: 'metric',
      partition: null,
    },
    {
      name: 'default_DOT_num_repair_orders',
      display_name: 'Num Repair Orders',
      node_name: 'default.num_repair_orders',
      type: 'metric',
      partition: null,
    },
    {
      name: 'country',
      display_name: 'Country',
      node_name: 'default.hard_hat',
      type: 'dimension',
      partition: null,
    },
    {
      name: 'state',
      display_name: 'State',
      node_name: 'default.hard_hat',
      type: 'dimension',
      partition: null,
    },
  ],
  query: '',
  columns: [
    {
      name: 'default.total_repair_cost',
      display_name: 'Total Repair Cost',
      type: 'double',
      attributes: [],
      dimension: null,
      partition: null,
    },
    {
      name: 'default.num_repair_orders',
      display_name: 'Num Repair Orders',
      type: 'bigint',
      attributes: [],
      dimension: null,
      partition: null,
    },
    {
      name: 'default.hard_hat.country',
      display_name: 'Country',
      type: 'string',
      attributes: [],
      dimension: null,
      partition: null,
    },
    {
      name: 'default.hard_hat.state',
      display_name: 'State',
      type: 'string',
      attributes: [],
      dimension: null,
      partition: null,
    },
  ],
  updated_at: '2023-12-03T06:51:09.598532+00:00',
  materializations: [],
};

const mockCommonDimensions = [
  {
    name: 'default.date_dim.dateint',
    type: 'timestamp',
    node_name: 'default.date_dim',
    node_display_name: 'Date',
    is_primary_key: false,
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
    is_primary_key: true,
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
    is_primary_key: false,
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
    is_primary_key: false,
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
    is_primary_key: false,
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
    is_primary_key: false,
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
    is_primary_key: false,
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
    is_primary_key: false,
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.hire_date',
    ],
  },
];

describe('CubeBuilderPage', () => {
  beforeEach(() => {
    mockDjClient.metrics.mockResolvedValue(mockMetrics);
    mockDjClient.commonDimensions.mockResolvedValue(mockCommonDimensions);
    mockDjClient.createCube.mockResolvedValue({ status: 201, json: {} });
    mockDjClient.namespaces.mockResolvedValue(['default']);
    mockDjClient.cube.mockResolvedValue(mockCube);
    mockDjClient.node.mockResolvedValue(mockCube);
    mockDjClient.listTags.mockResolvedValue([]);
    mockDjClient.tagsNode.mockResolvedValue([]);
    mockDjClient.patchCube.mockResolvedValue({ status: 201, json: {} });

    window.scrollTo = jest.fn();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('renders without crashing', () => {
    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <CubeBuilderPage />
      </DJClientContext.Provider>,
    );
    expect(screen.getByText('Cube')).toBeInTheDocument();
  });

  it('renders the Metrics section', () => {
    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <CubeBuilderPage />
      </DJClientContext.Provider>,
    );
    expect(screen.getByText('Metrics *')).toBeInTheDocument();
  });

  it('renders the Dimensions section', () => {
    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <CubeBuilderPage />
      </DJClientContext.Provider>,
    );
    expect(screen.getByText('Dimensions *')).toBeInTheDocument();
  });

  it('creates a new cube', async () => {
    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <CubeBuilderPage />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.metrics).toHaveBeenCalled();
    });

    const selectMetrics = screen.getAllByTestId('select-metrics')[0];
    expect(selectMetrics).toBeDefined();
    expect(selectMetrics).not.toBeNull();
    expect(screen.getAllByText('3 Available Metrics')[0]).toBeInTheDocument();

    fireEvent.keyDown(selectMetrics.firstChild, { key: 'ArrowDown' });
    for (const metric of mockMetrics) {
      await waitFor(() => {
        expect(screen.getByText(metric)).toBeInTheDocument();
        fireEvent.click(screen.getByText(metric));
      });
    }
    fireEvent.click(screen.getAllByText('Dimensions *')[0]);

    expect(mockDjClient.commonDimensions).toHaveBeenCalled();

    const selectDimensions = screen.getAllByTestId('select-dimensions')[0];
    expect(selectDimensions).toBeDefined();
    expect(selectDimensions).not.toBeNull();
    expect(
      screen.getByText(
        'default.repair_order_details.repair_order_id → default.repair_order.hard_hat_id → default.hard_hat.birth_date',
      ),
    ).toBeInTheDocument();

    const selectDimensionsDate = screen.getAllByTestId(
      'dimensions-default.date_dim',
    )[0];

    fireEvent.keyDown(selectDimensionsDate.firstChild, { key: 'ArrowDown' });
    fireEvent.click(screen.getByText('Day'));
    fireEvent.click(screen.getByText('Month'));
    fireEvent.click(screen.getByText('Year'));
    fireEvent.click(screen.getByText('Dateint'));

    // Save
    const createCube = screen.getAllByRole('button', {
      name: 'CreateCube',
    })[0];
    expect(createCube).toBeInTheDocument();

    await waitFor(() => {
      fireEvent.click(createCube);
    });
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

  it('updates an existing cube', async () => {
    renderEditNode(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <CubeBuilderPage />
      </DJClientContext.Provider>,
    );
    expect(screen.getAllByText('Edit')[0]).toBeInTheDocument();
    await waitFor(() => {
      expect(mockDjClient.cube).toHaveBeenCalled();
    });
    await waitFor(() => {
      expect(mockDjClient.metrics).toHaveBeenCalled();
    });

    const selectMetrics = screen.getAllByTestId('select-metrics')[0];
    expect(selectMetrics).toBeDefined();
    expect(selectMetrics).not.toBeNull();
    expect(screen.getByText('default.num_repair_orders')).toBeInTheDocument();

    fireEvent.click(screen.getAllByText('Dimensions *')[0]);

    expect(mockDjClient.commonDimensions).toHaveBeenCalled();

    const selectDimensions = screen.getAllByTestId('select-dimensions')[0];
    expect(selectDimensions).toBeDefined();
    expect(selectDimensions).not.toBeNull();
    expect(
      screen.getByText(
        'default.repair_order_details.repair_order_id → default.repair_order.hard_hat_id → default.hard_hat.birth_date',
      ),
    ).toBeInTheDocument();

    const selectDimensionsDate = screen.getAllByTestId(
      'dimensions-default.date_dim',
    )[0];

    fireEvent.keyDown(selectDimensionsDate.firstChild, { key: 'ArrowDown' });
    fireEvent.click(screen.getByText('Day'));
    fireEvent.click(screen.getByText('Month'));
    fireEvent.click(screen.getByText('Year'));
    fireEvent.click(screen.getByText('Dateint'));

    // Save
    const createCube = screen.getAllByRole('button', {
      name: 'CreateCube',
    })[0];
    expect(createCube).toBeInTheDocument();

    await waitFor(() => {
      fireEvent.click(createCube);
    });
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
      );
    });
  });
});
