import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import DJClientContext from '../../../providers/djclient';
import { CubeBuilderPage } from '../index';

const mockDjClient = {
  metrics: jest.fn(),
  commonDimensions: jest.fn(),
  createCube: jest.fn(),
  namespaces: jest.fn(),
};

const mockMetrics = [
  'default.num_repair_orders',
  'default.avg_repair_price',
  'default.total_repair_cost',
];

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

    window.scrollTo = jest.fn();

    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <CubeBuilderPage />
      </DJClientContext.Provider>,
    );
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('renders without crashing', () => {
    expect(screen.getByText('Cube')).toBeInTheDocument();
  });

  it('renders the Metrics section', () => {
    expect(screen.getByText('Metrics *')).toBeInTheDocument();
  });

  it('renders the Dimensions section', () => {
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
        'draft',
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
});
