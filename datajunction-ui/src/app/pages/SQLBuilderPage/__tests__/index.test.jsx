import {
  render,
  screen,
  fireEvent,
  waitFor,
  waitForElement,
  act,
} from '@testing-library/react';
import DJClientContext from '../../../providers/djclient';
import { SQLBuilderPage } from '../index';

const mockDjClient = {
  metrics: jest.fn(),
  commonDimensions: jest.fn(),
  sqls: jest.fn(),
  data: jest.fn(),
  stream: jest.fn(),
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
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.birth_date',
    ],
  },
  {
    name: 'default.date_dim.dateint',
    type: 'timestamp',
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.hire_date',
    ],
  },
  {
    name: 'default.date_dim.day',
    type: 'int',
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.birth_date',
    ],
  },
  {
    name: 'default.date_dim.day',
    type: 'int',
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.hire_date',
    ],
  },
  {
    name: 'default.date_dim.month',
    type: 'int',
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.birth_date',
    ],
  },
  {
    name: 'default.date_dim.month',
    type: 'int',
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.hire_date',
    ],
  },
  {
    name: 'default.date_dim.year',
    type: 'int',
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.birth_date',
    ],
  },
  {
    name: 'default.date_dim.year',
    type: 'int',
    path: [
      'default.repair_order_details.repair_order_id',
      'default.repair_order.hard_hat_id',
      'default.hard_hat.hire_date',
    ],
  },
];

// Additional dimensions for testing type handling
const mockDimensionsWithBool = [
  {
    name: 'default.is_active',
    type: 'bool',
    path: ['default.repair_order'],
  },
  ...mockCommonDimensions,
];

describe('SQLBuilderPage', () => {
  beforeEach(() => {
    mockDjClient.metrics.mockResolvedValue(mockMetrics);
    mockDjClient.commonDimensions.mockResolvedValue(mockCommonDimensions);
    mockDjClient.sqls.mockResolvedValue({ sql: 'SELECT ...' });
    mockDjClient.data.mockResolvedValue({});

    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <SQLBuilderPage />
      </DJClientContext.Provider>,
    );
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('renders without crashing', () => {
    expect(screen.getByText('Using the SQL Builder')).toBeInTheDocument();
  });

  it('renders the Metrics section', () => {
    expect(screen.getByText('Metrics')).toBeInTheDocument();
  });

  it('renders the Group By section', () => {
    expect(screen.getByText('Group By')).toBeInTheDocument();
  });

  it('renders the Filter By section', () => {
    expect(screen.getByText('Filter By')).toBeInTheDocument();
  });

  it('fetches metrics on mount', async () => {
    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <SQLBuilderPage />
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
    fireEvent.click(screen.getAllByText('Group By')[0]);

    expect(mockDjClient.commonDimensions).toHaveBeenCalled();

    const selectDimensions = screen.getAllByTestId('select-dimensions')[0];
    expect(selectDimensions).toBeDefined();
    expect(selectDimensions).not.toBeNull();
    expect(screen.getAllByText('8 Shared Dimensions')[0]).toBeInTheDocument();
    fireEvent.keyDown(selectDimensions.firstChild, { key: 'ArrowDown' });

    for (const dim of mockCommonDimensions) {
      expect(screen.getAllByText(dim.name)[0]).toBeInTheDocument();
      fireEvent.click(screen.getAllByText(dim.name)[0]);
    }
    expect(mockDjClient.sqls).toHaveBeenCalled();
  });
});

describe('SQLBuilderPage - Data fetching', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockDjClient.metrics.mockResolvedValue(mockMetrics);
    mockDjClient.commonDimensions.mockResolvedValue(mockCommonDimensions);
    mockDjClient.sqls.mockResolvedValue({ sql: 'SELECT * FROM table' });
    mockDjClient.data.mockResolvedValue({});
  });

  it('fetches metrics on initial render', async () => {
    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <SQLBuilderPage />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.metrics).toHaveBeenCalled();
    });

    await waitFor(() => {
      expect(screen.getAllByText('3 Available Metrics')[0]).toBeInTheDocument();
    });
  });

  it('displays instruction card when no selections', async () => {
    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <SQLBuilderPage />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(screen.getByText('Using the SQL Builder')).toBeInTheDocument();
    });

    expect(
      screen.getByText(/Start by selecting one or more/),
    ).toBeInTheDocument();
  });

  it('clears dimensions when no common dimensions found', async () => {
    mockDjClient.commonDimensions.mockResolvedValue([]);

    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <SQLBuilderPage />
      </DJClientContext.Provider>,
    );

    // Wait for metrics
    await waitFor(() => {
      expect(screen.getAllByText('3 Available Metrics')[0]).toBeInTheDocument();
    });

    // Select metric
    const selectMetrics = screen.getAllByTestId('select-metrics')[0];
    fireEvent.keyDown(selectMetrics.firstChild, { key: 'ArrowDown' });
    await waitFor(() => {
      expect(screen.getByText('default.num_repair_orders')).toBeInTheDocument();
    });
    fireEvent.click(screen.getByText('default.num_repair_orders'));
    fireEvent.click(screen.getAllByText('Group By')[0]);

    // When no common dimensions
    await waitFor(() => {
      expect(screen.getAllByText('0 Shared Dimensions')[0]).toBeInTheDocument();
    });
  });
});
