import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import DJClientContext from '../../../providers/djclient';
import { SQLBuilderPage } from '../index';

const mockDjClient = {
  metrics: jest.fn(),
  commonDimensions: jest.fn(),
  sqls: jest.fn(),
  data: jest.fn(),
};

describe('SQLBuilderPage', () => {
  beforeEach(() => {
    mockDjClient.metrics.mockResolvedValue([
      'default.num_repair_orders',
      'default.avg_repair_price',
      'default.total_repair_cost',
    ]);
    mockDjClient.commonDimensions.mockResolvedValue([
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
    ]);
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
      fireEvent.focus(screen.getByRole('combobox'));
      fireEvent.keyDown(screen.getByText('Metrics'), { key: 'ArrowDown' });
    });
  });

  // it('handles Run Query button click', () => {
  //   mockDjClient.data.mockResolvedValue({
  //     results: [
  //       {
  //         columns: ['column1', 'column2'],
  //         rows: [
  //           ['data1', 'data2'],
  //           ['data3', 'data4'],
  //         ],
  //       },
  //     ],
  //   });
  //   fireEvent.click(screen.getByRole('button', { name: 'RunQuery' }));
  //   // screen.getByText('Run Query').click();
  //   expect(mockDjClient.data).toHaveBeenCalled();
  // });
});
