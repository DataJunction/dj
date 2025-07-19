import { render, screen, waitFor } from '@testing-library/react';
import DJClientContext from '../../../providers/djclient';
import { TrendsPanel } from '../TrendsPanel';

jest.mock('recharts', () => {
  const Original = jest.requireActual('recharts');
  return {
    ...Original,
    ResponsiveContainer: ({ children, ...props }) => (
      <div data-testid="responsive-container">{children}</div>
    ),
    BarChart: ({ children, data, ...props }) => (
      <div data-testid="barchart" data-data={JSON.stringify(data)}>
        {children}
      </div>
    ),
    CartesianGrid: props => <div data-testid="cartesian-grid" />,
    XAxis: props => <div data-testid="x-axis" />,
    YAxis: props => <div data-testid="y-axis" />,
    Tooltip: props => <div data-testid="tooltip" />,
    Legend: props => <div data-testid="legend" />,
    Bar: props => (
      <div data-testid={`bar-${props.dataKey}`} data-fill={props.fill} />
    ),
  };
});

describe('<TrendsPanel />', () => {
  it('fetches and renders node trends', async () => {
    const mockNodeTrends = [
      {
        date: '2024-01-01',
        source: 2,
        dimension: 1,
        transform: 3,
        metric: 5,
        cube: 0,
      },
      {
        date: '2024-01-02',
        source: 1,
        dimension: 2,
        transform: 1,
        metric: 3,
        cube: 4,
      },
    ];

    const mockDjClient = {
      analytics: {
        node_trends: jest.fn().mockResolvedValue(mockNodeTrends),
      },
    };

    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <TrendsPanel />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.analytics.node_trends).toHaveBeenCalled();
    });

    // Chart title
    expect(screen.getByText('Trends')).toBeInTheDocument();

    // The Recharts containers render
    expect(screen.getByTestId('responsive-container')).toBeInTheDocument();
    expect(screen.getByTestId('barchart')).toBeInTheDocument();

    // Should contain grid, axes, tooltip, legend
    expect(screen.getByTestId('cartesian-grid')).toBeInTheDocument();
    expect(screen.getByTestId('x-axis')).toBeInTheDocument();
    expect(screen.getByTestId('y-axis')).toBeInTheDocument();
    expect(screen.getByTestId('tooltip')).toBeInTheDocument();
    expect(screen.getByTestId('legend')).toBeInTheDocument();

    // Should render bars with expected colors
    const expectedBars = {
      source: '#00C49F',
      dimension: '#FFBB28',
      transform: '#0088FE',
      metric: '#FF91A3',
      cube: '#AA46BE',
    };

    Object.entries(expectedBars).forEach(([key, color]) => {
      const bar = screen.getByTestId(`bar-${key}`);
      expect(bar).toBeInTheDocument();
      expect(bar).toHaveAttribute('data-fill', color);
    });

    // BarChart gets correct data
    const barChart = screen.getByTestId('barchart');
    expect(barChart.getAttribute('data-data')).toContain('2024-01-01');
  });

  it('renders with empty trends', async () => {
    const mockDjClient = {
      analytics: {
        node_trends: jest.fn().mockResolvedValue([]),
      },
    };

    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <TrendsPanel />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.analytics.node_trends).toHaveBeenCalled();
    });

    expect(screen.getByText('Trends')).toBeInTheDocument();
    expect(screen.getByTestId('responsive-container')).toBeInTheDocument();
    expect(screen.getByTestId('barchart')).toBeInTheDocument();
  });
});
