import {
  render,
  screen,
  waitFor,
  fireEvent,
  within,
} from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import DJClientContext from '../../../providers/djclient';
import { SystemMetricsExplorerPage } from '../index';

// Recharts is heavy and renders SVG; stub it out so tests focus on the
// page's data plumbing rather than chart geometry.
jest.mock('recharts', () => ({
  ResponsiveContainer: ({ children }) => (
    <div data-testid="responsive-container">{children}</div>
  ),
  LineChart: ({ children, data }) => (
    <div data-testid="linechart" data-data={JSON.stringify(data)}>
      {children}
    </div>
  ),
  Line: ({ dataKey, name }) => (
    <div data-testid={`line-${dataKey}`} data-name={name} />
  ),
  AreaChart: ({ children, data }) => (
    <div data-testid="areachart" data-data={JSON.stringify(data)}>
      {children}
    </div>
  ),
  Area: ({ dataKey, name }) => (
    <div data-testid={`area-${dataKey}`} data-name={name} />
  ),
  BarChart: ({ children, data }) => (
    <div data-testid="barchart" data-data={JSON.stringify(data)}>
      {children}
    </div>
  ),
  Bar: ({ dataKey, name, fill }) => (
    <div data-testid={`bar-${dataKey}`} data-name={name} data-fill={fill} />
  ),
  CartesianGrid: () => <div data-testid="cartesian-grid" />,
  XAxis: ({ dataKey }) => <div data-testid="x-axis" data-key={dataKey} />,
  YAxis: () => <div data-testid="y-axis" />,
  Tooltip: () => <div data-testid="tooltip" />,
  Legend: () => <div data-testid="legend" />,
}));

const SAMPLE_METRICS = [
  {
    name: 'system.dj.number_of_nodes',
    display_name: 'Number of Nodes',
    description: 'Total count of DJ nodes.',
    custom_metadata: {
      group: 'Catalog',
      subgroup: 'Counts',
      suggested_compare_by: ['system.dj.node_type.type'],
    },
  },
  {
    name: 'system.dj.number_of_orphan_nodes',
    display_name: 'Number of Orphan Nodes',
    description: 'Count of nodes with no downstream consumers.',
    custom_metadata: {
      group: 'Quality',
      subgroup: 'Hygiene',
    },
  },
];

const SAMPLE_DIMS = [
  {
    name: 'system.dj.node_type.type',
    node_name: 'system.dj.node_type',
    node_display_name: 'Node Type',
    column_display_name: 'Type',
    properties: [],
    type: 'string',
    path: ['system.dj.nodes'],
  },
  {
    name: 'system.dj.nodes.created_at_week',
    node_name: 'system.dj.nodes',
    node_display_name: 'Nodes',
    column_display_name: 'Created At (Week)',
    properties: [],
    type: 'integer',
    path: [],
  },
];

function mockClient(overrides = {}) {
  return {
    system: {
      list: jest.fn().mockResolvedValue(SAMPLE_METRICS),
    },
    commonDimensions: jest.fn().mockResolvedValue(SAMPLE_DIMS),
    querySystemMetric: jest.fn().mockResolvedValue({
      columns: ['system.dj.nodes.created_at_week', 'system.dj.number_of_nodes'],
      rows: [
        [20240101, 5],
        [20240108, 7],
      ],
    }),
    ...overrides,
  };
}

function renderPage(client) {
  return render(
    <MemoryRouter initialEntries={['/overview/explore']}>
      <DJClientContext.Provider value={{ DataJunctionAPI: client }}>
        <SystemMetricsExplorerPage />
      </DJClientContext.Provider>
    </MemoryRouter>,
  );
}

describe('<SystemMetricsExplorerPage />', () => {
  it('loads metrics and renders them grouped by group/subgroup', async () => {
    const client = mockClient();
    renderPage(client);

    await waitFor(() => expect(client.system.list).toHaveBeenCalled());

    // Group + subgroup headers appear in the rail.
    expect(screen.getByText('Catalog')).toBeInTheDocument();
    expect(screen.getByText('Counts')).toBeInTheDocument();
    expect(screen.getByText('Quality')).toBeInTheDocument();
    expect(screen.getByText('Hygiene')).toBeInTheDocument();

    // Metric display names rendered as rail items (also in the chart title).
    expect(screen.getAllByText('Number of Nodes').length).toBeGreaterThan(0);
    expect(screen.getByText('Number of Orphan Nodes')).toBeInTheDocument();
  });

  it('auto-selects the first metric and loads its dimensions', async () => {
    const client = mockClient();
    renderPage(client);

    await waitFor(() =>
      expect(client.commonDimensions).toHaveBeenCalledWith([
        'system.dj.number_of_nodes',
      ]),
    );

    // Chart title reflects the selected metric.
    await waitFor(() =>
      expect(
        screen.getAllByText('Number of Nodes').length,
      ).toBeGreaterThanOrEqual(1),
    );

    // Description renders below the title.
    expect(screen.getByText('Total count of DJ nodes.')).toBeInTheDocument();
  });

  it('renders the table view when toggled, with API rows', async () => {
    const client = mockClient();
    renderPage(client);

    await waitFor(() => expect(client.querySystemMetric).toHaveBeenCalled());

    fireEvent.click(screen.getByRole('button', { name: /Table/i }));

    // Both row values come through.
    await waitFor(() => {
      expect(screen.getByText('20240101')).toBeInTheDocument();
      expect(screen.getByText('20240108')).toBeInTheDocument();
    });
  });

  it('switching metrics fetches new dims and re-queries data', async () => {
    const client = mockClient();
    renderPage(client);

    await waitFor(() =>
      expect(client.commonDimensions).toHaveBeenCalledTimes(1),
    );

    fireEvent.click(screen.getByText('Number of Orphan Nodes'));

    await waitFor(() =>
      expect(client.commonDimensions).toHaveBeenLastCalledWith([
        'system.dj.number_of_orphan_nodes',
      ]),
    );

    await waitFor(() =>
      expect(client.querySystemMetric).toHaveBeenLastCalledWith(
        expect.objectContaining({ metric: 'system.dj.number_of_orphan_nodes' }),
      ),
    );
  });

  it('searches metric list by display name', async () => {
    const client = mockClient();
    renderPage(client);

    await waitFor(() => expect(client.system.list).toHaveBeenCalled());

    fireEvent.change(screen.getByPlaceholderText('Search metrics'), {
      target: { value: 'orphan' },
    });

    // "Number of Nodes" still appears in the chart title (selected metric).
    // What we care about is the rail no longer shows it as an option.
    const rail = document.querySelector('.sme-rail');
    expect(within(rail).queryByText('Number of Nodes')).toBeNull();
    expect(
      within(rail).getByText('Number of Orphan Nodes'),
    ).toBeInTheDocument();
  });

  it('applies suggested_compare_by after the metric loads its dims', async () => {
    const client = mockClient();
    renderPage(client);

    await waitFor(() => expect(client.querySystemMetric).toHaveBeenCalled());

    // The latest query call should include the suggested compare-by dim
    // alongside the auto-picked temporal X-axis.
    await waitFor(() => {
      const last =
        client.querySystemMetric.mock.calls[
          client.querySystemMetric.mock.calls.length - 1
        ][0];
      expect(last.dimensions).toEqual(
        expect.arrayContaining(['system.dj.node_type.type']),
      );
    });
  });

  it('handles legacy string-array metric responses', async () => {
    const client = mockClient({
      system: {
        list: jest
          .fn()
          .mockResolvedValue([
            'system.dj.number_of_nodes',
            'system.dj.number_of_orphan_nodes',
          ]),
      },
    });
    renderPage(client);

    await waitFor(() => expect(client.system.list).toHaveBeenCalled());

    // Falls back to "Other" group AND "Other" subgroup for untagged metrics —
    // so the literal "Other" string appears twice in the rail.
    expect(screen.getAllByText('Other').length).toBeGreaterThanOrEqual(1);
  });

  it('surfaces an explicit error when /system/metrics returns a non-array', async () => {
    const client = mockClient({
      system: {
        list: jest.fn().mockResolvedValue({ message: 'Not authenticated' }),
      },
    });
    renderPage(client);

    await waitFor(() =>
      expect(
        screen.getByText(/Unexpected response from \/system\/metrics/i),
      ).toBeInTheDocument(),
    );
  });

  it('renders the chart-title link to the underlying node page', async () => {
    const client = mockClient();
    renderPage(client);

    await waitFor(() => expect(client.commonDimensions).toHaveBeenCalled());

    const title = screen.getByRole('link', { name: /Number of Nodes/i });
    expect(title).toHaveAttribute('href', '/nodes/system.dj.number_of_nodes');
    expect(title).toHaveAttribute('target', '_blank');
  });

  it('renders the bar chart when X-axis is non-temporal (after switching to bar)', async () => {
    const client = mockClient();
    renderPage(client);

    await waitFor(() => expect(client.querySystemMetric).toHaveBeenCalled());

    fireEvent.click(screen.getByRole('button', { name: /^Bar$/i }));

    await waitFor(() =>
      expect(screen.getByTestId('barchart')).toBeInTheDocument(),
    );
  });
});
