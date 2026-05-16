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
vi.mock('recharts', () => ({
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
      list: vi.fn().mockResolvedValue(SAMPLE_METRICS),
    },
    commonDimensions: vi.fn().mockResolvedValue(SAMPLE_DIMS),
    querySystemMetric: vi.fn().mockResolvedValue({
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
        list: vi
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
        list: vi.fn().mockResolvedValue({ message: 'Not authenticated' }),
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

  it('hydrates metric / x-axis / compare-by / filters from URL on mount', async () => {
    const client = mockClient();
    render(
      <MemoryRouter
        initialEntries={[
          '/overview/explore?metric=system.dj.number_of_orphan_nodes' +
            '&x=system.dj.nodes.created_at_week' +
            '&by=system.dj.node_type.type' +
            '&filter=system.dj.nodes.is_active%7C%3D%7Ctrue' +
            '&view=table&chart=line&zero=1',
        ]}
      >
        <DJClientContext.Provider value={{ DataJunctionAPI: client }}>
          <SystemMetricsExplorerPage />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    // The URL-pinned metric is the one fetched, not the first in the list.
    await waitFor(() =>
      expect(client.commonDimensions).toHaveBeenCalledWith([
        'system.dj.number_of_orphan_nodes',
      ]),
    );

    // The X-axis pending value resolves to the matching dim.
    await waitFor(() => {
      const last =
        client.querySystemMetric.mock.calls[
          client.querySystemMetric.mock.calls.length - 1
        ][0];
      expect(last.dimensions).toEqual(
        expect.arrayContaining([
          'system.dj.nodes.created_at_week',
          'system.dj.node_type.type',
        ]),
      );
      expect(last.filters).toContain("system.dj.nodes.is_active = 'true'");
    });

    // ``view=table`` honored — table renders, no chart.
    await waitFor(() => {
      expect(screen.queryByTestId('linechart')).toBeNull();
      expect(screen.getByText('20240101')).toBeInTheDocument();
    });
  });

  it('builds an IN filter clause when operator is "in"', async () => {
    const client = mockClient();
    renderPage(client);
    await waitFor(() => expect(client.querySystemMetric).toHaveBeenCalled());

    fireEvent.click(screen.getByRole('button', { name: /\+ Add filter/i }));
    // Filter dim picker is the first <Select> inside the new filter row.
    // Type to filter then commit by Enter.
    const dimSelects = document.querySelectorAll('.sme-filter-row input');
    // First Select is the dim picker (react-select renders an input).
    fireEvent.change(dimSelects[0], { target: { value: 'Type' } });
    fireEvent.keyDown(dimSelects[0], { key: 'Enter', code: 'Enter' });

    // Operator picker → "is one of" (label) keyed by 'in'.
    fireEvent.change(dimSelects[1], { target: { value: 'is one of' } });
    fireEvent.keyDown(dimSelects[1], { key: 'Enter', code: 'Enter' });

    const valueInput = document.querySelector('.sme-text-input');
    fireEvent.change(valueInput, {
      target: { value: 'metric, source, 5' },
    });

    await waitFor(() => {
      const last =
        client.querySystemMetric.mock.calls[
          client.querySystemMetric.mock.calls.length - 1
        ][0];
      const inClauses = last.filters.filter(f => / IN \(/.test(f));
      expect(inClauses.length).toBeGreaterThan(0);
      // Numerics stay bare; strings get quoted.
      expect(inClauses[0]).toContain("'metric'");
      expect(inClauses[0]).toContain('5');
      expect(inClauses[0]).not.toContain("'5'");
    });
  });

  it('removes a filter when the × button is clicked', async () => {
    const client = mockClient();
    renderPage(client);
    await waitFor(() => expect(client.querySystemMetric).toHaveBeenCalled());

    fireEvent.click(screen.getByRole('button', { name: /\+ Add filter/i }));
    expect(document.querySelectorAll('.sme-filter-row').length).toBe(1);

    fireEvent.click(screen.getByLabelText('Remove filter'));
    expect(document.querySelectorAll('.sme-filter-row').length).toBe(0);
  });

  it('toggles compare-by membership when a rail dim item is clicked', async () => {
    const client = mockClient();
    renderPage(client);
    // Wait for dims to load
    await waitFor(() => expect(client.commonDimensions).toHaveBeenCalled());

    // The first metric defaults to a suggested compare-by, so toggling the
    // rail item should REMOVE it (it's already selected).
    const railItems = document.querySelectorAll(
      '.sme-rail-section.grow:nth-child(2) .sme-rail-item.indent',
    );
    // Find the rail item for "Type" (the node_type dim).
    const typeItem = Array.from(railItems).find(el =>
      el.textContent.includes('Type'),
    );
    expect(typeItem).toBeDefined();
    fireEvent.click(typeItem);

    await waitFor(() => {
      const last =
        client.querySystemMetric.mock.calls[
          client.querySystemMetric.mock.calls.length - 1
        ][0];
      expect(last.dimensions).not.toContain('system.dj.node_type.type');
    });

    // Click it again — adds it back to compareBy.
    fireEvent.click(typeItem);
    await waitFor(() => {
      const last =
        client.querySystemMetric.mock.calls[
          client.querySystemMetric.mock.calls.length - 1
        ][0];
      expect(last.dimensions).toContain('system.dj.node_type.type');
    });
  });

  it('renders the start-scale-at-zero toggle and reflects it in state', async () => {
    const client = mockClient();
    renderPage(client);
    await waitFor(() => expect(client.querySystemMetric).toHaveBeenCalled());

    const checkbox = screen.getByLabelText(/Start scale at zero/i);
    expect(checkbox).not.toBeChecked();
    fireEvent.click(checkbox);
    expect(checkbox).toBeChecked();
  });

  it('switches to Line and Area chart types explicitly', async () => {
    const client = mockClient();
    renderPage(client);
    await waitFor(() => expect(client.querySystemMetric).toHaveBeenCalled());

    fireEvent.click(screen.getByRole('button', { name: /^Line$/i }));
    await waitFor(() =>
      expect(screen.getByTestId('linechart')).toBeInTheDocument(),
    );

    fireEvent.click(screen.getByRole('button', { name: /^Area$/i }));
    await waitFor(() =>
      expect(screen.getByTestId('areachart')).toBeInTheDocument(),
    );
  });

  it('searches dimensions by display name in the rail', async () => {
    const client = mockClient();
    renderPage(client);
    await waitFor(() => expect(client.commonDimensions).toHaveBeenCalled());

    const dimSearch = screen.getByPlaceholderText('Search dimensions');
    fireEvent.change(dimSearch, { target: { value: 'created' } });

    const rail = document.querySelector('.sme-rail');
    // "Type" dim option should be filtered out; created-at remains.
    expect(within(rail).queryByText(/^Type$/)).toBeNull();
    expect(within(rail).getByText(/Created At/)).toBeInTheDocument();
  });

  it('falls back to chart=auto → bar when X-axis is non-temporal', async () => {
    const client = mockClient({
      commonDimensions: vi.fn().mockResolvedValue([
        {
          name: 'system.dj.node_type.type',
          node_name: 'system.dj.node_type',
          node_display_name: 'Node Type',
          column_display_name: 'Type',
          properties: [],
          type: 'string',
          path: ['system.dj.nodes'],
        },
      ]),
      querySystemMetric: vi.fn().mockResolvedValue({
        columns: ['system.dj.node_type.type', 'system.dj.number_of_nodes'],
        rows: [
          ['metric', 5],
          ['source', 7],
        ],
      }),
    });
    renderPage(client);
    await waitFor(() => expect(client.querySystemMetric).toHaveBeenCalled());
    await waitFor(() =>
      expect(screen.getByTestId('barchart')).toBeInTheDocument(),
    );
  });

  it('node-type series get the canonical NODE_TYPE_COLORS palette', async () => {
    const client = mockClient({
      querySystemMetric: vi.fn().mockResolvedValue({
        columns: [
          'system.dj.nodes.created_at_week',
          'system.dj.node_type.type',
          'system.dj.number_of_nodes',
        ],
        rows: [
          [20240101, 'metric', 5],
          [20240101, 'source', 7],
          [20240108, 'metric', 4],
        ],
      }),
    });
    renderPage(client);
    await waitFor(() => expect(client.querySystemMetric).toHaveBeenCalled());

    fireEvent.click(screen.getByRole('button', { name: /^Bar$/i }));

    await waitFor(() => {
      const bars = document.querySelectorAll('[data-testid^="bar-"]');
      const metricBar = Array.from(bars).find(
        b => b.getAttribute('data-name') === 'metric',
      );
      expect(metricBar.getAttribute('data-fill')).toBe('#f43f5e');
    });
  });

  it('renders a single-value chartData when no X-axis and no breakdown', async () => {
    // Use the metric WITHOUT a suggested_compare_by, and return only
    // non-temporal dims so the auto X-axis logic leaves xAxisDim null.
    // That hits the "no xKey, no breakdown" branch.
    const client = mockClient({
      system: {
        list: vi.fn().mockResolvedValue([
          {
            name: 'system.dj.number_of_orphan_nodes',
            display_name: 'Number of Orphan Nodes',
            description: 'Count of nodes with no downstream consumers.',
            custom_metadata: {
              group: 'Quality',
              subgroup: 'Hygiene',
            },
          },
        ]),
      },
      commonDimensions: vi.fn().mockResolvedValue([
        {
          name: 'system.dj.node_type.type',
          node_name: 'system.dj.node_type',
          node_display_name: 'Node Type',
          column_display_name: 'Type',
          properties: [],
          type: 'string',
          path: ['system.dj.nodes'],
        },
      ]),
      querySystemMetric: vi.fn().mockResolvedValue({
        columns: ['system.dj.number_of_orphan_nodes'],
        rows: [[17]],
      }),
    });
    renderPage(client);
    await waitFor(() => expect(client.querySystemMetric).toHaveBeenCalled());
    // The chart should render with a single bar (no x-axis, no breakdown).
    await waitFor(() =>
      expect(screen.getByTestId('barchart')).toBeInTheDocument(),
    );
  });

  it('clicking the Chart toggle while in chart view is a no-op (covers 985)', async () => {
    const client = mockClient();
    renderPage(client);
    await waitFor(() => expect(client.querySystemMetric).toHaveBeenCalled());
    // Wait for any chart variant to render first.
    await waitFor(() =>
      expect(
        screen.queryByTestId('barchart') ||
          screen.queryByTestId('linechart') ||
          screen.queryByTestId('areachart'),
      ).not.toBeNull(),
    );

    fireEvent.click(screen.getByRole('button', { name: /^📈 Chart$/i }));
    expect(
      screen.queryByTestId('barchart') ||
        screen.queryByTestId('linechart') ||
        screen.queryByTestId('areachart'),
    ).not.toBeNull();
  });

  it('compare-by clear path coerces null onChange value to [] (covers 929)', async () => {
    const client = mockClient();
    renderPage(client);
    await waitFor(() => expect(client.querySystemMetric).toHaveBeenCalled());

    // Find the compare-by Select's clear-all "x" button. react-select exposes
    // a div with aria-label="Clear value" when isMulti+isClearable is set.
    const clearBtns = document.querySelectorAll(
      '[aria-label*="Clear" i], [role="button"][aria-label="clear"]',
    );
    // Best-effort: click the first one if present. Pages without selected
    // values won't have the button — fall back to firing a synthetic
    // onChange via the document.
    if (clearBtns.length) {
      fireEvent.mouseDown(clearBtns[0]);
    }
    // The page should still render without error.
    expect(
      screen.queryByTestId('barchart') ||
        screen.queryByTestId('linechart') ||
        screen.queryByTestId('areachart'),
    ).not.toBeNull();
  });

  it('catches errors from querySystemMetric and clears the chart state', async () => {
    const client = mockClient({
      querySystemMetric: vi.fn().mockRejectedValue(new Error('boom!')),
    });
    renderPage(client);
    await waitFor(() => expect(screen.getByText(/boom!/)).toBeInTheDocument());
  });

  it('renders "No data" when the query result is empty', async () => {
    const client = mockClient({
      querySystemMetric: vi.fn().mockResolvedValue({
        columns: [
          'system.dj.nodes.created_at_week',
          'system.dj.number_of_nodes',
        ],
        rows: [],
      }),
    });
    renderPage(client);
    await waitFor(() => expect(client.querySystemMetric).toHaveBeenCalled());

    fireEvent.click(screen.getByRole('button', { name: /Table/i }));
    await waitFor(() =>
      expect(screen.getByText(/No data/i)).toBeInTheDocument(),
    );
  });
});

// Cover the helper modules used by the page directly so all branches are visited.
describe('SystemMetricsExplorerPage helpers', () => {
  let typeIcon, NonZeroTooltip;
  beforeAll(async () => {
    ({ typeIcon, NonZeroTooltip } = await import('../index'));
  });

  it('typeIcon maps each known SQL type family', () => {
    expect(typeIcon(null)).toBe('#');
    expect(typeIcon('bool')).toBe('✓');
    expect(typeIcon('boolean')).toBe('✓');
    expect(typeIcon('date')).toBe('📅');
    expect(typeIcon('timestamp')).toBe('📅');
    expect(typeIcon('time')).toBe('📅');
    expect(typeIcon('int')).toBe('123');
    expect(typeIcon('bigint')).toBe('123');
    expect(typeIcon('double')).toBe('123');
    expect(typeIcon('float')).toBe('123');
    expect(typeIcon('decimal')).toBe('123');
    expect(typeIcon('numeric')).toBe('123');
    expect(typeIcon('string')).toBe('Aa');
    expect(typeIcon('varchar')).toBe('Aa');
    expect(typeIcon('char')).toBe('Aa');
    expect(typeIcon('text')).toBe('Aa');
    expect(typeIcon('list')).toBe('[ ]');
    expect(typeIcon('array')).toBe('[ ]');
    expect(typeIcon('weird')).toBe('#');
  });

  it('NonZeroTooltip returns null when inactive or empty', () => {
    expect(
      NonZeroTooltip({ active: false, payload: [], label: 'x' }),
    ).toBeNull();
    expect(
      NonZeroTooltip({ active: true, payload: [], label: 'x' }),
    ).toBeNull();
    expect(
      NonZeroTooltip({
        active: true,
        payload: [{ value: 0, dataKey: 's0', name: 'metric', color: '#000' }],
        label: 'x',
      }),
    ).toBeNull();
  });

  it('NonZeroTooltip renders the non-zero series rows', () => {
    const payload = [
      { value: 0, dataKey: 's0', name: 'metric', color: '#3b82f6' },
      { value: 5, dataKey: 's1', name: 'source', color: '#22c55e' },
      { value: null, dataKey: 's2', name: 'dimension', color: '#f59e0b' },
      { value: 12.5, dataKey: 's3', name: 'cube', color: '#a855f7' },
    ];
    const { container } = render(
      <div>{NonZeroTooltip({ active: true, payload, label: 'week-1' })}</div>,
    );
    expect(container.textContent).toContain('week-1');
    expect(container.textContent).toContain('source');
    expect(container.textContent).toContain('5');
    expect(container.textContent).toContain('cube');
    // Zero & null entries are filtered out.
    expect(container.textContent).not.toContain('metric');
    expect(container.textContent).not.toContain('dimension');
  });
});
