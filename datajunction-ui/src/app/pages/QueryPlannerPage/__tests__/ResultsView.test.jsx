import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { ResultsView } from '../ResultsView';

// Mock react-syntax-highlighter
jest.mock('react-syntax-highlighter', () => {
  const MockLight = ({ children }) => (
    <pre data-testid="syntax-highlighter">{children}</pre>
  );
  MockLight.registerLanguage = jest.fn();
  return { Light: MockLight };
});
jest.mock('react-syntax-highlighter/src/styles/hljs', () => ({
  foundation: {},
}));
jest.mock('react-syntax-highlighter/dist/esm/languages/hljs/sql', () => ({}));

// Mock recharts to avoid canvas rendering issues in jsdom.
// YAxis calls tickFormatter with sample values (covering the formatYAxis helper).
jest.mock('recharts', () => {
  const React = require('react');
  const mockComponent =
    name =>
    ({ children, ...props }) =>
      React.createElement('div', { 'data-testid': name, ...props }, children);

  // YAxis: call tickFormatter with a spread of magnitudes so formatYAxis branches are hit
  const MockYAxis = ({ children, tickFormatter, ...props }) => {
    if (tickFormatter) {
      // Exercise all four branches of formatYAxis
      tickFormatter(1_500_000_000); // >=1B
      tickFormatter(2_500_000); // >=1M
      tickFormatter(5_000); // >=1K
      tickFormatter(42); // plain
    }
    return React.createElement(
      'div',
      { 'data-testid': 'YAxis', ...props },
      children,
    );
  };

  return {
    LineChart: mockComponent('LineChart'),
    BarChart: mockComponent('BarChart'),
    Line: mockComponent('Line'),
    Bar: mockComponent('Bar'),
    XAxis: mockComponent('XAxis'),
    YAxis: MockYAxis,
    CartesianGrid: mockComponent('CartesianGrid'),
    Tooltip: mockComponent('Tooltip'),
    ResponsiveContainer: ({ children }) =>
      React.createElement(
        'div',
        { 'data-testid': 'ResponsiveContainer' },
        children,
      ),
  };
});

// Mock clipboard API
const mockWriteText = jest.fn();
Object.assign(navigator, {
  clipboard: {
    writeText: mockWriteText,
  },
});

describe('ResultsView', () => {
  const defaultProps = {
    sql: 'SELECT * FROM table',
    results: {
      results: [
        {
          columns: [
            { name: 'id', type: 'INT' },
            { name: 'name', type: 'STRING' },
          ],
          rows: [
            [1, 'Alice'],
            [2, 'Bob'],
          ],
        },
      ],
    },
    loading: false,
    error: null,
    elapsedTime: 1.5,
    onBackToPlan: jest.fn(),
    selectedMetrics: ['metric1', 'metric2'],
    selectedDimensions: ['dim1'],
    filters: [],
    dialect: 'SPARK',
    cubeName: null,
    availability: null,
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('Header', () => {
    it('renders back to plan button', () => {
      render(<ResultsView {...defaultProps} />);
      expect(screen.getByText('Back to Plan')).toBeInTheDocument();
    });

    it('calls onBackToPlan when back button is clicked', () => {
      const onBackToPlan = jest.fn();
      render(<ResultsView {...defaultProps} onBackToPlan={onBackToPlan} />);

      fireEvent.click(screen.getByText('Back to Plan'));
      expect(onBackToPlan).toHaveBeenCalled();
    });

    it('shows row count and elapsed time', () => {
      render(<ResultsView {...defaultProps} />);
      // Row count appears in both header and table section
      expect(screen.getAllByText('2 rows').length).toBeGreaterThanOrEqual(1);
      expect(screen.getByText('1.50s')).toBeInTheDocument();
    });

    it('shows loading state', () => {
      render(<ResultsView {...defaultProps} loading={true} />);
      expect(screen.getByText('Running query...')).toBeInTheDocument();
    });

    it('shows error state in header', () => {
      render(<ResultsView {...defaultProps} error="Query timeout" />);
      expect(screen.getByText('Query failed')).toBeInTheDocument();
    });

    it('handles null elapsedTime', () => {
      render(<ResultsView {...defaultProps} elapsedTime={null} />);
      expect(screen.getAllByText('2 rows').length).toBeGreaterThanOrEqual(1);
      // Should not show elapsed time when null
      expect(screen.queryByText(/^\d+\.\d+s$/)).not.toBeInTheDocument();
    });
  });

  describe('SQL Pane', () => {
    it('displays SQL query', () => {
      render(<ResultsView {...defaultProps} />);
      expect(screen.getByText('SELECT * FROM table')).toBeInTheDocument();
    });

    it('shows SQL Query title', () => {
      render(<ResultsView {...defaultProps} />);
      expect(screen.getByText('SQL Query')).toBeInTheDocument();
    });

    it('shows generating message when no SQL', () => {
      render(<ResultsView {...defaultProps} sql={null} />);
      expect(screen.getByText('Generating SQL...')).toBeInTheDocument();
    });

    it('renders copy button', () => {
      render(<ResultsView {...defaultProps} />);
      expect(screen.getByText('Copy')).toBeInTheDocument();
    });

    it('copies SQL to clipboard when copy button clicked', async () => {
      render(<ResultsView {...defaultProps} />);

      fireEvent.click(screen.getByText('Copy'));

      expect(mockWriteText).toHaveBeenCalledWith('SELECT * FROM table');
      expect(screen.getByText('✓ Copied')).toBeInTheDocument();
    });

    it('disables copy button when no SQL', () => {
      render(<ResultsView {...defaultProps} sql={null} />);
      expect(screen.getByText('Copy')).toBeDisabled();
    });

    it('shows materialization info when cubeName is provided', () => {
      render(
        <ResultsView
          {...defaultProps}
          cubeName="test_cube"
          availability={{
            catalog: 'catalog',
            schema_: 'schema',
            table: 'table',
            validThroughTs: 1705363200000, // Jan 16, 2024
          }}
        />,
      );

      expect(screen.getByText(/Using materialized cube/)).toBeInTheDocument();
      expect(screen.getByText(/Valid thru/)).toBeInTheDocument();
    });

    it('shows materialization info without availability date', () => {
      render(<ResultsView {...defaultProps} cubeName="test_cube" />);

      expect(screen.getByText(/Using materialized cube/)).toBeInTheDocument();
      expect(screen.queryByText(/Valid thru/)).not.toBeInTheDocument();
    });
  });

  describe('Loading State', () => {
    it('shows loading spinner and message', () => {
      render(<ResultsView {...defaultProps} loading={true} />);

      expect(screen.getByText('Executing query...')).toBeInTheDocument();
      expect(
        screen.getByText(/Querying 2 metric\(s\) with 1 dimension\(s\)/),
      ).toBeInTheDocument();
    });
  });

  describe('Error State', () => {
    it('shows error message', () => {
      render(<ResultsView {...defaultProps} error="Connection failed" />);

      expect(screen.getByText('Query Failed')).toBeInTheDocument();
      expect(screen.getByText('Connection failed')).toBeInTheDocument();
    });

    it('shows back to plan button in error state', () => {
      const onBackToPlan = jest.fn();
      render(
        <ResultsView
          {...defaultProps}
          error="Error"
          onBackToPlan={onBackToPlan}
        />,
      );

      // There are two back buttons - one in header, one in error state
      const backButtons = screen.getAllByText('Back to Plan');
      expect(backButtons.length).toBe(2);

      fireEvent.click(backButtons[1]);
      expect(onBackToPlan).toHaveBeenCalled();
    });
  });

  describe('Results Table', () => {
    it('renders table with columns and rows', () => {
      render(<ResultsView {...defaultProps} />);

      expect(screen.getByText('id')).toBeInTheDocument();
      expect(screen.getByText('name')).toBeInTheDocument();
      expect(screen.getByText('Alice')).toBeInTheDocument();
      expect(screen.getByText('Bob')).toBeInTheDocument();
    });

    it('shows column types', () => {
      render(<ResultsView {...defaultProps} />);

      expect(screen.getByText('INT')).toBeInTheDocument();
      expect(screen.getByText('STRING')).toBeInTheDocument();
    });

    it('shows row count in table header', () => {
      render(<ResultsView {...defaultProps} />);

      // Row count appears in both header and table section
      const rowCounts = screen.getAllByText('2 rows');
      expect(rowCounts.length).toBeGreaterThanOrEqual(1);
    });

    it('shows empty state when no results', () => {
      render(
        <ResultsView
          {...defaultProps}
          results={{ results: [{ columns: [], rows: [] }] }}
        />,
      );

      expect(screen.getByText('No results returned')).toBeInTheDocument();
    });

    it('handles null values in cells', () => {
      render(
        <ResultsView
          {...defaultProps}
          results={{
            results: [
              {
                columns: [{ name: 'value', type: 'STRING' }],
                rows: [[null], ['data']],
              },
            ],
          }}
        />,
      );

      expect(screen.getByText('NULL')).toBeInTheDocument();
      expect(screen.getByText('data')).toBeInTheDocument();
    });

    it('displays filters as chips', () => {
      render(
        <ResultsView
          {...defaultProps}
          filters={["date >= '2024-01-01'", "status = 'active'"]}
        />,
      );

      expect(screen.getByText("date >= '2024-01-01'")).toBeInTheDocument();
      expect(screen.getByText("status = 'active'")).toBeInTheDocument();
    });
  });

  describe('Sorting', () => {
    it('sorts by column when header is clicked', () => {
      render(<ResultsView {...defaultProps} />);

      // Click on 'name' column header to sort
      const nameHeader = screen.getByText('name').closest('th');
      fireEvent.click(nameHeader);

      // First row should now be Alice (ascending)
      const rows = screen.getAllByRole('row');
      expect(rows[1]).toHaveTextContent('Alice');
    });

    it('toggles sort direction on second click', () => {
      render(<ResultsView {...defaultProps} />);

      const nameHeader = screen.getByText('name').closest('th');

      // First click - ascending
      fireEvent.click(nameHeader);
      let rows = screen.getAllByRole('row');
      expect(rows[1]).toHaveTextContent('Alice');

      // Second click - descending
      fireEvent.click(nameHeader);
      rows = screen.getAllByRole('row');
      expect(rows[1]).toHaveTextContent('Bob');
    });

    it('shows active sort indicator', () => {
      render(<ResultsView {...defaultProps} />);

      const nameHeader = screen.getByText('name').closest('th');
      fireEvent.click(nameHeader);

      expect(nameHeader).toHaveClass('sorted');
    });

    it('sorts numeric columns correctly', () => {
      render(
        <ResultsView
          {...defaultProps}
          results={{
            results: [
              {
                columns: [{ name: 'count', type: 'INT' }],
                rows: [[10], [2], [100]],
              },
            ],
          }}
        />,
      );

      const countHeader = screen.getByText('count').closest('th');
      fireEvent.click(countHeader);

      const cells = screen.getAllByRole('cell');
      expect(cells[0]).toHaveTextContent('2');
      expect(cells[1]).toHaveTextContent('10');
      expect(cells[2]).toHaveTextContent('100');
    });

    it('handles null values in sorting - nulls go last', () => {
      render(
        <ResultsView
          {...defaultProps}
          results={{
            results: [
              {
                columns: [{ name: 'value', type: 'STRING' }],
                rows: [[null], ['b'], ['a']],
              },
            ],
          }}
        />,
      );

      const valueHeader = screen.getByText('value').closest('th');
      fireEvent.click(valueHeader);

      const cells = screen.getAllByRole('cell');
      expect(cells[0]).toHaveTextContent('a');
      expect(cells[1]).toHaveTextContent('b');
      expect(cells[2]).toHaveTextContent('NULL');
    });
  });

  describe('Edge Cases', () => {
    it('handles missing results gracefully', () => {
      render(<ResultsView {...defaultProps} results={null} />);

      // Row count appears in both header and table section
      expect(screen.getAllByText('0 rows').length).toBeGreaterThanOrEqual(1);
    });

    it('handles empty results object', () => {
      render(<ResultsView {...defaultProps} results={{}} />);

      // Row count appears in both header and table section
      expect(screen.getAllByText('0 rows').length).toBeGreaterThanOrEqual(1);
    });

    it('formats large row counts with locale', () => {
      const manyRows = Array.from({ length: 1000 }, (_, i) => [i]);
      render(
        <ResultsView
          {...defaultProps}
          results={{
            results: [
              {
                columns: [{ name: 'id', type: 'INT' }],
                rows: manyRows,
              },
            ],
          }}
        />,
      );

      // Should show "1,000 rows" with locale formatting (appears in both header and table)
      expect(screen.getAllByText('1,000 rows').length).toBeGreaterThanOrEqual(
        1,
      );
    });
  });

  describe('Chart Tab', () => {
    // Results with a string dimension + numeric metric → bar chart config
    const barChartResults = {
      results: [
        {
          columns: [
            { name: 'country', type: 'STRING' },
            { name: 'revenue', type: 'FLOAT' },
          ],
          rows: [
            ['US', 1000],
            ['UK', 500],
          ],
        },
      ],
    };

    // Results with a time dimension + numeric metric → line chart config
    const lineChartResults = {
      results: [
        {
          columns: [
            { name: 'date', type: 'DATE' },
            { name: 'revenue', type: 'FLOAT' },
          ],
          rows: [
            ['2024-01-01', 1000],
            ['2024-01-02', 1500],
          ],
        },
      ],
    };

    // Results with two numeric columns (no string/time dim) → line with first as x
    const allNumericResults = {
      results: [
        {
          columns: [
            { name: 'x_val', type: 'FLOAT' },
            { name: 'y_val', type: 'FLOAT' },
          ],
          rows: [
            [1.0, 10.5],
            [2.0, 20.5],
          ],
        },
      ],
    };

    // Scalar result: single numeric column, one row → KPI cards
    const scalarResults = {
      results: [
        {
          columns: [{ name: 'total_revenue', type: 'FLOAT' }],
          rows: [[1234567.89]],
        },
      ],
    };

    it('renders Chart tab button', () => {
      render(<ResultsView {...defaultProps} results={barChartResults} />);
      expect(screen.getByText('Chart')).toBeInTheDocument();
    });

    it('Chart tab is enabled when data is chartable (bar chart data)', () => {
      render(<ResultsView {...defaultProps} results={barChartResults} />);
      const chartTab = screen.getByText('Chart').closest('button');
      expect(chartTab).not.toHaveClass('disabled');
    });

    it('Chart tab is enabled for any data with rows, showing no-data message when unchartable', () => {
      render(
        <ResultsView
          {...defaultProps}
          results={{
            results: [
              {
                columns: [{ name: 'name', type: 'STRING' }],
                rows: [['Alice']],
              },
            ],
          }}
        />,
      );
      const chartTab = screen.getByText('Chart').closest('button');
      expect(chartTab).not.toHaveClass('disabled');

      fireEvent.click(chartTab);
      expect(
        screen.getByText('No chartable data detected'),
      ).toBeInTheDocument();
    });

    it('switches to chart view when Chart tab is clicked (bar chart)', () => {
      render(<ResultsView {...defaultProps} results={barChartResults} />);

      fireEvent.click(screen.getByText('Chart'));

      // The chart wrapper should appear
      expect(
        document.querySelector('.results-chart-wrapper'),
      ).toBeInTheDocument();
      expect(screen.getByTestId('BarChart')).toBeInTheDocument();
    });

    it('switches to chart view when Chart tab is clicked (line chart)', () => {
      render(<ResultsView {...defaultProps} results={lineChartResults} />);

      fireEvent.click(screen.getByText('Chart'));

      expect(
        document.querySelector('.results-chart-wrapper'),
      ).toBeInTheDocument();
      expect(screen.getByTestId('LineChart')).toBeInTheDocument();
    });

    it('switches back to table view when Table tab is clicked', () => {
      render(<ResultsView {...defaultProps} results={barChartResults} />);

      // Switch to chart
      fireEvent.click(screen.getByText('Chart'));
      expect(
        document.querySelector('.results-chart-wrapper'),
      ).toBeInTheDocument();

      // Switch back to table
      fireEvent.click(screen.getByText('Table'));
      expect(
        document.querySelector('.results-table-wrapper'),
      ).toBeInTheDocument();
    });

    it('renders line chart for all-numeric columns (lines 93-95: no time/string dim)', () => {
      render(<ResultsView {...defaultProps} results={allNumericResults} />);

      fireEvent.click(screen.getByText('Chart'));

      expect(screen.getByTestId('LineChart')).toBeInTheDocument();
    });

    it('renders KPI cards for scalar numeric result', () => {
      render(<ResultsView {...defaultProps} results={scalarResults} />);

      fireEvent.click(screen.getByText('Chart'));

      expect(document.querySelector('.kpi-cards')).toBeInTheDocument();
      expect(document.querySelector('.kpi-label')).toHaveTextContent(
        'total_revenue',
      );
    });

    it('KPI card formats null value as em-dash', () => {
      render(
        <ResultsView
          {...defaultProps}
          results={{
            results: [
              {
                columns: [{ name: 'revenue', type: 'FLOAT' }],
                rows: [[null]],
              },
            ],
          }}
        />,
      );

      fireEvent.click(screen.getByText('Chart'));

      expect(document.querySelector('.kpi-value')).toHaveTextContent('—');
    });

    it('KPI card formats numeric value with toLocaleString', () => {
      render(<ResultsView {...defaultProps} results={scalarResults} />);

      fireEvent.click(screen.getByText('Chart'));

      // 1234567.89 formatted
      const kpiValue = document.querySelector('.kpi-value');
      expect(kpiValue).toBeInTheDocument();
      // Value should be a localized number string (not null)
      expect(kpiValue.textContent).not.toBe('—');
    });

    it('KPI card shows column type when present', () => {
      render(<ResultsView {...defaultProps} results={scalarResults} />);

      fireEvent.click(screen.getByText('Chart'));

      expect(document.querySelector('.kpi-type')).toHaveTextContent('FLOAT');
    });

    it('renders small multiples when 3+ metric columns present', () => {
      const threeMetricResults = {
        results: [
          {
            columns: [
              { name: 'date', type: 'DATE' },
              { name: 'metric_a', type: 'FLOAT' },
              { name: 'metric_b', type: 'FLOAT' },
              { name: 'metric_c', type: 'FLOAT' },
            ],
            rows: [
              ['2024-01-01', 10, 20, 30],
              ['2024-01-02', 15, 25, 35],
            ],
          },
        ],
      };

      render(<ResultsView {...defaultProps} results={threeMetricResults} />);

      fireEvent.click(screen.getByText('Chart'));

      // SMALL_MULTIPLES_THRESHOLD is 2, so 3 metric cols triggers small multiples
      expect(document.querySelector('.small-multiples')).toBeInTheDocument();
      const labels = document.querySelectorAll('.small-multiple-label');
      expect(labels.length).toBe(3);
    });

    it('shows no-data message when Chart tab clicked with unchartable data', () => {
      // Single string column → not chartable, but tab is still clickable
      render(
        <ResultsView
          {...defaultProps}
          results={{
            results: [
              {
                columns: [{ name: 'label', type: 'STRING' }],
                rows: [['x']],
              },
            ],
          }}
        />,
      );

      const chartTab = screen.getByText('Chart').closest('button');
      fireEvent.click(chartTab);

      expect(
        screen.getByText('No chartable data detected'),
      ).toBeInTheDocument();
    });

    it('resets to table view if new results are not chartable while on chart tab', () => {
      const { rerender } = render(
        <ResultsView {...defaultProps} results={barChartResults} />,
      );

      // Switch to chart view
      fireEvent.click(screen.getByText('Chart'));
      expect(
        document.querySelector('.results-chart-wrapper'),
      ).toBeInTheDocument();

      // Re-render with non-chartable results (empty rows)
      rerender(
        <ResultsView
          {...defaultProps}
          results={{ results: [{ columns: [], rows: [] }] }}
        />,
      );

      // Should auto-reset to table view
      expect(
        document.querySelector('.results-table-wrapper'),
      ).toBeInTheDocument();
    });

    it('shows links during loading when links prop is provided', () => {
      render(
        <ResultsView
          {...defaultProps}
          loading={true}
          links={['https://example.com/query/123']}
        />,
      );

      const link = screen.getByText('View query ↗');
      expect(link).toBeInTheDocument();
      expect(link).toHaveAttribute('href', 'https://example.com/query/123');
    });

    // ── Pivoted chart paths (buildPivotedData + ChartView branches) ──────────

    it('renders pivoted line chart for time + 1 categorical + 1 metric (groupByCol path)', () => {
      // detectChartConfig line 84-90: timeCols + nonTimeCatCols.length === 1
      // buildPivotedData lines 131-178, useMemo lines 505-512, ChartView line 353
      const pivotedLineResults = {
        results: [
          {
            columns: [
              { name: 'date', type: 'DATE' },
              { name: 'country', type: 'STRING' },
              { name: 'revenue', type: 'FLOAT' },
            ],
            rows: [
              ['2024-01-01', 'US', 1000],
              ['2024-01-01', 'UK', 500],
              ['2024-01-02', 'US', 1200],
              ['2024-01-02', null, 300], // null group value → '(null)'
            ],
          },
        ],
      };

      render(<ResultsView {...defaultProps} results={pivotedLineResults} />);
      fireEvent.click(screen.getByText('Chart'));

      expect(screen.getByTestId('LineChart')).toBeInTheDocument();
    });

    it('renders pivoted multi-metric small multiples (time + 1 cat + 2 metrics)', () => {
      // ChartView lines 331-334: pivotedByMetric.length > 1
      const pivotedMultiMetricResults = {
        results: [
          {
            columns: [
              { name: 'date', type: 'DATE' },
              { name: 'country', type: 'STRING' },
              { name: 'revenue', type: 'FLOAT' },
              { name: 'orders', type: 'FLOAT' },
            ],
            rows: [
              ['2024-01-01', 'US', 1000, 10],
              ['2024-01-01', 'UK', 500, 5],
              ['2024-01-02', 'US', 1200, 12],
            ],
          },
        ],
      };

      render(<ResultsView {...defaultProps} results={pivotedMultiMetricResults} />);
      fireEvent.click(screen.getByText('Chart'));

      // pivotedByMetric.length === 2 → small-multiples wrapper
      expect(document.querySelector('.small-multiples')).toBeInTheDocument();
      const labels = document.querySelectorAll('.small-multiple-label');
      expect(labels.length).toBe(2);
      expect(labels[0]).toHaveTextContent('revenue');
      expect(labels[1]).toHaveTextContent('orders');
    });

    it('renders grouped bar chart for 2 categorical columns (lines 101-107)', () => {
      // detectChartConfig: nonTimeCatCols.length === 2 → bar with groupByCol
      const groupedBarResults = {
        results: [
          {
            columns: [
              { name: 'region', type: 'STRING' },
              { name: 'category', type: 'STRING' },
              { name: 'revenue', type: 'FLOAT' },
            ],
            rows: [
              ['North', 'A', 1000],
              ['South', 'A', 800],
              ['North', 'B', 600],
              ['South', null, 400], // null group value
            ],
          },
        ],
      };

      render(<ResultsView {...defaultProps} results={groupedBarResults} />);
      fireEvent.click(screen.getByText('Chart'));

      expect(screen.getByTestId('BarChart')).toBeInTheDocument();
    });

    it('renders bar chart for 3+ categorical columns falling back to first as x-axis (lines 108-110)', () => {
      const threeCatResults = {
        results: [
          {
            columns: [
              { name: 'a', type: 'STRING' },
              { name: 'b', type: 'STRING' },
              { name: 'c', type: 'STRING' },
              { name: 'metric', type: 'FLOAT' },
            ],
            rows: [
              ['x1', 'y1', 'z1', 100],
              ['x2', 'y2', 'z2', 200],
            ],
          },
        ],
      };

      render(<ResultsView {...defaultProps} results={threeCatResults} />);
      fireEvent.click(screen.getByText('Chart'));

      expect(screen.getByTestId('BarChart')).toBeInTheDocument();
    });
  });

  describe('Pane Resize', () => {
    it('handles SQL pane drag resize via horizontal-resizer', () => {
      render(<ResultsView {...defaultProps} />);

      const resizer = document.querySelector('.horizontal-resizer');
      expect(resizer).toBeInTheDocument();

      // Mousedown starts the drag
      fireEvent.mouseDown(resizer, { clientY: 300 });

      // Mousemove updates height (in jsdom offsetHeight is 0, so newHeight clamps to min 80)
      fireEvent.mouseMove(document, { clientY: 350 });

      // Inline style should be applied to sql-pane
      const sqlPane = document.querySelector('.sql-pane');
      expect(sqlPane.style.maxHeight).toBeTruthy();

      // Mouseup removes event listeners
      fireEvent.mouseUp(document);

      // Further moves should not update (listeners removed)
      const heightAfterUp = sqlPane.style.maxHeight;
      fireEvent.mouseMove(document, { clientY: 500 });
      expect(sqlPane.style.maxHeight).toBe(heightAfterUp);
    });

    it('does not update height when no resultsPanesRef container', () => {
      // Renders and immediately fires mousedown on resizer
      render(<ResultsView {...defaultProps} />);
      const resizer = document.querySelector('.horizontal-resizer');
      // Should not throw even if container measures are 0
      expect(() => {
        fireEvent.mouseDown(resizer, { clientY: 100 });
        fireEvent.mouseMove(document, { clientY: 200 });
        fireEvent.mouseUp(document);
      }).not.toThrow();
    });
  });
