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
      expect(screen.getAllByText('1,000 rows').length).toBeGreaterThanOrEqual(1);
    });
  });
});
