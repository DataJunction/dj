import { render, screen, waitFor } from '@testing-library/react';
import DJClientContext from '../../../providers/djclient';
import { DimensionNodeUsagePanel } from '../DimensionNodeUsagePanel';

describe('<DimensionNodeUsagePanel />', () => {
  it('fetches dimension nodes and displays them in sorted order', async () => {
    const mockDimensions = [
      { name: 'dimension_a', indegree: 2, cube_count: 5 }, // 7
      { name: 'dimension_b', indegree: 1, cube_count: 10 }, // 11
      { name: 'dimension_c', indegree: 3, cube_count: 3 }, // 6
    ];

    const mockDjClient = {
      system: {
        dimensions: jest.fn().mockResolvedValue(mockDimensions),
      },
    };

    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <DimensionNodeUsagePanel />
      </DJClientContext.Provider>,
    );

    // Wait for the API to be called
    await waitFor(() => {
      expect(mockDjClient.system.dimensions).toHaveBeenCalled();
    });

    // Check that rows are rendered
    expect(screen.getByText('dimension_b')).toBeInTheDocument();
    expect(screen.getByText('dimension_a')).toBeInTheDocument();
    expect(screen.getByText('dimension_c')).toBeInTheDocument();

    // Check that sorting is correct: b (11), a (7), c (6)
    const rows = screen.getAllByRole('row').slice(1); // skip the header row
    const names = rows.map(row => row.querySelector('a').textContent);
    expect(names).toEqual(['dimension_b', 'dimension_a', 'dimension_c']);

    // Check that links have correct hrefs
    expect(screen.getByText('dimension_b').closest('a')).toHaveAttribute(
      'href',
      '/nodes/dimension_b',
    );

    // Check indegree and cube_count cells
    expect(screen.getByText('1')).toBeInTheDocument(); // b indegree
    expect(screen.getByText('10')).toBeInTheDocument(); // b cube_count
    expect(screen.getByText('2')).toBeInTheDocument(); // a indegree
    expect(screen.getByText('5')).toBeInTheDocument(); // a cube_count
  });

  it('handles empty dimensions gracefully', async () => {
    const mockDjClient = {
      system: {
        dimensions: jest.fn().mockResolvedValue([]),
      },
    };

    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <DimensionNodeUsagePanel />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.system.dimensions).toHaveBeenCalled();
    });

    // Table should still be there, but no rows
    expect(screen.getByText('Dimension Node Usage')).toBeInTheDocument();
    expect(
      screen.queryByRole('row', { name: /dimension_/ }),
    ).not.toBeInTheDocument();
  });
});
