import { render, screen, waitFor } from '@testing-library/react';
import DJClientContext from '../../../providers/djclient';
import { GovernanceWarningsPanel } from '../GovernanceWarningsPanel';

describe('<GovernanceWarningsPanel />', () => {
  it('fetches governance warnings and displays percentages and orphaned dimension count', async () => {
    const mockNodesWithoutDescription = [
      { name: 'SOURCE', value: 0.1 }, // 10%
      { name: 'METRIC', value: 0.2 }, // 20%
    ];

    const mockDimensions = [
      { name: 'dim_1', indegree: 0, cube_count: 1 }, // orphaned
      { name: 'dim_2', indegree: 1, cube_count: 0 }, // orphaned
      { name: 'dim_3', indegree: 2, cube_count: 1 }, // not orphaned
    ];

    const mockDjClient = {
      system: {
        nodes_without_description: jest
          .fn()
          .mockResolvedValue(mockNodesWithoutDescription),
        dimensions: jest.fn().mockResolvedValue(mockDimensions),
      },
    };

    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <GovernanceWarningsPanel />
      </DJClientContext.Provider>,
    );

    // Wait for both calls to be made and data rendered
    await waitFor(() => {
      expect(mockDjClient.system.nodes_without_description).toHaveBeenCalled();
      expect(mockDjClient.system.dimensions).toHaveBeenCalled();
    });

    // Check missing description badges
    expect(screen.getByText('10%')).toBeInTheDocument();
    expect(screen.getByText('20%')).toBeInTheDocument();
    expect(screen.getByText('sources')).toBeInTheDocument();
    expect(screen.getByText('metrics')).toBeInTheDocument();

    // Check orphaned dimension count: should be 2
    expect(screen.getByText('2')).toBeInTheDocument();
    expect(screen.getByText(/dimension nodes/)).toBeInTheDocument();

    // Should show the title
    expect(screen.getByText('Governance Warnings')).toBeInTheDocument();
    expect(screen.getByText('Missing Description')).toBeInTheDocument();
    expect(screen.getByText('Orphaned Dimensions')).toBeInTheDocument();
  });

  it('shows fallback if no data returned', async () => {
    const mockDjClient = {
      system: {
        nodes_without_description: jest.fn().mockResolvedValue([]),
        dimensions: jest.fn().mockResolvedValue([]),
      },
    };

    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <GovernanceWarningsPanel />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.system.nodes_without_description).toHaveBeenCalled();
      expect(mockDjClient.system.dimensions).toHaveBeenCalled();
    });

    // Should show fallback value for orphaned nodes
    expect(screen.getByText('...')).toBeInTheDocument();
  });
});
