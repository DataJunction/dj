import { render, screen, waitFor } from '@testing-library/react';
import DJClientContext from '../../../providers/djclient';
import { NodesByTypePanel } from '../NodesByTypePanel';

describe('<NodesByTypePanel />', () => {
  it('fetches nodes & materializations by type and renders them correctly', async () => {
    const mockNodesByType = [
      { name: 'source', value: 5 },
      { name: 'cube', value: 2 },
    ];
    const mockMaterializationsByType = [
      { name: 'transform', value: 3 },
      { name: 'metric', value: 1 },
    ];

    const mockDjClient = {
      system: {
        node_counts_by_type: jest.fn().mockResolvedValue(mockNodesByType),
        materialization_counts_by_type: jest
          .fn()
          .mockResolvedValue(mockMaterializationsByType),
      },
    };

    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <NodesByTypePanel />
      </DJClientContext.Provider>,
    );

    // Wait for async calls to complete
    await waitFor(() => {
      expect(mockDjClient.system.node_counts_by_type).toHaveBeenCalled();
      expect(
        mockDjClient.system.materialization_counts_by_type,
      ).toHaveBeenCalled();
    });

    // Check that nodes by type appear
    expect(screen.getByText('Nodes by Type')).toBeInTheDocument();
    expect(screen.getByText('5')).toBeInTheDocument();
    expect(screen.getByText('sources')).toBeInTheDocument();
    expect(screen.getByText('2')).toBeInTheDocument();
    expect(screen.getByText('cubes')).toBeInTheDocument();

    // Check that materializations by type appear
    expect(screen.getByText('Materializations by Type')).toBeInTheDocument();
    expect(screen.getByText('3')).toBeInTheDocument();
    expect(screen.getByText('transforms')).toBeInTheDocument();
    expect(screen.getByText('1')).toBeInTheDocument();
    expect(screen.getByText('metrics')).toBeInTheDocument();

    // Should render an icon for each entry
    const icons = screen.getAllByTestId('node-icon');
    expect(icons).toHaveLength(
      mockNodesByType.length + mockMaterializationsByType.length,
    );
  });

  it('renders nothing if no data returned', async () => {
    const mockDjClient = {
      system: {
        node_counts_by_type: jest.fn().mockResolvedValue([]),
        materialization_counts_by_type: jest.fn().mockResolvedValue([]),
      },
    };

    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <NodesByTypePanel />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.system.node_counts_by_type).toHaveBeenCalled();
      expect(
        mockDjClient.system.materialization_counts_by_type,
      ).toHaveBeenCalled();
    });

    expect(screen.getByText('Nodes by Type')).toBeInTheDocument();
    expect(screen.getByText('Materializations by Type')).toBeInTheDocument();
    expect(screen.queryByText('sources')).not.toBeInTheDocument();
    expect(screen.queryByText('metrics')).not.toBeInTheDocument();
  });
});
