import { render, screen, waitFor } from '@testing-library/react';
import DJClientContext from '../../../providers/djclient';
import { OverviewPanel } from '../OverviewPanel';

describe('<OverviewPanel />', () => {
  it('renders Active Nodes and Valid/Invalid nodes correctly', async () => {
    const mockNodesByActive = [
      { name: 'true', value: 7 },
      { name: 'false', value: 2 }, // Should be filtered out
    ];

    const mockNodesByStatus = [
      { name: 'VALID', value: 5 },
      { name: 'INVALID', value: 3 },
    ];

    const mockDjClient = {
      system: {
        node_counts_by_active: jest.fn().mockResolvedValue(mockNodesByActive),
        node_counts_by_status: jest.fn().mockResolvedValue(mockNodesByStatus),
      },
    };

    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <OverviewPanel />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.system.node_counts_by_active).toHaveBeenCalled();
      expect(mockDjClient.system.node_counts_by_status).toHaveBeenCalled();
    });

    // Chart title
    expect(screen.getByText('Overview')).toBeInTheDocument();

    // Active Nodes section
    expect(screen.getByText('7')).toBeInTheDocument();
    expect(screen.getByText('Active Nodes')).toBeInTheDocument();
    expect(screen.getAllByTestId('node-icon')).toHaveLength(1);

    // Valid/Invalid status section
    expect(screen.getByText('5')).toBeInTheDocument();
    expect(screen.getByText('valid')).toBeInTheDocument();
    expect(screen.getByText('3')).toBeInTheDocument();
    expect(screen.getByText('invalid')).toBeInTheDocument();

    // Should render one ValidIcon and one InvalidIcon
    expect(screen.getAllByTestId('valid-icon')).toHaveLength(1);
    expect(screen.getAllByTestId('invalid-icon')).toHaveLength(1);
  });

  it('renders no badges if data is empty', async () => {
    const mockDjClient = {
      system: {
        node_counts_by_active: jest.fn().mockResolvedValue([]),
        node_counts_by_status: jest.fn().mockResolvedValue([]),
      },
    };

    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <OverviewPanel />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.system.node_counts_by_active).toHaveBeenCalled();
      expect(mockDjClient.system.node_counts_by_status).toHaveBeenCalled();
    });

    expect(screen.getByText('Overview')).toBeInTheDocument();
    expect(screen.queryByTestId('node-icon')).not.toBeInTheDocument();
    expect(screen.queryByTestId('valid-icon')).not.toBeInTheDocument();
    expect(screen.queryByTestId('invalid-icon')).not.toBeInTheDocument();
  });
});
