import { render, screen, waitFor } from '@testing-library/react';
import { ByStatusPanel } from '../ByStatusPanel';
import DJClientContext from '../../../providers/djclient';

describe('<ByStatusPanel />', () => {
  it('fetches nodes by status and displays them correctly', async () => {
    const mockNodeCounts = [
      { name: 'VALID', value: 10 },
      { name: 'INVALID', value: 5 },
    ];

    const mockDjClient = {
      system: {
        node_counts_by_status: jest.fn().mockResolvedValue(mockNodeCounts),
      },
    };

    render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <ByStatusPanel />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.system.node_counts_by_status).toHaveBeenCalled();
    });

    // Check that counts are rendered
    expect(screen.getByText('10')).toBeInTheDocument();
    expect(screen.getByText('5')).toBeInTheDocument();

    // Check that labels are rendered
    expect(screen.getByText('valid nodes')).toBeInTheDocument();
    expect(screen.getByText('invalid nodes')).toBeInTheDocument();
  });
});
