import React from 'react';
import { screen, waitFor } from '@testing-library/react';
import fetchMock from 'jest-fetch-mock';
import userEvent from '@testing-library/user-event';
import { render } from '../../../setupTests';
import DJClientContext from '../../providers/djclient';
import DeleteNode from '../DeleteNode';

describe('<DeleteNode />', () => {
  beforeEach(() => {
    fetchMock.resetMocks();
    jest.clearAllMocks();
    window.scrollTo = jest.fn();
  });

  const renderElement = djClient => {
    return render(
      <DJClientContext.Provider value={djClient}>
        <DeleteNode nodeName="default.hard_hat" />
      </DJClientContext.Provider>,
    );
  };

  const initializeMockDJClient = () => {
    return {
      DataJunctionAPI: {
        deactivate: jest.fn(),
      },
    };
  };

  it('deletes a node when clicked', async () => {
    const mockDjClient = initializeMockDJClient();
    mockDjClient.DataJunctionAPI.deactivate.mockReturnValue({
      status: 204,
      json: { name: 'source.warehouse.schema.some_table' },
    });

    renderElement(mockDjClient);

    await userEvent.click(screen.getByRole('button'));

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.deactivate).toBeCalled();
      expect(mockDjClient.DataJunctionAPI.deactivate).toBeCalledWith(
        'default.hard_hat',
      );
    });
    expect(
      screen.getByText('Successfully deleted node default.hard_hat'),
    ).toBeInTheDocument();
  }, 60000);
});
