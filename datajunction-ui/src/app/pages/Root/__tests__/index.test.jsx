import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { Root } from '../index';
import DJClientContext from '../../../providers/djclient';
import { HelmetProvider } from 'react-helmet-async';

describe('<Root />', () => {
  const mockDjClient = {
    logout: jest.fn(),
    nodeDetails: jest.fn(),
    listTags: jest.fn().mockResolvedValue([]),
    nodes: jest.fn().mockResolvedValue([]),
    whoami: jest.fn().mockResolvedValue({
      id: 1,
      username: 'testuser',
      email: 'test@example.com',
    }),
    getSubscribedHistory: jest.fn().mockResolvedValue([]),
    markNotificationsRead: jest.fn().mockResolvedValue({}),
    getNodesByNames: jest.fn().mockResolvedValue([]),
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders with the correct title and navigation', async () => {
    render(
      <HelmetProvider>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <Root />
        </DJClientContext.Provider>
      </HelmetProvider>,
    );

    await waitFor(() => {
      expect(document.title).toEqual('DataJunction');
    });

    // Check navigation links exist
    expect(screen.getByText('Explore')).toBeInTheDocument();
    expect(screen.getByText('SQL')).toBeInTheDocument();
  });
});
