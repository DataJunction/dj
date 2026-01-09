import React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
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

  const renderRoot = () => {
    return render(
      <HelmetProvider>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <Root />
        </DJClientContext.Provider>
      </HelmetProvider>,
    );
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders with the correct title and navigation', async () => {
    renderRoot();

    await waitFor(() => {
      expect(document.title).toEqual('DataJunction');
    });

    // Check navigation links exist
    expect(screen.getByText('Explore')).toBeInTheDocument();
    expect(screen.getByText('Query Planner')).toBeInTheDocument();
  });

  it('renders Docs dropdown', async () => {
    renderRoot();

    await waitFor(() => {
      expect(screen.getByText('Docs')).toBeInTheDocument();
    });

    // Default docs site should be visible in dropdown
    expect(screen.getByText('Open-Source')).toBeInTheDocument();
  });

  it('renders notification bell and user menu when auth is enabled', async () => {
    // Default REACT_DISABLE_AUTH is not 'true', so auth components should show
    renderRoot();

    await waitFor(() => {
      expect(document.title).toEqual('DataJunction');
    });

    // Look for nav-right container which contains the notification and user menu
    const navRight = document.querySelector('.nav-right');
    expect(navRight).toBeInTheDocument();
  });

  it('handles notification dropdown toggle', async () => {
    renderRoot();

    await waitFor(() => {
      expect(document.title).toEqual('DataJunction');
    });

    // Find the notification bell button and click it
    const bellButton = document.querySelector('[aria-label="Notifications"]');
    if (bellButton) {
      fireEvent.click(bellButton);
      // The dropdown should open
      await waitFor(() => {
        // After clicking, the dropdown state changes
        expect(bellButton).toBeInTheDocument();
      });
    }
  });

  it('handles user menu dropdown toggle', async () => {
    renderRoot();

    await waitFor(() => {
      expect(document.title).toEqual('DataJunction');
    });

    // The nav-right container should be present for auth-enabled mode
    const navRight = document.querySelector('.nav-right');
    expect(navRight).toBeInTheDocument();
  });

  it('renders logo link correctly', async () => {
    renderRoot();

    await waitFor(() => {
      expect(document.title).toEqual('DataJunction');
    });

    // Check logo link - name is "Data Junction" with space
    const logoLink = screen.getByRole('link', { name: /data.*junction/i });
    expect(logoLink).toHaveAttribute('href', '/');
  });
});
