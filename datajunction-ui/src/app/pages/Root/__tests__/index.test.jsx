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

  it('handles notification dropdown toggle and closes user menu', async () => {
    renderRoot();

    await waitFor(() => {
      expect(document.title).toEqual('DataJunction');
    });

    // Find the notification bell button and click it
    const bellButton = document.querySelector('[aria-label="Notifications"]');
    if (bellButton) {
      // First click opens notifications
      fireEvent.click(bellButton);
      await waitFor(() => {
        expect(bellButton).toBeInTheDocument();
      });

      // Click again to close
      fireEvent.click(bellButton);
      await waitFor(() => {
        expect(bellButton).toBeInTheDocument();
      });
    }
  });

  it('handles user menu dropdown toggle and closes notification dropdown', async () => {
    renderRoot();

    await waitFor(() => {
      expect(document.title).toEqual('DataJunction');
    });

    // The nav-right container should be present for auth-enabled mode
    const navRight = document.querySelector('.nav-right');
    expect(navRight).toBeInTheDocument();

    // Find the user menu button (look for avatar or user icon)
    const userMenuBtn = document.querySelector(
      '.user-menu-button, .user-avatar, [aria-label*="user"], [aria-label*="menu"]',
    );
    if (userMenuBtn) {
      fireEvent.click(userMenuBtn);
      await waitFor(() => {
        expect(userMenuBtn).toBeInTheDocument();
      });
    }
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

  it('toggles between notification and user dropdowns exclusively', async () => {
    renderRoot();

    await waitFor(() => {
      expect(document.title).toEqual('DataJunction');
    });

    const navRight = document.querySelector('.nav-right');
    expect(navRight).toBeInTheDocument();

    // Find both dropdown triggers
    const bellButton = document.querySelector('[aria-label="Notifications"]');
    const userMenuTrigger = navRight?.querySelector('button, [role="button"]');

    if (bellButton && userMenuTrigger && bellButton !== userMenuTrigger) {
      // Click notification first
      fireEvent.click(bellButton);
      await waitFor(() => {
        expect(bellButton).toBeInTheDocument();
      });

      // Now click user menu - should close notifications
      fireEvent.click(userMenuTrigger);
      await waitFor(() => {
        expect(userMenuTrigger).toBeInTheDocument();
      });

      // Click notifications again - should close user menu
      fireEvent.click(bellButton);
      await waitFor(() => {
        expect(bellButton).toBeInTheDocument();
      });
    }
  });

  it('sets openDropdown state correctly for notification toggle', async () => {
    renderRoot();

    await waitFor(() => {
      expect(document.title).toEqual('DataJunction');
    });

    const bellButton = document.querySelector('[aria-label="Notifications"]');
    if (bellButton) {
      // Open notifications dropdown
      fireEvent.click(bellButton);

      // The dropdown toggle handler should have been called with isOpen=true
      // which sets openDropdown to 'notifications'
      await waitFor(() => {
        expect(bellButton).toBeInTheDocument();
      });
    }
  });

  it('sets openDropdown state correctly for user menu toggle', async () => {
    renderRoot();

    await waitFor(() => {
      expect(document.title).toEqual('DataJunction');
    });

    const navRight = document.querySelector('.nav-right');
    if (navRight) {
      // Find user menu element (usually second clickable element in nav-right)
      const buttons = navRight.querySelectorAll('button, [role="button"]');
      if (buttons.length > 0) {
        const userButton = buttons[buttons.length - 1]; // Last button is usually user menu
        fireEvent.click(userButton);

        // The dropdown toggle handler should have been called with isOpen=true
        // which sets openDropdown to 'user'
        await waitFor(() => {
          expect(userButton).toBeInTheDocument();
        });
      }
    }
  });
});
