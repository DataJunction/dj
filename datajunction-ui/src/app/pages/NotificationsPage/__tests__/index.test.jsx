import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { NotificationsPage } from '../index';
import DJClientContext from '../../../providers/djclient';

describe('<NotificationsPage />', () => {
  const mockNotifications = [
    {
      id: 1,
      entity_type: 'node',
      entity_name: 'default.metrics.revenue',
      node: 'default.metrics.revenue',
      activity_type: 'update',
      user: 'alice',
      created_at: new Date().toISOString(),
      details: { version: 'v2' },
    },
    {
      id: 2,
      entity_type: 'node',
      entity_name: 'default.dimensions.country',
      node: 'default.dimensions.country',
      activity_type: 'create',
      user: 'bob',
      created_at: new Date().toISOString(),
      details: { version: 'v1' },
    },
  ];

  const mockNodes = [
    {
      name: 'default.metrics.revenue',
      type: 'metric',
      current: { displayName: 'Revenue Metric' },
    },
    {
      name: 'default.dimensions.country',
      type: 'dimension',
      current: { displayName: 'Country' },
    },
  ];

  const createMockDjClient = (overrides = {}) => ({
    getSubscribedHistory: jest.fn().mockResolvedValue(mockNotifications),
    getNodesByNames: jest.fn().mockResolvedValue(mockNodes),
    ...overrides,
  });

  const renderWithContext = mockDjClient => {
    return render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <NotificationsPage />
      </DJClientContext.Provider>,
    );
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders the page title', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    // Wait for async effects to complete
    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    expect(screen.getByText('Notifications')).toBeInTheDocument();
  });

  it('shows loading state initially', async () => {
    // Use a controlled promise that we can resolve after the test
    let resolvePromise;
    const pendingPromise = new Promise(resolve => {
      resolvePromise = resolve;
    });

    const mockDjClient = createMockDjClient({
      getSubscribedHistory: jest.fn().mockImplementation(() => pendingPromise),
    });
    renderWithContext(mockDjClient);

    // LoadingIcon should be present (check for the container)
    const loadingContainer = document.querySelector(
      '[style*="text-align: center"]',
    );
    expect(loadingContainer).toBeInTheDocument();

    // Resolve the promise to allow cleanup without act() warnings
    resolvePromise([]);
    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });
  });

  it('shows empty state when no notifications', async () => {
    const mockDjClient = createMockDjClient({
      getSubscribedHistory: jest.fn().mockResolvedValue([]),
    });
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    expect(screen.getByText(/No notifications yet/i)).toBeInTheDocument();
    expect(
      screen.getByText(/Watch nodes to receive updates/i),
    ).toBeInTheDocument();
  });

  it('displays notifications with display names', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    // Display names should be shown
    expect(await screen.findByText('Revenue Metric')).toBeInTheDocument();
    expect(await screen.findByText('Country')).toBeInTheDocument();
  });

  it('displays entity names below display names', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    // Entity names should be shown
    expect(
      await screen.findByText('default.metrics.revenue'),
    ).toBeInTheDocument();
    expect(
      await screen.findByText('default.dimensions.country'),
    ).toBeInTheDocument();
  });

  it('falls back to entity_name when no display_name', async () => {
    const mockDjClient = createMockDjClient({
      getNodesByNames: jest.fn().mockResolvedValue([]), // No node info
    });
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    // Entity names should be shown as the title (no display names)
    const revenueElements = await screen.findAllByText(
      'default.metrics.revenue',
    );
    expect(revenueElements.length).toBeGreaterThan(0);
  });

  it('shows version badge when version is available', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    expect(await screen.findByText('v2')).toBeInTheDocument();
    expect(await screen.findByText('v1')).toBeInTheDocument();
  });

  it('links to revision page when version is available', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    await waitFor(() => {
      const links = document.querySelectorAll('a.notification-item');
      expect(links.length).toBe(2);

      const revenueLink = Array.from(links).find(l =>
        l.textContent.includes('Revenue Metric'),
      );
      expect(revenueLink).toHaveAttribute(
        'href',
        '/nodes/default.metrics.revenue/revisions/v2',
      );
    });
  });

  it('links to history page when no version', async () => {
    const mockDjClient = createMockDjClient({
      getSubscribedHistory: jest.fn().mockResolvedValue([
        {
          id: 1,
          entity_type: 'node',
          entity_name: 'default.source.orders',
          node: 'default.source.orders',
          activity_type: 'update',
          user: 'alice',
          created_at: new Date().toISOString(),
          details: {}, // No version
        },
      ]),
      getNodesByNames: jest.fn().mockResolvedValue([]),
    });
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    await waitFor(() => {
      const link = document.querySelector('a.notification-item');
      expect(link).toHaveAttribute(
        'href',
        '/nodes/default.source.orders/history',
      );
    });
  });

  it('shows node type badge', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    expect(await screen.findByText('METRIC')).toBeInTheDocument();
    expect(await screen.findByText('DIMENSION')).toBeInTheDocument();
  });

  it('shows activity type and user', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    expect(await screen.findByText('alice')).toBeInTheDocument();
    expect(await screen.findByText('bob')).toBeInTheDocument();
  });

  it('groups notifications by date', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    // Both notifications are from today
    expect(await screen.findByText('Today')).toBeInTheDocument();
  });

  it('fetches node info via GraphQL', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalledWith(50);
    });

    await waitFor(() => {
      expect(mockDjClient.getNodesByNames).toHaveBeenCalledWith([
        'default.metrics.revenue',
        'default.dimensions.country',
      ]);
    });
  });

  it('handles errors gracefully', async () => {
    const consoleSpy = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});

    const mockDjClient = createMockDjClient({
      getSubscribedHistory: jest
        .fn()
        .mockRejectedValue(new Error('Network error')),
    });
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(consoleSpy).toHaveBeenCalledWith(
        'Error fetching notifications:',
        expect.any(Error),
      );
    });

    // Should show empty state after error
    expect(screen.getByText(/No notifications yet/i)).toBeInTheDocument();

    consoleSpy.mockRestore();
  });

  it('handles null response from getSubscribedHistory', async () => {
    const mockDjClient = createMockDjClient({
      getSubscribedHistory: jest.fn().mockResolvedValue(null),
    });
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    // Should show empty state when API returns null
    expect(screen.getByText(/No notifications yet/i)).toBeInTheDocument();
  });

  it('handles undefined response from getSubscribedHistory', async () => {
    const mockDjClient = createMockDjClient({
      getSubscribedHistory: jest.fn().mockResolvedValue(undefined),
    });
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    // Should show empty state when API returns undefined
    expect(screen.getByText(/No notifications yet/i)).toBeInTheDocument();
  });
});
