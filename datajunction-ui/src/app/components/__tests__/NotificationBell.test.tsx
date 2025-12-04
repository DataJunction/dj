import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import NotificationBell from '../NotificationBell';
import DJClientContext from '../../providers/djclient';

describe('<NotificationBell />', () => {
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
      created_at: new Date(Date.now() - 3600000).toISOString(), // 1 hour ago
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
    whoami: jest.fn().mockResolvedValue({
      id: 1,
      username: 'testuser',
      last_viewed_notifications_at: null,
    }),
    getSubscribedHistory: jest.fn().mockResolvedValue(mockNotifications),
    getNodesByNames: jest.fn().mockResolvedValue(mockNodes),
    markNotificationsRead: jest.fn().mockResolvedValue({}),
    ...overrides,
  });

  const renderWithContext = (mockDjClient: any) => {
    return render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <NotificationBell />
      </DJClientContext.Provider>,
    );
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders the notification bell button', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    const button = screen.getByRole('button');
    expect(button).toBeInTheDocument();
  });

  it('shows unread badge when there are unread notifications', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    // Wait for notifications to load
    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    // Badge should show count of 2 (all notifications are unread since last_viewed is null)
    const badge = await screen.findByText('2');
    expect(badge).toHaveClass('notification-badge');
  });

  it('does not show badge when all notifications have been viewed', async () => {
    const mockDjClient = createMockDjClient({
      whoami: jest.fn().mockResolvedValue({
        id: 1,
        username: 'testuser',
        // Set last_viewed to future date so all notifications are "read"
        last_viewed_notifications_at: new Date(
          Date.now() + 10000,
        ).toISOString(),
      }),
    });
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    // Badge should not be present (no unread count shown)
    const badge = document.querySelector('.notification-badge');
    expect(badge).toBeNull();
  });

  it('opens dropdown when bell is clicked', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    const button = screen.getByRole('button');
    fireEvent.click(button);

    expect(screen.getByText('Updates')).toBeInTheDocument();
  });

  it('displays notifications in the dropdown', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    const button = screen.getByRole('button');
    fireEvent.click(button);

    // Check that display names are shown
    expect(screen.getByText('Revenue Metric')).toBeInTheDocument();
    expect(screen.getByText('Country')).toBeInTheDocument();

    // Check that entity names are shown below
    expect(screen.getByText('default.metrics.revenue')).toBeInTheDocument();
    expect(screen.getByText('default.dimensions.country')).toBeInTheDocument();
  });

  it('shows empty state when no notifications', async () => {
    const mockDjClient = createMockDjClient({
      getSubscribedHistory: jest.fn().mockResolvedValue([]),
    });
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    const button = screen.getByRole('button');
    fireEvent.click(button);

    expect(screen.getByText('No updates on watched nodes')).toBeInTheDocument();
  });

  it('marks notifications as read when dropdown is opened', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    const button = screen.getByRole('button');
    fireEvent.click(button);

    expect(mockDjClient.markNotificationsRead).toHaveBeenCalled();
  });

  it('does not mark as read if already all read', async () => {
    const mockDjClient = createMockDjClient({
      whoami: jest.fn().mockResolvedValue({
        id: 1,
        username: 'testuser',
        last_viewed_notifications_at: new Date(
          Date.now() + 10000,
        ).toISOString(),
      }),
    });
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    const button = screen.getByRole('button');
    fireEvent.click(button);

    // Should not call markNotificationsRead since unreadCount is 0
    expect(mockDjClient.markNotificationsRead).not.toHaveBeenCalled();
  });

  it('shows View all link when there are notifications', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    const button = screen.getByRole('button');
    fireEvent.click(button);

    const viewAllLink = screen.getByText('View all');
    expect(viewAllLink).toHaveAttribute('href', '/notifications');
  });

  it('calls onDropdownToggle when dropdown state changes', async () => {
    const mockDjClient = createMockDjClient();
    const onDropdownToggle = jest.fn();

    render(
      <DJClientContext.Provider
        value={{ DataJunctionAPI: mockDjClient as any }}
      >
        <NotificationBell onDropdownToggle={onDropdownToggle} />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    const button = screen.getByRole('button');
    fireEvent.click(button);

    expect(onDropdownToggle).toHaveBeenCalledWith(true);
  });

  it('closes dropdown when forceClose becomes true', async () => {
    const mockDjClient = createMockDjClient();

    const { rerender } = render(
      <DJClientContext.Provider
        value={{ DataJunctionAPI: mockDjClient as any }}
      >
        <NotificationBell forceClose={false} />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    // Open the dropdown
    const button = screen.getByRole('button');
    fireEvent.click(button);

    // Verify dropdown is open
    expect(screen.getByText('Updates')).toBeInTheDocument();

    // Rerender with forceClose=true
    rerender(
      <DJClientContext.Provider
        value={{ DataJunctionAPI: mockDjClient as any }}
      >
        <NotificationBell forceClose={true} />
      </DJClientContext.Provider>,
    );

    // Dropdown should be closed
    expect(screen.queryByText('Updates')).not.toBeInTheDocument();
  });

  it('closes dropdown when clicking outside', async () => {
    const mockDjClient = createMockDjClient();
    const onDropdownToggle = jest.fn();

    render(
      <DJClientContext.Provider
        value={{ DataJunctionAPI: mockDjClient as any }}
      >
        <NotificationBell onDropdownToggle={onDropdownToggle} />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.getSubscribedHistory).toHaveBeenCalled();
    });

    // Open the dropdown
    const button = screen.getByRole('button');
    fireEvent.click(button);

    // Verify dropdown is open
    expect(screen.getByText('Updates')).toBeInTheDocument();

    // Click outside the dropdown
    fireEvent.click(document.body);

    // Dropdown should be closed
    expect(screen.queryByText('Updates')).not.toBeInTheDocument();

    // onDropdownToggle should have been called with false
    expect(onDropdownToggle).toHaveBeenCalledWith(false);
  });
});
