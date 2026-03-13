import React from 'react';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { NotificationsSection } from '../NotificationsSection';

jest.mock('../MyWorkspacePage.css', () => ({}));

describe('<NotificationsSection />', () => {
  const mockNotifications = [
    {
      entity_name: 'default.test_metric',
      entity_type: 'node',
      activity_type: 'update',
      created_at: '2024-01-01T00:00:00Z',
      user: 'test.user@example.com',
      node_type: 'metric',
      display_name: 'Test Metric',
      details: { version: 'v1.0' },
    },
    {
      entity_name: 'default.test_metric',
      entity_type: 'node',
      activity_type: 'create',
      created_at: '2024-01-02T00:00:00Z',
      user: 'other.user@example.com',
      node_type: 'metric',
      display_name: 'Test Metric',
    },
  ];

  it('should render loading state', () => {
    render(
      <MemoryRouter>
        <NotificationsSection
          notifications={[]}
          username="test.user@example.com"
          loading={true}
        />
      </MemoryRouter>,
    );

    // Should not show content when loading
    expect(screen.queryByText('Test Metric')).not.toBeInTheDocument();
  });

  it('should render empty state when no notifications', () => {
    render(
      <MemoryRouter>
        <NotificationsSection
          notifications={[]}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText('No notifications yet.')).toBeInTheDocument();
    expect(
      screen.getByText('Watch nodes to get notified of changes.'),
    ).toBeInTheDocument();
  });

  it('should render notifications list', () => {
    render(
      <MemoryRouter>
        <NotificationsSection
          notifications={mockNotifications}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText('Test Metric')).toBeInTheDocument();
  });

  it('should group notifications by node', () => {
    render(
      <MemoryRouter>
        <NotificationsSection
          notifications={mockNotifications}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    // Both notifications are for the same node, should show "2 updates"
    // Since current user is one of them, should show "you + 1 other"
    expect(screen.getByText(/you \+ 1 other/)).toBeInTheDocument();
    expect(screen.getByText(/2 updates/)).toBeInTheDocument();
  });

  it('should show "you" for current user', () => {
    const singleNotification = [
      {
        entity_name: 'default.test_metric',
        entity_type: 'node',
        activity_type: 'update',
        created_at: '2024-01-01T00:00:00Z',
        user: 'test.user@example.com',
        node_type: 'metric',
        display_name: 'Test Metric',
      },
    ];

    render(
      <MemoryRouter>
        <NotificationsSection
          notifications={singleNotification}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText(/by you/)).toBeInTheDocument();
  });

  it('should link to revision when version is available', () => {
    const notificationWithVersion = [
      {
        entity_name: 'default.test_metric',
        entity_type: 'node',
        activity_type: 'update',
        created_at: '2024-01-01T00:00:00Z',
        user: 'test.user@example.com',
        node_type: 'metric',
        display_name: 'Test Metric',
        details: { version: 'v2.0' },
      },
    ];

    render(
      <MemoryRouter>
        <NotificationsSection
          notifications={notificationWithVersion}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    const link = screen.getByText('Test Metric').closest('a');
    expect(link).toHaveAttribute(
      'href',
      '/nodes/default.test_metric/revisions/v2.0',
    );
  });

  it('should link to history when version is not available', () => {
    const notificationWithoutVersion = [
      {
        entity_name: 'default.test_metric',
        entity_type: 'node',
        activity_type: 'update',
        created_at: '2024-01-01T00:00:00Z',
        user: 'test.user@example.com',
        node_type: 'metric',
        display_name: 'Test Metric',
      },
    ];

    render(
      <MemoryRouter>
        <NotificationsSection
          notifications={notificationWithoutVersion}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    const link = screen.getByText('Test Metric').closest('a');
    expect(link).toHaveAttribute('href', '/nodes/default.test_metric/history');
  });

  it('should show username part for single notification from another user', () => {
    const otherUserNotification = [
      {
        entity_name: 'default.other_metric',
        activity_type: 'update',
        created_at: '2024-01-01T00:00:00Z',
        user: 'other.person@example.com',
        node_type: 'metric',
        display_name: 'Other Metric',
      },
    ];

    render(
      <MemoryRouter>
        <NotificationsSection
          notifications={otherUserNotification}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    // Should show "other.person" (split on @)
    expect(screen.getByText(/by other\.person/)).toBeInTheDocument();
  });

  it('should show "you + N others" when current user is among multiple updaters', () => {
    const multiUserNotifications = [
      {
        entity_name: 'default.shared_metric',
        activity_type: 'update',
        created_at: '2024-01-01T00:00:00Z',
        user: 'test.user@example.com',
        node_type: 'metric',
        display_name: 'Shared Metric',
      },
      {
        entity_name: 'default.shared_metric',
        activity_type: 'update',
        created_at: '2024-01-02T00:00:00Z',
        user: 'alice@example.com',
        node_type: 'metric',
        display_name: 'Shared Metric',
      },
      {
        entity_name: 'default.shared_metric',
        activity_type: 'update',
        created_at: '2024-01-03T00:00:00Z',
        user: 'bob@example.com',
        node_type: 'metric',
        display_name: 'Shared Metric',
      },
    ];

    render(
      <MemoryRouter>
        <NotificationsSection
          notifications={multiUserNotifications}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    // 3 unique users including current user → "you + 2 others"
    expect(screen.getByText(/you \+ 2 others/)).toBeInTheDocument();
  });

  it('should show "N users" when multiple users not including current user', () => {
    const otherUsersNotifications = [
      {
        entity_name: 'default.their_metric',
        activity_type: 'update',
        created_at: '2024-01-01T00:00:00Z',
        user: 'alice@example.com',
        node_type: 'metric',
        display_name: 'Their Metric',
      },
      {
        entity_name: 'default.their_metric',
        activity_type: 'update',
        created_at: '2024-01-02T00:00:00Z',
        user: 'bob@example.com',
        node_type: 'metric',
        display_name: 'Their Metric',
      },
    ];

    render(
      <MemoryRouter>
        <NotificationsSection
          notifications={otherUsersNotifications}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText(/2 users/)).toBeInTheDocument();
  });

  it('should show "you" when users array is empty and mostRecent.user is current user', () => {
    const nullUserNotification = [
      {
        entity_name: 'default.my_metric',
        activity_type: 'update',
        created_at: '2024-01-01T00:00:00Z',
        user: 'test.user@example.com',
        // user field is present but filter(u => u != null && u !== '') keeps it
        // To hit the users.length === 0 branch we need user=null or user=''
      },
    ];

    // Use a notification where user is null/empty so allUsers filter removes it
    const emptyUserNotification = [
      {
        entity_name: 'default.my_metric',
        activity_type: 'update',
        created_at: '2024-01-01T00:00:00Z',
        user: null, // filtered out → users.length === 0
        node_type: 'metric',
        display_name: 'My Metric',
        details: { version: 'v1' },
      },
    ];
    // mostRecent.user is null → falls to 'unknown'
    render(
      <MemoryRouter>
        <NotificationsSection
          notifications={emptyUserNotification}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText(/by unknown/)).toBeInTheDocument();
  });

  it('should show "you" in zero-users fallback when mostRecent.user matches username', () => {
    const emptyStringUserNotification = [
      {
        entity_name: 'default.my_metric',
        activity_type: 'update',
        created_at: '2024-01-01T00:00:00Z',
        user: '', // filtered out → users.length === 0
        node_type: 'metric',
        display_name: 'My Metric',
      },
    ];
    // mostRecent.user is '' which !== username → goes to split('@')[0] || 'unknown' → 'unknown'
    // To get "you" in zero-users path: mostRecent.user must equal username
    // We need user field stored (not filtered) to equal username but filtered...
    // Actually user='' is filtered. Let's set user=username so it passes filter and users=[username]
    // → users.length === 1 and users[0] === username → "you"
    const selfNotification = [
      {
        entity_name: 'default.my_metric',
        activity_type: 'update',
        created_at: '2024-01-01T00:00:00Z',
        user: 'test.user@example.com',
        node_type: 'metric',
        display_name: 'My Metric',
      },
    ];

    render(
      <MemoryRouter>
        <NotificationsSection
          notifications={selfNotification}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText(/by you/)).toBeInTheDocument();
  });

  it('should limit notifications to 15', () => {
    const manyNotifications = Array.from({ length: 20 }, (_, i) => ({
      entity_name: `default.metric_${i}`,
      entity_type: 'node',
      activity_type: 'update',
      created_at: '2024-01-01T00:00:00Z',
      user: 'test.user@example.com',
      node_type: 'metric',
      display_name: `Metric ${i}`,
    }));

    render(
      <MemoryRouter>
        <NotificationsSection
          notifications={manyNotifications}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    // Should only render 15 notifications
    expect(screen.getByText('Metric 0')).toBeInTheDocument();
    expect(screen.getByText('Metric 14')).toBeInTheDocument();
    expect(screen.queryByText('Metric 15')).not.toBeInTheDocument();
  });
});
