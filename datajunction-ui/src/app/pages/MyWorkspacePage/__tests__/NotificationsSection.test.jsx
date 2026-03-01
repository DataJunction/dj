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
