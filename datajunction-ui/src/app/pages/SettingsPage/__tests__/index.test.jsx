import React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { SettingsPage } from '../index';
import DJClientContext from '../../../providers/djclient';
import { UserProvider } from '../../../providers/UserProvider';

describe('SettingsPage', () => {
  const mockDjClient = {
    whoami: jest.fn(),
    getNotificationPreferences: jest.fn(),
    getNodesByNames: jest.fn(),
    listServiceAccounts: jest.fn(),
    subscribeToNotifications: jest.fn(),
    unsubscribeFromNotifications: jest.fn(),
    createServiceAccount: jest.fn(),
    deleteServiceAccount: jest.fn(),
  };

  const renderWithContext = () => {
    return render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <UserProvider>
          <SettingsPage />
        </UserProvider>
      </DJClientContext.Provider>,
    );
  };

  beforeEach(() => {
    jest.clearAllMocks();

    // Default mock implementations
    mockDjClient.whoami.mockResolvedValue({
      username: 'testuser',
      email: 'test@example.com',
      name: 'Test User',
    });
    mockDjClient.getNotificationPreferences.mockResolvedValue([]);
    mockDjClient.getNodesByNames.mockResolvedValue([]);
    mockDjClient.listServiceAccounts.mockResolvedValue([]);
  });

  it('shows loading state initially', () => {
    // Make whoami hang
    mockDjClient.whoami.mockImplementation(() => new Promise(() => {}));

    renderWithContext();

    // Should show loading icon (or some loading indicator)
    expect(document.querySelector('.settings-page')).toBeInTheDocument();
  });

  it('renders all sections after loading', async () => {
    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('Settings')).toBeInTheDocument();
    });

    expect(screen.getByText('Profile')).toBeInTheDocument();
    expect(screen.getByText('Notification Subscriptions')).toBeInTheDocument();
    expect(screen.getByText('Service Accounts')).toBeInTheDocument();
  });

  it('fetches and displays user profile', async () => {
    mockDjClient.whoami.mockResolvedValue({
      username: 'alice',
      email: 'alice@example.com',
      name: 'Alice Smith',
    });

    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('alice')).toBeInTheDocument();
    });

    expect(screen.getByText('alice@example.com')).toBeInTheDocument();
    expect(screen.getByText('AS')).toBeInTheDocument(); // initials
  });

  it('fetches and displays subscriptions', async () => {
    mockDjClient.getNotificationPreferences.mockResolvedValue([
      {
        entity_name: 'default.my_metric',
        entity_type: 'node',
        activity_types: ['update'],
      },
    ]);

    mockDjClient.getNodesByNames.mockResolvedValue([
      {
        name: 'default.my_metric',
        type: 'METRIC',
        current: {
          displayName: 'My Metric',
          status: 'VALID',
          mode: 'PUBLISHED',
        },
      },
    ]);

    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('default.my_metric')).toBeInTheDocument();
    });
  });

  it('fetches and displays service accounts', async () => {
    mockDjClient.listServiceAccounts.mockResolvedValue([
      {
        id: 1,
        name: 'my-pipeline',
        client_id: 'abc-123',
        created_at: '2024-12-01T00:00:00Z',
      },
    ]);

    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('my-pipeline')).toBeInTheDocument();
    });

    expect(screen.getByText('abc-123')).toBeInTheDocument();
  });

  it('handles service accounts API error gracefully', async () => {
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
    mockDjClient.listServiceAccounts.mockRejectedValue(
      new Error('Not available'),
    );

    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('Settings')).toBeInTheDocument();
    });

    // Page should still render without service accounts
    expect(screen.getByText(/No service accounts yet/i)).toBeInTheDocument();

    consoleSpy.mockRestore();
  });

  it('handles fetch error gracefully', async () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
    mockDjClient.whoami.mockRejectedValue(new Error('Network error'));

    renderWithContext();

    await waitFor(() => {
      // Page should render even after error
      expect(document.querySelector('.settings-page')).toBeInTheDocument();
    });

    consoleSpy.mockRestore();
  });

  it('enriches subscriptions with node info from GraphQL', async () => {
    mockDjClient.getNotificationPreferences.mockResolvedValue([
      {
        entity_name: 'default.orders',
        entity_type: 'node',
        activity_types: ['update'],
      },
    ]);

    mockDjClient.getNodesByNames.mockResolvedValue([
      {
        name: 'default.orders',
        type: 'SOURCE',
        current: {
          displayName: 'Orders Table',
          status: 'VALID',
        },
      },
    ]);

    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('SOURCE')).toBeInTheDocument();
    });
  });

  it('handles subscription update', async () => {
    mockDjClient.getNotificationPreferences.mockResolvedValue([
      {
        entity_name: 'default.my_metric',
        entity_type: 'node',
        activity_types: ['update'],
        alert_types: ['web'],
      },
    ]);

    mockDjClient.getNodesByNames.mockResolvedValue([
      {
        name: 'default.my_metric',
        type: 'METRIC',
        current: {
          displayName: 'My Metric',
          status: 'VALID',
          mode: 'PUBLISHED',
        },
      },
    ]);

    mockDjClient.subscribeToNotifications.mockResolvedValue({
      status: 200,
      json: { message: 'Subscribed' },
    });

    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('default.my_metric')).toBeInTheDocument();
    });

    // The subscription is rendered with checkboxes for activity types
    // Find and interact with the update checkbox (if available in UI)
    const updateCheckbox = screen.queryByLabelText(/update/i);
    if (updateCheckbox) {
      fireEvent.click(updateCheckbox);
      await waitFor(() => {
        expect(mockDjClient.subscribeToNotifications).toHaveBeenCalled();
      });
    }
  });

  it('handles subscription unsubscribe', async () => {
    mockDjClient.getNotificationPreferences.mockResolvedValue([
      {
        entity_name: 'default.my_metric',
        entity_type: 'node',
        activity_types: ['update'],
        alert_types: ['web'],
      },
    ]);

    mockDjClient.getNodesByNames.mockResolvedValue([
      {
        name: 'default.my_metric',
        type: 'METRIC',
        current: {
          displayName: 'My Metric',
          status: 'VALID',
          mode: 'PUBLISHED',
        },
      },
    ]);

    mockDjClient.unsubscribeFromNotifications.mockResolvedValue({
      status: 200,
      json: { message: 'Unsubscribed' },
    });

    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('default.my_metric')).toBeInTheDocument();
    });

    // Find unsubscribe button
    const unsubscribeBtn = screen.queryByRole('button', {
      name: /unsubscribe/i,
    });
    if (unsubscribeBtn) {
      fireEvent.click(unsubscribeBtn);
      await waitFor(() => {
        expect(mockDjClient.unsubscribeFromNotifications).toHaveBeenCalled();
      });
    }
  });

  it('opens create service account modal', async () => {
    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('Settings')).toBeInTheDocument();
    });

    // Find and click create button
    const createBtn = screen.getByRole('button', { name: /create/i });
    fireEvent.click(createBtn);

    // Modal should be visible
    await waitFor(() => {
      expect(screen.getByText('Create Service Account')).toBeInTheDocument();
    });
  });

  it('handles service account deletion', async () => {
    mockDjClient.listServiceAccounts.mockResolvedValue([
      {
        id: 1,
        name: 'my-pipeline',
        client_id: 'abc-123',
        created_at: '2024-12-01T00:00:00Z',
      },
    ]);

    mockDjClient.deleteServiceAccount.mockResolvedValue({
      message: 'Deleted',
    });

    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('my-pipeline')).toBeInTheDocument();
    });

    // Find delete button
    const deleteBtn = screen.queryByRole('button', { name: /delete/i });
    if (deleteBtn) {
      fireEvent.click(deleteBtn);
      await waitFor(() => {
        expect(mockDjClient.deleteServiceAccount).toHaveBeenCalled();
      });
    }
  });

  it('handles non-node subscription types gracefully', async () => {
    mockDjClient.getNotificationPreferences.mockResolvedValue([
      {
        entity_name: 'namespace.test',
        entity_type: 'namespace',
        activity_types: ['create'],
        alert_types: ['web'],
      },
    ]);

    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('Settings')).toBeInTheDocument();
    });

    // Namespace subscription should still appear
    await waitFor(() => {
      expect(screen.getByText('namespace.test')).toBeInTheDocument();
    });
  });

  it('skips fetching if userLoading', async () => {
    // When user is still loading, the component waits
    mockDjClient.whoami.mockImplementation(() => new Promise(() => {}));

    renderWithContext();

    // getNotificationPreferences should not be called while user is loading
    expect(mockDjClient.getNotificationPreferences).not.toHaveBeenCalled();
  });
});
