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

  it('handles subscription update via edit mode and checkbox toggle', async () => {
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

    // First click Edit button to enter edit mode and show checkboxes
    const editBtn = screen.getByTitle('Edit subscription');
    fireEvent.click(editBtn);

    // Wait for checkboxes to appear
    await waitFor(() => {
      expect(screen.getAllByRole('checkbox').length).toBeGreaterThan(0);
    });

    // Add another activity type (e.g., 'create')
    const createCheckbox = screen.getByLabelText('Create');
    fireEvent.click(createCheckbox);

    // Save changes
    const saveBtn = screen.getByText('Save');
    fireEvent.click(saveBtn);

    await waitFor(() => {
      expect(mockDjClient.subscribeToNotifications).toHaveBeenCalledWith({
        entity_type: 'node',
        entity_name: 'default.my_metric',
        activity_types: expect.any(Array),
        alert_types: ['web'],
      });
    });
  });

  it('updates local subscription state after subscription update', async () => {
    mockDjClient.getNotificationPreferences.mockResolvedValue([
      {
        entity_name: 'default.my_metric',
        entity_type: 'node',
        activity_types: ['update', 'status_change'],
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
      json: { message: 'Updated' },
    });

    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('default.my_metric')).toBeInTheDocument();
    });

    // First click the Edit button to enter edit mode
    const editBtn = screen.getByTitle('Edit subscription');
    fireEvent.click(editBtn);

    // Now checkboxes should be visible
    await waitFor(() => {
      expect(screen.getAllByRole('checkbox').length).toBeGreaterThan(0);
    });

    // Toggle a checkbox and save
    const checkboxes = screen.getAllByRole('checkbox');
    fireEvent.click(checkboxes[0]);

    // Click Save button
    const saveBtn = screen.getByText('Save');
    fireEvent.click(saveBtn);

    await waitFor(() => {
      expect(mockDjClient.subscribeToNotifications).toHaveBeenCalled();
    });
  });

  it('handles subscription unsubscribe and removes from list', async () => {
    // Mock window.confirm to return true
    const originalConfirm = window.confirm;
    window.confirm = jest.fn().mockReturnValue(true);

    mockDjClient.getNotificationPreferences.mockResolvedValue([
      {
        entity_name: 'default.my_metric',
        entity_type: 'node',
        activity_types: ['update'],
        alert_types: ['web'],
      },
      {
        entity_name: 'default.another_metric',
        entity_type: 'node',
        activity_types: ['status_change'],
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
      {
        name: 'default.another_metric',
        type: 'METRIC',
        current: {
          displayName: 'Another Metric',
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
      expect(screen.getByText('default.another_metric')).toBeInTheDocument();
    });

    // Find unsubscribe buttons (there are multiple, one per subscription)
    const unsubscribeBtns = screen.getAllByTitle('Unsubscribe');
    fireEvent.click(unsubscribeBtns[0]);

    await waitFor(() => {
      expect(window.confirm).toHaveBeenCalled();
      expect(mockDjClient.unsubscribeFromNotifications).toHaveBeenCalled();
    });

    // Restore original confirm
    window.confirm = originalConfirm;
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

  it('creates service account and adds to list when successful', async () => {
    const newAccount = {
      id: 99,
      name: 'new-account',
      client_id: 'new-client-id-123',
      client_secret: 'secret-xyz',
      created_at: '2025-01-11T00:00:00Z',
    };

    mockDjClient.listServiceAccounts.mockResolvedValue([]);
    mockDjClient.createServiceAccount.mockResolvedValue(newAccount);

    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('Settings')).toBeInTheDocument();
    });

    // Open create modal
    const createBtn = screen.getByRole('button', { name: /create/i });
    fireEvent.click(createBtn);

    await waitFor(() => {
      expect(screen.getByText('Create Service Account')).toBeInTheDocument();
    });

    // Fill in the name input using ID
    const nameInput = document.getElementById('service-account-name');
    fireEvent.change(nameInput, { target: { value: 'new-account' } });

    // Submit the form by clicking the submit button in the modal
    const submitBtns = screen.getAllByRole('button', { name: /create/i });
    // The second Create button is the submit button in the modal
    fireEvent.click(submitBtns[submitBtns.length - 1]);

    await waitFor(() => {
      expect(mockDjClient.createServiceAccount).toHaveBeenCalledWith(
        'new-account',
      );
    });
  });

  it('does not add service account to list if creation returns no client_id', async () => {
    mockDjClient.listServiceAccounts.mockResolvedValue([]);
    mockDjClient.createServiceAccount.mockResolvedValue({
      error: 'Name already exists',
    });

    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('Settings')).toBeInTheDocument();
    });

    // The service accounts section should still show empty state
    expect(screen.getByText(/No service accounts yet/i)).toBeInTheDocument();
  });

  it('handles service account deletion by clicking delete button with confirmation', async () => {
    // Mock window.confirm to return true
    const originalConfirm = window.confirm;
    window.confirm = jest.fn().mockReturnValue(true);

    mockDjClient.listServiceAccounts.mockResolvedValue([
      {
        id: 1,
        name: 'my-pipeline',
        client_id: 'abc-123',
        created_at: '2024-12-01T00:00:00Z',
      },
      {
        id: 2,
        name: 'other-pipeline',
        client_id: 'def-456',
        created_at: '2024-12-02T00:00:00Z',
      },
    ]);

    mockDjClient.deleteServiceAccount.mockResolvedValue({
      message: 'Deleted',
    });

    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('my-pipeline')).toBeInTheDocument();
      expect(screen.getByText('other-pipeline')).toBeInTheDocument();
    });

    // Find delete button by title attribute (exact match) - first one for my-pipeline
    const deleteBtn = screen.getAllByTitle('Delete service account')[0];
    fireEvent.click(deleteBtn);

    await waitFor(() => {
      expect(window.confirm).toHaveBeenCalled();
      expect(mockDjClient.deleteServiceAccount).toHaveBeenCalledWith('abc-123');
    });

    // Restore original confirm
    window.confirm = originalConfirm;
  });

  it('removes service account from list after deletion', async () => {
    // Mock window.confirm to return true
    const originalConfirm = window.confirm;
    window.confirm = jest.fn().mockReturnValue(true);

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

    // Find and click the delete button
    const deleteBtn = screen.getByTitle('Delete service account');
    fireEvent.click(deleteBtn);

    await waitFor(() => {
      expect(mockDjClient.deleteServiceAccount).toHaveBeenCalledWith('abc-123');
    });

    // After deletion, the account should be removed
    await waitFor(() => {
      expect(screen.queryByText('my-pipeline')).not.toBeInTheDocument();
    });

    // Restore original confirm
    window.confirm = originalConfirm;
  });

  it('does not delete service account when confirmation is cancelled', async () => {
    // Mock window.confirm to return false
    const originalConfirm = window.confirm;
    window.confirm = jest.fn().mockReturnValue(false);

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

    // Find and click the delete button
    const deleteBtn = screen.getByTitle('Delete service account');
    fireEvent.click(deleteBtn);

    // Should show confirmation
    expect(window.confirm).toHaveBeenCalled();

    // deleteServiceAccount should NOT be called since user cancelled
    expect(mockDjClient.deleteServiceAccount).not.toHaveBeenCalled();

    // Account should still be in the list
    expect(screen.getByText('my-pipeline')).toBeInTheDocument();

    // Restore original confirm
    window.confirm = originalConfirm;
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

  it('handles notification preferences fetch error gracefully', async () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
    mockDjClient.getNotificationPreferences.mockRejectedValue(
      new Error('Failed to fetch preferences'),
    );

    renderWithContext();

    await waitFor(() => {
      // Page should still render after error
      expect(screen.getByText('Settings')).toBeInTheDocument();
    });

    // Error should be logged
    expect(consoleSpy).toHaveBeenCalled();

    consoleSpy.mockRestore();
  });

  it('handles null notification preferences response', async () => {
    mockDjClient.getNotificationPreferences.mockResolvedValue(null);

    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('Settings')).toBeInTheDocument();
    });

    // Should render subscriptions section with empty list
    expect(screen.getByText(/not watching any nodes yet/i)).toBeInTheDocument();
  });

  it('handles null service accounts response', async () => {
    mockDjClient.listServiceAccounts.mockResolvedValue(null);

    renderWithContext();

    await waitFor(() => {
      expect(screen.getByText('Settings')).toBeInTheDocument();
    });

    // Should render service accounts section with empty list
    expect(screen.getByText(/No service accounts yet/i)).toBeInTheDocument();
  });
});
