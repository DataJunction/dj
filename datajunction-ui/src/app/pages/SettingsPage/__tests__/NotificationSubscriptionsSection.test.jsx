import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { NotificationSubscriptionsSection } from '../NotificationSubscriptionsSection';

describe('NotificationSubscriptionsSection', () => {
  const mockOnUpdate = jest.fn();
  const mockOnUnsubscribe = jest.fn();

  const mockSubscriptions = [
    {
      entity_name: 'default.orders_count',
      entity_type: 'node',
      node_type: 'metric',
      activity_types: ['update', 'delete'],
      status: 'valid',
    },
    {
      entity_name: 'default.dim_customers',
      entity_type: 'node',
      node_type: 'dimension',
      activity_types: ['create'],
      status: 'invalid',
    },
  ];

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders empty state when no subscriptions', () => {
    render(
      <NotificationSubscriptionsSection
        subscriptions={[]}
        onUpdate={mockOnUpdate}
        onUnsubscribe={mockOnUnsubscribe}
      />,
    );

    expect(screen.getByText(/not watching any nodes/i)).toBeInTheDocument();
  });

  it('renders subscriptions list', () => {
    render(
      <NotificationSubscriptionsSection
        subscriptions={mockSubscriptions}
        onUpdate={mockOnUpdate}
        onUnsubscribe={mockOnUnsubscribe}
      />,
    );

    expect(screen.getByText('default.orders_count')).toBeInTheDocument();
    expect(screen.getByText('default.dim_customers')).toBeInTheDocument();
    expect(screen.getByText('METRIC')).toBeInTheDocument();
    expect(screen.getByText('DIMENSION')).toBeInTheDocument();
  });

  it('shows invalid badge for invalid status', () => {
    render(
      <NotificationSubscriptionsSection
        subscriptions={mockSubscriptions}
        onUpdate={mockOnUpdate}
        onUnsubscribe={mockOnUnsubscribe}
      />,
    );

    expect(screen.getByText('INVALID')).toBeInTheDocument();
  });

  it('displays activity types as badges', () => {
    render(
      <NotificationSubscriptionsSection
        subscriptions={mockSubscriptions}
        onUpdate={mockOnUpdate}
        onUnsubscribe={mockOnUnsubscribe}
      />,
    );

    expect(screen.getByText('update')).toBeInTheDocument();
    expect(screen.getByText('delete')).toBeInTheDocument();
    expect(screen.getByText('create')).toBeInTheDocument();
  });

  it('enters edit mode when edit button is clicked', () => {
    render(
      <NotificationSubscriptionsSection
        subscriptions={mockSubscriptions}
        onUpdate={mockOnUpdate}
        onUnsubscribe={mockOnUnsubscribe}
      />,
    );

    const editButtons = screen.getAllByTitle('Edit subscription');
    fireEvent.click(editButtons[0]);

    expect(screen.getByText('Save')).toBeInTheDocument();
    expect(screen.getByText('Cancel')).toBeInTheDocument();
    expect(screen.getByLabelText('Update')).toBeInTheDocument();
  });

  it('cancels editing when cancel button is clicked', () => {
    render(
      <NotificationSubscriptionsSection
        subscriptions={mockSubscriptions}
        onUpdate={mockOnUpdate}
        onUnsubscribe={mockOnUnsubscribe}
      />,
    );

    const editButtons = screen.getAllByTitle('Edit subscription');
    fireEvent.click(editButtons[0]);

    expect(screen.getByText('Cancel')).toBeInTheDocument();

    fireEvent.click(screen.getByText('Cancel'));

    expect(screen.queryByText('Cancel')).not.toBeInTheDocument();
  });

  it('calls onUnsubscribe when unsubscribe is confirmed', async () => {
    window.confirm = jest.fn().mockReturnValue(true);
    mockOnUnsubscribe.mockResolvedValue();

    render(
      <NotificationSubscriptionsSection
        subscriptions={mockSubscriptions}
        onUpdate={mockOnUpdate}
        onUnsubscribe={mockOnUnsubscribe}
      />,
    );

    const unsubscribeButtons = screen.getAllByTitle('Unsubscribe');
    fireEvent.click(unsubscribeButtons[0]);

    expect(window.confirm).toHaveBeenCalledWith(
      'Unsubscribe from notifications for "default.orders_count"?',
    );
    expect(mockOnUnsubscribe).toHaveBeenCalledWith(mockSubscriptions[0]);
  });

  it('does not call onUnsubscribe when unsubscribe is cancelled', () => {
    window.confirm = jest.fn().mockReturnValue(false);

    render(
      <NotificationSubscriptionsSection
        subscriptions={mockSubscriptions}
        onUpdate={mockOnUpdate}
        onUnsubscribe={mockOnUnsubscribe}
      />,
    );

    const unsubscribeButtons = screen.getAllByTitle('Unsubscribe');
    fireEvent.click(unsubscribeButtons[0]);

    expect(mockOnUnsubscribe).not.toHaveBeenCalled();
  });

  it('shows alert when trying to save with no activity types', () => {
    window.alert = jest.fn();

    render(
      <NotificationSubscriptionsSection
        subscriptions={mockSubscriptions}
        onUpdate={mockOnUpdate}
        onUnsubscribe={mockOnUnsubscribe}
      />,
    );

    const editButtons = screen.getAllByTitle('Edit subscription');
    fireEvent.click(editButtons[0]);

    // Uncheck all activity types
    const updateCheckbox = screen.getByLabelText('Update');
    const deleteCheckbox = screen.getByLabelText('Delete');
    fireEvent.click(updateCheckbox);
    fireEvent.click(deleteCheckbox);

    fireEvent.click(screen.getByText('Save'));

    expect(window.alert).toHaveBeenCalledWith(
      'Please select at least one activity type',
    );
    expect(mockOnUpdate).not.toHaveBeenCalled();
  });

  it('calls onUpdate with new activity types when saved', async () => {
    mockOnUpdate.mockResolvedValue();

    render(
      <NotificationSubscriptionsSection
        subscriptions={mockSubscriptions}
        onUpdate={mockOnUpdate}
        onUnsubscribe={mockOnUnsubscribe}
      />,
    );

    const editButtons = screen.getAllByTitle('Edit subscription');
    fireEvent.click(editButtons[0]);

    // Add 'create' activity type
    const createCheckbox = screen.getByLabelText('Create');
    fireEvent.click(createCheckbox);

    fireEvent.click(screen.getByText('Save'));

    await waitFor(() => {
      expect(mockOnUpdate).toHaveBeenCalledWith(mockSubscriptions[0], [
        'update',
        'delete',
        'create',
      ]);
    });
  });

  it('renders entity type badge when node_type is not available', () => {
    const subscriptionsWithoutNodeType = [
      {
        entity_name: 'some_entity',
        entity_type: 'namespace',
        activity_types: ['create'],
      },
    ];

    render(
      <NotificationSubscriptionsSection
        subscriptions={subscriptionsWithoutNodeType}
        onUpdate={mockOnUpdate}
        onUnsubscribe={mockOnUnsubscribe}
      />,
    );

    expect(screen.getByText('namespace')).toBeInTheDocument();
  });

  it('handles onUpdate error gracefully', async () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
    window.alert = jest.fn();
    mockOnUpdate.mockRejectedValue(new Error('Update failed'));

    render(
      <NotificationSubscriptionsSection
        subscriptions={mockSubscriptions}
        onUpdate={mockOnUpdate}
        onUnsubscribe={mockOnUnsubscribe}
      />,
    );

    const editButtons = screen.getAllByTitle('Edit subscription');
    fireEvent.click(editButtons[0]);

    fireEvent.click(screen.getByText('Save'));

    await waitFor(() => {
      expect(consoleSpy).toHaveBeenCalled();
      expect(window.alert).toHaveBeenCalledWith(
        'Failed to update subscription',
      );
    });

    consoleSpy.mockRestore();
  });

  it('handles onUnsubscribe error gracefully', async () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
    window.confirm = jest.fn().mockReturnValue(true);
    mockOnUnsubscribe.mockRejectedValue(new Error('Unsubscribe failed'));

    render(
      <NotificationSubscriptionsSection
        subscriptions={mockSubscriptions}
        onUpdate={mockOnUpdate}
        onUnsubscribe={mockOnUnsubscribe}
      />,
    );

    const unsubscribeButtons = screen.getAllByTitle('Unsubscribe');
    fireEvent.click(unsubscribeButtons[0]);

    await waitFor(() => {
      expect(consoleSpy).toHaveBeenCalled();
    });

    consoleSpy.mockRestore();
  });

  it('shows "All" when subscription has no activity_types', () => {
    const subscriptionsWithoutActivityTypes = [
      {
        entity_name: 'default.some_node',
        entity_type: 'node',
        node_type: 'metric',
        activity_types: null,
        status: 'valid',
      },
    ];

    render(
      <NotificationSubscriptionsSection
        subscriptions={subscriptionsWithoutActivityTypes}
        onUpdate={mockOnUpdate}
        onUnsubscribe={mockOnUnsubscribe}
      />,
    );

    expect(screen.getByText('All')).toBeInTheDocument();
  });

  it('shows "All" when subscription has undefined activity_types', () => {
    const subscriptionsWithUndefinedActivityTypes = [
      {
        entity_name: 'default.other_node',
        entity_type: 'node',
        node_type: 'dimension',
        status: 'valid',
        // activity_types not defined
      },
    ];

    render(
      <NotificationSubscriptionsSection
        subscriptions={subscriptionsWithUndefinedActivityTypes}
        onUpdate={mockOnUpdate}
        onUnsubscribe={mockOnUnsubscribe}
      />,
    );

    expect(screen.getByText('All')).toBeInTheDocument();
  });
});
