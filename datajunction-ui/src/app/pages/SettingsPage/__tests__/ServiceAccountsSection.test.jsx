import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { ServiceAccountsSection } from '../ServiceAccountsSection';

describe('ServiceAccountsSection', () => {
  const mockOnCreate = jest.fn();
  const mockOnDelete = jest.fn();

  const mockAccounts = [
    {
      id: 1,
      name: 'my-pipeline',
      client_id: 'abc-123-xyz',
      created_at: '2024-12-01T00:00:00Z',
    },
    {
      id: 2,
      name: 'etl-job',
      client_id: 'def-456-uvw',
      created_at: '2024-12-02T00:00:00Z',
    },
  ];

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders empty state when no accounts', () => {
    render(
      <ServiceAccountsSection
        accounts={[]}
        onCreate={mockOnCreate}
        onDelete={mockOnDelete}
      />,
    );

    expect(screen.getByText(/No service accounts yet/i)).toBeInTheDocument();
  });

  it('renders accounts list', () => {
    render(
      <ServiceAccountsSection
        accounts={mockAccounts}
        onCreate={mockOnCreate}
        onDelete={mockOnDelete}
      />,
    );

    expect(screen.getByText('my-pipeline')).toBeInTheDocument();
    expect(screen.getByText('etl-job')).toBeInTheDocument();
    expect(screen.getByText('abc-123-xyz')).toBeInTheDocument();
    expect(screen.getByText('def-456-uvw')).toBeInTheDocument();
  });

  it('renders section title and create button', () => {
    render(
      <ServiceAccountsSection
        accounts={[]}
        onCreate={mockOnCreate}
        onDelete={mockOnDelete}
      />,
    );

    expect(screen.getByText('Service Accounts')).toBeInTheDocument();
    expect(screen.getByText('+ Create')).toBeInTheDocument();
  });

  it('opens create modal when create button is clicked', () => {
    render(
      <ServiceAccountsSection
        accounts={[]}
        onCreate={mockOnCreate}
        onDelete={mockOnDelete}
      />,
    );

    fireEvent.click(screen.getByText('+ Create'));

    expect(screen.getByText('Create Service Account')).toBeInTheDocument();
  });

  it('calls onDelete when delete is confirmed', async () => {
    window.confirm = jest.fn().mockReturnValue(true);
    mockOnDelete.mockResolvedValue();

    render(
      <ServiceAccountsSection
        accounts={mockAccounts}
        onCreate={mockOnCreate}
        onDelete={mockOnDelete}
      />,
    );

    const deleteButtons = screen.getAllByTitle('Delete service account');
    fireEvent.click(deleteButtons[0]);

    expect(window.confirm).toHaveBeenCalledWith(
      'Delete service account "my-pipeline"?\n\nThis will revoke all access for this account and cannot be undone.',
    );

    await waitFor(() => {
      expect(mockOnDelete).toHaveBeenCalledWith('abc-123-xyz');
    });
  });

  it('does not call onDelete when delete is cancelled', () => {
    window.confirm = jest.fn().mockReturnValue(false);

    render(
      <ServiceAccountsSection
        accounts={mockAccounts}
        onCreate={mockOnCreate}
        onDelete={mockOnDelete}
      />,
    );

    const deleteButtons = screen.getAllByTitle('Delete service account');
    fireEvent.click(deleteButtons[0]);

    expect(mockOnDelete).not.toHaveBeenCalled();
  });

  it('renders description text', () => {
    render(
      <ServiceAccountsSection
        accounts={[]}
        onCreate={mockOnCreate}
        onDelete={mockOnDelete}
      />,
    );

    expect(
      screen.getByText(/Service accounts allow programmatic access/i),
    ).toBeInTheDocument();
  });

  it('shows table headers when accounts exist', () => {
    render(
      <ServiceAccountsSection
        accounts={mockAccounts}
        onCreate={mockOnCreate}
        onDelete={mockOnDelete}
      />,
    );

    expect(screen.getByText('Name')).toBeInTheDocument();
    expect(screen.getByText('Client ID')).toBeInTheDocument();
    expect(screen.getByText('Created')).toBeInTheDocument();
  });
});
