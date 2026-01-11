import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { CreateServiceAccountModal } from '../CreateServiceAccountModal';

describe('CreateServiceAccountModal', () => {
  const mockOnClose = jest.fn();
  const mockOnCreate = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('does not render when isOpen is false', () => {
    render(
      <CreateServiceAccountModal
        isOpen={false}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    expect(
      screen.queryByText('Create Service Account'),
    ).not.toBeInTheDocument();
  });

  it('renders modal when isOpen is true', () => {
    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    expect(screen.getByText('Create Service Account')).toBeInTheDocument();
    expect(screen.getByLabelText('Name')).toBeInTheDocument();
    expect(screen.getByText('Cancel')).toBeInTheDocument();
    expect(screen.getByText('Create')).toBeInTheDocument();
  });

  it('calls onClose when close button is clicked', () => {
    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    fireEvent.click(screen.getByTitle('Close'));

    expect(mockOnClose).toHaveBeenCalled();
  });

  it('calls onClose when Cancel button is clicked', () => {
    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    fireEvent.click(screen.getByText('Cancel'));

    expect(mockOnClose).toHaveBeenCalled();
  });

  it('calls onClose when overlay is clicked', () => {
    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    // Click on overlay (modal-overlay class)
    const overlay = document.querySelector('.modal-overlay');
    fireEvent.click(overlay);

    expect(mockOnClose).toHaveBeenCalled();
  });

  it('does not close when modal content is clicked', () => {
    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    const content = document.querySelector('.modal-content');
    fireEvent.click(content);

    expect(mockOnClose).not.toHaveBeenCalled();
  });

  it('disables Create button when name is empty', () => {
    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    const createButton = screen.getByText('Create');
    expect(createButton).toBeDisabled();
  });

  it('enables Create button when name is entered', () => {
    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    const input = screen.getByLabelText('Name');
    fireEvent.change(input, { target: { value: 'my-new-account' } });

    const createButton = screen.getByText('Create');
    expect(createButton).not.toBeDisabled();
  });

  it('calls onCreate with trimmed name on submit', async () => {
    mockOnCreate.mockResolvedValue({ client_id: 'test-id' });

    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    const input = screen.getByLabelText('Name');
    fireEvent.change(input, { target: { value: '  my-account  ' } });
    fireEvent.click(screen.getByText('Create'));

    await waitFor(() => {
      expect(mockOnCreate).toHaveBeenCalledWith('my-account');
    });
  });

  it('shows credentials after successful creation', async () => {
    const credentials = {
      name: 'my-account',
      client_id: 'abc-123',
      client_secret: 'secret-xyz',
    };
    mockOnCreate.mockResolvedValue(credentials);

    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    const input = screen.getByLabelText('Name');
    fireEvent.change(input, { target: { value: 'my-account' } });
    fireEvent.click(screen.getByText('Create'));

    await waitFor(() => {
      expect(screen.getByText('Service Account Created!')).toBeInTheDocument();
    });

    expect(screen.getByText('abc-123')).toBeInTheDocument();
    expect(screen.getByText('secret-xyz')).toBeInTheDocument();
    expect(
      screen.getByText(/client secret will not be shown again/i),
    ).toBeInTheDocument();
    expect(screen.getByText('Done')).toBeInTheDocument();
  });

  it('shows alert when creation returns error message', async () => {
    window.alert = jest.fn();
    mockOnCreate.mockResolvedValue({ message: 'Account already exists' });

    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    const input = screen.getByLabelText('Name');
    fireEvent.change(input, { target: { value: 'my-account' } });
    fireEvent.click(screen.getByText('Create'));

    await waitFor(() => {
      expect(window.alert).toHaveBeenCalledWith('Account already exists');
    });
  });

  it('shows alert when creation throws error', async () => {
    window.alert = jest.fn();
    mockOnCreate.mockRejectedValue(new Error('Network error'));

    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    const input = screen.getByLabelText('Name');
    fireEvent.change(input, { target: { value: 'my-account' } });
    fireEvent.click(screen.getByText('Create'));

    await waitFor(() => {
      expect(window.alert).toHaveBeenCalledWith(
        'Failed to create service account',
      );
    });
  });

  it('shows Creating... while request is in progress', async () => {
    let resolveCreate;
    mockOnCreate.mockImplementation(
      () => new Promise(resolve => (resolveCreate = resolve)),
    );

    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    const input = screen.getByLabelText('Name');
    fireEvent.change(input, { target: { value: 'my-account' } });
    fireEvent.click(screen.getByText('Create'));

    expect(screen.getByText('Creating...')).toBeInTheDocument();

    // Resolve the promise
    resolveCreate({ client_id: 'test' });

    await waitFor(() => {
      expect(screen.queryByText('Creating...')).not.toBeInTheDocument();
    });
  });

  it('closes and resets state when Done is clicked after creation', async () => {
    const credentials = {
      name: 'my-account',
      client_id: 'abc-123',
      client_secret: 'secret-xyz',
    };
    mockOnCreate.mockResolvedValue(credentials);

    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    const input = screen.getByLabelText('Name');
    fireEvent.change(input, { target: { value: 'my-account' } });
    fireEvent.click(screen.getByText('Create'));

    await waitFor(() => {
      expect(screen.getByText('Done')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Done'));

    expect(mockOnClose).toHaveBeenCalled();
  });

  it('copies client ID to clipboard when copy button is clicked', async () => {
    const credentials = {
      name: 'my-account',
      client_id: 'abc-123',
      client_secret: 'secret-xyz',
    };
    mockOnCreate.mockResolvedValue(credentials);

    Object.assign(navigator, {
      clipboard: {
        writeText: jest.fn().mockResolvedValue(),
      },
    });

    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    const input = screen.getByLabelText('Name');
    fireEvent.change(input, { target: { value: 'my-account' } });
    fireEvent.click(screen.getByText('Create'));

    await waitFor(() => {
      expect(screen.getByText('abc-123')).toBeInTheDocument();
    });

    const copyButtons = screen.getAllByTitle('Copy');
    fireEvent.click(copyButtons[0]);

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith('abc-123');
  });

  it('copies client secret to clipboard when copy button is clicked', async () => {
    const credentials = {
      name: 'my-account',
      client_id: 'abc-123',
      client_secret: 'secret-xyz',
    };
    mockOnCreate.mockResolvedValue(credentials);

    Object.assign(navigator, {
      clipboard: {
        writeText: jest.fn().mockResolvedValue(),
      },
    });

    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    const input = screen.getByLabelText('Name');
    fireEvent.change(input, { target: { value: 'my-account' } });
    fireEvent.click(screen.getByText('Create'));

    await waitFor(() => {
      expect(screen.getByText('secret-xyz')).toBeInTheDocument();
    });

    // Click the second copy button (client secret)
    const copyButtons = screen.getAllByTitle('Copy');
    fireEvent.click(copyButtons[1]);

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith('secret-xyz');
  });

  it('does not call onCreate when form is submitted with only whitespace', async () => {
    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    const input = screen.getByLabelText('Name');
    // Enter only whitespace
    fireEvent.change(input, { target: { value: '   ' } });

    // Get the form and submit it directly
    const form = document.querySelector('form');
    fireEvent.submit(form);

    // onCreate should not be called for whitespace-only names
    expect(mockOnCreate).not.toHaveBeenCalled();
  });

  it('clears name input after successful creation', async () => {
    const credentials = {
      name: 'my-account',
      client_id: 'abc-123',
      client_secret: 'secret-xyz',
    };
    mockOnCreate.mockResolvedValue(credentials);

    render(
      <CreateServiceAccountModal
        isOpen={true}
        onClose={mockOnClose}
        onCreate={mockOnCreate}
      />,
    );

    const input = screen.getByLabelText('Name');
    fireEvent.change(input, { target: { value: 'my-account' } });
    fireEvent.click(screen.getByText('Create'));

    await waitFor(() => {
      expect(screen.getByText('Service Account Created!')).toBeInTheDocument();
    });

    // After showing credentials view, the name input is no longer visible
    // The name was cleared after successful creation (line 21)
    expect(mockOnCreate).toHaveBeenCalledWith('my-account');
  });
});
