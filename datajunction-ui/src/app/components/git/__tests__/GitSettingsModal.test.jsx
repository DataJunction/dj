import * as React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import GitSettingsModal from '../GitSettingsModal';

describe('<GitSettingsModal />', () => {
  const mockOnClose = jest.fn();
  const mockOnSave = jest.fn();
  const mockOnRemove = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    // Mock window.confirm
    global.confirm = jest.fn(() => true);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('should render with existing config', () => {
    const currentConfig = {
      github_repo_path: 'test/repo',
      git_branch: 'main',
      git_path: 'nodes/',
      git_only: false,
    };

    render(
      <GitSettingsModal
        isOpen={true}
        onClose={mockOnClose}
        onSave={mockOnSave}
        onRemove={mockOnRemove}
        currentConfig={currentConfig}
        namespace="test.namespace"
      />,
    );

    expect(screen.getByDisplayValue('test/repo')).toBeInTheDocument();
    expect(screen.getByDisplayValue('main')).toBeInTheDocument();
    expect(screen.getByDisplayValue('nodes/')).toBeInTheDocument();
  });

  it('should call onRemove when Reset button is clicked and confirmed', async () => {
    const currentConfig = {
      github_repo_path: 'test/repo',
      git_branch: 'main',
      git_path: 'nodes/',
      git_only: false,
    };

    mockOnRemove.mockResolvedValue({ success: true });

    render(
      <GitSettingsModal
        isOpen={true}
        onClose={mockOnClose}
        onSave={mockOnSave}
        onRemove={mockOnRemove}
        currentConfig={currentConfig}
        namespace="test.namespace"
      />,
    );

    const removeButton = screen.getByText('Reset');
    fireEvent.click(removeButton);

    // Verify confirmation dialog was shown
    expect(global.confirm).toHaveBeenCalledWith(
      expect.stringContaining('Remove git configuration?'),
    );

    await waitFor(() => {
      expect(mockOnRemove).toHaveBeenCalledWith({
        github_repo_path: null,
        git_branch: null,
        git_path: null,
        git_only: false,
      });
    });
  });

  it('should not call onRemove when user cancels confirmation', async () => {
    global.confirm = jest.fn(() => false);

    const currentConfig = {
      github_repo_path: 'test/repo',
      git_branch: 'main',
      git_path: 'nodes/',
      git_only: false,
    };

    render(
      <GitSettingsModal
        isOpen={true}
        onClose={mockOnClose}
        onSave={mockOnSave}
        onRemove={mockOnRemove}
        currentConfig={currentConfig}
        namespace="test.namespace"
      />,
    );

    const removeButton = screen.getByText('Reset');
    fireEvent.click(removeButton);

    expect(global.confirm).toHaveBeenCalled();
    expect(mockOnRemove).not.toHaveBeenCalled();
  });

  it('should show error message when remove fails', async () => {
    const currentConfig = {
      github_repo_path: 'test/repo',
      git_branch: 'main',
      git_path: 'nodes/',
      git_only: false,
    };

    mockOnRemove.mockResolvedValue({
      _error: true,
      message: 'Failed to remove git configuration',
    });

    render(
      <GitSettingsModal
        isOpen={true}
        onClose={mockOnClose}
        onSave={mockOnSave}
        onRemove={mockOnRemove}
        currentConfig={currentConfig}
        namespace="test.namespace"
      />,
    );

    const removeButton = screen.getByText('Reset');
    fireEvent.click(removeButton);

    await waitFor(() => {
      expect(
        screen.getByText(/Failed to remove git configuration/),
      ).toBeInTheDocument();
    });

    // Modal should stay open on error
    expect(mockOnClose).not.toHaveBeenCalled();
  });

  it('should show success message and close modal after successful removal', async () => {
    jest.useFakeTimers();

    const currentConfig = {
      github_repo_path: 'test/repo',
      git_branch: 'main',
      git_path: 'nodes/',
      git_only: false,
    };

    mockOnRemove.mockResolvedValue({ success: true });

    render(
      <GitSettingsModal
        isOpen={true}
        onClose={mockOnClose}
        onSave={mockOnSave}
        onRemove={mockOnRemove}
        currentConfig={currentConfig}
        namespace="test.namespace"
      />,
    );

    const removeButton = screen.getByText('Reset');
    fireEvent.click(removeButton);

    await waitFor(() => {
      expect(mockOnRemove).toHaveBeenCalled();
    });

    // Success message should appear
    await waitFor(() => {
      expect(
        screen.getByText(/Git configuration removed successfully/),
      ).toBeInTheDocument();
    });

    // Fast-forward time to trigger modal close
    jest.advanceTimersByTime(1500);

    await waitFor(() => {
      expect(mockOnClose).toHaveBeenCalled();
    });

    jest.useRealTimers();
  });

  it('should handle exception during remove', async () => {
    const currentConfig = {
      github_repo_path: 'test/repo',
      git_branch: 'main',
      git_path: 'nodes/',
      git_only: false,
    };

    mockOnRemove.mockRejectedValue(new Error('Network error'));

    render(
      <GitSettingsModal
        isOpen={true}
        onClose={mockOnClose}
        onSave={mockOnSave}
        onRemove={mockOnRemove}
        currentConfig={currentConfig}
        namespace="test.namespace"
      />,
    );

    const removeButton = screen.getByText('Reset');
    fireEvent.click(removeButton);

    await waitFor(() => {
      expect(screen.getByText(/Network error/)).toBeInTheDocument();
    });
  });

  it('should show removing state while operation is in progress', async () => {
    const currentConfig = {
      github_repo_path: 'test/repo',
      git_branch: 'main',
      git_path: 'nodes/',
      git_only: false,
    };

    // Create a promise that we can control
    let resolveRemove;
    const removePromise = new Promise(resolve => {
      resolveRemove = resolve;
    });
    mockOnRemove.mockReturnValue(removePromise);

    render(
      <GitSettingsModal
        isOpen={true}
        onClose={mockOnClose}
        onSave={mockOnSave}
        onRemove={mockOnRemove}
        currentConfig={currentConfig}
        namespace="test.namespace"
      />,
    );

    const removeButton = screen.getByText('Reset');
    fireEvent.click(removeButton);

    // Button text should change to "Removing..." and be disabled
    await waitFor(() => {
      expect(screen.getByText('Removing...')).toBeInTheDocument();
      expect(screen.getByText('Removing...')).toBeDisabled();
    });

    // Resolve the promise
    resolveRemove({ success: true });

    // Button should disappear after success (replaced by success message)
    await waitFor(() => {
      expect(screen.queryByText('Reset')).not.toBeInTheDocument();
      expect(screen.queryByText('Removing...')).not.toBeInTheDocument();
    });
  });

  it('should call onSave when Save Settings is clicked', async () => {
    mockOnSave.mockResolvedValue({ success: true });

    render(
      <GitSettingsModal
        isOpen={true}
        onClose={mockOnClose}
        onSave={mockOnSave}
        onRemove={mockOnRemove}
        currentConfig={null}
        namespace="test.namespace"
      />,
    );

    fireEvent.change(screen.getByLabelText('Repository'), {
      target: { value: 'myorg/repo' },
    });
    fireEvent.change(screen.getByLabelText('Branch'), {
      target: { value: 'main' },
    });

    fireEvent.click(screen.getByText('Save Settings'));

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledWith(
        expect.objectContaining({
          github_repo_path: 'myorg/repo',
          git_branch: 'main',
        }),
      );
    });
  });
});
