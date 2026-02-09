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
    // Mock fetch for parent config fetching
    global.fetch = jest.fn(() =>
      Promise.resolve({
        ok: true,
        json: () =>
          Promise.resolve({
            github_repo_path: 'test/repo',
            git_path: 'nodes/',
          }),
      }),
    );
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('should render with existing git root config', () => {
    const currentConfig = {
      github_repo_path: 'test/repo',
      git_path: 'nodes/',
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
    expect(screen.getByDisplayValue('nodes/')).toBeInTheDocument();
    // Branch field should not be present in git root mode
    expect(screen.queryByDisplayValue('main')).not.toBeInTheDocument();
  });

  it('should render with existing branch namespace config', () => {
    const currentConfig = {
      parent_namespace: 'test',
      git_branch: 'main',
      git_only: false,
    };

    render(
      <GitSettingsModal
        isOpen={true}
        onClose={mockOnClose}
        onSave={mockOnSave}
        onRemove={mockOnRemove}
        currentConfig={currentConfig}
        namespace="test.feature"
      />,
    );

    expect(screen.getByDisplayValue('main')).toBeInTheDocument();
    expect(screen.getByText('test')).toBeInTheDocument(); // Parent shown as read-only
    // git_only is false, so checkbox should not be checked
    const checkbox = screen.getByRole('checkbox');
    expect(checkbox).not.toBeChecked();
  });

  it('should call onRemove when Reset button is clicked and confirmed', async () => {
    const currentConfig = {
      github_repo_path: 'test/repo',
      git_path: 'nodes/',
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
      git_path: 'nodes/',
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
      git_path: 'nodes/',
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
      git_path: 'nodes/',
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
      git_path: 'nodes/',
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
      git_path: 'nodes/',
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

  it('should call onSave when Save Settings is clicked in git root mode', async () => {
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

    // Default mode is git root
    fireEvent.change(screen.getByLabelText(/Repository/), {
      target: { value: 'myorg/repo' },
    });

    fireEvent.click(screen.getByText('Save Settings'));

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledWith({
        github_repo_path: 'myorg/repo',
        git_path: 'nodes/', // Default value
      });
    });
  });

  it('should call onSave when Save Settings is clicked in branch mode', async () => {
    mockOnSave.mockResolvedValue({ success: true });

    render(
      <GitSettingsModal
        isOpen={true}
        onClose={mockOnClose}
        onSave={mockOnSave}
        onRemove={mockOnRemove}
        currentConfig={null}
        namespace="test.feature"
      />,
    );

    // Switch to branch mode
    fireEvent.click(screen.getByText('Branch Namespace'));

    // Fill in branch name
    fireEvent.change(screen.getByLabelText(/Branch/), {
      target: { value: 'feature-x' },
    });

    fireEvent.click(screen.getByText('Save Settings'));

    await waitFor(() => {
      expect(mockOnSave).toHaveBeenCalledWith({
        git_branch: 'feature-x',
        parent_namespace: 'test',
        git_only: true, // Default for branch mode
      });
    });
  });

  it('should show error when branch name is empty in branch mode', async () => {
    render(
      <GitSettingsModal
        isOpen={true}
        onClose={mockOnClose}
        onSave={mockOnSave}
        onRemove={mockOnRemove}
        currentConfig={null}
        namespace="test.feature"
      />,
    );

    // Switch to branch mode
    fireEvent.click(screen.getByText('Branch Namespace'));

    // Try to save without entering a branch name
    fireEvent.click(screen.getByText('Save Settings'));

    await waitFor(() => {
      expect(
        screen.getByText('Git branch is required for branch mode'),
      ).toBeInTheDocument();
    });

    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('should show error when namespace has no parent in branch mode', async () => {
    render(
      <GitSettingsModal
        isOpen={true}
        onClose={mockOnClose}
        onSave={mockOnSave}
        onRemove={mockOnRemove}
        currentConfig={null}
        namespace="toplevel"
      />,
    );

    // Switch to branch mode (should fail because namespace has no parent)
    fireEvent.click(screen.getByText('Branch Namespace'));

    // Try to save
    fireEvent.click(screen.getByText('Save Settings'));

    await waitFor(() => {
      expect(
        screen.getByText(
          /Cannot configure as branch namespace: namespace has no parent/,
        ),
      ).toBeInTheDocument();
    });

    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('should show error when repository is empty in git root mode', async () => {
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

    // Default is git root mode
    // Clear the repository field if it has any default value
    const repoInput = screen.getByLabelText(/Repository/);
    fireEvent.change(repoInput, { target: { value: '' } });

    // Try to save without entering a repository
    fireEvent.click(screen.getByText('Save Settings'));

    await waitFor(() => {
      expect(screen.getByText('Repository is required')).toBeInTheDocument();
    });

    expect(mockOnSave).not.toHaveBeenCalled();
  });

  it('should handle parent config fetch failure gracefully', async () => {
    // Mock fetch to fail
    global.fetch = jest.fn(() => Promise.reject(new Error('Network error')));

    render(
      <GitSettingsModal
        isOpen={true}
        onClose={mockOnClose}
        onSave={mockOnSave}
        onRemove={mockOnRemove}
        currentConfig={{
          parent_namespace: 'test',
          git_branch: 'main',
        }}
        namespace="test.feature"
      />,
    );

    // Wait for the component to attempt fetching parent config
    await waitFor(() => {
      // Should still render even if parent config fetch fails
      expect(screen.getByDisplayValue('main')).toBeInTheDocument();
    });

    // Parent config should not be shown (fetch failed)
    expect(screen.queryByText('test/repo')).not.toBeInTheDocument();
  });

  it('should allow switching from branch mode to root mode', async () => {
    render(
      <GitSettingsModal
        isOpen={true}
        onClose={mockOnClose}
        onSave={mockOnSave}
        onRemove={mockOnRemove}
        currentConfig={null}
        namespace="test.feature"
      />,
    );

    // Start in branch mode
    fireEvent.click(screen.getByText('Branch Namespace'));
    expect(screen.getByLabelText(/Branch/)).toBeInTheDocument();

    // Switch to root mode
    fireEvent.click(screen.getByText('Git Root'));

    // Should show repository field instead of branch field
    expect(screen.getByLabelText(/Repository/)).toBeInTheDocument();
    expect(screen.queryByLabelText(/Branch/)).not.toBeInTheDocument();
  });
});
