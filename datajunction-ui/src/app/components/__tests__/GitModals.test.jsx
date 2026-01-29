import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';

import { CreateBranchModal } from '../git/CreateBranchModal';
import { DeleteBranchModal } from '../git/DeleteBranchModal';
import { SyncToGitModal } from '../git/SyncToGitModal';
import { GitSettingsModal } from '../git/GitSettingsModal';
import { CreatePRModal } from '../git/CreatePRModal';

describe('<CreateBranchModal />', () => {
  const defaultProps = {
    isOpen: true,
    onClose: jest.fn(),
    onCreate: jest.fn(),
    namespace: 'analytics.prod',
    gitBranch: 'main',
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should not render when isOpen is false', () => {
    render(<CreateBranchModal {...defaultProps} isOpen={false} />);
    expect(screen.queryByText('Create Branch')).not.toBeInTheDocument();
  });

  it('should render the modal when open', () => {
    render(<CreateBranchModal {...defaultProps} />);
    expect(screen.getByText('Create Branch')).toBeInTheDocument();
    expect(screen.getByLabelText('Branch Name')).toBeInTheDocument();
  });

  it('should show preview when branch name is entered', async () => {
    render(<CreateBranchModal {...defaultProps} />);

    const input = screen.getByLabelText('Branch Name');
    await userEvent.type(input, 'feature-xyz');

    expect(
      screen.getByText(/Create git branch "feature-xyz"/),
    ).toBeInTheDocument();
    expect(screen.getByText(/analytics\.feature_xyz/)).toBeInTheDocument();
  });

  it('should handle branch name with slashes', async () => {
    render(<CreateBranchModal {...defaultProps} />);

    const input = screen.getByLabelText('Branch Name');
    await userEvent.type(input, 'feature/new-thing');

    // Slashes should be converted to underscores in namespace preview
    expect(
      screen.getByText(/analytics\.feature_new_thing/),
    ).toBeInTheDocument();
  });

  it('should disable submit button when branch name is empty', () => {
    render(<CreateBranchModal {...defaultProps} />);

    const submitButton = screen.getByRole('button', { name: 'Create Branch' });
    expect(submitButton).toBeDisabled();
  });

  it('should disable submit button when branch name is whitespace only', async () => {
    render(<CreateBranchModal {...defaultProps} />);

    const input = screen.getByLabelText('Branch Name');
    await userEvent.type(input, '   ');

    const submitButton = screen.getByRole('button', { name: 'Create Branch' });
    expect(submitButton).toBeDisabled();
  });

  it('should call onCreate when form is submitted', async () => {
    defaultProps.onCreate.mockResolvedValue({
      branch: {
        namespace: 'analytics.feature_xyz',
        git_branch: 'feature-xyz',
        parent_namespace: 'analytics.prod',
      },
      deployment_results: [],
    });

    render(<CreateBranchModal {...defaultProps} />);

    const input = screen.getByLabelText('Branch Name');
    await userEvent.type(input, 'feature-xyz');

    const submitButton = screen.getByRole('button', { name: 'Create Branch' });
    await userEvent.click(submitButton);

    await waitFor(() => {
      expect(defaultProps.onCreate).toHaveBeenCalledWith('feature-xyz');
    });
  });

  it('should show success view after branch creation', async () => {
    defaultProps.onCreate.mockResolvedValue({
      branch: {
        namespace: 'analytics.feature_xyz',
        git_branch: 'feature-xyz',
        parent_namespace: 'analytics.prod',
      },
      deployment_results: [{ node: 'test' }],
    });

    render(
      <MemoryRouter>
        <CreateBranchModal {...defaultProps} />
      </MemoryRouter>,
    );

    const input = screen.getByLabelText('Branch Name');
    await userEvent.type(input, 'feature-xyz');
    await userEvent.click(
      screen.getByRole('button', { name: 'Create Branch' }),
    );

    await waitFor(() => {
      expect(screen.getByText('Branch Created!')).toBeInTheDocument();
    });
    expect(screen.getByText('analytics.feature_xyz')).toBeInTheDocument();
    expect(screen.getByText('Nodes copied:')).toBeInTheDocument();
  });

  it('should show success view without nodes copied section when no deployment results', async () => {
    defaultProps.onCreate.mockResolvedValue({
      branch: {
        namespace: 'analytics.feature_xyz',
        git_branch: 'feature-xyz',
        parent_namespace: 'analytics.prod',
      },
      deployment_results: [],
    });

    render(
      <MemoryRouter>
        <CreateBranchModal {...defaultProps} />
      </MemoryRouter>,
    );

    const input = screen.getByLabelText('Branch Name');
    await userEvent.type(input, 'feature-xyz');
    await userEvent.click(
      screen.getByRole('button', { name: 'Create Branch' }),
    );

    await waitFor(() => {
      expect(screen.getByText('Branch Created!')).toBeInTheDocument();
    });
    expect(screen.queryByText('Nodes copied:')).not.toBeInTheDocument();
  });

  it('should show error when creation fails', async () => {
    defaultProps.onCreate.mockResolvedValue({
      _error: true,
      message: 'Branch already exists',
    });

    render(<CreateBranchModal {...defaultProps} />);

    const input = screen.getByLabelText('Branch Name');
    await userEvent.type(input, 'feature-xyz');
    await userEvent.click(
      screen.getByRole('button', { name: 'Create Branch' }),
    );

    await waitFor(() => {
      expect(screen.getByText('Branch already exists')).toBeInTheDocument();
    });
  });

  it('should show error when onCreate throws exception', async () => {
    defaultProps.onCreate.mockRejectedValue(new Error('Network error'));

    render(<CreateBranchModal {...defaultProps} />);

    const input = screen.getByLabelText('Branch Name');
    await userEvent.type(input, 'feature-xyz');
    await userEvent.click(
      screen.getByRole('button', { name: 'Create Branch' }),
    );

    await waitFor(() => {
      expect(screen.getByText('Network error')).toBeInTheDocument();
    });
  });

  it('should show default error when onCreate throws without message', async () => {
    defaultProps.onCreate.mockRejectedValue({});

    render(<CreateBranchModal {...defaultProps} />);

    const input = screen.getByLabelText('Branch Name');
    await userEvent.type(input, 'feature-xyz');
    await userEvent.click(
      screen.getByRole('button', { name: 'Create Branch' }),
    );

    await waitFor(() => {
      expect(screen.getByText('Failed to create branch')).toBeInTheDocument();
    });
  });

  it('should call onClose when Cancel is clicked', async () => {
    render(<CreateBranchModal {...defaultProps} />);

    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should call onClose when clicking overlay', async () => {
    render(<CreateBranchModal {...defaultProps} />);

    const overlay = document.querySelector('.modal-overlay');
    fireEvent.click(overlay);
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should call onClose when clicking close button', async () => {
    render(<CreateBranchModal {...defaultProps} />);

    await userEvent.click(screen.getByTitle('Close'));
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should not propagate click from modal content to overlay', async () => {
    render(<CreateBranchModal {...defaultProps} />);

    const modalContent = document.querySelector('.modal-content');
    fireEvent.click(modalContent);
    expect(defaultProps.onClose).not.toHaveBeenCalled();
  });

  it('should reset state when closed from success view', async () => {
    defaultProps.onCreate.mockResolvedValue({
      branch: {
        namespace: 'analytics.feature_xyz',
        git_branch: 'feature-xyz',
        parent_namespace: 'analytics.prod',
      },
      deployment_results: [],
    });

    render(
      <MemoryRouter>
        <CreateBranchModal {...defaultProps} />
      </MemoryRouter>,
    );

    const input = screen.getByLabelText('Branch Name');
    await userEvent.type(input, 'feature-xyz');
    await userEvent.click(
      screen.getByRole('button', { name: 'Create Branch' }),
    );

    await waitFor(() => {
      expect(screen.getByText('Branch Created!')).toBeInTheDocument();
    });

    // Click Close button in success view
    await userEvent.click(screen.getByRole('button', { name: 'Close' }));
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should show Creating... button text while creating', async () => {
    let resolveCreate;
    defaultProps.onCreate.mockImplementation(
      () =>
        new Promise(resolve => {
          resolveCreate = resolve;
        }),
    );

    render(<CreateBranchModal {...defaultProps} />);

    const input = screen.getByLabelText('Branch Name');
    await userEvent.type(input, 'feature-xyz');
    await userEvent.click(
      screen.getByRole('button', { name: 'Create Branch' }),
    );

    expect(screen.getByText('Creating...')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Creating...' })).toBeDisabled();

    // Resolve to complete the test
    resolveCreate({
      branch: { namespace: 'test', git_branch: 'test', parent_namespace: 'p' },
      deployment_results: [],
    });
  });

  it('should handle single-part namespace', async () => {
    render(<CreateBranchModal {...defaultProps} namespace="analytics" />);

    const input = screen.getByLabelText('Branch Name');
    await userEvent.type(input, 'feature');

    // For single-part namespace, result should be "analytics.feature"
    expect(screen.getByText(/analytics\.feature/)).toBeInTheDocument();
  });
});

describe('<DeleteBranchModal />', () => {
  const defaultProps = {
    isOpen: true,
    onClose: jest.fn(),
    onDelete: jest.fn(),
    namespace: 'analytics.feature_xyz',
    gitBranch: 'feature-xyz',
    parentNamespace: 'analytics.prod',
  };

  beforeEach(() => {
    jest.clearAllMocks();
    delete window.location;
    window.location = { href: '' };
  });

  it('should not render when isOpen is false', () => {
    render(<DeleteBranchModal {...defaultProps} isOpen={false} />);
    expect(screen.queryByText('Delete Branch')).not.toBeInTheDocument();
  });

  it('should render the modal with branch info', () => {
    render(<DeleteBranchModal {...defaultProps} />);

    expect(screen.getByText('Delete Branch')).toBeInTheDocument();
    expect(screen.getByText('analytics.feature_xyz')).toBeInTheDocument();
    expect(screen.getByText('feature-xyz')).toBeInTheDocument();
    expect(screen.getByText('analytics.prod')).toBeInTheDocument();
  });

  it('should have checkbox for deleting git branch checked by default', () => {
    render(<DeleteBranchModal {...defaultProps} />);

    const checkbox = screen.getByRole('checkbox');
    expect(checkbox).toBeChecked();
  });

  it('should toggle checkbox', async () => {
    render(<DeleteBranchModal {...defaultProps} />);

    const checkbox = screen.getByRole('checkbox');
    await userEvent.click(checkbox);
    expect(checkbox).not.toBeChecked();
  });

  it('should call onDelete with checkbox value when submitted', async () => {
    defaultProps.onDelete.mockResolvedValue({ success: true });

    render(<DeleteBranchModal {...defaultProps} />);

    await userEvent.click(
      screen.getByRole('button', { name: 'Delete Branch' }),
    );

    await waitFor(() => {
      expect(defaultProps.onDelete).toHaveBeenCalledWith(true);
    });
  });

  it('should call onDelete with false when checkbox is unchecked', async () => {
    defaultProps.onDelete.mockResolvedValue({ success: true });

    render(<DeleteBranchModal {...defaultProps} />);

    // Uncheck the checkbox
    await userEvent.click(screen.getByRole('checkbox'));

    await userEvent.click(
      screen.getByRole('button', { name: 'Delete Branch' }),
    );

    await waitFor(() => {
      expect(defaultProps.onDelete).toHaveBeenCalledWith(false);
    });
  });

  it('should redirect after successful deletion', async () => {
    defaultProps.onDelete.mockResolvedValue({ success: true });

    render(<DeleteBranchModal {...defaultProps} />);

    await userEvent.click(
      screen.getByRole('button', { name: 'Delete Branch' }),
    );

    await waitFor(() => {
      expect(window.location.href).toBe('/namespaces/analytics.prod');
    });
  });

  it('should show error when deletion fails', async () => {
    defaultProps.onDelete.mockResolvedValue({
      _error: true,
      message: 'Cannot delete branch with open PR',
    });

    render(<DeleteBranchModal {...defaultProps} />);

    await userEvent.click(
      screen.getByRole('button', { name: 'Delete Branch' }),
    );

    await waitFor(() => {
      expect(
        screen.getByText('Cannot delete branch with open PR'),
      ).toBeInTheDocument();
    });
  });

  it('should show error when onDelete throws exception', async () => {
    defaultProps.onDelete.mockRejectedValue(new Error('Network failure'));

    render(<DeleteBranchModal {...defaultProps} />);

    await userEvent.click(
      screen.getByRole('button', { name: 'Delete Branch' }),
    );

    await waitFor(() => {
      expect(screen.getByText('Network failure')).toBeInTheDocument();
    });
  });

  it('should show default error when onDelete throws without message', async () => {
    defaultProps.onDelete.mockRejectedValue({});

    render(<DeleteBranchModal {...defaultProps} />);

    await userEvent.click(
      screen.getByRole('button', { name: 'Delete Branch' }),
    );

    await waitFor(() => {
      expect(screen.getByText('Failed to delete branch')).toBeInTheDocument();
    });
  });

  it('should call onClose when Cancel is clicked', async () => {
    render(<DeleteBranchModal {...defaultProps} />);

    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should call onClose when clicking overlay', async () => {
    render(<DeleteBranchModal {...defaultProps} />);

    const overlay = document.querySelector('.modal-overlay');
    fireEvent.click(overlay);
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should call onClose when clicking close button', async () => {
    render(<DeleteBranchModal {...defaultProps} />);

    await userEvent.click(screen.getByTitle('Close'));
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should show Deleting... button text while deleting', async () => {
    let resolveDelete;
    defaultProps.onDelete.mockImplementation(
      () =>
        new Promise(resolve => {
          resolveDelete = resolve;
        }),
    );

    render(<DeleteBranchModal {...defaultProps} />);

    await userEvent.click(
      screen.getByRole('button', { name: 'Delete Branch' }),
    );

    expect(screen.getByText('Deleting...')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Deleting...' })).toBeDisabled();

    // Resolve to complete the test
    resolveDelete({ success: true });
  });
});

describe('<SyncToGitModal />', () => {
  const defaultProps = {
    isOpen: true,
    onClose: jest.fn(),
    onSync: jest.fn(),
    namespace: 'analytics.prod',
    gitBranch: 'main',
    repoPath: 'myorg/dj-definitions',
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should not render when isOpen is false', () => {
    render(<SyncToGitModal {...defaultProps} isOpen={false} />);
    expect(screen.queryByText('Sync to Git')).not.toBeInTheDocument();
  });

  it('should render the modal with namespace info', () => {
    render(<SyncToGitModal {...defaultProps} />);

    expect(screen.getByText('Sync to Git')).toBeInTheDocument();
    expect(screen.getByText('myorg/dj-definitions')).toBeInTheDocument();
    expect(screen.getByText('main')).toBeInTheDocument();
    expect(screen.getByText(/Sync all nodes in/)).toBeInTheDocument();
    expect(screen.getByText('analytics.prod')).toBeInTheDocument();
  });

  it('should allow entering commit message', async () => {
    render(<SyncToGitModal {...defaultProps} />);

    const input = screen.getByLabelText(/Commit Message/);
    await userEvent.type(input, 'Add new metrics');

    expect(input).toHaveValue('Add new metrics');
  });

  it('should call onSync when form is submitted', async () => {
    defaultProps.onSync.mockResolvedValue({
      files_synced: 5,
      commit_sha: 'abc123def',
      commit_url: 'https://github.com/myorg/repo/commit/abc123',
    });

    render(<SyncToGitModal {...defaultProps} />);

    const input = screen.getByLabelText(/Commit Message/);
    await userEvent.type(input, 'Update metrics');
    await userEvent.click(screen.getByRole('button', { name: 'Sync Now' }));

    await waitFor(() => {
      expect(defaultProps.onSync).toHaveBeenCalledWith('Update metrics');
    });
  });

  it('should call onSync with null for empty commit message', async () => {
    defaultProps.onSync.mockResolvedValue({
      files_synced: 3,
      commit_sha: 'abc123',
      commit_url: 'https://github.com/myorg/repo/commit/abc123',
    });

    render(<SyncToGitModal {...defaultProps} />);
    await userEvent.click(screen.getByRole('button', { name: 'Sync Now' }));

    await waitFor(() => {
      expect(defaultProps.onSync).toHaveBeenCalledWith(null);
    });
  });

  it('should call onSync with null for whitespace-only commit message', async () => {
    defaultProps.onSync.mockResolvedValue({
      files_synced: 3,
      commit_sha: 'abc123',
      commit_url: 'https://github.com/myorg/repo/commit/abc123',
    });

    render(<SyncToGitModal {...defaultProps} />);

    const input = screen.getByLabelText(/Commit Message/);
    await userEvent.type(input, '   ');
    await userEvent.click(screen.getByRole('button', { name: 'Sync Now' }));

    await waitFor(() => {
      expect(defaultProps.onSync).toHaveBeenCalledWith(null);
    });
  });

  it('should show success view after sync', async () => {
    defaultProps.onSync.mockResolvedValue({
      files_synced: 5,
      commit_sha: 'abc123def456',
      commit_url: 'https://github.com/myorg/repo/commit/abc123',
    });

    render(<SyncToGitModal {...defaultProps} />);
    await userEvent.click(screen.getByRole('button', { name: 'Sync Now' }));

    await waitFor(() => {
      expect(screen.getByText('Synced 5 files!')).toBeInTheDocument();
    });
    expect(screen.getByText('abc123d')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'View Commit' })).toHaveAttribute(
      'href',
      'https://github.com/myorg/repo/commit/abc123',
    );
  });

  it('should show singular "file" for 1 file synced', async () => {
    defaultProps.onSync.mockResolvedValue({
      files_synced: 1,
      commit_sha: 'abc123',
      commit_url: 'https://github.com/myorg/repo/commit/abc123',
    });

    render(<SyncToGitModal {...defaultProps} />);
    await userEvent.click(screen.getByRole('button', { name: 'Sync Now' }));

    await waitFor(() => {
      expect(screen.getByText('Synced 1 file!')).toBeInTheDocument();
    });
  });

  it('should show error when sync fails', async () => {
    defaultProps.onSync.mockResolvedValue({
      _error: true,
      message: 'No nodes to sync',
    });

    render(<SyncToGitModal {...defaultProps} />);
    await userEvent.click(screen.getByRole('button', { name: 'Sync Now' }));

    await waitFor(() => {
      expect(screen.getByText('No nodes to sync')).toBeInTheDocument();
    });
  });

  it('should show error when onSync throws exception', async () => {
    defaultProps.onSync.mockRejectedValue(new Error('GitHub API error'));

    render(<SyncToGitModal {...defaultProps} />);
    await userEvent.click(screen.getByRole('button', { name: 'Sync Now' }));

    await waitFor(() => {
      expect(screen.getByText('GitHub API error')).toBeInTheDocument();
    });
  });

  it('should show default error when onSync throws without message', async () => {
    defaultProps.onSync.mockRejectedValue({});

    render(<SyncToGitModal {...defaultProps} />);
    await userEvent.click(screen.getByRole('button', { name: 'Sync Now' }));

    await waitFor(() => {
      expect(screen.getByText('Failed to sync to git')).toBeInTheDocument();
    });
  });

  it('should call onClose when Cancel is clicked', async () => {
    render(<SyncToGitModal {...defaultProps} />);

    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should call onClose when clicking overlay', async () => {
    render(<SyncToGitModal {...defaultProps} />);

    const overlay = document.querySelector('.modal-overlay');
    fireEvent.click(overlay);
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should call onClose when clicking close button', async () => {
    render(<SyncToGitModal {...defaultProps} />);

    await userEvent.click(screen.getByTitle('Close'));
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should reset state when closed from success view', async () => {
    defaultProps.onSync.mockResolvedValue({
      files_synced: 5,
      commit_sha: 'abc123',
      commit_url: 'https://github.com/myorg/repo/commit/abc123',
    });

    render(<SyncToGitModal {...defaultProps} />);
    await userEvent.click(screen.getByRole('button', { name: 'Sync Now' }));

    await waitFor(() => {
      expect(screen.getByText('Synced 5 files!')).toBeInTheDocument();
    });

    // Click Close button in success view
    await userEvent.click(screen.getByRole('button', { name: 'Close' }));
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should show Syncing... button text while syncing', async () => {
    let resolveSync;
    defaultProps.onSync.mockImplementation(
      () =>
        new Promise(resolve => {
          resolveSync = resolve;
        }),
    );

    render(<SyncToGitModal {...defaultProps} />);
    await userEvent.click(screen.getByRole('button', { name: 'Sync Now' }));

    expect(screen.getByText('Syncing...')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Syncing...' })).toBeDisabled();

    // Resolve to complete the test
    resolveSync({
      files_synced: 1,
      commit_sha: 'abc',
      commit_url: 'https://github.com/test',
    });
  });

  it('should display branch info in success view', async () => {
    defaultProps.onSync.mockResolvedValue({
      files_synced: 5,
      commit_sha: 'abc123',
      commit_url: 'https://github.com/myorg/repo/commit/abc123',
    });

    render(<SyncToGitModal {...defaultProps} />);
    await userEvent.click(screen.getByRole('button', { name: 'Sync Now' }));

    await waitFor(() => {
      expect(screen.getByText('Synced 5 files!')).toBeInTheDocument();
    });

    // Branch info should be shown
    expect(screen.getByText('Branch:')).toBeInTheDocument();
    expect(screen.getByText('main')).toBeInTheDocument();
  });
});

describe('<GitSettingsModal />', () => {
  const defaultProps = {
    isOpen: true,
    onClose: jest.fn(),
    onSave: jest.fn(),
    currentConfig: null,
    namespace: 'analytics.prod',
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should not render when isOpen is false', () => {
    render(<GitSettingsModal {...defaultProps} isOpen={false} />);
    expect(screen.queryByText('Git Configuration')).not.toBeInTheDocument();
  });

  it('should render the modal with form fields', () => {
    render(<GitSettingsModal {...defaultProps} />);

    expect(screen.getByText('Git Configuration')).toBeInTheDocument();
    expect(screen.getByLabelText('Repository')).toBeInTheDocument();
    expect(screen.getByLabelText('Branch')).toBeInTheDocument();
    expect(screen.getByLabelText('Path')).toBeInTheDocument();
  });

  it('should pre-fill form with current config', () => {
    const config = {
      github_repo_path: 'myorg/repo',
      git_branch: 'main',
      git_path: 'definitions/',
      git_only: true,
    };

    render(<GitSettingsModal {...defaultProps} currentConfig={config} />);

    expect(screen.getByLabelText('Repository')).toHaveValue('myorg/repo');
    expect(screen.getByLabelText('Branch')).toHaveValue('main');
    expect(screen.getByLabelText('Path')).toHaveValue('definitions/');
  });

  it('should pre-fill git_only checkbox from config', () => {
    const config = {
      github_repo_path: 'myorg/repo',
      git_branch: 'main',
      git_path: 'definitions/',
      git_only: false,
    };

    render(<GitSettingsModal {...defaultProps} currentConfig={config} />);

    const checkbox = screen.getByRole('checkbox');
    expect(checkbox).not.toBeChecked();
  });

  it('should default path to nodes/ for new config', () => {
    render(<GitSettingsModal {...defaultProps} />);
    expect(screen.getByLabelText('Path')).toHaveValue('nodes/');
  });

  it('should default git_only to true for new config', () => {
    render(<GitSettingsModal {...defaultProps} />);
    const checkbox = screen.getByRole('checkbox');
    expect(checkbox).toBeChecked();
  });

  it('should call onSave when form is submitted', async () => {
    defaultProps.onSave.mockResolvedValue({ success: true });

    render(<GitSettingsModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText('Repository'), 'myorg/repo');
    await userEvent.type(screen.getByLabelText('Branch'), 'main');
    await userEvent.click(
      screen.getByRole('button', { name: 'Save Settings' }),
    );

    await waitFor(() => {
      expect(defaultProps.onSave).toHaveBeenCalledWith({
        github_repo_path: 'myorg/repo',
        git_branch: 'main',
        git_path: 'nodes/',
        git_only: true,
      });
    });
  });

  it('should call onSave with git_only false when unchecked', async () => {
    defaultProps.onSave.mockResolvedValue({ success: true });

    render(<GitSettingsModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText('Repository'), 'myorg/repo');
    await userEvent.type(screen.getByLabelText('Branch'), 'main');
    await userEvent.click(screen.getByRole('checkbox')); // Uncheck git_only
    await userEvent.click(
      screen.getByRole('button', { name: 'Save Settings' }),
    );

    await waitFor(() => {
      expect(defaultProps.onSave).toHaveBeenCalledWith({
        github_repo_path: 'myorg/repo',
        git_branch: 'main',
        git_path: 'nodes/',
        git_only: false,
      });
    });
  });

  it('should show success message after save', async () => {
    defaultProps.onSave.mockResolvedValue({ success: true });

    render(<GitSettingsModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText('Repository'), 'myorg/repo');
    await userEvent.click(
      screen.getByRole('button', { name: 'Save Settings' }),
    );

    await waitFor(() => {
      expect(
        screen.getByText('Git configuration saved successfully!'),
      ).toBeInTheDocument();
    });
  });

  it('should show error when save fails', async () => {
    defaultProps.onSave.mockResolvedValue({
      _error: true,
      message: 'Invalid repository',
    });

    render(<GitSettingsModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText('Repository'), 'invalid');
    await userEvent.click(
      screen.getByRole('button', { name: 'Save Settings' }),
    );

    await waitFor(() => {
      expect(screen.getByText('Invalid repository')).toBeInTheDocument();
    });
  });

  it('should show error when onSave throws exception', async () => {
    defaultProps.onSave.mockRejectedValue(new Error('Network error'));

    render(<GitSettingsModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText('Repository'), 'myorg/repo');
    await userEvent.click(
      screen.getByRole('button', { name: 'Save Settings' }),
    );

    await waitFor(() => {
      expect(screen.getByText('Network error')).toBeInTheDocument();
    });
  });

  it('should show default error when onSave throws without message', async () => {
    defaultProps.onSave.mockRejectedValue({});

    render(<GitSettingsModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText('Repository'), 'myorg/repo');
    await userEvent.click(
      screen.getByRole('button', { name: 'Save Settings' }),
    );

    await waitFor(() => {
      expect(
        screen.getByText('Failed to save configuration'),
      ).toBeInTheDocument();
    });
  });

  it('should toggle git-only checkbox', async () => {
    render(<GitSettingsModal {...defaultProps} />);

    const checkbox = screen.getByRole('checkbox');
    expect(checkbox).toBeChecked(); // Default is true

    await userEvent.click(checkbox);
    expect(checkbox).not.toBeChecked();
  });

  it('should call onClose when Cancel is clicked', async () => {
    render(<GitSettingsModal {...defaultProps} />);

    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should call onClose when clicking overlay', async () => {
    render(<GitSettingsModal {...defaultProps} />);

    const overlay = document.querySelector('.modal-overlay');
    fireEvent.click(overlay);
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should call onClose when clicking close button', async () => {
    render(<GitSettingsModal {...defaultProps} />);

    await userEvent.click(screen.getByTitle('Close'));
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should show Saving... button text while saving', async () => {
    let resolveSave;
    defaultProps.onSave.mockImplementation(
      () =>
        new Promise(resolve => {
          resolveSave = resolve;
        }),
    );

    render(<GitSettingsModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText('Repository'), 'myorg/repo');
    await userEvent.click(
      screen.getByRole('button', { name: 'Save Settings' }),
    );

    expect(screen.getByText('Saving...')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Saving...' })).toBeDisabled();

    // Resolve to complete the test
    resolveSave({ success: true });
  });

  it('should clear error and success when closed', async () => {
    defaultProps.onSave.mockResolvedValue({ success: true });

    render(<GitSettingsModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText('Repository'), 'myorg/repo');
    await userEvent.click(
      screen.getByRole('button', { name: 'Save Settings' }),
    );

    await waitFor(() => {
      expect(
        screen.getByText('Git configuration saved successfully!'),
      ).toBeInTheDocument();
    });

    // Close and verify onClose was called
    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should update path field', async () => {
    render(<GitSettingsModal {...defaultProps} />);

    const pathInput = screen.getByLabelText('Path');
    await userEvent.clear(pathInput);
    await userEvent.type(pathInput, 'definitions/');

    expect(pathInput).toHaveValue('definitions/');
  });
});

describe('<CreatePRModal />', () => {
  const defaultProps = {
    isOpen: true,
    onClose: jest.fn(),
    onCreate: jest.fn(),
    namespace: 'analytics.feature_xyz',
    gitBranch: 'feature-xyz',
    parentBranch: 'main',
    repoPath: 'myorg/dj-definitions',
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should not render when isOpen is false', () => {
    render(<CreatePRModal {...defaultProps} isOpen={false} />);
    expect(screen.queryByText('Create Pull Request')).not.toBeInTheDocument();
  });

  it('should render the modal with branch flow', () => {
    render(<CreatePRModal {...defaultProps} />);

    expect(screen.getByText('Create Pull Request')).toBeInTheDocument();
    expect(screen.getByText('feature-xyz')).toBeInTheDocument();
    expect(screen.getByText('main')).toBeInTheDocument();
  });

  it('should render repository info', () => {
    render(<CreatePRModal {...defaultProps} />);

    expect(screen.getByText('myorg/dj-definitions')).toBeInTheDocument();
  });

  it('should disable submit when title is empty', () => {
    render(<CreatePRModal {...defaultProps} />);

    const submitButton = screen.getByRole('button', { name: 'Create PR' });
    expect(submitButton).toBeDisabled();
  });

  it('should disable submit when title is whitespace only', async () => {
    render(<CreatePRModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText(/Title/), '   ');

    const submitButton = screen.getByRole('button', { name: 'Create PR' });
    expect(submitButton).toBeDisabled();
  });

  it('should enable submit when title is entered', async () => {
    render(<CreatePRModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText(/Title/), 'My PR title');

    const submitButton = screen.getByRole('button', { name: 'Create PR' });
    expect(submitButton).not.toBeDisabled();
  });

  it('should call onCreate with title and body', async () => {
    defaultProps.onCreate.mockResolvedValue({
      pr_number: 42,
      pr_url: 'https://github.com/myorg/repo/pull/42',
      head_branch: 'feature-xyz',
      base_branch: 'main',
    });

    render(<CreatePRModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText(/Title/), 'Add new metrics');
    await userEvent.type(
      screen.getByLabelText(/Description/),
      'This PR adds...',
    );
    await userEvent.click(screen.getByRole('button', { name: 'Create PR' }));

    await waitFor(() => {
      expect(defaultProps.onCreate).toHaveBeenCalledWith(
        'Add new metrics',
        'This PR adds...',
        expect.any(Function), // progress callback
      );
    });
  });

  it('should call onCreate with empty body if description is empty', async () => {
    defaultProps.onCreate.mockResolvedValue({
      pr_number: 42,
      pr_url: 'https://github.com/myorg/repo/pull/42',
      head_branch: 'feature-xyz',
      base_branch: 'main',
    });

    render(<CreatePRModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText(/Title/), 'Add new metrics');
    // Don't fill in description
    await userEvent.click(screen.getByRole('button', { name: 'Create PR' }));

    await waitFor(() => {
      expect(defaultProps.onCreate).toHaveBeenCalledWith(
        'Add new metrics',
        '',
        expect.any(Function),
      );
    });
  });

  it('should show success view after PR creation', async () => {
    defaultProps.onCreate.mockResolvedValue({
      pr_number: 42,
      pr_url: 'https://github.com/myorg/repo/pull/42',
      head_branch: 'feature-xyz',
      base_branch: 'main',
    });

    render(<CreatePRModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText(/Title/), 'Add new metrics');
    await userEvent.click(screen.getByRole('button', { name: 'Create PR' }));

    await waitFor(() => {
      expect(screen.getByText('Pull Request #42 Created!')).toBeInTheDocument();
    });
    expect(
      screen.getByRole('link', { name: 'View on GitHub' }),
    ).toHaveAttribute('href', 'https://github.com/myorg/repo/pull/42');
  });

  it('should show error when PR creation fails', async () => {
    defaultProps.onCreate.mockResolvedValue({
      _error: true,
      message: 'A PR already exists for this branch',
    });

    render(<CreatePRModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText(/Title/), 'Add new metrics');
    await userEvent.click(screen.getByRole('button', { name: 'Create PR' }));

    await waitFor(() => {
      expect(
        screen.getByText('A PR already exists for this branch'),
      ).toBeInTheDocument();
    });
  });

  it('should show error when onCreate throws exception', async () => {
    defaultProps.onCreate.mockRejectedValue(new Error('GitHub API error'));

    render(<CreatePRModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText(/Title/), 'Add new metrics');
    await userEvent.click(screen.getByRole('button', { name: 'Create PR' }));

    await waitFor(() => {
      expect(screen.getByText('GitHub API error')).toBeInTheDocument();
    });
  });

  it('should show default error when onCreate throws without message', async () => {
    defaultProps.onCreate.mockRejectedValue({});

    render(<CreatePRModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText(/Title/), 'Add new metrics');
    await userEvent.click(screen.getByRole('button', { name: 'Create PR' }));

    await waitFor(() => {
      expect(
        screen.getByText('Failed to create pull request'),
      ).toBeInTheDocument();
    });
  });

  it('should show progress states', async () => {
    let resolvePromise;
    defaultProps.onCreate.mockImplementation((title, body, onProgress) => {
      return new Promise(resolve => {
        resolvePromise = resolve;
        // Simulate progress
        setTimeout(() => onProgress('syncing'), 10);
        setTimeout(() => onProgress('creating'), 50);
      });
    });

    render(<CreatePRModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText(/Title/), 'Test');
    await userEvent.click(screen.getByRole('button', { name: 'Create PR' }));

    await waitFor(() => {
      expect(screen.getByText('Syncing to git...')).toBeInTheDocument();
    });

    // Resolve the promise to prevent test timeout
    resolvePromise({
      pr_number: 1,
      pr_url: 'https://github.com/test',
      head_branch: 'test',
      base_branch: 'main',
    });
  });

  it('should show Creating PR... progress state', async () => {
    let resolvePromise;
    defaultProps.onCreate.mockImplementation((title, body, onProgress) => {
      return new Promise(resolve => {
        resolvePromise = resolve;
        // Skip syncing, go straight to creating
        onProgress('creating');
      });
    });

    render(<CreatePRModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText(/Title/), 'Test');
    await userEvent.click(screen.getByRole('button', { name: 'Create PR' }));

    await waitFor(() => {
      expect(screen.getByText('Creating PR...')).toBeInTheDocument();
    });

    // Resolve the promise to prevent test timeout
    resolvePromise({
      pr_number: 1,
      pr_url: 'https://github.com/test',
      head_branch: 'test',
      base_branch: 'main',
    });
  });

  it('should call onClose when Cancel is clicked', async () => {
    render(<CreatePRModal {...defaultProps} />);

    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should call onClose when clicking overlay', async () => {
    render(<CreatePRModal {...defaultProps} />);

    const overlay = document.querySelector('.modal-overlay');
    fireEvent.click(overlay);
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should call onClose when clicking close button', async () => {
    render(<CreatePRModal {...defaultProps} />);

    await userEvent.click(screen.getByTitle('Close'));
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should reset state when closed from success view', async () => {
    defaultProps.onCreate.mockResolvedValue({
      pr_number: 42,
      pr_url: 'https://github.com/myorg/repo/pull/42',
      head_branch: 'feature-xyz',
      base_branch: 'main',
    });

    render(<CreatePRModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText(/Title/), 'Add new metrics');
    await userEvent.click(screen.getByRole('button', { name: 'Create PR' }));

    await waitFor(() => {
      expect(screen.getByText('Pull Request #42 Created!')).toBeInTheDocument();
    });

    // Click Close button in success view
    await userEvent.click(screen.getByRole('button', { name: 'Close' }));
    expect(defaultProps.onClose).toHaveBeenCalled();
  });

  it('should display branch flow info in success view', async () => {
    defaultProps.onCreate.mockResolvedValue({
      pr_number: 42,
      pr_url: 'https://github.com/myorg/repo/pull/42',
      head_branch: 'feature-xyz',
      base_branch: 'main',
    });

    render(<CreatePRModal {...defaultProps} />);

    await userEvent.type(screen.getByLabelText(/Title/), 'Add new metrics');
    await userEvent.click(screen.getByRole('button', { name: 'Create PR' }));

    await waitFor(() => {
      expect(screen.getByText('Pull Request #42 Created!')).toBeInTheDocument();
    });

    // Success view should show branch flow
    expect(screen.getByText('feature-xyz')).toBeInTheDocument();
    expect(screen.getByText('main')).toBeInTheDocument();
  });
});
