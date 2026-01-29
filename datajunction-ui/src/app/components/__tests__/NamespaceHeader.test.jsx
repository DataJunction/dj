import * as React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { createRenderer } from 'react-test-renderer/shallow';
import { MemoryRouter } from 'react-router-dom';

import NamespaceHeader from '../NamespaceHeader';
import DJClientContext from '../../providers/djclient';

const renderer = createRenderer();

describe('<NamespaceHeader />', () => {
  it('should render and match the snapshot', () => {
    renderer.render(<NamespaceHeader namespace="shared.dimensions.accounts" />);
    const renderedOutput = renderer.getRenderOutput();
    expect(renderedOutput).toMatchSnapshot();
  });

  it('should render git source badge when source type is git with branch', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 5,
        primary_source: {
          type: 'git',
          repository: 'github.com/test/repo',
          branch: 'main',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(mockDjClient.namespaceSources).toHaveBeenCalledWith(
        'test.namespace',
      );
    });

    // Should render Git Managed badge for git source
    expect(screen.getByText(/Git Managed/)).toBeInTheDocument();
  });

  it('should render git source badge when source type is git without branch', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 3,
        primary_source: {
          type: 'git',
          repository: 'github.com/test/repo',
          branch: null,
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(mockDjClient.namespaceSources).toHaveBeenCalledWith(
        'test.namespace',
      );
    });

    // Should render Git Managed badge for git source even without branch
    expect(screen.getByText(/Git Managed/)).toBeInTheDocument();
  });

  it('should render local source badge when source type is local', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 2,
        primary_source: {
          type: 'local',
          hostname: 'localhost',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(mockDjClient.namespaceSources).toHaveBeenCalledWith(
        'test.namespace',
      );
    });

    // Should render Local Deploy badge for local source
    expect(screen.getByText(/Local Deploy/)).toBeInTheDocument();
  });

  it('should not render badge when no deployments', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(mockDjClient.namespaceSources).toHaveBeenCalledWith(
        'test.namespace',
      );
    });

    // Should not render any source badge
    expect(screen.queryByText(/Git Managed/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Local Deploy/)).not.toBeInTheDocument();
  });

  it('should handle API error gracefully', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockRejectedValue(new Error('API Error')),
      listDeployments: jest.fn().mockResolvedValue([]),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(mockDjClient.namespaceSources).toHaveBeenCalledWith(
        'test.namespace',
      );
    });

    // Should still render breadcrumb without badge
    expect(screen.getByText('test')).toBeInTheDocument();
    expect(screen.getByText('namespace')).toBeInTheDocument();
    expect(screen.queryByText(/Git Managed/)).not.toBeInTheDocument();
  });

  it('should open dropdown when clicking the git managed button', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 5,
        primary_source: {
          type: 'git',
          repository: 'github.com/test/repo',
          branch: 'main',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([
        {
          uuid: 'deploy-1',
          status: 'success',
          created_at: '2024-01-15T10:00:00Z',
          source: {
            type: 'git',
            repository: 'github.com/test/repo',
            branch: 'main',
            commit_sha: 'abc1234567890',
          },
        },
      ]),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText(/Git Managed/)).toBeInTheDocument();
    });

    // Click the dropdown button
    fireEvent.click(screen.getByText(/Git Managed/));

    // Should show repository link in dropdown
    await waitFor(() => {
      expect(screen.getByText(/github.com\/test\/repo/)).toBeInTheDocument();
    });
  });

  it('should open dropdown when clicking local deploy button', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 2,
        primary_source: {
          type: 'local',
          hostname: 'localhost',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([
        {
          uuid: 'deploy-1',
          status: 'success',
          created_at: '2024-01-15T10:00:00Z',
          created_by: 'testuser',
          source: {
            type: 'local',
            hostname: 'localhost',
            reason: 'testing',
          },
        },
      ]),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText(/Local Deploy/)).toBeInTheDocument();
    });

    // Click the dropdown button
    fireEvent.click(screen.getByText(/Local Deploy/));

    // Should show local deploy info in dropdown
    await waitFor(() => {
      expect(screen.getByText(/Local deploys by testuser/)).toBeInTheDocument();
    });
  });

  it('should show recent deployments list with git source', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 3,
        primary_source: {
          type: 'git',
          repository: 'github.com/test/repo',
          branch: 'main',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([
        {
          uuid: 'deploy-1',
          status: 'success',
          created_at: '2024-01-15T10:00:00Z',
          source: {
            type: 'git',
            repository: 'github.com/test/repo',
            branch: 'feature-branch',
            commit_sha: 'abc1234567890',
          },
        },
        {
          uuid: 'deploy-2',
          status: 'failed',
          created_at: '2024-01-14T10:00:00Z',
          source: {
            type: 'git',
            repository: 'github.com/test/repo',
            branch: 'main',
            commit_sha: 'def4567890123',
          },
        },
      ]),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText(/Git Managed/)).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText(/Git Managed/));

    // Should show branch names in deployment list
    await waitFor(() => {
      expect(screen.getByText(/feature-branch/)).toBeInTheDocument();
    });

    // Should show short commit SHA
    expect(screen.getByText(/abc1234/)).toBeInTheDocument();
  });

  it('should show local deployments with reason', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 2,
        primary_source: {
          type: 'local',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([
        {
          uuid: 'deploy-1',
          status: 'success',
          created_at: '2024-01-15T10:00:00Z',
          source: {
            type: 'local',
            reason: 'hotfix deployment',
            hostname: 'dev-machine',
          },
        },
      ]),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText(/Local Deploy/)).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText(/Local Deploy/));

    // Should show reason in deployment list
    await waitFor(() => {
      expect(screen.getByText(/hotfix deployment/)).toBeInTheDocument();
    });
  });

  it('should close dropdown when clicking outside', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 5,
        primary_source: {
          type: 'git',
          repository: 'github.com/test/repo',
          branch: 'main',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText(/Git Managed/)).toBeInTheDocument();
    });

    // Open dropdown
    fireEvent.click(screen.getByText(/Git Managed/));

    await waitFor(() => {
      expect(screen.getByText(/github.com\/test\/repo/)).toBeInTheDocument();
    });

    // Click outside (on the breadcrumb)
    fireEvent.mouseDown(document.body);

    // Dropdown should close
    await waitFor(() => {
      expect(
        screen.queryByText(/github.com\/test\/repo.*\(main\)/),
      ).not.toBeInTheDocument();
    });
  });

  it('should toggle dropdown arrow indicator', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 5,
        primary_source: {
          type: 'git',
          repository: 'github.com/test/repo',
          branch: 'main',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText(/Git Managed/)).toBeInTheDocument();
    });

    // Initially shows down arrow
    expect(screen.getByText('▼')).toBeInTheDocument();

    // Click to open
    fireEvent.click(screen.getByText(/Git Managed/));

    // Should show up arrow when open
    await waitFor(() => {
      expect(screen.getByText('▲')).toBeInTheDocument();
    });
  });

  it('should handle git repository URL with https prefix', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'git',
          repository: 'https://github.com/test/repo',
          branch: 'main',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText(/Git Managed/)).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText(/Git Managed/));

    await waitFor(() => {
      // Find link by its text content (repository URL)
      const link = screen.getByRole('link', {
        name: /github\.com\/test\/repo/,
      });
      expect(link).toHaveAttribute('href', 'https://github.com/test/repo');
    });
  });

  it('should render adhoc deployment label when no created_by', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'local',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([
        {
          uuid: 'deploy-1',
          status: 'success',
          created_at: '2024-01-15T10:00:00Z',
          created_by: null,
          source: {
            type: 'local',
          },
        },
      ]),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText(/Local Deploy/)).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText(/Local Deploy/));

    await waitFor(() => {
      expect(screen.getByText(/Local\/adhoc deployments/)).toBeInTheDocument();
    });
  });

  it('should show Configure Git button and open modal', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
      getNamespaceGitConfig: jest.fn().mockResolvedValue(null),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText('Configure Git')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Configure Git'));

    await waitFor(() => {
      expect(screen.getByText('Git Configuration')).toBeInTheDocument();
    });
  });

  it('should show git action buttons when git is configured', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'git',
          repository: 'test/repo',
          branch: 'main',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
      getNamespaceGitConfig: jest.fn().mockResolvedValue({
        github_repo_path: 'test/repo',
        git_branch: 'main',
        git_path: 'nodes/',
        git_only: false,
      }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText('Create Branch')).toBeInTheDocument();
    });
    expect(screen.getByText('Sync to Git')).toBeInTheDocument();
  });

  it('should show Create PR and Delete Branch for branch namespaces', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'git',
          repository: 'test/repo',
          branch: 'feature',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
      getNamespaceGitConfig: jest.fn().mockResolvedValue({
        github_repo_path: 'test/repo',
        git_branch: 'feature',
        git_path: 'nodes/',
        git_only: false,
        parent_namespace: 'test.main',
      }),
      getNamespacePR: jest.fn().mockResolvedValue(null),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.feature" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText('Create PR')).toBeInTheDocument();
    });
    expect(screen.getByText('Delete Branch')).toBeInTheDocument();
  });

  it('should open Create Branch modal when button is clicked', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'git',
          repository: 'test/repo',
          branch: 'main',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
      getNamespaceGitConfig: jest.fn().mockResolvedValue({
        github_repo_path: 'test/repo',
        git_branch: 'main',
        git_path: 'nodes/',
        git_only: false,
      }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText('Create Branch')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Create Branch'));

    await waitFor(() => {
      expect(screen.getByLabelText('Branch Name')).toBeInTheDocument();
    });
  });

  it('should open Sync to Git modal when button is clicked', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'git',
          repository: 'test/repo',
          branch: 'main',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
      getNamespaceGitConfig: jest.fn().mockResolvedValue({
        github_repo_path: 'test/repo',
        git_branch: 'main',
        git_path: 'nodes/',
        git_only: false,
      }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText('Sync to Git')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Sync to Git'));

    await waitFor(() => {
      expect(screen.getByText(/Sync all nodes in/)).toBeInTheDocument();
    });
  });

  it('should call updateNamespaceGitConfig when saving git settings', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
      getNamespaceGitConfig: jest.fn().mockResolvedValue(null),
      updateNamespaceGitConfig: jest.fn().mockResolvedValue({
        github_repo_path: 'myorg/repo',
        git_branch: 'main',
        git_path: 'nodes/',
        git_only: true,
      }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText('Configure Git')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Configure Git'));

    await waitFor(() => {
      expect(screen.getByLabelText('Repository')).toBeInTheDocument();
    });

    fireEvent.change(screen.getByLabelText('Repository'), {
      target: { value: 'myorg/repo' },
    });
    fireEvent.change(screen.getByLabelText('Branch'), {
      target: { value: 'main' },
    });

    fireEvent.click(screen.getByText('Save Settings'));

    await waitFor(() => {
      expect(mockDjClient.updateNamespaceGitConfig).toHaveBeenCalledWith(
        'test.namespace',
        expect.objectContaining({
          github_repo_path: 'myorg/repo',
          git_branch: 'main',
        }),
      );
    });
  });

  it('should call createBranch when creating a branch', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'git',
          repository: 'test/repo',
          branch: 'main',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
      getNamespaceGitConfig: jest.fn().mockResolvedValue({
        github_repo_path: 'test/repo',
        git_branch: 'main',
        git_path: 'nodes/',
        git_only: false,
      }),
      createBranch: jest.fn().mockResolvedValue({
        branch: {
          namespace: 'test.feature_xyz',
          git_branch: 'feature-xyz',
          parent_namespace: 'test.namespace',
        },
        deployment_results: [],
      }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText('Create Branch')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Create Branch'));

    await waitFor(() => {
      expect(screen.getByLabelText('Branch Name')).toBeInTheDocument();
    });

    fireEvent.change(screen.getByLabelText('Branch Name'), {
      target: { value: 'feature-xyz' },
    });

    fireEvent.click(screen.getByRole('button', { name: 'Create Branch' }));

    await waitFor(() => {
      expect(mockDjClient.createBranch).toHaveBeenCalledWith(
        'test.namespace',
        'feature-xyz',
      );
    });
  });

  it('should call syncNamespaceToGit when syncing', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'git',
          repository: 'test/repo',
          branch: 'main',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
      getNamespaceGitConfig: jest.fn().mockResolvedValue({
        github_repo_path: 'test/repo',
        git_branch: 'main',
        git_path: 'nodes/',
        git_only: false,
      }),
      syncNamespaceToGit: jest.fn().mockResolvedValue({
        files_synced: 5,
        commit_sha: 'abc123',
        commit_url: 'https://github.com/test/repo/commit/abc123',
      }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText('Sync to Git')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Sync to Git'));

    await waitFor(() => {
      expect(screen.getByLabelText(/Commit Message/)).toBeInTheDocument();
    });

    fireEvent.change(screen.getByLabelText(/Commit Message/), {
      target: { value: 'Test commit' },
    });

    fireEvent.click(screen.getByRole('button', { name: 'Sync Now' }));

    await waitFor(() => {
      expect(mockDjClient.syncNamespaceToGit).toHaveBeenCalledWith(
        'test.namespace',
        'Test commit',
      );
    });
  });

  it('should show View PR button when PR exists', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'git',
          repository: 'test/repo',
          branch: 'feature',
        },
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
      getNamespaceGitConfig: jest.fn().mockResolvedValue({
        github_repo_path: 'test/repo',
        git_branch: 'feature',
        git_path: 'nodes/',
        git_only: false,
        parent_namespace: 'test.main',
      }),
      getNamespacePR: jest.fn().mockResolvedValue({
        pr_number: 42,
        pr_url: 'https://github.com/test/repo/pull/42',
      }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.feature" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText(/View PR #42/)).toBeInTheDocument();
    });
  });
});
