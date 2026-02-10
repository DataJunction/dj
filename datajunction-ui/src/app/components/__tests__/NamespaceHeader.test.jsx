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

    // Should render Deployed from Git badge for git source
    expect(screen.getByText(/Deployed from Git/)).toBeInTheDocument();
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

    // Should render Deployed from Git badge for git source even without branch
    expect(screen.getByText(/Deployed from Git/)).toBeInTheDocument();
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
    expect(screen.queryByText(/Deployed from Git/)).not.toBeInTheDocument();
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
    expect(screen.queryByText(/Deployed from Git/)).not.toBeInTheDocument();
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
      expect(screen.getByText(/Deployed from Git/)).toBeInTheDocument();
    });

    // Click the dropdown button
    fireEvent.click(screen.getByText(/Deployed from Git/));

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
      expect(screen.getByText(/Deployed from Git/)).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText(/Deployed from Git/));

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
      expect(screen.getByText(/Deployed from Git/)).toBeInTheDocument();
    });

    // Open dropdown
    fireEvent.click(screen.getByText(/Deployed from Git/));

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
      expect(screen.getByText(/Deployed from Git/)).toBeInTheDocument();
    });

    // Initially shows down arrow
    expect(screen.getByText('▼')).toBeInTheDocument();

    // Click to open
    fireEvent.click(screen.getByText(/Deployed from Git/));

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
      expect(screen.getByText(/Deployed from Git/)).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText(/Deployed from Git/));

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

  it('should show Git Settings button and open modal', async () => {
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
      expect(screen.getByText('Git Settings')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Git Settings'));

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
        git_path: 'nodes/',
        default_branch: 'main',
        // No git_branch or parent_namespace - this is a git root with default_branch
      }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    // For git root namespaces with default_branch, button is labeled "New Branch"
    await waitFor(() => {
      expect(screen.getByText('New Branch')).toBeInTheDocument();
    });
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
      getNamespaceGitConfig: jest
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
        })
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'main',
          git_path: 'nodes/',
        }),
      getPullRequest: jest.fn().mockResolvedValue(null),
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
    // Delete Branch button only has an icon with title attribute
    expect(screen.getByTitle('Delete Branch')).toBeInTheDocument();
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
        git_path: 'nodes/',
        default_branch: 'main',
        // No git_branch or parent_namespace - this is a git root with default_branch
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
      expect(screen.getByText('New Branch')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('New Branch'));

    await waitFor(() => {
      expect(screen.getByLabelText('Branch Name')).toBeInTheDocument();
    });
  });

  it('should open Sync to Git modal when button is clicked', async () => {
    // Sync to Git only shows for branch namespaces
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
      getNamespaceGitConfig: jest
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
        })
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'main',
          git_path: 'nodes/',
        }),
      getPullRequest: jest.fn().mockResolvedValue(null),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.feature" />
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
        git_path: 'nodes/',
        // git_branch and git_only not present in git root config
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
      expect(screen.getByText('Git Settings')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Git Settings'));

    await waitFor(() => {
      expect(screen.getByLabelText(/Repository/)).toBeInTheDocument();
    });

    fireEvent.change(screen.getByLabelText(/Repository/), {
      target: { value: 'myorg/repo' },
    });
    // Branch field is not present in git root mode

    fireEvent.click(screen.getByText('Save Settings'));

    await waitFor(() => {
      expect(mockDjClient.updateNamespaceGitConfig).toHaveBeenCalledWith(
        'test.namespace',
        expect.objectContaining({
          github_repo_path: 'myorg/repo',
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
        git_path: 'nodes/',
        default_branch: 'main',
        // No git_branch or parent_namespace - this is a git root
      }),
      createBranch: jest.fn().mockResolvedValue({
        branch: {
          namespace: 'test.namespace.feature_xyz',
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
      expect(screen.getByText('New Branch')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('New Branch'));

    await waitFor(() => {
      expect(screen.getByLabelText('Branch Name')).toBeInTheDocument();
    });

    fireEvent.change(screen.getByLabelText('Branch Name'), {
      target: { value: 'feature-xyz' },
    });

    // The button inside the modal is labeled "Create Branch"
    fireEvent.click(screen.getByRole('button', { name: 'Create Branch' }));

    await waitFor(() => {
      expect(mockDjClient.createBranch).toHaveBeenCalledWith(
        'test.namespace',
        'feature-xyz',
      );
    });
  });

  it('should call syncNamespaceToGit when syncing', async () => {
    // Sync to Git only shows for branch namespaces
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
      getNamespaceGitConfig: jest
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
        })
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'main',
          git_path: 'nodes/',
        }),
      getPullRequest: jest.fn().mockResolvedValue(null),
      syncNamespaceToGit: jest.fn().mockResolvedValue({
        files_synced: 5,
        commit_sha: 'abc123',
        commit_url: 'https://github.com/test/repo/commit/abc123',
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
        'test.feature',
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
      getPullRequest: jest.fn().mockResolvedValue({
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

  it('should call createPullRequest when creating a PR', async () => {
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
      getNamespaceGitConfig: jest
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
        })
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'main',
          git_path: 'nodes/',
        }),
      getPullRequest: jest.fn().mockResolvedValue(null),
      syncNamespaceToGit: jest.fn().mockResolvedValue({
        files_synced: 3,
        commit_sha: 'abc123',
        commit_url: 'https://github.com/test/repo/commit/abc123',
      }),
      createPullRequest: jest.fn().mockResolvedValue({
        pr_number: 99,
        pr_url: 'https://github.com/test/repo/pull/99',
        head_branch: 'feature',
        base_branch: 'main',
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
      expect(screen.getByText('Create PR')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Create PR'));

    await waitFor(() => {
      expect(screen.getByLabelText(/Title/)).toBeInTheDocument();
    });

    fireEvent.change(screen.getByLabelText(/Title/), {
      target: { value: 'My PR Title' },
    });
    fireEvent.change(screen.getByLabelText(/Description/), {
      target: { value: 'PR description' },
    });

    // There are two "Create PR" buttons - one in header, one in modal
    // Get all and click the last one (modal's submit button)
    const createPRButtons = screen.getAllByRole('button', {
      name: 'Create PR',
    });
    fireEvent.click(createPRButtons[createPRButtons.length - 1]);

    await waitFor(() => {
      expect(mockDjClient.syncNamespaceToGit).toHaveBeenCalledWith(
        'test.feature',
        'My PR Title',
      );
    });

    await waitFor(() => {
      expect(mockDjClient.createPullRequest).toHaveBeenCalledWith(
        'test.feature',
        'My PR Title',
        'PR description',
      );
    });
  });

  it('should call deleteBranch when deleting a branch', async () => {
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
      getNamespaceGitConfig: jest
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
        })
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'main',
          git_path: 'nodes/',
        }),
      getPullRequest: jest.fn().mockResolvedValue(null),
      deleteBranch: jest.fn().mockResolvedValue({ success: true }),
    };

    // Mock window.location
    delete window.location;
    window.location = { href: '' };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.feature" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      // Delete Branch button in header only has icon with title attribute
      expect(screen.getByTitle('Delete Branch')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTitle('Delete Branch'));

    await waitFor(() => {
      expect(screen.getByRole('checkbox')).toBeInTheDocument();
    });

    // There are two buttons with "Delete Branch" - header icon and modal button
    // Get all and click the last one (modal's submit button)
    const deleteBranchButtons = screen.getAllByRole('button', {
      name: 'Delete Branch',
    });
    fireEvent.click(deleteBranchButtons[deleteBranchButtons.length - 1]);

    await waitFor(() => {
      expect(mockDjClient.deleteBranch).toHaveBeenCalledWith(
        'test.main',
        'test.feature',
        true,
      );
    });
  });

  it('should fetch parent git config for branch namespace', async () => {
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
      getNamespaceGitConfig: jest
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
        })
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'main',
          git_path: 'nodes/',
        }),
      getPullRequest: jest.fn().mockResolvedValue(null),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.feature" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(mockDjClient.getNamespaceGitConfig).toHaveBeenCalledWith(
        'test.feature',
      );
    });

    await waitFor(() => {
      expect(mockDjClient.getNamespaceGitConfig).toHaveBeenCalledWith(
        'test.main',
      );
    });

    await waitFor(() => {
      expect(mockDjClient.getPullRequest).toHaveBeenCalledWith('test.feature');
    });
  });

  it('should handle error fetching parent git config gracefully', async () => {
    const consoleSpy = jest.spyOn(console, 'error').mockImplementation();

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
      getNamespaceGitConfig: jest
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
        })
        .mockRejectedValueOnce(new Error('Parent not found')),
      getPullRequest: jest.fn().mockResolvedValue(null),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.feature" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(consoleSpy).toHaveBeenCalledWith(
        'Failed to fetch parent git config:',
        expect.any(Error),
      );
    });

    consoleSpy.mockRestore();
  });

  it('should handle error fetching PR gracefully', async () => {
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
      getNamespaceGitConfig: jest
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
        })
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'main',
          git_path: 'nodes/',
        }),
      getPullRequest: jest.fn().mockRejectedValue(new Error('API Error')),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.feature" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    // Should render without crashing and show Create PR button
    await waitFor(() => {
      expect(screen.getByText('Create PR')).toBeInTheDocument();
    });
  });

  it('should call onGitConfigLoaded callback when config is fetched', async () => {
    const onGitConfigLoaded = jest.fn();
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
      getNamespaceGitConfig: jest.fn().mockResolvedValue({
        github_repo_path: 'test/repo',
        git_branch: 'main',
      }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader
            namespace="test.namespace"
            onGitConfigLoaded={onGitConfigLoaded}
          />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(onGitConfigLoaded).toHaveBeenCalledWith({
        github_repo_path: 'test/repo',
        git_branch: 'main',
      });
    });
  });

  it('should call onGitConfigLoaded with null when git config fetch fails', async () => {
    const onGitConfigLoaded = jest.fn();
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
      getNamespaceGitConfig: jest
        .fn()
        .mockRejectedValue(new Error('Config not found')),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader
            namespace="test.namespace"
            onGitConfigLoaded={onGitConfigLoaded}
          />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(onGitConfigLoaded).toHaveBeenCalledWith(null);
    });
  });

  it('should call deleteNamespaceGitConfig when removing git settings', async () => {
    // Mock window.confirm for this test
    global.confirm = jest.fn(() => true);

    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: jest.fn().mockResolvedValue([]),
      getNamespaceGitConfig: jest.fn().mockResolvedValue({
        github_repo_path: 'test/repo',
        git_branch: 'main',
        git_path: 'nodes/',
        git_only: false,
      }),
      deleteNamespaceGitConfig: jest.fn().mockResolvedValue({ success: true }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText('Git Settings')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Git Settings'));

    await waitFor(() => {
      expect(screen.getByText('Git Configuration')).toBeInTheDocument();
    });

    // Click reset button in the modal (button text is "Reset")
    const removeButton = screen.getByText('Reset');
    fireEvent.click(removeButton);

    await waitFor(() => {
      expect(mockDjClient.deleteNamespaceGitConfig).toHaveBeenCalledWith(
        'test.namespace',
      );
    });

    // Clean up mock
    jest.restoreAllMocks();
  });

  it('should handle sync error in handleCreatePR', async () => {
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
      getNamespaceGitConfig: jest
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
        })
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'main',
          git_path: 'nodes/',
        }),
      getPullRequest: jest.fn().mockResolvedValue(null),
      syncNamespaceToGit: jest.fn().mockResolvedValue({
        _error: true,
        message: 'Sync failed: merge conflict',
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
      expect(screen.getByText('Create PR')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Create PR'));

    await waitFor(() => {
      expect(screen.getByLabelText(/Title/)).toBeInTheDocument();
    });

    fireEvent.change(screen.getByLabelText(/Title/), {
      target: { value: 'My PR Title' },
    });

    const createPRButtons = screen.getAllByRole('button', {
      name: 'Create PR',
    });
    fireEvent.click(createPRButtons[createPRButtons.length - 1]);

    await waitFor(() => {
      expect(mockDjClient.syncNamespaceToGit).toHaveBeenCalledWith(
        'test.feature',
        'My PR Title',
      );
    });

    // Should show error message from sync failure
    await waitFor(() => {
      expect(
        screen.getByText(/Sync failed: merge conflict/),
      ).toBeInTheDocument();
    });
  });
});
