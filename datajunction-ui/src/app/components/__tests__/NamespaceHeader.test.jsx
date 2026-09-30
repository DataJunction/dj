import * as React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router-dom';

import NamespaceHeader from '../NamespaceHeader';
import DJClientContext from '../../providers/djclient';

// Helper: render NamespaceHeader with a minimal mock DJ client.
// Options:
//   namespace  - namespace string (default 'test.namespace')
//   gitConfig  - value returned by getNamespaceGitConfig (default: rejects)
const renderHeader = ({ namespace = 'test.namespace', gitConfig } = {}) => {
  const mockDjClient = {
    namespaceSources: vi.fn().mockResolvedValue({
      total_deployments: 0,
      primary_source: null,
    }),
    listDeployments: vi.fn().mockResolvedValue([]),
    getNamespaceGitConfig:
      gitConfig !== undefined
        ? vi.fn().mockResolvedValue(gitConfig)
        : vi.fn().mockRejectedValue(new Error('no config')),
    getNamespaceBranches: vi.fn().mockResolvedValue([]),
    deleteNamespaceGitConfig: vi.fn().mockResolvedValue({ success: true }),
    updateNamespaceGitConfig: vi.fn().mockResolvedValue(gitConfig || {}),
  };
  return render(
    <MemoryRouter>
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <NamespaceHeader namespace={namespace} />
      </DJClientContext.Provider>
    </MemoryRouter>,
  );
};

// Render with explicit git config + deployment sources so we can exercise the
// three toolbar states. getPullRequest is mocked because branch namespaces
// fetch an existing PR on mount.
const renderHeaderState = ({ namespace, gitConfig, sources }) => {
  const mockDjClient = {
    namespaceSources: vi
      .fn()
      .mockResolvedValue(
        sources || { total_deployments: 0, primary_source: null },
      ),
    listDeployments: vi.fn().mockResolvedValue([]),
    getNamespaceGitConfig: vi.fn().mockResolvedValue(gitConfig),
    getNamespaceBranches: vi.fn().mockResolvedValue([]),
    getPullRequest: vi.fn().mockResolvedValue(null),
    deleteNamespaceGitConfig: vi.fn().mockResolvedValue({ success: true }),
    updateNamespaceGitConfig: vi.fn().mockResolvedValue(gitConfig || {}),
  };
  return render(
    <MemoryRouter>
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <NamespaceHeader namespace={namespace} />
      </DJClientContext.Provider>
    </MemoryRouter>,
  );
};

// Render for the read-only verdict (onReadOnlyChange). `configByNamespace` maps
// each namespace NamespaceHeader fetches (the target, its branch, the git root)
// to its git config — namespace-keyed so it's independent of fetch order.
const renderReadOnlyVerdict = ({ namespace, configByNamespace, sources }) => {
  const onReadOnlyChange = vi.fn();
  const mockDjClient = {
    namespaceSources: vi
      .fn()
      .mockResolvedValue(
        sources || { total_deployments: 0, primary_source: null },
      ),
    listDeployments: vi.fn().mockResolvedValue([]),
    getNamespaceGitConfig: vi.fn(ns => Promise.resolve(configByNamespace[ns])),
    getNamespaceBranches: vi.fn().mockResolvedValue([]),
    getPullRequest: vi.fn().mockResolvedValue(null),
  };
  render(
    <MemoryRouter>
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <NamespaceHeader
          namespace={namespace}
          onReadOnlyChange={onReadOnlyChange}
        />
      </DJClientContext.Provider>
    </MemoryRouter>,
  );
  return { onReadOnlyChange };
};

// Git configs for a repo `test` whose default branch is `main`, a feature
// branch `feature`, and their sub-namespaces. Reused across the verdict cases.
const GIT_ROOT = {
  github_repo_path: 'test/repo',
  git_branch: null,
  default_branch: 'main',
  git_root_namespace: 'test',
};
const branchCfg = (branch, ns, extra = {}) => ({
  github_repo_path: 'test/repo',
  git_branch: branch,
  git_only: false,
  branch_namespace: `test.${branch}`,
  git_root_namespace: 'test',
  ...(ns === `test.${branch}` ? { parent_namespace: 'test' } : {}),
  ...extra,
});

describe('<NamespaceHeader />', () => {
  it('should render and match the snapshot', async () => {
    const mockDjClient = {
      namespaceSources: vi
        .fn()
        .mockResolvedValue({ total_deployments: 0, primary_source: null }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getPullRequest: vi.fn().mockResolvedValue(null),
      getNamespaceBranches: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi.fn().mockResolvedValue({}),
    };
    const { asFragment } = render(
      <MemoryRouter initialEntries={['/namespaces/shared.dimensions.accounts']}>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="shared.dimensions.accounts" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );
    // Let git config resolve so the snapshot is the settled state.
    await waitFor(() =>
      expect(mockDjClient.getNamespaceGitConfig).toHaveBeenCalled(),
    );
    expect(asFragment()).toMatchSnapshot();
  });

  it('should render git source badge when source type is git with branch', async () => {
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 5,
        primary_source: {
          type: 'git',
          repository: 'github.com/test/repo',
          branch: 'main',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
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

    // Git-deployed namespaces are read-only; the strip was removed, but the
    // Read-only badge in the breadcrumb area still shows for isGitManaged.
    expect(screen.getByText('Read-only')).toBeInTheDocument();
    // No "Deployed from Git" text (strip removed) and no local-deploy badge
    expect(screen.queryByText(/Local Deploy/)).not.toBeInTheDocument();
  });

  it('should render git source badge when source type is git without branch', async () => {
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 3,
        primary_source: {
          type: 'git',
          repository: 'github.com/test/repo',
          branch: null,
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
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

    // Git-deployed namespaces show the Read-only badge (strip removed).
    expect(screen.getByText('Read-only')).toBeInTheDocument();
    expect(screen.queryByText(/Local Deploy/)).not.toBeInTheDocument();
  });

  it('should render local source badge when source type is local', async () => {
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 2,
        primary_source: {
          type: 'local',
          hostname: 'localhost',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
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
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
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
      namespaceSources: vi.fn().mockRejectedValue(new Error('API Error')),
      listDeployments: vi.fn().mockResolvedValue([]),
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

  it('should show read-only controls (not a dropdown) for git-deployed namespace', async () => {
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 5,
        primary_source: {
          type: 'git',
          repository: 'github.com/test/repo',
          branch: 'main',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([
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

    // Git-deployed namespace shows the Read-only badge in the breadcrumb area.
    await waitFor(() => {
      expect(screen.getByText('Read-only')).toBeInTheDocument();
    });

    // Should NOT show "Edited in DataJunction" or "Connect to Git"
    expect(
      screen.queryByText(/Edited in DataJunction/),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: /connect to git/i }),
    ).not.toBeInTheDocument();

    // Status strip is removed; no .git-status-strip element
    expect(document.querySelector('.git-status-strip')).toBeNull();

    // Git settings is available via the Git menu
    expect(
      screen.getAllByRole('button', { name: /git settings/i }).length,
    ).toBeGreaterThanOrEqual(1);
  });

  it('should open dropdown when clicking local deploy button', async () => {
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 2,
        primary_source: {
          type: 'local',
          hostname: 'localhost',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([
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
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 3,
        primary_source: {
          type: 'git',
          repository: 'github.com/test/repo',
          branch: 'main',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([
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

    // Git-deployed namespaces show the Read-only badge (strip removed).
    await waitFor(() => {
      expect(screen.getByText('Read-only')).toBeInTheDocument();
    });

    // Git deployments are not shown in a dropdown; no local-deploy-style list.
    expect(screen.queryByText(/feature-branch/)).not.toBeInTheDocument();
  });

  it('should show local deployments with reason', async () => {
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 2,
        primary_source: {
          type: 'local',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([
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

  it('should close local deploy dropdown when clicking outside', async () => {
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 2,
        primary_source: {
          type: 'local',
          hostname: 'localhost',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([
        {
          uuid: 'deploy-1',
          status: 'success',
          created_at: '2024-01-15T10:00:00Z',
          created_by: 'testuser',
          source: { type: 'local', hostname: 'localhost', reason: 'testing' },
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

    // Open dropdown
    fireEvent.click(screen.getByText(/Local Deploy/));

    await waitFor(() => {
      expect(screen.getByText(/Local deploys by testuser/)).toBeInTheDocument();
    });

    // Click outside
    fireEvent.mouseDown(document.body);

    // Dropdown should close
    await waitFor(() => {
      expect(
        screen.queryByText(/Local deploys by testuser/),
      ).not.toBeInTheDocument();
    });
  });

  it('should toggle local deploy dropdown arrow indicator', async () => {
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 2,
        primary_source: {
          type: 'local',
          hostname: 'localhost',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
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

    // Initially shows down arrow
    expect(screen.getByText('▼')).toBeInTheDocument();

    // Click to open
    fireEvent.click(screen.getByText(/Local Deploy/));

    // Should show up arrow when open
    await waitFor(() => {
      expect(screen.getByText('▲')).toBeInTheDocument();
    });
  });

  it('should show read-only controls even with https-prefixed repo in sources', async () => {
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'git',
          repository: 'https://github.com/test/repo',
          branch: 'main',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    // Git-deployed namespaces show the Read-only badge (strip removed).
    await waitFor(() => {
      expect(screen.getByText('Read-only')).toBeInTheDocument();
    });

    // Should NOT show "Connect to Git" or "Edited in DataJunction"
    expect(
      screen.queryByText(/Edited in DataJunction/),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: /connect to git/i }),
    ).not.toBeInTheDocument();
  });

  it('should render adhoc deployment label when no created_by', async () => {
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'local',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([
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

  it('should show Connect to Git button and open git settings modal', async () => {
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi.fn().mockResolvedValue(null),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    // Plain (not-git) state shows a "Connect to Git" button that opens the modal.
    await waitFor(() => {
      expect(screen.getByText('Connect to Git')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Connect to Git'));

    await waitFor(() => {
      expect(screen.getByText('Git Configuration')).toBeInTheDocument();
    });
  });

  it('should show git action buttons when git is configured', async () => {
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'git',
          repository: 'test/repo',
          branch: 'main',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi.fn().mockResolvedValue({
        github_repo_path: 'test/repo',
        git_path: 'nodes/',
        default_branch: 'main',
        // No git_branch or parent_namespace - this is a git root with default_branch
        git_root_namespace: 'test.namespace',
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
      namespaceSources: vi.fn().mockResolvedValue({
        // No git deployments so isGitManaged stays false and editable-branch controls show.
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
          branch_namespace: 'test.feature',
        })
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'main',
          git_path: 'nodes/',
        }),
      getNamespaceBranches: vi.fn().mockResolvedValue([]),
      getPullRequest: vi.fn().mockResolvedValue(null),
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
    expect(
      screen.getByRole('button', { name: 'Delete branch' }),
    ).toBeInTheDocument();
  });

  describe('read-only verdict (onReadOnlyChange)', () => {
    // Regression: a sub-namespace inside a feature branch resolves as `flat`
    // (inherited git_branch, no parent_namespace of its own). It must be
    // editable (its branch isn't the default), while the default branch — and
    // a git-deployed feature branch stays editable too.
    const cases = [
      {
        what: 'a sub-namespace of a feature branch as EDITABLE',
        namespace: 'test.feature.metrics',
        configByNamespace: {
          'test.feature.metrics': branchCfg('feature', 'test.feature.metrics'),
          'test.feature': branchCfg('feature', 'test.feature'),
          test: GIT_ROOT,
        },
        expectedReadOnly: false,
      },
      {
        what: 'a sub-namespace of the default branch as READ-ONLY',
        namespace: 'test.main.metrics',
        configByNamespace: {
          'test.main.metrics': branchCfg('main', 'test.main.metrics'),
          'test.main': branchCfg('main', 'test.main'),
          test: GIT_ROOT,
        },
        expectedReadOnly: true,
      },
      {
        what: 'a git-deployed feature branch as EDITABLE (deploy status does not lock it)',
        namespace: 'test.feature',
        configByNamespace: {
          'test.feature': branchCfg('feature', 'test.feature'),
          test: GIT_ROOT,
        },
        sources: { total_deployments: 2, primary_source: { type: 'git' } },
        expectedReadOnly: false,
      },
    ];

    cases.forEach(
      ({ what, namespace, configByNamespace, sources, expectedReadOnly }) => {
        it(`reports ${what}`, async () => {
          const { onReadOnlyChange } = renderReadOnlyVerdict({
            namespace,
            configByNamespace,
            sources,
          });
          await waitFor(() => expect(onReadOnlyChange).toHaveBeenCalled());
          expect(onReadOnlyChange).toHaveBeenLastCalledWith(expectedReadOnly);
        });
      },
    );

    // Regression: same `flat`-shape trap on the git action button. A feature
    // branch sub-namespace is editable, so it must show the "Sync to Git"
    // control — not the not-git "Connect to Git" button.
    it('shows Sync to Git (not Connect to Git) for a feature branch sub-namespace', async () => {
      renderReadOnlyVerdict({
        namespace: 'test.feature.metrics',
        configByNamespace: {
          'test.feature.metrics': branchCfg('feature', 'test.feature.metrics'),
          'test.feature': branchCfg('feature', 'test.feature'),
          test: GIT_ROOT,
        },
      });
      await waitFor(() =>
        expect(screen.getByText('Sync to Git')).toBeInTheDocument(),
      );
      expect(screen.queryByText('Connect to Git')).not.toBeInTheDocument();
    });
  });

  it('should open Create Branch modal when button is clicked', async () => {
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'git',
          repository: 'test/repo',
          branch: 'main',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi.fn().mockResolvedValue({
        github_repo_path: 'test/repo',
        git_path: 'nodes/',
        default_branch: 'main',
        // No git_branch or parent_namespace - this is a git root with default_branch
        git_root_namespace: 'test.namespace',
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
    // Sync to Git only shows for editable branch namespaces (no git deployments).
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        // No git deployments so isGitManaged stays false and editable-branch controls show.
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
          branch_namespace: 'test.feature',
        })
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'main',
          git_path: 'nodes/',
        }),
      getNamespaceBranches: vi.fn().mockResolvedValue([]),
      getPullRequest: vi.fn().mockResolvedValue(null),
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
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi.fn().mockResolvedValue(null),
      updateNamespaceGitConfig: vi.fn().mockResolvedValue({
        github_repo_path: 'myorg/repo',
        git_branch: 'main',
      }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    // Plain (not-git) state shows Connect to Git which opens the git settings modal.
    await waitFor(() => {
      expect(screen.getByText('Connect to Git')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Connect to Git'));

    await waitFor(() => {
      expect(screen.getByText('Git Configuration')).toBeInTheDocument();
    });

    // Q1: choose "Git repo" (modal opens with 'dj' selected since config is null)
    fireEvent.click(screen.getByRole('button', { name: 'Git repo' }));

    // Q2: choose "Tracks a branch" (flat model)
    await waitFor(() => {
      expect(
        screen.getByRole('button', { name: 'Tracks a branch' }),
      ).toBeInTheDocument();
    });
    fireEvent.click(screen.getByRole('button', { name: 'Tracks a branch' }));

    // Fill Repository and Branch fields
    fireEvent.change(screen.getByLabelText(/Repository/), {
      target: { value: 'myorg/repo' },
    });
    fireEvent.change(screen.getByLabelText(/Branch/), {
      target: { value: 'main' },
    });

    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

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
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'git',
          repository: 'test/repo',
          branch: 'main',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi.fn().mockResolvedValue({
        github_repo_path: 'test/repo',
        git_path: 'nodes/',
        default_branch: 'main',
        // No git_branch or parent_namespace - this is a git root
        git_root_namespace: 'test.namespace',
      }),
      createBranch: vi.fn().mockResolvedValue({
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

  it('should create a branch from the git root when on a branch page', async () => {
    // Regression: git roots redirect to their default branch, so the only place
    // to start a new branch is the branch switcher on the branch page. Creating
    // one must still target the git root (`test`), not the current branch.
    const gitConfigByNamespace = {
      // The default-branch page (the git root redirects here). It's read-only,
      // so it renders the inline "New Branch" toolbar flow this test drives.
      'test.main': {
        github_repo_path: 'test/repo',
        git_branch: 'main',
        git_path: 'nodes/',
        git_only: false,
        parent_namespace: 'test',
        branch_namespace: 'test.main',
        git_root_namespace: 'test',
      },
      test: {
        github_repo_path: 'test/repo',
        git_branch: 'main',
        default_branch: 'main',
        git_path: 'nodes/',
        git_root_namespace: 'test',
      },
    };
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'git',
          repository: 'test/repo',
          branch: 'feature',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi.fn(ns =>
        Promise.resolve(gitConfigByNamespace[ns]),
      ),
      getNamespaceBranches: vi
        .fn()
        .mockResolvedValue([
          { namespace: 'test.feature', git_branch: 'feature', num_nodes: 0 },
        ]),
      getPullRequest: vi.fn().mockResolvedValue(null),
      createBranch: vi.fn().mockResolvedValue({
        branch: {
          namespace: 'test.feature_xyz',
          git_branch: 'feature-xyz',
          parent_namespace: 'test',
        },
        deployment_results: [],
      }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.main" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    // Both entry points are present on a branch page: the toolbar button and
    // the branch switcher dropdown footer.
    await waitFor(() => {
      expect(screen.getByText('New Branch')).toBeInTheDocument();
    });
    // Click the branch-crumb button (not the <code> in the git strip).
    fireEvent.click(
      screen
        .getAllByText('main')
        .find(el => el.closest('button') && el.tagName !== 'CODE'),
    );
    expect(await screen.findByText('New branch')).toBeInTheDocument();

    // Drive creation via the toolbar button.
    fireEvent.click(screen.getByText('New Branch'));
    await waitFor(() => {
      expect(screen.getByLabelText('Branch Name')).toBeInTheDocument();
    });
    fireEvent.change(screen.getByLabelText('Branch Name'), {
      target: { value: 'feature-xyz' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Create Branch' }));

    await waitFor(() => {
      expect(mockDjClient.createBranch).toHaveBeenCalledWith(
        'test',
        'feature-xyz',
      );
    });
  });

  it('should call syncNamespaceToGit when syncing', async () => {
    // Sync to Git only shows for editable branch namespaces (no git deployments).
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        // No git deployments so isGitManaged stays false and editable-branch controls show.
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
          branch_namespace: 'test.feature',
        })
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'main',
          git_path: 'nodes/',
        }),
      getNamespaceBranches: vi.fn().mockResolvedValue([]),
      getPullRequest: vi.fn().mockResolvedValue(null),
      syncNamespaceToGit: vi.fn().mockResolvedValue({
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
      namespaceSources: vi.fn().mockResolvedValue({
        // No git deployments so isGitManaged stays false and editable-branch controls show.
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi.fn().mockResolvedValue({
        github_repo_path: 'test/repo',
        git_branch: 'feature',
        git_path: 'nodes/',
        git_only: false,
        parent_namespace: 'test.main',
        branch_namespace: 'test.feature',
      }),
      getNamespaceBranches: vi.fn().mockResolvedValue([]),
      getPullRequest: vi.fn().mockResolvedValue({
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
      namespaceSources: vi.fn().mockResolvedValue({
        // No git deployments so isGitManaged stays false and editable-branch controls show.
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
          branch_namespace: 'test.feature',
        })
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'main',
          git_path: 'nodes/',
        }),
      getNamespaceBranches: vi.fn().mockResolvedValue([]),
      getPullRequest: vi.fn().mockResolvedValue(null),
      syncNamespaceToGit: vi.fn().mockResolvedValue({
        files_synced: 3,
        commit_sha: 'abc123',
        commit_url: 'https://github.com/test/repo/commit/abc123',
      }),
      createPullRequest: vi.fn().mockResolvedValue({
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
      namespaceSources: vi.fn().mockResolvedValue({
        // No git deployments so isGitManaged stays false and editable-branch controls show.
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
          branch_namespace: 'test.feature',
        })
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'main',
          git_path: 'nodes/',
        }),
      getNamespaceBranches: vi.fn().mockResolvedValue([]),
      getPullRequest: vi.fn().mockResolvedValue(null),
      deleteBranch: vi.fn().mockResolvedValue({ success: true }),
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
      expect(
        screen.getByRole('button', { name: 'Delete branch' }),
      ).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole('button', { name: 'Delete branch' }));

    await waitFor(() => {
      expect(screen.getByRole('checkbox')).toBeInTheDocument();
    });

    // The modal's submit button is named "Delete Branch"; click it.
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
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'git',
          repository: 'test/repo',
          branch: 'feature',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
          branch_namespace: 'test.feature',
        })
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'main',
          git_path: 'nodes/',
        }),
      getNamespaceBranches: vi.fn().mockResolvedValue([]),
      getPullRequest: vi.fn().mockResolvedValue(null),
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
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation();

    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'git',
          repository: 'test/repo',
          branch: 'feature',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
          branch_namespace: 'test.feature',
        })
        .mockRejectedValueOnce(new Error('Parent not found')),
      getNamespaceBranches: vi.fn().mockResolvedValue([]),
      getPullRequest: vi.fn().mockResolvedValue(null),
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
      namespaceSources: vi.fn().mockResolvedValue({
        // No git deployments so isGitManaged stays false and editable-branch controls show.
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
          branch_namespace: 'test.feature',
        })
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'main',
          git_path: 'nodes/',
        }),
      getNamespaceBranches: vi.fn().mockResolvedValue([]),
      getPullRequest: vi.fn().mockRejectedValue(new Error('API Error')),
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
    const onGitConfigLoaded = vi.fn();
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi.fn().mockResolvedValue({
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
    const onGitConfigLoaded = vi.fn();
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi
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
    vi.spyOn(window, 'confirm').mockReturnValue(true);
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi.fn().mockResolvedValue({
        github_repo_path: 'test/repo',
        git_branch: 'main',
        git_path: 'nodes/',
        git_only: false,
      }),
      deleteNamespaceGitConfig: vi.fn().mockResolvedValue({ success: true }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    // Flat (read-only) state: Git settings is accessible via the Git menu.
    await waitFor(() => {
      expect(screen.getByText('Git settings')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Git settings'));

    await waitFor(() => {
      expect(screen.getByText('Git Configuration')).toBeInTheDocument();
    });

    // The config has git_branch so detectShape returns 'flat' → modal opens with
    // source='git'. Switch to "Edited in DataJunction" then Save to trigger onRemove
    // which calls deleteNamespaceGitConfig.
    fireEvent.click(
      screen.getByRole('button', { name: 'Edited in DataJunction' }),
    );

    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => {
      expect(mockDjClient.deleteNamespaceGitConfig).toHaveBeenCalledWith(
        'test.namespace',
      );
    });
  });

  it('should handle sync error in handleCreatePR', async () => {
    const mockDjClient = {
      namespaceSources: vi.fn().mockResolvedValue({
        // No git deployments so isGitManaged stays false and editable-branch controls show.
        total_deployments: 0,
        primary_source: null,
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi
        .fn()
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'feature',
          git_path: 'nodes/',
          git_only: false,
          parent_namespace: 'test.main',
          branch_namespace: 'test.feature',
        })
        .mockResolvedValueOnce({
          github_repo_path: 'test/repo',
          git_branch: 'main',
          git_path: 'nodes/',
        }),
      getNamespaceBranches: vi.fn().mockResolvedValue([]),
      getPullRequest: vi.fn().mockResolvedValue(null),
      syncNamespaceToGit: vi.fn().mockResolvedValue({
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

  describe('git toolbar (formerly git strip)', () => {
    it('renders read-only toolbar (no Unlink, no strip) for a flat namespace', async () => {
      renderHeader({
        namespace: 'magnesium.tech',
        gitConfig: {
          github_repo_path: 'corp/tech-realtime-guardrails',
          git_branch: 'main',
        },
      });
      // Status strip removed; verify the Git menu is present instead.
      await waitFor(() => {
        expect(screen.getByTestId('git-menu')).toBeInTheDocument();
      });
      // No .git-status-strip element
      expect(document.querySelector('.git-status-strip')).toBeNull();
      // Repo info shown in Git menu header
      expect(
        screen.getByText(/corp\/tech-realtime-guardrails/),
      ).toBeInTheDocument();
      // Git settings button present via the Git menu
      expect(
        screen.getAllByRole('button', { name: /git settings/i }).length,
      ).toBeGreaterThanOrEqual(1);
      // Unlink button NOT present
      expect(
        screen.queryByRole('button', { name: /unlink/i }),
      ).not.toBeInTheDocument();
      // "Edited in DataJunction" NOT present
      expect(
        screen.queryByText(/Edited in DataJunction/),
      ).not.toBeInTheDocument();
      // "Connect to Git" NOT present
      expect(
        screen.queryByRole('button', { name: /connect to git/i }),
      ).not.toBeInTheDocument();
    });

    it('renders read-only controls when sources signal git deployment (empty gitConfig)', async () => {
      // isGitDeployed should trigger even without a gitConfig
      const mockDjClient = {
        namespaceSources: vi.fn().mockResolvedValue({
          total_deployments: 3,
          primary_source: {
            type: 'git',
            repository: 'corp/repo',
            branch: 'main',
          },
        }),
        listDeployments: vi.fn().mockResolvedValue([]),
        getNamespaceGitConfig: vi
          .fn()
          .mockRejectedValue(new Error('no config')),
        deleteNamespaceGitConfig: vi.fn().mockResolvedValue({ success: true }),
        updateNamespaceGitConfig: vi.fn().mockResolvedValue({}),
      };
      render(
        <MemoryRouter>
          <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
            <NamespaceHeader namespace="corp.deployed" />
          </DJClientContext.Provider>
        </MemoryRouter>,
      );
      // isGitDeployed → isGitManaged → read-only badge shown
      await waitFor(() => {
        expect(screen.getByText('Read-only')).toBeInTheDocument();
      });
      // Status strip removed
      expect(document.querySelector('.git-status-strip')).toBeNull();
      expect(
        screen.queryByText(/Edited in DataJunction/),
      ).not.toBeInTheDocument();
      expect(
        screen.queryByRole('button', { name: /connect to git/i }),
      ).not.toBeInTheDocument();
      expect(
        screen.queryByRole('button', { name: /unlink/i }),
      ).not.toBeInTheDocument();
    });

    it('renders Connect to Git button for a non-git namespace (no strip, no "Edited in DJ" text)', async () => {
      renderHeader({ namespace: 'some.sandbox', gitConfig: {} });
      // Plain (not-git) state: single Connect to Git button, no strip text.
      await waitFor(() => {
        expect(
          screen.getByRole('button', { name: /connect to git/i }),
        ).toBeInTheDocument();
      });
      expect(document.querySelector('.git-status-strip')).toBeNull();
      // "Edited in DataJunction" was strip text — no longer rendered.
      expect(
        screen.queryByText(/Edited in DataJunction/i),
      ).not.toBeInTheDocument();
    });

    it('shows read-only Git menu for flat namespace even when git_only is false', async () => {
      // detectShape returns 'flat' when github_repo_path + git_branch present.
      // isReadOnly is true → isGitManaged is true → read-only toolbar (GitMenu, no strip).
      renderHeader({
        namespace: 'flat.ns',
        gitConfig: {
          github_repo_path: 'corp/some-repo',
          git_branch: 'main',
          git_only: false,
        },
      });
      await waitFor(() => {
        expect(screen.getByTestId('git-menu')).toBeInTheDocument();
      });
      // Status strip is removed.
      expect(document.querySelector('.git-status-strip')).toBeNull();
    });
  });

  describe('handleRemoveGitConfig confirm guard', () => {
    it('Unlink button is NOT rendered on the page (only accessible via modal)', async () => {
      // handleRemoveGitConfig still exists for modal use, but is not exposed
      // as a standalone page button. The status strip (which previously had it)
      // has been removed entirely.
      renderHeader({
        namespace: 'flat.ns',
        gitConfig: {
          github_repo_path: 'corp/some-repo',
          git_branch: 'main',
          git_only: false,
        },
      });

      // Wait for the Git menu to appear (flat namespace → isGitManaged → GitMenu).
      await waitFor(() => {
        expect(screen.getByTestId('git-menu')).toBeInTheDocument();
      });

      expect(
        screen.queryByRole('button', { name: /unlink/i }),
      ).not.toBeInTheDocument();
    });
  });

  describe('git toolbar states', () => {
    it('read-only namespace shows New Branch + Git menu, no edit actions, no strip', async () => {
      // shape "root": github_repo_path + default_branch, no git_branch, no parent
      renderHeaderState({
        namespace: 'ads',
        gitConfig: {
          github_repo_path: 'corp/ads-dj',
          default_branch: 'main',
          git_root_namespace: 'ads',
        },
      });
      expect(await screen.findByText('New Branch')).toBeInTheDocument();
      expect(screen.getByText('Git settings')).toBeInTheDocument();
      expect(screen.queryByText('Sync to Git')).not.toBeInTheDocument();
      expect(screen.queryByText('Create PR')).not.toBeInTheDocument();
      expect(screen.queryByText('Delete branch')).not.toBeInTheDocument();
      expect(screen.queryByText(/Deployed from Git/)).not.toBeInTheDocument();
      expect(document.querySelector('.git-status-strip')).toBeNull();
      // exactly one "Git settings" entry point
      expect(screen.getAllByText('Git settings')).toHaveLength(1);
    });

    it('editable branch shows Sync to Git + Create PR inline and a Git menu', async () => {
      // shape "branch": has parent_namespace; not git-deployed => editable
      renderHeaderState({
        namespace: 'ads.feature',
        gitConfig: {
          github_repo_path: 'corp/ads-dj',
          git_branch: 'feature',
          parent_namespace: 'ads',
          branch_namespace: 'ads.feature',
          git_root_namespace: 'ads',
        },
        sources: { total_deployments: 0, primary_source: null },
      });
      expect(await screen.findByText('Sync to Git')).toBeInTheDocument();
      expect(screen.getByText('Create PR')).toBeInTheDocument();
      expect(screen.getByText('Delete branch')).toBeInTheDocument();
      expect(screen.queryByText('Git settings')).not.toBeInTheDocument();
      expect(document.querySelector('.git-status-strip')).toBeNull();
    });

    it('plain (not-git) namespace shows a single Connect to Git control', async () => {
      renderHeaderState({ namespace: 'plain.ns', gitConfig: {} });
      expect(await screen.findByText('Connect to Git')).toBeInTheDocument();
      expect(screen.queryByTestId('git-menu')).toBeNull();
      expect(screen.queryByText('Sync to Git')).not.toBeInTheDocument();
    });

    it('fires onReadOnlyChange(true) for a git-managed namespace', async () => {
      const onReadOnlyChange = vi.fn();
      const mockDjClient = {
        namespaceSources: vi
          .fn()
          .mockResolvedValue({ total_deployments: 0, primary_source: null }),
        listDeployments: vi.fn().mockResolvedValue([]),
        getNamespaceGitConfig: vi.fn().mockResolvedValue({
          github_repo_path: 'corp/ads-dj',
          default_branch: 'main',
          git_root_namespace: 'ads',
        }),
        getPullRequest: vi.fn().mockResolvedValue(null),
      };
      render(
        <MemoryRouter>
          <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
            <NamespaceHeader
              namespace="ads"
              onReadOnlyChange={onReadOnlyChange}
            />
          </DJClientContext.Provider>
        </MemoryRouter>,
      );
      await waitFor(() => expect(onReadOnlyChange).toHaveBeenCalledWith(true));
    });

    it('fires onReadOnlyChange(false) for a plain namespace', async () => {
      const onReadOnlyChange = vi.fn();
      const mockDjClient = {
        namespaceSources: vi
          .fn()
          .mockResolvedValue({ total_deployments: 0, primary_source: null }),
        listDeployments: vi.fn().mockResolvedValue([]),
        getNamespaceGitConfig: vi.fn().mockResolvedValue({}),
        getPullRequest: vi.fn().mockResolvedValue(null),
      };
      render(
        <MemoryRouter>
          <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
            <NamespaceHeader
              namespace="plain.ns"
              onReadOnlyChange={onReadOnlyChange}
            />
          </DJClientContext.Provider>
        </MemoryRouter>,
      );
      await waitFor(() => expect(onReadOnlyChange).toHaveBeenCalledWith(false));
    });
  });

  describe('manage git from the branch (option B)', () => {
    const LocationDisplay = () => (
      <div data-testid="loc">
        {useLocation().pathname}
        {useLocation().search}
      </div>
    );

    // Read-only default branch: deployed from git, has a parent (the root).
    const deployedBranchClient = updateSpy => ({
      namespaceSources: vi.fn().mockResolvedValue({
        total_deployments: 1,
        primary_source: {
          type: 'git',
          repository: 'corp/ads-dj',
          branch: 'main',
        },
      }),
      listDeployments: vi.fn().mockResolvedValue([]),
      getPullRequest: vi.fn().mockResolvedValue(null),
      getNamespaceBranches: vi.fn().mockResolvedValue([]),
      getNamespaceGitConfig: vi.fn().mockImplementation(ns =>
        Promise.resolve(
          ns === 'ads'
            ? {
                github_repo_path: 'corp/ads-dj',
                default_branch: 'main',
                git_root_namespace: 'ads',
              }
            : {
                github_repo_path: 'corp/ads-dj',
                git_branch: 'main',
                parent_namespace: 'ads',
                branch_namespace: 'ads.main',
                git_root_namespace: 'ads',
              },
        ),
      ),
      updateNamespaceGitConfig:
        updateSpy ||
        vi.fn().mockResolvedValue({ github_repo_path: 'corp/ads-dj' }),
    });

    it('read-only default branch: Git settings edits the ROOT in place', async () => {
      const updateSpy = vi
        .fn()
        .mockResolvedValue({ github_repo_path: 'corp/ads-dj' });
      render(
        <MemoryRouter initialEntries={['/namespaces/ads.main']}>
          <DJClientContext.Provider
            value={{ DataJunctionAPI: deployedBranchClient(updateSpy) }}
          >
            <LocationDisplay />
            <NamespaceHeader namespace="ads.main" />
          </DJClientContext.Provider>
        </MemoryRouter>,
      );
      fireEvent.click(await screen.findByText('Git settings'));
      // Modal opens in place (no navigation).
      expect(await screen.findByText('Git Configuration')).toBeInTheDocument();
      expect(screen.getByTestId('loc').textContent).toBe(
        '/namespaces/ads.main',
      );
      // Wait for the modal to preselect from the ROOT's config (async fetch)
      // before saving — otherwise it defaults to "not-git" and Save would remove.
      await screen.findByDisplayValue('corp/ads-dj');
      // Saving targets the ROOT namespace, not the branch.
      fireEvent.click(screen.getByText('Save'));
      await waitFor(() =>
        expect(updateSpy).toHaveBeenCalledWith('ads', expect.anything()),
      );
    });

    it('editable working branch: NO Git settings item', async () => {
      const client = {
        namespaceSources: vi
          .fn()
          .mockResolvedValue({ total_deployments: 0, primary_source: null }),
        listDeployments: vi.fn().mockResolvedValue([]),
        getPullRequest: vi.fn().mockResolvedValue(null),
        getNamespaceBranches: vi.fn().mockResolvedValue([]),
        getNamespaceGitConfig: vi.fn().mockResolvedValue({
          github_repo_path: 'corp/ads-dj',
          git_branch: 'random_branch',
          parent_namespace: 'ads',
          branch_namespace: 'ads.random_branch',
          git_root_namespace: 'ads',
        }),
      };
      render(
        <MemoryRouter initialEntries={['/namespaces/ads.random_branch']}>
          <DJClientContext.Provider value={{ DataJunctionAPI: client }}>
            <NamespaceHeader namespace="ads.random_branch" />
          </DJClientContext.Provider>
        </MemoryRouter>,
      );
      // The edit->ship loop is present...
      expect(await screen.findByText('Sync to Git')).toBeInTheDocument();
      expect(screen.getByText('Create PR')).toBeInTheDocument();
      // ...but no repo-binding management here.
      expect(screen.queryByText('Git settings')).not.toBeInTheDocument();
    });

    it('read-only default branch: New Branch survives a branch-side save with thin PATCH response', async () => {
      // Regression guard for the merge-vs-replace bug: when a save returns a
      // PATCH response that omits `default_branch`, parentGitConfig must be
      // MERGED (not replaced) so rootDefaultBranch stays set and canCreateBranch
      // remains true — keeping the "+ New Branch" control visible.
      const updateSpy = vi
        .fn()
        .mockResolvedValue({ github_repo_path: 'corp/ads-dj' }); // NO default_branch
      render(
        <MemoryRouter initialEntries={['/namespaces/ads.main']}>
          <DJClientContext.Provider
            value={{ DataJunctionAPI: deployedBranchClient(updateSpy) }}
          >
            <NamespaceHeader namespace="ads.main" />
          </DJClientContext.Provider>
        </MemoryRouter>,
      );
      // "New Branch" must be present before any save.
      expect(await screen.findByText('New Branch')).toBeInTheDocument();
      // Open Git settings modal.
      fireEvent.click(screen.getByText('Git settings'));
      // Wait for modal to pre-populate from the ROOT's config.
      await screen.findByDisplayValue('corp/ads-dj');
      // Save — returns the thin PATCH response (no default_branch).
      fireEvent.click(screen.getByText('Save'));
      await waitFor(() =>
        expect(updateSpy).toHaveBeenCalledWith('ads', expect.anything()),
      );
      // "New Branch" must still be present after the save.
      expect(screen.getByText('New Branch')).toBeInTheDocument();
    });

    it('flat namespace: Git settings edits ITSELF', async () => {
      const updateSpy = vi.fn().mockResolvedValue({
        github_repo_path: 'corp/tech-realtime-guardrails',
      });
      const client = {
        namespaceSources: vi
          .fn()
          .mockResolvedValue({ total_deployments: 0, primary_source: null }),
        listDeployments: vi.fn().mockResolvedValue([]),
        getPullRequest: vi.fn().mockResolvedValue(null),
        getNamespaceBranches: vi.fn().mockResolvedValue([]),
        getNamespaceGitConfig: vi.fn().mockResolvedValue({
          github_repo_path: 'corp/tech-realtime-guardrails',
          git_branch: 'main',
          git_root_namespace: 'magnesium.tech',
        }),
        updateNamespaceGitConfig: updateSpy,
      };
      render(
        <MemoryRouter initialEntries={['/namespaces/magnesium.tech']}>
          <DJClientContext.Provider value={{ DataJunctionAPI: client }}>
            <NamespaceHeader namespace="magnesium.tech" />
          </DJClientContext.Provider>
        </MemoryRouter>,
      );
      fireEvent.click(await screen.findByText('Git settings'));
      // Wait for the modal to preselect from its OWN config before saving.
      await screen.findByDisplayValue('corp/tech-realtime-guardrails');
      fireEvent.click(screen.getByText('Save'));
      await waitFor(() =>
        expect(updateSpy).toHaveBeenCalledWith(
          'magnesium.tech',
          expect.anything(),
        ),
      );
    });
  });
});
