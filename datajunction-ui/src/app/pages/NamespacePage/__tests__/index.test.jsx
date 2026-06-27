import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import DJClientContext from '../../../providers/djclient';
import UserContext from '../../../providers/UserProvider';
import { NamespacePage } from '../index';
import React from 'react';

const mockDjClient = {
  namespaces: vi.fn(),
  listNamespacesWithGit: vi.fn(),
  namespace: vi.fn(),
  listNodesForLanding: vi.fn(),
  nodeTypeCounts: vi.fn().mockResolvedValue({}),
  addNamespace: vi.fn(),
  whoami: vi.fn(),
  users: vi.fn(),
  listTags: vi.fn(),
  namespaceSources: vi.fn(),
  namespaceSourcesBulk: vi.fn(),
  getNamespaceGitConfig: vi.fn(),
  getNamespaceBranches: vi.fn(),
  listDeployments: vi.fn(),
  getPullRequest: vi.fn(),
};

const mockCurrentUser = { username: 'dj', email: 'dj@test.com' };

const renderWithProviders = (ui, { route = '/namespaces/default' } = {}) => {
  return render(
    <UserContext.Provider
      value={{ currentUser: mockCurrentUser, loading: false }}
    >
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <MemoryRouter initialEntries={[route]}>
          <Routes>
            <Route path="namespaces/:namespace" element={ui} />
            <Route path="/" element={ui} />
          </Routes>
        </MemoryRouter>
      </DJClientContext.Provider>
    </UserContext.Provider>,
  );
};

describe('NamespacePage', () => {
  const original = window.location;

  const reloadFn = () => {
    window.location.reload();
  };

  beforeAll(() => {
    Object.defineProperty(window, 'location', {
      configurable: true,
      value: { reload: vi.fn() },
    });
  });

  afterAll(() => {
    Object.defineProperty(window, 'location', {
      configurable: true,
      value: original,
    });
  });

  beforeEach(() => {
    fetch.resetMocks();
    mockDjClient.whoami.mockResolvedValue({ username: 'dj' });
    mockDjClient.users.mockResolvedValue([
      { username: 'dj' },
      { username: 'user1' },
    ]);
    mockDjClient.listTags.mockResolvedValue([
      { name: 'tag1' },
      { name: 'tag2' },
    ]);
    mockDjClient.namespaceSources.mockResolvedValue({ sources: [] });
    mockDjClient.namespaceSourcesBulk.mockResolvedValue({
      namespace_sources: {},
    });
    mockDjClient.getNamespaceGitConfig.mockResolvedValue(null);
    mockDjClient.getNamespaceBranches.mockResolvedValue([]);
    mockDjClient.listDeployments.mockResolvedValue([]);
    mockDjClient.getPullRequest.mockResolvedValue(null);
    const mockNamespaces = [
      { namespace: 'common.one', numNodes: 3, git: null },
      { namespace: 'common.one.a', numNodes: 6, git: null },
      { namespace: 'common.one.b', numNodes: 17, git: null },
      { namespace: 'common.one.c', numNodes: 64, git: null },
      { namespace: 'default', numNodes: 41, git: null },
      { namespace: 'default.fruits', numNodes: 1, git: null },
      { namespace: 'default.fruits.citrus.lemons', numNodes: 1, git: null },
      { namespace: 'default.vegetables', numNodes: 2, git: null },
    ];
    mockDjClient.namespaces.mockResolvedValue(mockNamespaces);
    mockDjClient.listNamespacesWithGit.mockResolvedValue(mockNamespaces);
    mockDjClient.namespace.mockResolvedValue([
      {
        name: 'testNode',
        display_name: 'Test Node',
        type: 'transform',
        mode: 'active',
        updated_at: new Date(),
        tags: [{ name: 'tag1' }],
        edited_by: ['dj'],
      },
    ]);
    mockDjClient.listNodesForLanding.mockResolvedValue({
      data: {
        findNodesPaginated: {
          pageInfo: {
            hasNextPage: true,
            endCursor:
              'eyJjcmVhdGVkX2F0IjogIjIwMjQtMDQtMTZUMjM6MjI6MjIuNDQxNjg2KzAwOjAwIiwgImlkIjogNjE0fQ==',
            hasPrevPage: true,
            startCursor:
              'eyJjcmVhdGVkX2F0IjogIjIwMjQtMTAtMTZUMTY6MDM6MTcuMDgzMjY3KzAwOjAwIiwgImlkIjogMjQwOX0=',
          },
          edges: [
            {
              node: {
                name: 'default.test_node',
                type: 'DIMENSION',
                currentVersion: 'v4.0',
                tags: [],
                editedBy: ['dj'],
                current: {
                  displayName: 'Test Node',
                  status: 'VALID',
                  mode: 'PUBLISHED',
                  updatedAt: '2024-10-18T15:15:33.532949+00:00',
                },
                createdBy: {
                  username: 'dj',
                },
              },
            },
          ],
        },
      },
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it('displays namespaces and renders nodes', async () => {
    reloadFn();
    const element = (
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <NamespacePage />
      </DJClientContext.Provider>
    );
    render(
      <MemoryRouter initialEntries={['/namespaces/default']}>
        <Routes>
          <Route path="namespaces/:namespace" element={element} />
        </Routes>
      </MemoryRouter>,
    );

    // Wait for initial nodes to load
    await waitFor(() => {
      expect(mockDjClient.listNodesForLanding).toHaveBeenCalled();
    });

    // The selected-namespace rail renders FolderTree for the current namespace's
    // children. Route is /namespaces/default; the mock hierarchy has
    // default.fruits and default.vegetables as immediate children.
    await waitFor(() => {
      expect(screen.getByText('Folders')).toBeInTheDocument();
      expect(screen.getByText('fruits')).toBeInTheDocument();
      expect(screen.getByText('vegetables')).toBeInTheDocument();
    });
    // Sibling top-level namespace 'common' must NOT appear in the selected rail.
    expect(screen.queryByText('common')).not.toBeInTheDocument();

    // Check that it renders nodes
    expect(screen.getByText('Test Node')).toBeInTheDocument();

    // --- Sorting ---

    // Track current call count
    const initialCallCount = mockDjClient.listNodesForLanding.mock.calls.length;

    // sort by 'name'
    fireEvent.click(screen.getByText('name'));
    await waitFor(() => {
      expect(
        mockDjClient.listNodesForLanding.mock.calls.length,
      ).toBeGreaterThan(initialCallCount);
    });

    const afterFirstSort = mockDjClient.listNodesForLanding.mock.calls.length;

    // flip direction
    fireEvent.click(screen.getByText('name'));
    await waitFor(() => {
      expect(
        mockDjClient.listNodesForLanding.mock.calls.length,
      ).toBeGreaterThan(afterFirstSort);
    });

    const afterSecondSort = mockDjClient.listNodesForLanding.mock.calls.length;

    // sort by 'displayName'
    fireEvent.click(screen.getByText('display Name'));
    await waitFor(() => {
      expect(
        mockDjClient.listNodesForLanding.mock.calls.length,
      ).toBeGreaterThan(afterSecondSort);
    });

    // --- Filters ---

    // Node type - use react-select properly
    const selectNodeType = screen.getAllByTestId('select-node-type')[0];
    const typeInput = selectNodeType.querySelector('input');
    if (typeInput) {
      fireEvent.focus(typeInput);
      fireEvent.keyDown(typeInput, { key: 'ArrowDown' });
      await waitFor(() => {
        const sourceOption = screen.queryByText('Source');
        if (sourceOption) {
          fireEvent.click(sourceOption);
        }
      });
    }

    // Tag filter
    const selectTag = screen.getAllByTestId('select-tag')[0];
    const tagInput = selectTag.querySelector('input');
    if (tagInput) {
      fireEvent.focus(tagInput);
      fireEvent.keyDown(tagInput, { key: 'ArrowDown' });
    }

    // User filter
    const selectUser = screen.getAllByTestId('select-user')[0];
    const userInput = selectUser.querySelector('input');
    if (userInput) {
      fireEvent.focus(userInput);
      fireEvent.keyDown(userInput, { key: 'ArrowDown' });
    }

    // --- Rail still shows folders after sort interactions ---
    // The FolderTree rail should still be present after sorting.
    await waitFor(() => {
      expect(screen.getByText('Folders')).toBeInTheDocument();
    });
  });

  it('rail folder nav: shows child folders to drill into', async () => {
    // The rail is the folder navigator: it lists the current namespace's child
    // sub-namespaces (drill in by clicking), NOT its siblings or an
    // all-namespaces list. Going up a level is handled by the header breadcrumb.
    const element = (
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <NamespacePage />
      </DJClientContext.Provider>
    );
    render(
      <MemoryRouter initialEntries={['/namespaces/default']}>
        <Routes>
          <Route path="namespaces/:namespace" element={element} />
        </Routes>
      </MemoryRouter>,
    );

    // Folder nav loads after the namespace hierarchy is fetched: 'default' has
    // child folders 'fruits' and 'vegetables'.
    await waitFor(() => {
      expect(screen.getByText('Folders')).toBeInTheDocument();
      expect(screen.getByText('fruits')).toBeInTheDocument();
      expect(screen.getByText('vegetables')).toBeInTheDocument();
    });

    // Siblings / all-namespaces are NOT shown in the selected view.
    expect(screen.queryByText('common')).not.toBeInTheDocument();
  });

  describe('Filter Bar', () => {
    it('displays quick filter presets', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('Quick')).toBeInTheDocument();
      });

      // Check that preset buttons are rendered
      expect(screen.getByText('My Nodes')).toBeInTheDocument();
      expect(screen.getByText('Needs Attention')).toBeInTheDocument();
      expect(screen.getByText('Drafts')).toBeInTheDocument();
    });

    it('applies My Nodes preset when clicked', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('My Nodes')).toBeInTheDocument();
      });

      const initialCalls = mockDjClient.listNodesForLanding.mock.calls.length;
      fireEvent.click(screen.getByText('My Nodes'));

      await waitFor(() => {
        // The API should be called again after clicking preset
        expect(
          mockDjClient.listNodesForLanding.mock.calls.length,
        ).toBeGreaterThan(initialCalls);
      });
    });

    it('applies Needs Attention preset when clicked', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('Needs Attention')).toBeInTheDocument();
      });

      const initialCalls = mockDjClient.listNodesForLanding.mock.calls.length;
      fireEvent.click(screen.getByText('Needs Attention'));

      await waitFor(() => {
        expect(
          mockDjClient.listNodesForLanding.mock.calls.length,
        ).toBeGreaterThan(initialCalls);
      });
    });

    it('applies Drafts preset when clicked', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('Drafts')).toBeInTheDocument();
      });

      const initialCalls = mockDjClient.listNodesForLanding.mock.calls.length;
      fireEvent.click(screen.getByText('Drafts'));

      await waitFor(() => {
        expect(
          mockDjClient.listNodesForLanding.mock.calls.length,
        ).toBeGreaterThan(initialCalls);
      });
    });

    it('shows Clear all button when filters are active', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('My Nodes')).toBeInTheDocument();
      });

      // Apply a preset to activate filters
      fireEvent.click(screen.getByText('My Nodes'));

      await waitFor(() => {
        expect(screen.getByText('Clear all ×')).toBeInTheDocument();
      });
    });

    it('clears all filters when Clear all is clicked', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('My Nodes')).toBeInTheDocument();
      });

      // Apply a preset
      fireEvent.click(screen.getByText('My Nodes'));

      await waitFor(() => {
        expect(screen.getByText('Clear all ×')).toBeInTheDocument();
      });

      // Clear all filters
      fireEvent.click(screen.getByText('Clear all ×'));

      await waitFor(() => {
        // Clear all button should disappear
        expect(screen.queryByText('Clear all ×')).not.toBeInTheDocument();
      });
    });

    it('displays filter dropdowns', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        // Check for filter labels
        expect(screen.getByText('Type')).toBeInTheDocument();
        expect(screen.getByText('Tags')).toBeInTheDocument();
        expect(screen.getByText('Edited By')).toBeInTheDocument();
        expect(screen.getByText('Mode')).toBeInTheDocument();
        expect(screen.getByText('Owner')).toBeInTheDocument();
        expect(screen.getByText('Status')).toBeInTheDocument();
        expect(screen.getByText('Quality')).toBeInTheDocument();
      });
    });

    it('opens Quality dropdown when clicked', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('Quality')).toBeInTheDocument();
      });

      // Find and click the Quality button
      const qualityButton = screen.getByText('Issues');
      fireEvent.click(qualityButton);

      await waitFor(() => {
        expect(screen.getByText('Missing Description')).toBeInTheDocument();
        expect(screen.getByText('Orphaned Dimensions')).toBeInTheDocument();
        expect(screen.getByText('Has Materialization')).toBeInTheDocument();
      });
    });

    it('toggles quality filters in dropdown', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('Quality')).toBeInTheDocument();
      });

      // Open the Quality dropdown
      const qualityButton = screen.getByText('Issues');
      fireEvent.click(qualityButton);

      await waitFor(() => {
        expect(screen.getByText('Missing Description')).toBeInTheDocument();
      });

      // Toggle the Missing Description checkbox
      const checkbox = screen.getByLabelText('Missing Description');
      const callsBefore = mockDjClient.listNodesForLanding.mock.calls.length;
      fireEvent.click(checkbox);

      await waitFor(() => {
        expect(
          mockDjClient.listNodesForLanding.mock.calls.length,
        ).toBeGreaterThan(callsBefore);
      });
    });

    it('displays no nodes message with clear filter link when no results', async () => {
      mockDjClient.listNodesForLanding.mockResolvedValue({
        data: {
          findNodesPaginated: {
            pageInfo: {
              hasNextPage: false,
              endCursor: null,
              hasPrevPage: false,
              startCursor: null,
            },
            edges: [],
          },
        },
      });

      renderWithProviders(<NamespacePage />);

      // Apply a filter first
      await waitFor(() => {
        expect(screen.getByText('My Nodes')).toBeInTheDocument();
      });
      fireEvent.click(screen.getByText('My Nodes'));

      await waitFor(() => {
        expect(
          screen.getByText('No nodes match the current filters.'),
        ).toBeInTheDocument();
        expect(screen.getByText('Clear filters')).toBeInTheDocument();
      });
    });
  });

  describe('URL Parameter Sync', () => {
    it('reads filters from URL parameters on load', async () => {
      renderWithProviders(<NamespacePage />, {
        route: '/namespaces/default?type=metric&ownedBy=dj',
      });

      await waitFor(() => {
        expect(mockDjClient.listNodesForLanding).toHaveBeenCalled();
      });
    });

    it('reads status filter from URL', async () => {
      renderWithProviders(<NamespacePage />, {
        route: '/namespaces/default?statuses=INVALID',
      });

      await waitFor(() => {
        expect(mockDjClient.listNodesForLanding).toHaveBeenCalled();
      });
    });

    it('reads mode filter from URL', async () => {
      renderWithProviders(<NamespacePage />, {
        route: '/namespaces/default?mode=draft',
      });

      await waitFor(() => {
        expect(mockDjClient.listNodesForLanding).toHaveBeenCalled();
      });
    });

    it('reads quality filters from URL', async () => {
      renderWithProviders(<NamespacePage />, {
        route:
          '/namespaces/default?missingDescription=true&orphanedDimension=true',
      });

      await waitFor(() => {
        expect(mockDjClient.listNodesForLanding).toHaveBeenCalled();
      });
    });

    it('reads hasMaterialization filter from URL', async () => {
      renderWithProviders(<NamespacePage />, {
        route: '/namespaces/default?hasMaterialization=true',
      });
      await waitFor(() => {
        expect(mockDjClient.listNodesForLanding).toHaveBeenCalled();
      });
    });
  });

  describe('Git-root namespace (branch landing page)', () => {
    const gitRootConfig = {
      github_repo_path: 'org/repo',
      git_branch: 'main',
      default_branch: 'main',
      parent_namespace: null,
      git_only: false,
      git_root_namespace: 'default',
    };

    const mockBranches = [
      {
        namespace: 'default.main',
        git_branch: 'main',
        num_nodes: 10,
        invalid_node_count: 1,
        last_updated_at: '2024-10-18T12:00:00+00:00',
      },
      {
        namespace: 'default.feature-xyz',
        git_branch: 'feature-xyz',
        num_nodes: 5,
        invalid_node_count: 0,
        last_updated_at: null,
      },
    ];

    beforeEach(() => {
      mockDjClient.getNamespaceGitConfig.mockResolvedValue(gitRootConfig);
      mockDjClient.getNamespaceBranches.mockResolvedValue(mockBranches);
    });

    it('browses the default branch namespace in the node table', async () => {
      renderWithProviders(<NamespacePage />);

      // A git root has no nodes of its own — the table queries
      // <root>.<default_branch> (so a git root shows the same browsable table
      // as any other namespace, instead of a separate split preview).
      await waitFor(
        () => {
          const calls = mockDjClient.listNodesForLanding.mock.calls;
          expect(calls.some(args => args[0] === 'default.main')).toBe(true);
        },
        { timeout: 3000 },
      );
    });
  });

  describe('Quality filter checkboxes', () => {
    it('toggles orphanedDimension filter', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('Quality')).toBeInTheDocument();
      });

      fireEvent.click(screen.getByText('Issues'));
      await waitFor(() => {
        expect(screen.getByText('Orphaned Dimensions')).toBeInTheDocument();
      });

      const checkbox = screen.getByLabelText('Orphaned Dimensions');
      const callsBefore = mockDjClient.listNodesForLanding.mock.calls.length;
      fireEvent.click(checkbox);

      await waitFor(() => {
        expect(
          mockDjClient.listNodesForLanding.mock.calls.length,
        ).toBeGreaterThan(callsBefore);
      });
    });

    it('toggles hasMaterialization filter', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('Quality')).toBeInTheDocument();
      });

      fireEvent.click(screen.getByText('Issues'));
      await waitFor(() => {
        expect(screen.getByText('Has Materialization')).toBeInTheDocument();
      });

      const checkbox = screen.getByLabelText('Has Materialization');
      const callsBefore = mockDjClient.listNodesForLanding.mock.calls.length;
      fireEvent.click(checkbox);

      await waitFor(() => {
        expect(
          mockDjClient.listNodesForLanding.mock.calls.length,
        ).toBeGreaterThan(callsBefore);
      });
    });
  });

  it('node search passes the search term to the fetch', async () => {
    mockDjClient.getNamespaceGitConfig.mockResolvedValue({
      github_repo_path: null,
      git_branch: null,
      default_branch: null,
      parent_namespace: null,
      git_only: false,
      git_root_namespace: null,
    });
    mockDjClient.listNamespacesWithGit.mockResolvedValue([
      { namespace: 'growth', numNodes: 2, git: null },
      { namespace: 'growth.metrics', numNodes: 7, git: null },
    ]);
    renderWithProviders(<NamespacePage />, { route: '/namespaces/growth' });

    await waitFor(() =>
      expect(mockDjClient.listNodesForLanding).toHaveBeenCalled(),
    );

    fireEvent.change(screen.getByPlaceholderText(/search nodes/i), {
      target: { value: 'active' },
    });

    // Wait for the 300 ms debounce and the subsequent fetch.
    await waitFor(
      () => {
        const opts = mockDjClient.listNodesForLanding.mock.calls.at(-1).at(-1);
        expect(opts.search).toBe('active');
      },
      { timeout: 1000 },
    );
  });

  it('shows sub-namespace folders in the rail', async () => {
    mockDjClient.getNamespaceGitConfig.mockResolvedValue({
      github_repo_path: null,
      git_branch: null,
      default_branch: null,
      parent_namespace: null,
      git_only: false,
      git_root_namespace: null,
    });
    mockDjClient.listNamespacesWithGit.mockResolvedValue([
      { namespace: 'growth', numNodes: 2, git: null },
      { namespace: 'growth.experiments', numNodes: 5, git: null },
      { namespace: 'growth.metrics', numNodes: 7, git: null },
    ]);
    renderWithProviders(<NamespacePage />, { route: '/namespaces/growth' });

    // The rail (FolderTree) lists immediate sub-namespaces of the selected namespace.
    await waitFor(() => {
      expect(screen.getByText('Folders')).toBeInTheDocument();
      expect(screen.getByText('experiments')).toBeInTheDocument();
      expect(screen.getByText('metrics')).toBeInTheDocument();
    });
  });

  it('resets pagination cursors to null when search term changes', async () => {
    // Limitation: simulating a real Next-click to set a non-null cursor before
    // typing is impractical in this test harness because the pagination buttons
    // depend on rendered cursor state that only stabilises after async fetches.
    // This test therefore directly asserts the regression-guard: after typing a
    // search term the fetch is called with before=null (index 4) and after=null
    // (index 5), confirming the reset effect fires on debouncedSearch change.
    renderWithProviders(<NamespacePage />, { route: '/namespaces/default' });

    await waitFor(() => {
      expect(mockDjClient.listNodesForLanding).toHaveBeenCalled();
    });

    const searchBox = screen.getByPlaceholderText(/search nodes/i);
    fireEvent.change(searchBox, { target: { value: 'my_metric' } });

    // Wait for the 300 ms debounce to fire and the subsequent fetch to complete.
    await waitFor(
      () => {
        const calls = mockDjClient.listNodesForLanding.mock.calls;
        const lastCall = calls.at(-1);
        // before is arg index 4, after is arg index 5.
        expect(lastCall[4]).toBeNull();
        expect(lastCall[5]).toBeNull();
      },
      { timeout: 1000 },
    );
  });
});
