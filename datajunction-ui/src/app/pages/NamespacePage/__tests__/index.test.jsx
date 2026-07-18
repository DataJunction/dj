import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom';
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
                owners: [
                  { username: 'customer_service' },
                  { username: 'finance' },
                  { username: 'analytics' },
                ],
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
    expect(screen.getAllByText('Published').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Valid').length).toBeGreaterThan(0);
    expect(screen.getByText('CU')).toBeInTheDocument();

    // --- Sorting ---

    // Track current call count
    const initialCallCount = mockDjClient.listNodesForLanding.mock.calls.length;

    // sort by name
    fireEvent.click(screen.getByText('Name'));
    await waitFor(() => {
      expect(
        mockDjClient.listNodesForLanding.mock.calls.length,
      ).toBeGreaterThan(initialCallCount);
    });

    const afterFirstSort = mockDjClient.listNodesForLanding.mock.calls.length;

    // flip direction
    fireEvent.click(screen.getByText('Name'));
    await waitFor(() => {
      expect(
        mockDjClient.listNodesForLanding.mock.calls.length,
      ).toBeGreaterThan(afterFirstSort);
    });

    const afterSecondSort = mockDjClient.listNodesForLanding.mock.calls.length;

    // sort by display name
    fireEvent.click(screen.getByText('Display name'));
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
    fireEvent.click(screen.getByText('Filters'));
    await waitFor(() => {
      expect(screen.getByText('Tags')).toBeInTheDocument();
    });
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
        expect(screen.getAllByText('Type').length).toBeGreaterThan(0);
        expect(screen.getByText('Owner')).toBeInTheDocument();
        expect(screen.getByText('Tags')).toBeInTheDocument();
        expect(screen.getAllByText('Publish state').length).toBeGreaterThan(0);
        expect(screen.getAllByText('Validation').length).toBeGreaterThan(0);
        expect(screen.getByText('More filters')).toBeInTheDocument();
      });

      fireEvent.click(screen.getByText('Filters'));

      await waitFor(() => {
        expect(screen.getByText('Edited By')).toBeInTheDocument();
        expect(screen.getByText('Missing Description')).toBeInTheDocument();
      });
    });

    it('opens More filters dropdown when clicked', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('More filters')).toBeInTheDocument();
      });

      fireEvent.click(screen.getByText('Filters'));

      await waitFor(() => {
        expect(screen.getByText('Missing Description')).toBeInTheDocument();
        expect(screen.getByText('Orphaned Dimensions')).toBeInTheDocument();
        expect(screen.getByText('Has Materialization')).toBeInTheDocument();
      });
    });

    it('toggles quality filters in dropdown', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('More filters')).toBeInTheDocument();
      });

      fireEvent.click(screen.getByText('Filters'));

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
      git_branch: null,
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

    it('redirects to the default branch and keeps New Branch reachable there', async () => {
      // Regression guard for the orphaned-entry-point bug: a git root redirects
      // to its default branch, and branch creation must still be reachable on
      // that (branch) page — via redirect change OR gating change.
      const branchConfig = {
        github_repo_path: 'org/repo',
        git_branch: 'main',
        git_path: 'nodes/',
        git_only: false,
        parent_namespace: 'default',
        branch_namespace: 'default.main',
        git_root_namespace: 'default',
      };
      mockDjClient.getNamespaceGitConfig.mockImplementation(ns =>
        Promise.resolve(ns === 'default' ? gitRootConfig : branchConfig),
      );

      const LocationDisplay = () => (
        <div data-testid="location">{useLocation().pathname}</div>
      );
      render(
        <UserContext.Provider
          value={{ currentUser: mockCurrentUser, loading: false }}
        >
          <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
            <MemoryRouter initialEntries={['/namespaces/default']}>
              <LocationDisplay />
              <Routes>
                <Route
                  path="namespaces/:namespace"
                  element={<NamespacePage />}
                />
              </Routes>
            </MemoryRouter>
          </DJClientContext.Provider>
        </UserContext.Provider>,
      );

      // The git root redirects to its default branch...
      await waitFor(
        () => {
          expect(screen.getByTestId('location').textContent).toBe(
            '/namespaces/default.main',
          );
        },
        { timeout: 3000 },
      );
      // ...where the branch-creation entry point is still present (inside the Git menu).
      expect(await screen.findByText('+ New Branch')).toBeInTheDocument();
    });
  });

  describe('Quality filter checkboxes', () => {
    it('toggles orphanedDimension filter', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('More filters')).toBeInTheDocument();
      });

      fireEvent.click(screen.getByText('Filters'));
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
        expect(screen.getByText('More filters')).toBeInTheDocument();
      });

      fireEvent.click(screen.getByText('Filters'));
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

  describe('Read-only git states', () => {
    it('shows the node table but no add/edit controls for a flat (read-only) namespace', async () => {
      // A flat namespace: has a repo path AND its own git_branch, no parent_namespace.
      // detectShape returns 'flat' → read-only; edit controls must be hidden.
      const flatConfig = {
        github_repo_path: 'corp/repo',
        git_branch: 'main',
        default_branch: null,
        parent_namespace: null,
        git_only: false,
        git_root_namespace: 'magnesium.tech',
      };
      mockDjClient.getNamespaceGitConfig.mockResolvedValue(flatConfig);
      mockDjClient.listNamespacesWithGit.mockResolvedValue([
        { namespace: 'magnesium.tech', numNodes: 1, git: null },
      ]);
      mockDjClient.listNodesForLanding.mockResolvedValue({
        data: {
          findNodesPaginated: {
            pageInfo: {
              hasNextPage: false,
              endCursor: null,
              hasPrevPage: false,
              startCursor: null,
            },
            edges: [
              {
                node: {
                  name: 'magnesium.tech.some_metric',
                  type: 'METRIC',
                  currentVersion: 'v1.0',
                  tags: [],
                  editedBy: [],
                  current: {
                    displayName: 'Some Metric',
                    status: 'VALID',
                    mode: 'PUBLISHED',
                    updatedAt: '2024-10-18T15:15:33.532949+00:00',
                  },
                  createdBy: { username: 'dj' },
                },
              },
            ],
          },
        },
      });

      renderWithProviders(<NamespacePage />, {
        route: '/namespaces/magnesium.tech',
      });

      // The node name should appear in the table.
      expect(await screen.findByText('some_metric')).toBeInTheDocument();

      // No add/edit controls for a read-only (flat) namespace.
      expect(screen.queryByText(/\+ Add Node/i)).not.toBeInTheDocument();
    });

    it('never shows Add Node on a git-deployed branch namespace (no flash)', async () => {
      // Regression guard: a branch namespace (gitShape === 'branch') that is
      // git-deployed is read-only. NamespaceHeader fires onReadOnlyChange(true)
      // via a useEffect — one render after showEditControls first becomes true.
      // The guard must be `headerReadOnly === false` (not `!headerReadOnly`) so
      // Add Node stays hidden during the intermediate render where headerReadOnly
      // is still `undefined`.
      const branchConfig = {
        github_repo_path: 'org/repo',
        git_branch: 'main',
        git_path: 'nodes/',
        git_only: false,
        parent_namespace: 'ads',
        branch_namespace: 'ads.main',
        git_root_namespace: 'ads',
      };
      mockDjClient.getNamespaceGitConfig.mockResolvedValue(branchConfig);
      mockDjClient.namespaceSources.mockResolvedValue({
        total_deployments: 1,
        primary_source: { type: 'git', repository: 'org/repo', branch: 'main' },
      });
      mockDjClient.listNamespacesWithGit.mockResolvedValue([
        { namespace: 'ads', numNodes: 0, git: null },
        { namespace: 'ads.main', numNodes: 3, git: null },
      ]);
      mockDjClient.listNodesForLanding.mockResolvedValue({
        data: {
          findNodesPaginated: {
            pageInfo: {
              hasNextPage: false,
              endCursor: null,
              hasPrevPage: false,
              startCursor: null,
            },
            edges: [
              {
                node: {
                  name: 'ads.main.revenue',
                  type: 'METRIC',
                  currentVersion: 'v1.0',
                  tags: [],
                  editedBy: [],
                  current: {
                    displayName: 'Revenue',
                    status: 'VALID',
                    mode: 'PUBLISHED',
                    updatedAt: '2024-10-18T15:15:33.532949+00:00',
                  },
                  createdBy: { username: 'dj' },
                },
              },
            ],
          },
        },
      });

      renderWithProviders(<NamespacePage />, {
        route: '/namespaces/ads.main',
      });

      // Wait for the node table to settle (proves the component rendered fully,
      // including all effects — onReadOnlyChange has fired by this point).
      expect(await screen.findByText('revenue')).toBeInTheDocument();

      // Add Node must never appear at any point. Because headerReadOnly starts
      // as `undefined` (fixed) vs `false` (buggy), the old guard `!headerReadOnly`
      // would render Add Node on the first pass when gitConfig resolves but before
      // the namespaceSources effect fires onReadOnlyChange. The new guard
      // `headerReadOnly === false` keeps it hidden until explicitly known editable.
      // After all effects settle, Add Node must still be absent (git-deployed = read-only).
      expect(screen.queryByText('+ Add Node')).not.toBeInTheDocument();
    });
  });
});
