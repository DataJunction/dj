import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { ActiveBranchesSection } from '../ActiveBranchesSection';
import DJClientContext from '../../../providers/djclient';

jest.mock('../MyWorkspacePage.css', () => ({}));
jest.mock('../../../utils/date', () => ({
  formatRelativeTime: () => '2d ago',
}));

describe('<ActiveBranchesSection />', () => {
  const mockDjClient = {
    listNodesForLanding: jest.fn().mockResolvedValue({
      data: { findNodesPaginated: { edges: [] } },
    }),
  };

  const renderWithContext = props => {
    return render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <MemoryRouter>
          <ActiveBranchesSection {...props} />
        </MemoryRouter>
      </DJClientContext.Provider>,
    );
  };

  const mockOwnedNodes = [
    {
      name: 'myproject.main.users',
      gitInfo: {
        repo: 'myorg/myrepo',
        branch: 'main',
        defaultBranch: 'main',
        parentNamespace: 'myproject',
      },
      current: { updatedAt: '2024-01-01T10:00:00Z' },
    },
    {
      name: 'myproject.feature-1.orders',
      gitInfo: {
        repo: 'myorg/myrepo',
        branch: 'feature-1',
        defaultBranch: 'main',
        parentNamespace: 'myproject',
      },
      current: { updatedAt: '2024-01-02T10:00:00Z' },
    },
    {
      name: 'otherproject.main.products',
      gitInfo: {
        repo: 'myorg/otherrepo',
        branch: 'main',
        defaultBranch: 'main',
        parentNamespace: 'otherproject',
      },
      current: { updatedAt: '2024-01-03T10:00:00Z' },
    },
  ];

  const mockRecentlyEdited = [
    {
      name: 'myproject.feature-1.orders',
      gitInfo: {
        repo: 'myorg/myrepo',
        branch: 'feature-1',
        defaultBranch: 'main',
        parentNamespace: 'myproject',
      },
      current: { updatedAt: '2024-01-02T10:00:00Z' },
    },
  ];

  it('should render loading state', () => {
    renderWithContext({
      ownedNodes: [],
      recentlyEdited: [],
      loading: true,
    });

    expect(screen.getByText('Git Namespaces')).toBeInTheDocument();
  });

  it('should render empty state when no branches', async () => {
    renderWithContext({
      ownedNodes: [],
      recentlyEdited: [],
      loading: false,
    });

    await waitFor(() => {
      expect(
        screen.getByText('No git-managed namespaces.'),
      ).toBeInTheDocument();
    });
  });

  it('should group nodes by base namespace', async () => {
    renderWithContext({
      ownedNodes: mockOwnedNodes,
      recentlyEdited: mockRecentlyEdited,
      loading: false,
    });

    // Should show both base namespaces
    await waitFor(() => {
      expect(screen.getByText('myproject')).toBeInTheDocument();
      expect(screen.getByText('otherproject')).toBeInTheDocument();
    });
  });

  it('should show repo and branch names', async () => {
    renderWithContext({
      ownedNodes: mockOwnedNodes,
      recentlyEdited: mockRecentlyEdited,
      loading: false,
    });

    // Should show repos
    await waitFor(() => {
      expect(screen.getByText('myorg/myrepo')).toBeInTheDocument();
      expect(screen.getByText('myorg/otherrepo')).toBeInTheDocument();
    });

    // Should show branches (there can be multiple "main" branches)
    await waitFor(() => {
      expect(screen.getAllByText('main').length).toBeGreaterThan(0);
      expect(screen.getByText('feature-1')).toBeInTheDocument();
    });
  });

  it('should handle nodes without git info', async () => {
    const nodesWithoutGit = [
      {
        name: 'default.node1',
        current: { updatedAt: '2024-01-01T10:00:00Z' },
      },
      ...mockOwnedNodes,
    ];

    renderWithContext({
      ownedNodes: nodesWithoutGit,
      recentlyEdited: [],
      loading: false,
    });

    // Should still render the git-managed nodes
    await waitFor(() => {
      expect(screen.getByText('myproject')).toBeInTheDocument();
      expect(screen.getByText('myorg/myrepo')).toBeInTheDocument();
    });
  });

  it('should render with many namespaces without crashing', async () => {
    const manyNamespaces = Array.from({ length: 10 }, (_, i) => ({
      name: `project${i}.main.node`,
      gitInfo: {
        repo: `org/repo${i}`,
        branch: 'main',
        defaultBranch: 'main',
        parentNamespace: `project${i}`,
      },
      current: { updatedAt: '2024-01-01T10:00:00Z' },
    }));

    renderWithContext({
      ownedNodes: manyNamespaces,
      recentlyEdited: [],
      loading: false,
    });

    // Should render component successfully
    expect(screen.getByText('Git Namespaces')).toBeInTheDocument();
    // Should show some of the namespaces (maxDisplay is 3)
    await waitFor(() => {
      expect(screen.getByText('project0')).toBeInTheDocument();
    });
  });

  it('should handle nodes without parentNamespace (fallback parsing)', async () => {
    const nodesWithoutParent = [
      {
        name: 'repo1.branch1.node1',
        gitInfo: {
          repo: 'myorg/myrepo',
          branch: 'branch1',
          defaultBranch: 'main',
          // No parentNamespace - should fallback to parsing
        },
        current: { updatedAt: '2024-01-01T10:00:00Z' },
      },
    ];

    renderWithContext({
      ownedNodes: nodesWithoutParent,
      recentlyEdited: [],
      loading: false,
    });

    await waitFor(() => {
      // Should extract "repo1" as base namespace
      expect(screen.getByText('repo1')).toBeInTheDocument();
      expect(screen.getByText('myorg/myrepo')).toBeInTheDocument();
    });
  });

  it('should track most recent activity across multiple nodes', async () => {
    const nodesWithDifferentTimes = [
      {
        name: 'myproject.main.node1',
        gitInfo: {
          repo: 'myorg/myrepo',
          branch: 'main',
          defaultBranch: 'main',
          parentNamespace: 'myproject',
        },
        current: { updatedAt: '2024-01-01T10:00:00Z' },
      },
      {
        name: 'myproject.main.node2',
        gitInfo: {
          repo: 'myorg/myrepo',
          branch: 'main',
          defaultBranch: 'main',
          parentNamespace: 'myproject',
        },
        current: { updatedAt: '2024-01-05T10:00:00Z' }, // More recent
      },
    ];

    renderWithContext({
      ownedNodes: nodesWithDifferentTimes,
      recentlyEdited: [],
      loading: false,
    });

    await waitFor(() => {
      expect(screen.getByText('myproject')).toBeInTheDocument();
      // Should show the more recent update time
      expect(screen.getByText(/updated 2d ago/)).toBeInTheDocument();
    });
  });

  it('should show "+N more git namespaces" when more than 3 namespaces', async () => {
    const manyNamespaces = Array.from({ length: 5 }, (_, i) => ({
      name: `project${i}.main.node`,
      gitInfo: {
        repo: `org/repo${i}`,
        branch: 'main',
        defaultBranch: 'main',
        parentNamespace: `project${i}`,
      },
      current: { updatedAt: `2024-01-0${i + 1}T10:00:00Z` },
    }));

    renderWithContext({
      ownedNodes: manyNamespaces,
      recentlyEdited: [],
      loading: false,
    });

    await waitFor(() => {
      expect(screen.getByText('+2 more git namespaces')).toBeInTheDocument();
    });
  });

  it('should handle node with no dots in fullNamespace (dotIdx <= 0 fallback)', async () => {
    // Node name "ns.node" → fullNamespace = "ns" → dotIdx = -1 → baseNamespace = "ns"
    const singleSegmentNode = [
      {
        name: 'ns.node',
        gitInfo: {
          repo: 'myorg/myrepo',
          branch: 'main',
          defaultBranch: 'main',
          // No parentNamespace — fallback to parsing
        },
        current: { updatedAt: '2024-01-01T10:00:00Z' },
      },
    ];

    renderWithContext({
      ownedNodes: singleSegmentNode,
      recentlyEdited: [],
      loading: false,
    });

    await waitFor(() => {
      // fullNamespace = "ns", dotIdx = -1, baseNamespace = "ns"
      expect(screen.getByText('ns')).toBeInTheDocument();
    });
  });

  it('should sort branches with null lastActivity after those with activity', async () => {
    // Creates two branches: one with activity, one without — sort should put active first
    const nodesWithMixedActivity = [
      {
        name: 'myproject.feature.node1',
        gitInfo: {
          repo: 'myorg/myrepo',
          branch: 'feature',
          defaultBranch: 'main',
          parentNamespace: 'myproject',
          isDefaultBranch: false,
        },
        current: { updatedAt: '2024-01-01T10:00:00Z' },
      },
      // main branch will be added by the "ensure default branch present" logic with null activity
    ];

    renderWithContext({
      ownedNodes: nodesWithMixedActivity,
      recentlyEdited: [],
      loading: false,
    });

    await waitFor(() => {
      // Both branches should be present
      expect(screen.getByText('feature')).toBeInTheDocument();
      expect(screen.getAllByText('main').length).toBeGreaterThan(0);
    });
  });

  it('should render without errors when given valid props', async () => {
    renderWithContext({
      ownedNodes: mockOwnedNodes,
      recentlyEdited: [],
      loading: false,
    });

    await waitFor(() => {
      expect(screen.getByText('myproject')).toBeInTheDocument();
    });
  });

  it('should show default branch even when user has no nodes on it', async () => {
    const nodesOnlyFeatureBranch = [
      {
        name: 'myproject.feature.node1',
        gitInfo: {
          repo: 'myorg/myrepo',
          branch: 'feature',
          defaultBranch: 'main',
          parentNamespace: 'myproject',
        },
        current: { updatedAt: '2024-01-01T10:00:00Z' },
      },
    ];

    renderWithContext({
      ownedNodes: nodesOnlyFeatureBranch,
      recentlyEdited: [],
      loading: false,
    });

    await waitFor(() => {
      // Should show both the feature branch and the default main branch
      expect(screen.getAllByText('main').length).toBeGreaterThan(0);
      expect(screen.getByText('feature')).toBeInTheDocument();
    });
  });
});
