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
});
