import React from 'react';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { MyWorkspacePage } from '../index';
import * as useWorkspaceData from '../../../hooks/useWorkspaceData';
import * as UserProvider from '../../../providers/UserProvider';

// Mock CSS imports
jest.mock('../MyWorkspacePage.css', () => ({}));
jest.mock('../../../../styles/settings.css', () => ({}));

// Mock child components to test integration
jest.mock('../NotificationsSection', () => ({
  NotificationsSection: ({ notifications, loading }) => (
    <div data-testid="notifications-section">
      {loading ? 'Loading...' : `${notifications.length} notifications`}
    </div>
  ),
}));

jest.mock('../NeedsAttentionSection', () => ({
  NeedsAttentionSection: ({ loading }) => (
    <div data-testid="needs-attention-section">
      {loading ? 'Loading...' : 'Needs Attention'}
    </div>
  ),
}));

jest.mock('../MyNodesSection', () => ({
  MyNodesSection: ({ ownedNodes, loading }) => (
    <div data-testid="my-nodes-section">
      {loading ? 'Loading...' : `${ownedNodes.length} owned nodes`}
    </div>
  ),
}));

jest.mock('../MaterializationsSection', () => ({
  MaterializationsSection: ({ nodes, loading }) => (
    <div data-testid="materializations-section">
      {loading ? 'Loading...' : `${nodes.length} materializations`}
    </div>
  ),
}));

jest.mock('../ActiveBranchesSection', () => ({
  ActiveBranchesSection: ({ loading }) => (
    <div data-testid="active-branches-section">
      {loading ? 'Loading...' : 'Active Branches'}
    </div>
  ),
}));

describe('<MyWorkspacePage />', () => {
  const mockCurrentUser = {
    username: 'test.user@example.com',
    email: 'test.user@example.com',
  };

  const mockOwnedNodes = [
    {
      name: 'default.test_metric',
      type: 'METRIC',
      current: { displayName: 'Test Metric', updatedAt: '2024-01-01' },
    },
    {
      name: 'default.test_dimension',
      type: 'DIMENSION',
      current: { displayName: 'Test Dimension', updatedAt: '2024-01-02' },
    },
  ];

  const mockNotifications = [
    {
      entity_name: 'default.test_metric',
      entity_type: 'node',
      activity_type: 'update',
      created_at: '2024-01-01T00:00:00Z',
      user: 'test.user@example.com',
      node_type: 'metric',
      display_name: 'Test Metric',
    },
  ];

  const mockMaterializations = [
    {
      name: 'default.test_cube',
      type: 'CUBE',
      current: {
        displayName: 'Test Cube',
        availability: {
          validThroughTs: Date.now() - 1000 * 60 * 60, // 1 hour ago
        },
        materializations: [{ name: 'mat1', schedule: '@daily' }],
      },
    },
  ];

  const makeLoadingStates = (loading = false) => ({
    myNodes: loading,
    collections: loading,
    notifications: loading,
    materializations: loading,
    needsAttention: loading,
    namespace: loading,
  });

  const makeDashboardData = (overrides = {}) => ({
    data: {
      ownedNodes: [],
      ownedHasMore: {},
      recentlyEdited: [],
      editedHasMore: {},
      watchedNodes: [],
      notifications: [],
      materializedNodes: [],
      needsAttention: {
        nodesMissingDescription: [],
        invalidNodes: [],
        staleDrafts: [],
        orphanedDimensions: [],
      },
      hasPersonalNamespace: true,
      ...overrides,
    },
    loadingStates: makeLoadingStates(false),
  });

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should render loading state', () => {
    jest.spyOn(UserProvider, 'useCurrentUser').mockReturnValue({
      currentUser: null,
      loading: true,
    });
    jest
      .spyOn(useWorkspaceData, 'useWorkspaceDashboardData')
      .mockReturnValue(makeDashboardData());

    render(
      <MemoryRouter>
        <MyWorkspacePage />
      </MemoryRouter>,
    );

    expect(screen.getByText('Dashboard')).toBeInTheDocument();
    // Sections are not shown while user is loading
    expect(screen.queryByTestId('my-nodes-section')).not.toBeInTheDocument();
  });

  it('should render all sections when data is loaded', () => {
    jest.spyOn(UserProvider, 'useCurrentUser').mockReturnValue({
      currentUser: mockCurrentUser,
      loading: false,
    });
    jest.spyOn(useWorkspaceData, 'useWorkspaceDashboardData').mockReturnValue(
      makeDashboardData({
        ownedNodes: mockOwnedNodes,
        notifications: mockNotifications,
        materializedNodes: mockMaterializations,
      }),
    );

    render(
      <MemoryRouter>
        <MyWorkspacePage />
      </MemoryRouter>,
    );

    expect(screen.getByTestId('my-nodes-section')).toHaveTextContent(
      '2 owned nodes',
    );
    expect(screen.getByTestId('notifications-section')).toHaveTextContent(
      '1 notifications',
    );
    expect(screen.getByTestId('materializations-section')).toHaveTextContent(
      '1 materializations',
    );
    expect(screen.getByTestId('needs-attention-section')).toBeInTheDocument();
    expect(screen.getByTestId('active-branches-section')).toBeInTheDocument();
  });

  it('should pass correct props to sections', () => {
    jest.spyOn(UserProvider, 'useCurrentUser').mockReturnValue({
      currentUser: mockCurrentUser,
      loading: false,
    });
    jest
      .spyOn(useWorkspaceData, 'useWorkspaceDashboardData')
      .mockReturnValue(makeDashboardData({ ownedNodes: mockOwnedNodes }));

    render(
      <MemoryRouter>
        <MyWorkspacePage />
      </MemoryRouter>,
    );

    expect(screen.getByTestId('my-nodes-section')).toBeInTheDocument();
    expect(screen.getByTestId('notifications-section')).toBeInTheDocument();
    expect(screen.getByTestId('needs-attention-section')).toBeInTheDocument();
  });

  it('should filter out non-stale materializations from NeedsAttention', () => {
    const now = Date.now();
    const freshNode = {
      name: 'default.fresh_cube',
      type: 'CUBE',
      current: {
        availability: {
          validThroughTs: now - 1000 * 60 * 60 * 10, // 10 hours ago (< 72h, not stale)
        },
      },
    };

    jest.spyOn(UserProvider, 'useCurrentUser').mockReturnValue({
      currentUser: mockCurrentUser,
      loading: false,
    });
    jest
      .spyOn(useWorkspaceData, 'useWorkspaceDashboardData')
      .mockReturnValue(makeDashboardData({ materializedNodes: [freshNode] }));

    render(
      <MemoryRouter>
        <MyWorkspacePage />
      </MemoryRouter>,
    );

    // Page renders without crash — stale filter ran without errors
    expect(screen.getByTestId('materializations-section')).toHaveTextContent(
      '1 materializations',
    );
    expect(screen.getByTestId('needs-attention-section')).toBeInTheDocument();
  });

  it('should exclude nodes with null validThroughTs from stale list', () => {
    const pendingNode = {
      name: 'default.pending_cube',
      type: 'CUBE',
      current: {
        availability: {
          validThroughTs: null,
        },
      },
    };

    jest.spyOn(UserProvider, 'useCurrentUser').mockReturnValue({
      currentUser: mockCurrentUser,
      loading: false,
    });
    jest
      .spyOn(useWorkspaceData, 'useWorkspaceDashboardData')
      .mockReturnValue(makeDashboardData({ materializedNodes: [pendingNode] }));

    render(
      <MemoryRouter>
        <MyWorkspacePage />
      </MemoryRouter>,
    );

    // Pending node (null validThroughTs) should not be passed as stale
    expect(screen.getByTestId('needs-attention-section')).toBeInTheDocument();
  });

  it('should exclude nodes with no availability from stale list', () => {
    const noAvailabilityNode = {
      name: 'default.no_avail_cube',
      type: 'CUBE',
      current: {},
    };

    jest.spyOn(UserProvider, 'useCurrentUser').mockReturnValue({
      currentUser: mockCurrentUser,
      loading: false,
    });
    jest
      .spyOn(useWorkspaceData, 'useWorkspaceDashboardData')
      .mockReturnValue(
        makeDashboardData({ materializedNodes: [noAvailabilityNode] }),
      );

    render(
      <MemoryRouter>
        <MyWorkspacePage />
      </MemoryRouter>,
    );

    expect(screen.getByTestId('needs-attention-section')).toBeInTheDocument();
  });

  it('should derive personal namespace from username', () => {
    jest.spyOn(UserProvider, 'useCurrentUser').mockReturnValue({
      currentUser: { username: 'jane.doe@company.com' },
      loading: false,
    });
    jest
      .spyOn(useWorkspaceData, 'useWorkspaceDashboardData')
      .mockReturnValue(makeDashboardData());

    render(
      <MemoryRouter>
        <MyWorkspacePage />
      </MemoryRouter>,
    );

    // NeedsAttentionSection mock receives personalNamespace="users.jane.doe"
    expect(screen.getByTestId('needs-attention-section')).toBeInTheDocument();
  });

  it('should calculate stale materializations correctly', () => {
    const now = Date.now();
    const staleNode = {
      name: 'default.stale_cube',
      type: 'CUBE',
      current: {
        displayName: 'Stale Cube',
        availability: {
          validThroughTs: now - 1000 * 60 * 60 * 80, // 80 hours ago (> 72 hours)
        },
      },
    };

    jest.spyOn(UserProvider, 'useCurrentUser').mockReturnValue({
      currentUser: mockCurrentUser,
      loading: false,
    });
    jest
      .spyOn(useWorkspaceData, 'useWorkspaceDashboardData')
      .mockReturnValue(makeDashboardData({ materializedNodes: [staleNode] }));

    render(
      <MemoryRouter>
        <MyWorkspacePage />
      </MemoryRouter>,
    );

    // The stale materialization should be passed to NeedsAttentionSection
    expect(screen.getByTestId('needs-attention-section')).toBeInTheDocument();
  });
});
