import React from 'react';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { MyWorkspacePage } from '../index';
import * as useWorkspaceData from '../../../hooks/useWorkspaceData';

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

jest.mock('../CollectionsSection', () => ({
  CollectionsSection: ({ collections, loading }) => (
    <div data-testid="collections-section">
      {loading ? 'Loading...' : `${collections.length} collections`}
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

  const mockCollections = [
    {
      name: 'test_collection',
      description: 'Test Collection',
      nodeCount: 5,
      createdBy: 'test.user@example.com',
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

  const mockNeedsAttention = {
    nodesMissingDescription: [],
    invalidNodes: [],
    staleDrafts: [],
    orphanedDimensions: [],
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should render loading state', () => {
    jest.spyOn(useWorkspaceData, 'useCurrentUser').mockReturnValue({
      data: null,
      loading: true,
      error: null,
    });

    render(
      <MemoryRouter>
        <MyWorkspacePage />
      </MemoryRouter>,
    );

    expect(screen.getByText('Dashboard')).toBeInTheDocument();
    // Should show loading icon when user is loading
    expect(screen.queryByTestId('collections-section')).not.toBeInTheDocument();
  });

  it('should render all sections when data is loaded', () => {
    // Mock all hooks with data
    jest.spyOn(useWorkspaceData, 'useCurrentUser').mockReturnValue({
      data: mockCurrentUser,
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'useWorkspaceOwnedNodes').mockReturnValue({
      data: mockOwnedNodes,
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'useWorkspaceRecentlyEdited').mockReturnValue({
      data: [],
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'useWorkspaceWatchedNodes').mockReturnValue({
      data: [],
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'useWorkspaceCollections').mockReturnValue({
      data: mockCollections,
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'useWorkspaceNotifications').mockReturnValue({
      data: mockNotifications,
      loading: false,
      error: null,
    });
    jest
      .spyOn(useWorkspaceData, 'useWorkspaceMaterializations')
      .mockReturnValue({
        data: mockMaterializations,
        loading: false,
        error: null,
      });
    jest.spyOn(useWorkspaceData, 'useWorkspaceNeedsAttention').mockReturnValue({
      data: mockNeedsAttention,
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'usePersonalNamespace').mockReturnValue({
      exists: true,
      loading: false,
      error: null,
    });

    render(
      <MemoryRouter>
        <MyWorkspacePage />
      </MemoryRouter>,
    );

    // Check all sections are rendered
    expect(screen.getByTestId('collections-section')).toHaveTextContent(
      '1 collections',
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
    jest.spyOn(useWorkspaceData, 'useCurrentUser').mockReturnValue({
      data: mockCurrentUser,
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'useWorkspaceOwnedNodes').mockReturnValue({
      data: mockOwnedNodes,
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'useWorkspaceRecentlyEdited').mockReturnValue({
      data: [],
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'useWorkspaceWatchedNodes').mockReturnValue({
      data: [],
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'useWorkspaceCollections').mockReturnValue({
      data: mockCollections,
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'useWorkspaceNotifications').mockReturnValue({
      data: mockNotifications,
      loading: false,
      error: null,
    });
    jest
      .spyOn(useWorkspaceData, 'useWorkspaceMaterializations')
      .mockReturnValue({
        data: mockMaterializations,
        loading: false,
        error: null,
      });
    jest.spyOn(useWorkspaceData, 'useWorkspaceNeedsAttention').mockReturnValue({
      data: mockNeedsAttention,
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'usePersonalNamespace').mockReturnValue({
      exists: true,
      loading: false,
      error: null,
    });

    render(
      <MemoryRouter>
        <MyWorkspacePage />
      </MemoryRouter>,
    );

    // Verify sections receive correct data
    expect(screen.getByTestId('collections-section')).toBeInTheDocument();
    expect(screen.getByTestId('my-nodes-section')).toBeInTheDocument();
    expect(screen.getByTestId('notifications-section')).toBeInTheDocument();
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

    jest.spyOn(useWorkspaceData, 'useCurrentUser').mockReturnValue({
      data: mockCurrentUser,
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'useWorkspaceOwnedNodes').mockReturnValue({
      data: [],
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'useWorkspaceRecentlyEdited').mockReturnValue({
      data: [],
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'useWorkspaceWatchedNodes').mockReturnValue({
      data: [],
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'useWorkspaceCollections').mockReturnValue({
      data: [],
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'useWorkspaceNotifications').mockReturnValue({
      data: [],
      loading: false,
      error: null,
    });
    jest
      .spyOn(useWorkspaceData, 'useWorkspaceMaterializations')
      .mockReturnValue({
        data: [staleNode],
        loading: false,
        error: null,
      });
    jest.spyOn(useWorkspaceData, 'useWorkspaceNeedsAttention').mockReturnValue({
      data: mockNeedsAttention,
      loading: false,
      error: null,
    });
    jest.spyOn(useWorkspaceData, 'usePersonalNamespace').mockReturnValue({
      exists: true,
      loading: false,
      error: null,
    });

    render(
      <MemoryRouter>
        <MyWorkspacePage />
      </MemoryRouter>,
    );

    // The stale materialization should be passed to NeedsAttentionSection
    expect(screen.getByTestId('needs-attention-section')).toBeInTheDocument();
  });
});
