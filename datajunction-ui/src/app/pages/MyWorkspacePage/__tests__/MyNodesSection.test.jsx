import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { MyNodesSection } from '../MyNodesSection';

jest.mock('../MyWorkspacePage.css', () => ({}));
jest.mock('../NodeList', () => ({
  NodeList: ({ nodes, showUpdatedAt }) => (
    <div data-testid="node-list">
      {nodes.map(node => (
        <div key={node.name}>{node.name}</div>
      ))}
    </div>
  ),
}));
jest.mock('../TypeGroupGrid', () => ({
  TypeGroupGrid: ({ groupedData }) => (
    <div data-testid="type-group-grid">
      {groupedData.map(group => (
        <div key={group.type}>
          {group.type}: {group.count} nodes
        </div>
      ))}
    </div>
  ),
}));

describe('<MyNodesSection />', () => {
  const mockOwnedNodes = [
    {
      name: 'default.owned_metric',
      type: 'METRIC',
      current: { displayName: 'Owned Metric', updatedAt: '2024-01-01' },
    },
  ];

  const mockWatchedNodes = [
    {
      name: 'default.watched_metric',
      type: 'METRIC',
      current: { displayName: 'Watched Metric', updatedAt: '2024-01-02' },
    },
    {
      name: 'default.owned_metric', // Also owned, should be filtered out
      type: 'METRIC',
      current: { displayName: 'Owned Metric', updatedAt: '2024-01-01' },
    },
  ];

  const mockRecentlyEdited = [
    {
      name: 'default.edited_metric',
      type: 'METRIC',
      current: { displayName: 'Edited Metric', updatedAt: '2024-01-03' },
    },
  ];

  it('should render loading state', () => {
    render(
      <MemoryRouter>
        <MyNodesSection
          ownedNodes={[]}
          watchedNodes={[]}
          recentlyEdited={[]}
          username="test.user@example.com"
          loading={true}
        />
      </MemoryRouter>,
    );

    expect(screen.queryByText('Owned (0)')).not.toBeInTheDocument();
  });

  it('should render empty state when no nodes', () => {
    render(
      <MemoryRouter>
        <MyNodesSection
          ownedNodes={[]}
          watchedNodes={[]}
          recentlyEdited={[]}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText('No nodes yet.')).toBeInTheDocument();
    expect(screen.getByText('Create a node')).toBeInTheDocument();
    expect(screen.getByText('Claim ownership')).toBeInTheDocument();
  });

  it('should render tabs with node counts', () => {
    render(
      <MemoryRouter>
        <MyNodesSection
          ownedNodes={mockOwnedNodes}
          watchedNodes={mockWatchedNodes}
          recentlyEdited={mockRecentlyEdited}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText('Owned (1)')).toBeInTheDocument();
    // Should only count watched nodes that aren't owned (1 watched-only)
    expect(screen.getByText('Watched (1)')).toBeInTheDocument();
    expect(screen.getByText('Recent Edits (1)')).toBeInTheDocument();
  });

  it('should show owned nodes by default', () => {
    render(
      <MemoryRouter>
        <MyNodesSection
          ownedNodes={mockOwnedNodes}
          watchedNodes={mockWatchedNodes}
          recentlyEdited={mockRecentlyEdited}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText('default.owned_metric')).toBeInTheDocument();
    expect(
      screen.queryByText('default.watched_metric'),
    ).not.toBeInTheDocument();
  });

  it('should switch to watched tab', () => {
    render(
      <MemoryRouter>
        <MyNodesSection
          ownedNodes={mockOwnedNodes}
          watchedNodes={mockWatchedNodes}
          recentlyEdited={mockRecentlyEdited}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByText('Watched (1)'));

    // Should show watched-only nodes (not owned)
    expect(screen.getByText('default.watched_metric')).toBeInTheDocument();
    expect(screen.queryByText('default.owned_metric')).not.toBeInTheDocument();
  });

  it('should switch to recent edits tab', () => {
    render(
      <MemoryRouter>
        <MyNodesSection
          ownedNodes={mockOwnedNodes}
          watchedNodes={mockWatchedNodes}
          recentlyEdited={mockRecentlyEdited}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByText('Recent Edits (1)'));

    expect(screen.getByText('default.edited_metric')).toBeInTheDocument();
  });

  it('should show empty state for active tab with no nodes', () => {
    render(
      <MemoryRouter>
        <MyNodesSection
          ownedNodes={mockOwnedNodes}
          watchedNodes={[]}
          recentlyEdited={[]}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByText('Watched (0)'));

    expect(screen.getByText('No watched nodes')).toBeInTheDocument();
  });

  it('should limit displayed nodes to 8', () => {
    const manyNodes = Array.from({ length: 15 }, (_, i) => ({
      name: `default.metric_${i}`,
      type: 'METRIC',
      current: { displayName: `Metric ${i}`, updatedAt: '2024-01-01' },
    }));

    render(
      <MemoryRouter>
        <MyNodesSection
          ownedNodes={manyNodes}
          watchedNodes={[]}
          recentlyEdited={[]}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    // Should show "+7 more" text
    expect(screen.getByText('+7 more')).toBeInTheDocument();
  });

  it('should link to browse page with username filter', () => {
    render(
      <MemoryRouter>
        <MyNodesSection
          ownedNodes={mockOwnedNodes}
          watchedNodes={[]}
          recentlyEdited={[]}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    // The "View All →" link should have the username filter
    const viewAllLink = screen.getByText('View All →').closest('a');
    expect(viewAllLink).toHaveAttribute(
      'href',
      '/?ownedBy=test.user@example.com',
    );
  });

  it('should filter watched nodes to exclude owned nodes', () => {
    const ownedNodes = [
      { name: 'node1', type: 'METRIC', current: {} },
      { name: 'node2', type: 'METRIC', current: {} },
    ];
    const watchedNodes = [
      { name: 'node1', type: 'METRIC', current: {} }, // Also owned
      { name: 'node3', type: 'METRIC', current: {} }, // Watched only
    ];

    render(
      <MemoryRouter>
        <MyNodesSection
          ownedNodes={ownedNodes}
          watchedNodes={watchedNodes}
          recentlyEdited={[]}
          username="test.user@example.com"
          loading={false}
        />
      </MemoryRouter>,
    );

    // Watched tab should only show 1 (watched-only nodes)
    expect(screen.getByText('Watched (1)')).toBeInTheDocument();
  });

  describe('Group by Type feature', () => {
    beforeEach(() => {
      localStorage.clear();
    });

    it('should show group by type toggle when nodes exist', () => {
      render(
        <MemoryRouter>
          <MyNodesSection
            ownedNodes={mockOwnedNodes}
            watchedNodes={[]}
            recentlyEdited={[]}
            username="test.user@example.com"
            loading={false}
          />
        </MemoryRouter>,
      );

      expect(screen.getByLabelText('Group by Type')).toBeInTheDocument();
    });

    it('should not show toggle when no nodes', () => {
      render(
        <MemoryRouter>
          <MyNodesSection
            ownedNodes={[]}
            watchedNodes={[]}
            recentlyEdited={[]}
            username="test.user@example.com"
            loading={false}
          />
        </MemoryRouter>,
      );

      expect(screen.queryByLabelText('Group by Type')).not.toBeInTheDocument();
    });

    it('should toggle between list and grouped view', () => {
      render(
        <MemoryRouter>
          <MyNodesSection
            ownedNodes={mockOwnedNodes}
            watchedNodes={[]}
            recentlyEdited={[]}
            username="test.user@example.com"
            loading={false}
          />
        </MemoryRouter>,
      );

      // Initially should show list view
      expect(screen.getByTestId('node-list')).toBeInTheDocument();
      expect(screen.queryByTestId('type-group-grid')).not.toBeInTheDocument();

      // Click toggle
      const toggle = screen.getByLabelText('Group by Type');
      fireEvent.click(toggle);

      // Should now show grouped view
      expect(screen.queryByTestId('node-list')).not.toBeInTheDocument();
      expect(screen.getByTestId('type-group-grid')).toBeInTheDocument();
    });

    it('should persist toggle state to localStorage', () => {
      render(
        <MemoryRouter>
          <MyNodesSection
            ownedNodes={mockOwnedNodes}
            watchedNodes={[]}
            recentlyEdited={[]}
            username="test.user@example.com"
            loading={false}
          />
        </MemoryRouter>,
      );

      const toggle = screen.getByLabelText('Group by Type');
      fireEvent.click(toggle);

      expect(localStorage.getItem('workspace_groupByType')).toBe('true');

      fireEvent.click(toggle);
      expect(localStorage.getItem('workspace_groupByType')).toBe('false');
    });

    it('should load toggle state from localStorage', () => {
      localStorage.setItem('workspace_groupByType', 'true');

      render(
        <MemoryRouter>
          <MyNodesSection
            ownedNodes={mockOwnedNodes}
            watchedNodes={[]}
            recentlyEdited={[]}
            username="test.user@example.com"
            loading={false}
          />
        </MemoryRouter>,
      );

      // Should show grouped view on mount
      expect(screen.getByTestId('type-group-grid')).toBeInTheDocument();
      expect(screen.getByLabelText('Group by Type')).toBeChecked();
    });

    it('should group nodes by type correctly', () => {
      const mixedNodes = [
        { name: 'metric1', type: 'metric', current: {} },
        { name: 'metric2', type: 'metric', current: {} },
        { name: 'dim1', type: 'dimension', current: {} },
        { name: 'source1', type: 'source', current: {} },
      ];

      render(
        <MemoryRouter>
          <MyNodesSection
            ownedNodes={mixedNodes}
            watchedNodes={[]}
            recentlyEdited={[]}
            username="test.user@example.com"
            loading={false}
          />
        </MemoryRouter>,
      );

      const toggle = screen.getByLabelText('Group by Type');
      fireEvent.click(toggle);

      expect(screen.getByText('metric: 2 nodes')).toBeInTheDocument();
      expect(screen.getByText('dimension: 1 nodes')).toBeInTheDocument();
      expect(screen.getByText('source: 1 nodes')).toBeInTheDocument();
    });
  });
});
