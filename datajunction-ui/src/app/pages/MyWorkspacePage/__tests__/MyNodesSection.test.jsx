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
});
