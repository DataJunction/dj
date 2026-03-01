import React from 'react';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { TypeGroupGrid } from '../TypeGroupGrid';

jest.mock('../MyWorkspacePage.css', () => ({}));
jest.mock('../../../components/NodeComponents', () => ({
  NodeBadge: ({ type }) => <span data-testid="badge">{type}</span>,
  NodeLink: ({ node }) => (
    <a href={`/nodes/${node.name}`} data-testid={`node-link-${node.name}`}>
      {node.name}
    </a>
  ),
}));
jest.mock('../../../components/NodeListActions', () => ({
  __esModule: true,
  default: ({ nodeName }) => (
    <div data-testid={`actions-${nodeName}`}>actions</div>
  ),
}));

describe('<TypeGroupGrid />', () => {
  const mockGroupedData = [
    {
      type: 'metric',
      count: 5,
      nodes: [
        {
          name: 'default.revenue',
          type: 'metric',
          current: {
            displayName: 'Revenue',
            updatedAt: '2024-01-01T10:00:00Z',
          },
        },
        {
          name: 'default.orders',
          type: 'metric',
          current: { displayName: 'Orders', updatedAt: '2024-01-01T12:00:00Z' },
        },
        {
          name: 'default.users',
          type: 'metric',
          current: { displayName: 'Users', updatedAt: '2024-01-01T14:00:00Z' },
        },
        {
          name: 'default.conversion',
          type: 'metric',
          current: {
            displayName: 'Conversion',
            updatedAt: '2024-01-01T16:00:00Z',
          },
        },
        {
          name: 'default.bounce_rate',
          type: 'metric',
          current: {
            displayName: 'Bounce Rate',
            updatedAt: '2024-01-01T18:00:00Z',
          },
        },
      ],
    },
    {
      type: 'dimension',
      count: 2,
      nodes: [
        {
          name: 'default.dim_users',
          type: 'dimension',
          current: { displayName: 'Users', updatedAt: '2024-01-01T08:00:00Z' },
        },
        {
          name: 'default.dim_products',
          type: 'dimension',
          current: {
            displayName: 'Products',
            updatedAt: '2024-01-01T09:00:00Z',
          },
        },
      ],
    },
  ];

  it('should render empty state when no data', () => {
    render(
      <MemoryRouter>
        <TypeGroupGrid
          groupedData={[]}
          username="test.user@example.com"
          activeTab="owned"
        />
      </MemoryRouter>,
    );

    expect(screen.getByText('No nodes to display')).toBeInTheDocument();
  });

  it('should render type cards with correct counts', () => {
    render(
      <MemoryRouter>
        <TypeGroupGrid
          groupedData={mockGroupedData}
          username="test.user@example.com"
          activeTab="owned"
        />
      </MemoryRouter>,
    );

    expect(screen.getByText('Metrics (5)')).toBeInTheDocument();
    expect(screen.getByText('Dimensions (2)')).toBeInTheDocument();
  });

  it('should display up to 10 nodes per type', () => {
    render(
      <MemoryRouter>
        <TypeGroupGrid
          groupedData={mockGroupedData}
          username="test.user@example.com"
          activeTab="owned"
        />
      </MemoryRouter>,
    );

    // Should show all 5 metrics (under the limit of 10)
    expect(screen.getByText('default.revenue')).toBeInTheDocument();
    expect(screen.getByText('default.orders')).toBeInTheDocument();
    expect(screen.getByText('default.users')).toBeInTheDocument();
    expect(screen.getByText('default.conversion')).toBeInTheDocument();
    expect(screen.getByText('default.bounce_rate')).toBeInTheDocument();
  });

  it('should show "+X more" link when more than 10 nodes', () => {
    const manyNodesData = [
      {
        type: 'metric',
        count: 15,
        nodes: Array.from({ length: 15 }, (_, i) => ({
          name: `default.metric_${i}`,
          type: 'metric',
          current: {
            displayName: `Metric ${i}`,
            updatedAt: '2024-01-01T10:00:00Z',
          },
        })),
      },
    ];
    render(
      <MemoryRouter>
        <TypeGroupGrid
          groupedData={manyNodesData}
          username="test.user@example.com"
          activeTab="owned"
        />
      </MemoryRouter>,
    );

    // Metrics: 15 nodes, showing 10, so +5 more
    expect(screen.getByText('+5 more →')).toBeInTheDocument();
  });

  it('should not show "+X more" link when 10 or fewer nodes', () => {
    render(
      <MemoryRouter>
        <TypeGroupGrid
          groupedData={mockGroupedData}
          username="test.user@example.com"
          activeTab="owned"
        />
      </MemoryRouter>,
    );

    // Metrics: 5 nodes (under 10), no "+X more" needed
    const metricCard = screen
      .getByText('Metrics (5)')
      .closest('.type-group-card');
    expect(metricCard).not.toHaveTextContent('more →');

    // Dimensions: 2 nodes, no "+X more" needed
    const dimensionCard = screen
      .getByText('Dimensions (2)')
      .closest('.type-group-card');
    expect(dimensionCard).not.toHaveTextContent('more →');
  });

  it('should render node badges and links', () => {
    render(
      <MemoryRouter>
        <TypeGroupGrid
          groupedData={mockGroupedData}
          username="test.user@example.com"
          activeTab="owned"
        />
      </MemoryRouter>,
    );

    // Should have badges for each displayed node
    const badges = screen.getAllByTestId('badge');
    expect(badges.length).toBeGreaterThan(0);

    // Should have clickable links
    const revenueLink = screen.getByText('default.revenue');
    expect(revenueLink).toHaveAttribute('href', '/nodes/default.revenue');
  });

  it('should render node actions', () => {
    render(
      <MemoryRouter>
        <TypeGroupGrid
          groupedData={mockGroupedData}
          username="test.user@example.com"
          activeTab="owned"
        />
      </MemoryRouter>,
    );

    // Should have actions for each node
    const actions = screen.getAllByText(/^actions$/);
    expect(actions.length).toBe(7); // 5 metrics + 2 dimensions displayed (all under maxDisplay=10)
  });

  it('should format relative time correctly', () => {
    const now = new Date();
    const oneHourAgo = new Date(now - 60 * 60 * 1000);
    const oneDayAgo = new Date(now - 24 * 60 * 60 * 1000);

    const recentNodes = [
      {
        type: 'metric',
        count: 2,
        nodes: [
          {
            name: 'default.recent',
            type: 'metric',
            current: {
              displayName: 'Recent',
              updatedAt: oneHourAgo.toISOString(),
            },
          },
          {
            name: 'default.older',
            type: 'metric',
            current: {
              displayName: 'Older',
              updatedAt: oneDayAgo.toISOString(),
            },
          },
        ],
      },
    ];

    render(
      <MemoryRouter>
        <TypeGroupGrid
          groupedData={recentNodes}
          username="test.user@example.com"
          activeTab="owned"
        />
      </MemoryRouter>,
    );

    // Should show time in hours or days format
    // Note: exact values depend on when test runs, so we just check they exist
    expect(screen.getAllByText(/\d+[mhd]$/)).toHaveLength(2);
  });

  it('should generate correct filter URLs for owned tab', () => {
    const manyNodesData = [
      {
        type: 'metric',
        count: 15,
        nodes: Array.from({ length: 15 }, (_, i) => ({
          name: `default.metric_${i}`,
          type: 'metric',
          current: {
            displayName: `Metric ${i}`,
            updatedAt: '2024-01-01T10:00:00Z',
          },
        })),
      },
    ];
    render(
      <MemoryRouter>
        <TypeGroupGrid
          groupedData={manyNodesData}
          username="test.user@example.com"
          activeTab="owned"
        />
      </MemoryRouter>,
    );

    const moreLink = screen.getByText('+5 more →');
    expect(moreLink).toHaveAttribute(
      'href',
      '/?ownedBy=test.user%40example.com&type=metric',
    );
  });

  it('should generate correct filter URLs for edited tab', () => {
    const manyNodesData = [
      {
        type: 'metric',
        count: 15,
        nodes: Array.from({ length: 15 }, (_, i) => ({
          name: `default.metric_${i}`,
          type: 'metric',
          current: {
            displayName: `Metric ${i}`,
            updatedAt: '2024-01-01T10:00:00Z',
          },
        })),
      },
    ];
    render(
      <MemoryRouter>
        <TypeGroupGrid
          groupedData={manyNodesData}
          username="test.user@example.com"
          activeTab="edited"
        />
      </MemoryRouter>,
    );

    const moreLink = screen.getByText('+5 more →');
    expect(moreLink).toHaveAttribute(
      'href',
      '/?updatedBy=test.user%40example.com&type=metric',
    );
  });

  it('should capitalize type names', () => {
    render(
      <MemoryRouter>
        <TypeGroupGrid
          groupedData={mockGroupedData}
          username="test.user@example.com"
          activeTab="owned"
        />
      </MemoryRouter>,
    );

    // "metric" should be displayed as "Metrics"
    expect(screen.getByText('Metrics (5)')).toBeInTheDocument();
    // "dimension" should be displayed as "Dimensions"
    expect(screen.getByText('Dimensions (2)')).toBeInTheDocument();
  });
});
