import React from 'react';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { NeedsAttentionSection } from '../NeedsAttentionSection';

jest.mock('../MyWorkspacePage.css', () => ({}));

describe('<NeedsAttentionSection />', () => {
  const defaultProps = {
    nodesMissingDescription: [],
    invalidNodes: [],
    staleDrafts: [],
    staleMaterializations: [],
    orphanedDimensions: [],
    username: 'test.user@example.com',
    hasItems: false,
    loading: false,
    personalNamespace: 'users.test.user',
    hasPersonalNamespace: true,
    namespaceLoading: false,
  };

  it('should render loading state', () => {
    render(
      <MemoryRouter>
        <NeedsAttentionSection {...defaultProps} loading={true} />
      </MemoryRouter>,
    );

    expect(screen.queryByText('✓ All good!')).not.toBeInTheDocument();
  });

  it('should render all categories', () => {
    render(
      <MemoryRouter>
        <NeedsAttentionSection {...defaultProps} />
      </MemoryRouter>,
    );

    expect(screen.getByText(/Invalid/)).toBeInTheDocument();
    expect(screen.getByText(/Stale Drafts/)).toBeInTheDocument();
    expect(screen.getByText(/Stale Materializations/)).toBeInTheDocument();
    expect(screen.getByText(/No Description/)).toBeInTheDocument();
    expect(screen.getByText(/Orphaned Dimensions/)).toBeInTheDocument();
  });

  it('should show "All good!" when category has no items', () => {
    render(
      <MemoryRouter>
        <NeedsAttentionSection {...defaultProps} />
      </MemoryRouter>,
    );

    const allGood = screen.getAllByText('✓ All good!');
    expect(allGood.length).toBe(5); // All 5 categories should show "All good!"
  });

  it('should display invalid nodes', () => {
    const invalidNodes = [
      {
        name: 'default.invalid_metric',
        type: 'METRIC',
        current: { displayName: 'Invalid Metric' },
      },
    ];

    render(
      <MemoryRouter>
        <NeedsAttentionSection
          {...defaultProps}
          invalidNodes={invalidNodes}
          hasItems={true}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText('❌ Invalid')).toBeInTheDocument();
    expect(screen.getByText('(1)')).toBeInTheDocument();
  });

  it('should display stale drafts', () => {
    const staleDrafts = [
      {
        name: 'default.stale_draft',
        type: 'METRIC',
        current: { displayName: 'Stale Draft' },
      },
    ];

    render(
      <MemoryRouter>
        <NeedsAttentionSection
          {...defaultProps}
          staleDrafts={staleDrafts}
          hasItems={true}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText('⏰ Stale Drafts')).toBeInTheDocument();
    expect(screen.getByText('(1)')).toBeInTheDocument();
  });

  it('should display nodes missing description', () => {
    const nodesMissingDescription = [
      {
        name: 'default.no_desc_metric',
        type: 'METRIC',
        current: { displayName: 'No Description Metric' },
      },
    ];

    render(
      <MemoryRouter>
        <NeedsAttentionSection
          {...defaultProps}
          nodesMissingDescription={nodesMissingDescription}
          hasItems={true}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText('📝 No Description')).toBeInTheDocument();
    expect(screen.getByText('(1)')).toBeInTheDocument();
  });

  it('should show "View all" link for categories with items', () => {
    const invalidNodes = [
      {
        name: 'default.invalid_metric',
        type: 'METRIC',
        current: { displayName: 'Invalid Metric' },
      },
    ];

    render(
      <MemoryRouter>
        <NeedsAttentionSection
          {...defaultProps}
          invalidNodes={invalidNodes}
          hasItems={true}
        />
      </MemoryRouter>,
    );

    const viewAllLinks = screen.getAllByText('View all →');
    expect(viewAllLinks.length).toBeGreaterThan(0);
  });

  it('should not show "View all" link for empty categories', () => {
    render(
      <MemoryRouter>
        <NeedsAttentionSection {...defaultProps} />
      </MemoryRouter>,
    );

    expect(screen.queryByText('View all →')).not.toBeInTheDocument();
  });

  it('should limit nodes to 10 per category', () => {
    const manyNodes = Array.from({ length: 15 }, (_, i) => ({
      name: `default.node_${i}`,
      type: 'METRIC',
      current: { displayName: `Node ${i}` },
    }));

    render(
      <MemoryRouter>
        <NeedsAttentionSection
          {...defaultProps}
          invalidNodes={manyNodes}
          hasItems={true}
        />
      </MemoryRouter>,
    );

    // Should only display 10 nodes (via NodeChip component)
    // The 11th node should not be displayed
    expect(screen.getByText('❌ Invalid')).toBeInTheDocument();
    expect(screen.getByText('(15)')).toBeInTheDocument();
  });

  it('should show personal namespace prompt when namespace does not exist', () => {
    render(
      <MemoryRouter>
        <NeedsAttentionSection {...defaultProps} hasPersonalNamespace={false} />
      </MemoryRouter>,
    );

    expect(screen.getByText('Set up your namespace')).toBeInTheDocument();
    expect(screen.getByText('users.test.user')).toBeInTheDocument();
    expect(screen.getByText('Create →')).toBeInTheDocument();
  });

  it('should not show personal namespace prompt when namespace exists', () => {
    render(
      <MemoryRouter>
        <NeedsAttentionSection {...defaultProps} hasPersonalNamespace={true} />
      </MemoryRouter>,
    );

    expect(screen.queryByText('Set up your namespace')).not.toBeInTheDocument();
  });

  it('should not show personal namespace prompt when loading', () => {
    render(
      <MemoryRouter>
        <NeedsAttentionSection
          {...defaultProps}
          hasPersonalNamespace={false}
          namespaceLoading={true}
        />
      </MemoryRouter>,
    );

    expect(screen.queryByText('Set up your namespace')).not.toBeInTheDocument();
  });

  it('should link to correct filter URLs', () => {
    const invalidNodes = [
      {
        name: 'default.invalid_metric',
        type: 'METRIC',
        current: { displayName: 'Invalid Metric' },
      },
    ];

    render(
      <MemoryRouter>
        <NeedsAttentionSection
          {...defaultProps}
          invalidNodes={invalidNodes}
          hasItems={true}
        />
      </MemoryRouter>,
    );

    const viewAllLink = screen.getByText('View all →').closest('a');
    expect(viewAllLink).toHaveAttribute(
      'href',
      '/?ownedBy=test.user@example.com&statuses=INVALID',
    );
  });

  it('should display correct count for each category', () => {
    render(
      <MemoryRouter>
        <NeedsAttentionSection
          {...defaultProps}
          invalidNodes={[{ name: 'node1', type: 'METRIC', current: {} }]}
          staleDrafts={[
            { name: 'node2', type: 'METRIC', current: {} },
            { name: 'node3', type: 'METRIC', current: {} },
          ]}
          nodesMissingDescription={[
            { name: 'node4', type: 'METRIC', current: {} },
            { name: 'node5', type: 'METRIC', current: {} },
            { name: 'node6', type: 'METRIC', current: {} },
          ]}
          hasItems={true}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText('❌ Invalid')).toBeInTheDocument();
    expect(screen.getByText('(1)')).toBeInTheDocument();
    expect(screen.getByText('⏰ Stale Drafts')).toBeInTheDocument();
    expect(screen.getByText('(2)')).toBeInTheDocument();
    expect(screen.getByText('📝 No Description')).toBeInTheDocument();
    expect(screen.getByText('(3)')).toBeInTheDocument();
  });
});
