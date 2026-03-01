import React from 'react';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { MaterializationsSection } from '../MaterializationsSection';

jest.mock('../MyWorkspacePage.css', () => ({}));

describe('<MaterializationsSection />', () => {
  const now = Date.now();

  const createMockNode = (name, hoursAgo, schedule = '@daily') => ({
    name,
    type: 'CUBE',
    current: {
      displayName: name,
      availability: {
        validThroughTs:
          hoursAgo !== null ? now - hoursAgo * 60 * 60 * 1000 : null,
        table: 'warehouse.materialized_table',
      },
      materializations: [{ name: 'mat1', schedule }],
    },
  });

  it('should render loading state', () => {
    render(
      <MemoryRouter>
        <MaterializationsSection nodes={[]} loading={true} />
      </MemoryRouter>,
    );

    expect(
      screen.queryByText('No materializations configured.'),
    ).not.toBeInTheDocument();
  });

  it('should render empty state when no materializations', () => {
    render(
      <MemoryRouter>
        <MaterializationsSection nodes={[]} loading={false} />
      </MemoryRouter>,
    );

    expect(
      screen.getByText('No materializations configured.'),
    ).toBeInTheDocument();
    expect(screen.getByText('Materialize a node')).toBeInTheDocument();
    expect(
      screen.getByText('Speed up queries with cached data'),
    ).toBeInTheDocument();
  });

  it('should render pending status for nodes without validThroughTs', () => {
    const pendingNode = createMockNode('pending_cube', null);

    render(
      <MemoryRouter>
        <MaterializationsSection nodes={[pendingNode]} loading={false} />
      </MemoryRouter>,
    );

    expect(screen.getByText('pending_cube')).toBeInTheDocument();
    expect(screen.getByText('⏳ Pending')).toBeInTheDocument();
  });

  it('should render green status for recent materializations (< 24h)', () => {
    const freshNode = createMockNode('fresh_cube', 12); // 12 hours ago

    render(
      <MemoryRouter>
        <MaterializationsSection nodes={[freshNode]} loading={false} />
      </MemoryRouter>,
    );

    expect(screen.getByText('fresh_cube')).toBeInTheDocument();
    expect(screen.getByText(/12h ago/)).toBeInTheDocument();
  });

  it('should render yellow status for stale materializations (24-72h)', () => {
    const staleNode = createMockNode('stale_cube', 48); // 48 hours ago

    render(
      <MemoryRouter>
        <MaterializationsSection nodes={[staleNode]} loading={false} />
      </MemoryRouter>,
    );

    expect(screen.getByText('stale_cube')).toBeInTheDocument();
    expect(screen.getByText(/2d ago/)).toBeInTheDocument();
  });

  it('should render red status for very stale materializations (> 72h)', () => {
    const veryStaleNode = createMockNode('very_stale_cube', 168); // 7 days ago

    render(
      <MemoryRouter>
        <MaterializationsSection nodes={[veryStaleNode]} loading={false} />
      </MemoryRouter>,
    );

    expect(screen.getByText('very_stale_cube')).toBeInTheDocument();
    expect(screen.getByText(/7d ago/)).toBeInTheDocument();
  });

  it('should sort nodes by validThroughTs (most recent first)', () => {
    const nodes = [
      createMockNode('old_cube', 72),
      createMockNode('recent_cube', 12),
      createMockNode('middle_cube', 36),
    ];

    render(
      <MemoryRouter>
        <MaterializationsSection nodes={nodes} loading={false} />
      </MemoryRouter>,
    );

    const allCubes = screen.getAllByText(/cube/);
    // Should be sorted: recent, middle, old
    expect(allCubes[0]).toHaveTextContent('recent_cube');
    expect(allCubes[1]).toHaveTextContent('middle_cube');
    expect(allCubes[2]).toHaveTextContent('old_cube');
  });

  it('should display materialization schedule', () => {
    const node = createMockNode('scheduled_cube', 12, '@hourly');

    render(
      <MemoryRouter>
        <MaterializationsSection nodes={[node]} loading={false} />
      </MemoryRouter>,
    );

    expect(screen.getByText('🕐 @hourly')).toBeInTheDocument();
  });

  it('should display materialization table', () => {
    const node = createMockNode('cube_with_table', 12);

    render(
      <MemoryRouter>
        <MaterializationsSection nodes={[node]} loading={false} />
      </MemoryRouter>,
    );

    expect(
      screen.getByText('→ warehouse.materialized_table'),
    ).toBeInTheDocument();
  });

  it('should handle nodes with no schedule', () => {
    const nodeWithoutSchedule = {
      name: 'no_schedule_cube',
      type: 'CUBE',
      current: {
        displayName: 'no_schedule_cube',
        availability: {
          validThroughTs: now - 12 * 60 * 60 * 1000,
        },
        materializations: [{ name: 'mat1' }], // No schedule
      },
    };

    render(
      <MemoryRouter>
        <MaterializationsSection
          nodes={[nodeWithoutSchedule]}
          loading={false}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText('🕐 No schedule')).toBeInTheDocument();
  });

  it('should limit display to 5 nodes', () => {
    const manyNodes = Array.from({ length: 10 }, (_, i) =>
      createMockNode(`cube_${i}`, 12),
    );

    render(
      <MemoryRouter>
        <MaterializationsSection nodes={manyNodes} loading={false} />
      </MemoryRouter>,
    );

    expect(screen.getByText('cube_0')).toBeInTheDocument();
    expect(screen.getByText('cube_4')).toBeInTheDocument();
    expect(screen.queryByText('cube_5')).not.toBeInTheDocument();
    expect(screen.getByText('+5 more')).toBeInTheDocument();
  });

  it('should handle undefined nodes gracefully', () => {
    render(
      <MemoryRouter>
        <MaterializationsSection nodes={undefined} loading={false} />
      </MemoryRouter>,
    );

    expect(
      screen.getByText('No materializations configured.'),
    ).toBeInTheDocument();
  });

  it('should handle nodes without availability', () => {
    const nodeWithoutAvailability = {
      name: 'no_availability_cube',
      type: 'CUBE',
      current: {
        displayName: 'no_availability_cube',
        materializations: [{ name: 'mat1', schedule: '@daily' }],
      },
    };

    render(
      <MemoryRouter>
        <MaterializationsSection
          nodes={[nodeWithoutAvailability]}
          loading={false}
        />
      </MemoryRouter>,
    );

    expect(screen.getByText('no_availability_cube')).toBeInTheDocument();
    expect(screen.getByText('⏳ Pending')).toBeInTheDocument();
  });

  it('should link to nodes with materialization filter', () => {
    render(
      <MemoryRouter>
        <MaterializationsSection nodes={[]} loading={false} />
      </MemoryRouter>,
    );

    const viewAllLink = screen.getByText('View All →').closest('a');
    expect(viewAllLink).toHaveAttribute('href', '/?hasMaterialization=true');
  });
});
