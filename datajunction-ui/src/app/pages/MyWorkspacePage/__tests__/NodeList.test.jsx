import React from 'react';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { NodeList } from '../NodeList';

jest.mock('../MyWorkspacePage.css', () => ({}));
jest.mock('../../../components/NodeListActions', () => {
  return function MockNodeListActions({ nodeName }) {
    return <div data-testid="node-actions">{nodeName}</div>;
  };
});

describe('<NodeList />', () => {
  const mockNodes = [
    {
      name: 'default.test_metric',
      type: 'METRIC',
      current: {
        displayName: 'Test Metric',
        updatedAt: '2024-01-01T12:00:00Z',
      },
    },
    {
      name: 'default.test_dimension',
      type: 'DIMENSION',
      current: {
        displayName: 'Test Dimension',
        updatedAt: '2024-01-02T15:30:00Z',
      },
    },
  ];

  it('should render list of nodes', () => {
    render(
      <MemoryRouter>
        <NodeList nodes={mockNodes} showUpdatedAt={false} />
      </MemoryRouter>,
    );

    // Node names appear twice (in NodeDisplay and as full name)
    expect(
      screen.getAllByText('default.test_metric').length,
    ).toBeGreaterThanOrEqual(1);
    expect(
      screen.getAllByText('default.test_dimension').length,
    ).toBeGreaterThanOrEqual(1);
  });

  it('should render node actions for each node', () => {
    render(
      <MemoryRouter>
        <NodeList nodes={mockNodes} showUpdatedAt={false} />
      </MemoryRouter>,
    );

    const actions = screen.getAllByTestId('node-actions');
    expect(actions).toHaveLength(2);
    expect(actions[0]).toHaveTextContent('default.test_metric');
    expect(actions[1]).toHaveTextContent('default.test_dimension');
  });

  it('should show updated timestamp when showUpdatedAt is true', () => {
    render(
      <MemoryRouter>
        <NodeList nodes={mockNodes} showUpdatedAt={true} />
      </MemoryRouter>,
    );

    // Should show formatted date
    expect(screen.getByText(/Jan 1/)).toBeInTheDocument();
    expect(screen.getByText(/Jan 2/)).toBeInTheDocument();
  });

  it('should not show updated timestamp when showUpdatedAt is false', () => {
    const { container } = render(
      <MemoryRouter>
        <NodeList nodes={mockNodes} showUpdatedAt={false} />
      </MemoryRouter>,
    );

    // Should not show formatted date
    expect(
      container.querySelector('.node-list-item-updated'),
    ).not.toBeInTheDocument();
  });

  it('should handle nodes without updatedAt gracefully', () => {
    const nodesWithoutDate = [
      {
        name: 'default.no_date_metric',
        type: 'METRIC',
        current: {
          displayName: 'No Date Metric',
        },
      },
    ];

    render(
      <MemoryRouter>
        <NodeList nodes={nodesWithoutDate} showUpdatedAt={true} />
      </MemoryRouter>,
    );

    // Should render the node name (appears twice - in display name area and as full name)
    const nodeNames = screen.getAllByText('default.no_date_metric');
    expect(nodeNames.length).toBeGreaterThanOrEqual(1);
    // Should not crash when no updatedAt
  });

  it('should render empty list when no nodes', () => {
    const { container } = render(
      <MemoryRouter>
        <NodeList nodes={[]} showUpdatedAt={false} />
      </MemoryRouter>,
    );

    // Should render the container but with no items
    expect(container.querySelector('.node-list')).toBeInTheDocument();
    expect(screen.queryByTestId('node-actions')).not.toBeInTheDocument();
  });

  it('should format datetime correctly', () => {
    const nodeWithDate = [
      {
        name: 'default.dated_metric',
        type: 'METRIC',
        current: {
          displayName: 'Dated Metric',
          updatedAt: '2024-03-15T14:30:00Z',
        },
      },
    ];

    const { container } = render(
      <MemoryRouter>
        <NodeList nodes={nodeWithDate} showUpdatedAt={true} />
      </MemoryRouter>,
    );

    // Should show month, day, and time in the updated timestamp
    const updatedSpan = container.querySelector('.node-list-item-updated');
    expect(updatedSpan).toBeInTheDocument();
    expect(updatedSpan.textContent).toMatch(/Mar 15/);
  });

  it('should apply correct CSS classes', () => {
    const { container } = render(
      <MemoryRouter>
        <NodeList nodes={mockNodes} showUpdatedAt={false} />
      </MemoryRouter>,
    );

    expect(container.querySelector('.node-list')).toBeInTheDocument();
    expect(container.querySelector('.node-list-item')).toBeInTheDocument();
    expect(
      container.querySelector('.node-list-item-content'),
    ).toBeInTheDocument();
    expect(
      container.querySelector('.node-list-item-actions'),
    ).toBeInTheDocument();
  });
});
