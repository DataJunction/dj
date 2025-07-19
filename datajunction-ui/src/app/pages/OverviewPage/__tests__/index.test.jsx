import { render, screen } from '@testing-library/react';
import { OverviewPage } from '../index';

// Mock child panels
jest.mock('../index', () => ({
  OverviewPanel: () => <div data-testid="overview-panel">OverviewPanel</div>,
}));
jest.mock('./NodesByTypePanel', () => ({
  NodesByTypePanel: () => (
    <div data-testid="nodes-by-type-panel">NodesByTypePanel</div>
  ),
}));
jest.mock('./GovernanceWarningsPanel', () => ({
  GovernanceWarningsPanel: () => (
    <div data-testid="governance-warnings-panel">GovernanceWarningsPanel</div>
  ),
}));
jest.mock('./TrendsPanel', () => ({
  TrendsPanel: () => <div data-testid="trends-panel">TrendsPanel</div>,
}));
jest.mock('./DimensionNodeUsagePanel', () => ({
  DimensionNodeUsagePanel: () => (
    <div data-testid="dimension-node-usage-panel">DimensionNodeUsagePanel</div>
  ),
}));

describe('<OverviewPage />', () => {
  it('renders the OverviewPage with all panels', () => {
    render(<OverviewPage />);

    // Check the main containers
    const containers = screen.getAllByClassName
      ? screen.getAllByClassName('chart-container')
      : document.querySelectorAll('.chart-container');

    expect(containers.length).toBe(2);

    // Check each mocked panel appears
    expect(screen.getByTestId('overview-panel')).toBeInTheDocument();
    expect(screen.getByTestId('nodes-by-type-panel')).toBeInTheDocument();
    expect(screen.getByTestId('governance-warnings-panel')).toBeInTheDocument();
    expect(screen.getByTestId('trends-panel')).toBeInTheDocument();
    expect(
      screen.getByTestId('dimension-node-usage-panel'),
    ).toBeInTheDocument();

    // Check panels are in the correct containers
    expect(containers[0].innerHTML).toContain('OverviewPanel');
    expect(containers[0].innerHTML).toContain('NodesByTypePanel');
    expect(containers[0].innerHTML).toContain('GovernanceWarningsPanel');

    expect(containers[1].innerHTML).toContain('TrendsPanel');
    expect(containers[1].innerHTML).toContain('DimensionNodeUsagePanel');
  });
});
