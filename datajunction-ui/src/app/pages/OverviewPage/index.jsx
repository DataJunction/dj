import { OverviewPanel } from './OverviewPanel';
import { NodesByTypePanel } from './NodesByTypePanel';
import { GovernanceWarningsPanel } from './GovernanceWarningsPanel';
import { TrendsPanel } from './TrendsPanel';
import { DimensionNodeUsagePanel } from './DimensionNodeUsagePanel';

export function OverviewPage() {
  return (
    <div className="mid">
      <div
        style={{
          display: 'flex',
          justifyContent: 'flex-end',
          padding: '0.5rem 1rem',
        }}
      >
        <a href="/overview/explore" className="button-3 neutral-button">
          Explore system metrics →
        </a>
      </div>
      <div className="chart-container">
        <OverviewPanel />
        <NodesByTypePanel />
        <GovernanceWarningsPanel />
      </div>

      <div className="chart-container">
        <TrendsPanel />
        <DimensionNodeUsagePanel />
      </div>
    </div>
  );
}
