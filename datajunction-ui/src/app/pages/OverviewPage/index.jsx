import { OverviewPanel } from './OverviewPanel';
import { NodesByTypePanel } from './NodesByTypePanel';
import { GovernanceWarningsPanel } from './GovernanceWarningsPanel';
import { TrendsPanel } from './TrendsPanel';
import { DimensionNodeUsagePanel } from './DimensionNodeUsagePanel';

export function OverviewPage() {
  return (
    <div className="mid">
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
