import { useContext, useEffect } from 'react';
import DJClientContext from '../../providers/djclient';
import { OverviewPanel } from './OverviewPanel';
import { NodesByTypePanel } from './NodesByTypePanel';
import { GovernanceWarningsPanel } from './GovernanceWarningsPanel';
import { TrendsPanel } from './TrendsPanel';
import { DimensionNodeUsagePanel } from './DimensionNodeUsagePanel';

export function OverviewPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  useEffect(() => {
    const fetchData = async () => {
      setNodesByUser(await djClient.analytics.node_counts_by_user());
      setDimensionNodes(await djClient.analytics.dimensions());
    };
    fetchData().catch(console.error);
  }, [djClient]);

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
