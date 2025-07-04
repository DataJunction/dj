import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';

export const OverviewChart = () => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [nodesByState, setNodesByState] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      setNodesByState(await djClient.analytics.node_counts_by_active());
    };
    fetchData().catch(console.error);
  }, [djClient]);

  return (
    <div className="chart-box" style={{ flex: '0 0 2%' }}>
      <div className="chart-title">Overview</div>
      <div className="jss314">
        {nodesByState?.map(entry =>
          entry.name === 'true' ? (
            <div className="jss313">
              <strong class="jss315">{entry.value}</strong>
              <span>{entry.name === 'true' ? 'Active' : 'Deactivated'}</span>
            </div>
          ) : (
            ''
          ),
        )}
      </div>
    </div>
  );
};
