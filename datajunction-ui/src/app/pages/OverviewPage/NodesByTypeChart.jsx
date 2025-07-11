import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { WithoutDescriptionPanel } from './WithoutDescriptionPanel';
import NodeIcon from '../../icons/NodeIcon';

const COLOR_MAPPING = {
  source: '#00C49F',
  dimension: '#FFBB28', //'#FF8042',
  transform: '#0088FE',
  metric: '#FF91A3', //'#FFBB28',
  cube: '#AA46BE',
  valid: '#00B368',
  invalid: '#B34B00',
};

export const NodesByTypeChart = () => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [nodesByType, setNodesByType] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      setNodesByType(await djClient.analytics.node_counts_by_type());
    };
    fetchData().catch(console.error);
  }, [djClient]);

  return (
    <div
      className="chart-box"
      style={{ flex: '0 0 30%', width: 'fit-content' }}
    >
      <div className="chart-title">Nodes by Type</div>
      <div className="jss314">
        {nodesByType?.map(entry => (
          <div className="jss313">
            <NodeIcon color={COLOR_MAPPING[entry.name]} />
            <strong class="jss315" style={{ color: COLOR_MAPPING[entry.name] }}>
              {entry.value}
            </strong>
            <span>{entry.name}s</span>
          </div>
        ))}
      </div>
      {/* <WithoutDescriptionPanel /> */}
    </div>
  );
};
