import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';

import ValidIcon from '../../icons/ValidIcon';
import InvalidIcon from '../../icons/InvalidIcon';

const COLOR_MAPPING = {
  source: '#00C49F',
  dimension: '#FFBB28', //'#FF8042',
  transform: '#0088FE',
  metric: '#ff91a3', //'#FFBB28',
  cube: '#AA46BE',
  valid: '#00b368',
  invalid: '#b34b00',
};

export const WithoutDescriptionPanel = () => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [nodesWithoutDescription, setNodesWithoutDescription] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      setNodesWithoutDescription(
        await djClient.analytics.nodes_without_description(),
      );
    };
    fetchData().catch(console.error);
  }, [djClient]);

  return (
    <div
      className="chart-box"
      style={{ flex: '1 1 10%', maxWidth: 'fit-content' }}
    >
      <div className="chart-title">Nodes without Description</div>
      <div className="jss314">
        {nodesWithoutDescription?.map(entry => (
          <div className="jss313">
            <strong
              class="jss315"
              style={{ color: COLOR_MAPPING[entry.name.toLowerCase()] }}
            >
              {Math.round(entry.value * 100)}%
            </strong>
            <span>{entry.name.toLowerCase()}s</span>
          </div>
        ))}
      </div>
    </div>
  );
};
