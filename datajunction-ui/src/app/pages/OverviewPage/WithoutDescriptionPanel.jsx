import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';

const COLOR_MAPPING = {
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
      style={{ flex: '0 0 10%', width: 'fit-content' }}
    >
      <div className="chart-title">Nodes without Description</div>
      <div className="jss314">
        {nodesWithoutDescription?.map(entry => (
          <div className="jss313">
            <span style={{ color: COLOR_MAPPING[entry.name.toLowerCase()] }}>
              {entry.name === 'VALID' ? (
                <ValidIcon width={'45px'} height={'45px'} />
              ) : (
                <InvalidIcon width={'45px'} height={'45px'} />
              )}
            </span>
            <strong
              class="jss315"
              style={{ color: COLOR_MAPPING[entry.name.toLowerCase()] }}
            >
              {Math.round(entry.value * 100)}%
            </strong>
            <span>{entry.name.toLowerCase()}</span>
          </div>
        ))}
      </div>
    </div>
  );
};
