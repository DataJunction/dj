import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';

const COLOR_MAPPING = {
  valid: '#00b368',
  invalid: '#b34b00',
};

export const ByStatusChart = () => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [nodesByStatus, setNodesByStatus] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      setNodesByStatus(await djClient.analytics.node_counts_by_status());
    };
    fetchData().catch(console.error);
  }, [djClient]);

  return (
    <div
      className="chart-box"
      style={{ flex: '0 0 10%', width: 'fit-content' }}
    >
      <div className="chart-title">By Status</div>
      <div className="jss314">
        {nodesByStatus?.map(entry => (
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
              {entry.value}
            </strong>
            <span>{entry.name.toLowerCase()}</span>
          </div>
        ))}
      </div>
    </div>
  );
};
