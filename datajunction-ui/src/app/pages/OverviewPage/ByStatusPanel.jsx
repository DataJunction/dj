import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';

import ValidIcon from '../../icons/ValidIcon';
import InvalidIcon from '../../icons/InvalidIcon';

const COLOR_MAPPING = {
  valid: '#00b368',
  invalid: '#FF91A3', // '#b34b00',
};

export const ByStatusPanel = () => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [nodesByStatus, setNodesByStatus] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      setNodesByStatus(await djClient.analytics.node_counts_by_status());
    };
    fetchData().catch(console.error);
  }, [djClient]);

  return (
    <>
      <div className="chart-box" style={{ flex: '0 0 2%' }}>
        <div className="horiz-box">
          <div className="chart-title">Nodes By Status</div>
          {nodesByStatus?.map(entry => (
            <div
              className="jss316 badge"
              style={{ color: '#000', margin: '0.2em' }}
            >
              <span style={{ color: COLOR_MAPPING[entry.name.toLowerCase()] }}>
                {entry.name === 'VALID' ? (
                  <ValidIcon
                    width={'45px'}
                    height={'45px'}
                    style={{ marginTop: '0.75em' }}
                  />
                ) : (
                  <InvalidIcon
                    width={'45px'}
                    height={'45px'}
                    style={{ marginTop: '0.75em' }}
                  />
                )}
              </span>

              <div style={{ display: 'inline-grid', alignItems: 'center' }}>
                <strong
                  class="horiz-box-value"
                  style={{
                    color: COLOR_MAPPING[entry.name.toLowerCase()],
                  }}
                >
                  {entry.value}
                </strong>
                <span className={'horiz-box-label'}>
                  {entry.name.toLowerCase()} nodes
                </span>
              </div>
            </div>
          ))}
        </div>
      </div>
    </>
  );
};
