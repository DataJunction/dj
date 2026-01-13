import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import NodeIcon from '../../icons/NodeIcon';

import ValidIcon from '../../icons/ValidIcon';
import InvalidIcon from '../../icons/InvalidIcon';

const COLOR_MAPPING = {
  valid: '#00b368',
  invalid: '#FF91A3', // '#b34b00',
};

export const OverviewPanel = () => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [nodesByActive, setNodesByActive] = useState(null);
  const [nodesByStatus, setNodesByStatus] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      setNodesByActive(await djClient.system.node_counts_by_active());
      setNodesByStatus(await djClient.system.node_counts_by_status());
    };
    fetchData().catch(console.error);
  }, [djClient]);

  return (
    <div className="chart-box" style={{ flex: '0 0 2%' }}>
      <div className="chart-title">Overview</div>
      <div className="horiz-box">
        {nodesByActive
          ?.filter(entry => entry.name === 'true')
          .map(entry => (
            <div
              className="jss316 badge"
              style={{ color: '#000', margin: '0.2em' }}
            >
              <NodeIcon color="#FFBB28" style={{ marginTop: '0.75em' }} />
              <div style={{ display: 'inline-grid', alignItems: 'center' }}>
                <strong className="horiz-box-value">{entry.value}</strong>
                <span className={'horiz-box-label'}>Active Nodes</span>
              </div>
            </div>
          ))}
      </div>
      <div className="horiz-box">
        {nodesByStatus?.map(entry => (
          <div
            className="jss316 badge"
            style={{ color: '#000', margin: '0.2em', marginLeft: '1.2em' }}
          >
            ↳
            <span
              style={{
                color: COLOR_MAPPING[entry.name.toLowerCase()],
                margin: '0 0.2em 0 0.4em',
              }}
            >
              {entry.name === 'VALID' ? (
                <ValidIcon
                  width={'25px'}
                  height={'25px'}
                  style={{ marginTop: '0.2em' }}
                />
              ) : (
                <InvalidIcon
                  width={'25px'}
                  height={'25px'}
                  style={{ marginTop: '0.2em' }}
                />
              )}
            </span>
            <div style={{ display: 'inline-flex', alignItems: 'center' }}>
              <strong
                style={{
                  color: COLOR_MAPPING[entry.name.toLowerCase()],
                  margin: '0 2px',
                  fontSize: '16px',
                  textAlign: 'left',
                }}
              >
                {entry.value}
              </strong>
              <span style={{ fontSize: 'smaller', padding: '5px 2px' }}>
                {entry.name.toLowerCase()}
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};
