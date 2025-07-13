import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import '../../../styles/node-creation.scss';
const COLOR_MAPPING = {
  source: '#00C49F',
  dimension: '#FFBB28', //'#FF8042',
  transform: '#0088FE',
  metric: '#ff91a3', //'#FFBB28',
  cube: '#AA46BE',
  valid: '#00b368',
  invalid: '#b34b00',
};

export const GovernanceWarningsPanel = () => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [nodesWithoutDescription, setNodesWithoutDescription] = useState(null);
  const [dimensionNodes, setDimensionNodes] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      setNodesWithoutDescription(
        await djClient.analytics.nodes_without_description(),
      );
      setDimensionNodes(await djClient.analytics.dimensions());
    };
    fetchData().catch(console.error);
  }, [djClient]);

  return (
    <div className="chart-box" style={{ flex: '1 1 10%', maxWidth: '470px' }}>
      <div className="chart-title">Governance Warnings</div>
      <div
        className="horiz-box"
        style={{
          padding: '5px 10px',
          marginTop: '10px',
          border: '1px solid #AA46BE30',
        }}
      >
        <span style={{ color: '#FF804255', fontSize: '30px' }}>⚠</span>
        <span style={{ padding: '10px 12px', fontSize: '18px' }}>
          Missing Description
        </span>
        <div style={{ display: 'block' }}>
          {nodesWithoutDescription?.map(entry => (
            <div
              className="jss316 badge"
              style={{
                margin: '5px 10px',
                fontSize: '14px',
                padding: '10px',
                color: COLOR_MAPPING[entry.name.toLowerCase()],
                backgroundColor: COLOR_MAPPING[entry.name.toLowerCase()] + '10',
              }}
            >
              <strong>{Math.round(entry.value * 100)}%</strong>{' '}
              <span>{entry.name.toLowerCase()}s</span>
            </div>
          ))}
        </div>
      </div>
      <div
        className="horiz-box"
        style={{
          padding: '5px 10px',
          marginTop: '10px',
          border: '1px solid #AA46BE30',
        }}
      >
        <div
          style={{ width: '100%', display: 'inline-flex', marginTop: '-10px' }}
        >
          <span style={{ color: '#FF804255', fontSize: '40px' }}>∅</span>
          <span
            style={{
              padding: '10px 12px',
              fontSize: '18px',
              marginTop: '10px',
            }}
          >
            Orphaned Dimensions
          </span>
        </div>
        <div style={{ display: 'block' }}>
          <div
            className="jss316 badge"
            style={{
              margin: '5px 10px',
              fontSize: '14px',
              padding: '10px',
              color: COLOR_MAPPING.dimension,
              backgroundColor: COLOR_MAPPING.dimension + '10',
            }}
          >
            <strong>
              {dimensionNodes?.filter(
                dim => dim.indegree === 0 || dim.cube_count === 0,
              ).length || '...'}
            </strong>
            <span> dimension nodes</span>
          </div>
        </div>
      </div>
    </div>
  );
};
