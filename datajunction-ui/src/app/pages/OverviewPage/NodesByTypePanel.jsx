import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import NodeIcon from '../../icons/NodeIcon';
import '../../../styles/overview.css';

const COLOR_MAPPING = {
  source: '#00C49F',
  dimension: '#FFBB28', //'#FF8042',
  transform: '#0088FE',
  metric: '#FF91A3', //'#FFBB28',
  cube: '#AA46BE',
  valid: '#00B368',
  invalid: '#B34B00',
};

export const NodesByTypePanel = () => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [nodesByType, setNodesByType] = useState(null);
  const [materializationsByType, setMaterializationsByType] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      setNodesByType(await djClient.system.node_counts_by_type());
      setMaterializationsByType(
        await djClient.system.materialization_counts_by_type(),
      );
    };
    fetchData().catch(console.error);
  }, [djClient]);

  return (
    <>
      <div className="chart-box" style={{ flex: '0 0 30%', maxWidth: '350px' }}>
        <div className="chart-title">Nodes by Type</div>
        <div className="horiz-box">
          {nodesByType?.map(entry => (
            <div className="vert-box" key={entry.name}>
              <NodeIcon color={COLOR_MAPPING[entry.name]} />
              <strong style={{ color: COLOR_MAPPING[entry.name] }}>
                {entry.value}
              </strong>
              <span>{entry.name}s</span>
            </div>
          ))}
        </div>
      </div>
      <div className="chart-box" style={{ flex: '0 0 30%', maxWidth: '350px' }}>
        <div className="chart-title">Materializations by Type</div>
        <div className="horiz-box">
          {materializationsByType?.map(entry => (
            <div className="vert-box" key={entry.name}>
              <NodeIcon color={COLOR_MAPPING[entry.name]} />
              <strong style={{ color: COLOR_MAPPING[entry.name] }}>
                {entry.value}
              </strong>
              <span>{entry.name}s</span>
            </div>
          ))}
        </div>
      </div>
    </>
  );
};
