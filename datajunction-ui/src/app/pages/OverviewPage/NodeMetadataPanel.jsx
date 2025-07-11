import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';

export const NodeMetadataPanel = () => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [dimensionNodes, setDimensionNodes] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      setDimensionNodes(await djClient.analytics.dimensions());
    };
    fetchData().catch(console.error);
  }, [djClient]);

  return (
    <>
      <div className="chart-box">
        <div className="chart-title">Orphaned Dimension Nodes</div>
        {
          dimensionNodes?.filter(
            dim => dim.indegree === 0 || dim.cube_count === 0,
          ).length
        }
      </div>
    </>
  );
};
