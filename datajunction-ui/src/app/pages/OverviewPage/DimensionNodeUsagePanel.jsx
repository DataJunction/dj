import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';

export const DimensionNodeUsagePanel = () => {
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
        <div className="chart-title">Dimension Node Usage</div>
        <table className="card-inner-table table" style={{ marginTop: '0' }}>
          <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
            <tr>
              <th className="text-start">Dimension</th>
              <th className="a">Links</th>
              <th className="a">Cubes</th>
            </tr>
          </thead>
          <tbody>
            {dimensionNodes
              ?.sort(
                (a, b) =>
                  b.cube_count + b.indegree - (a.cube_count + a.indegree),
              )
              .slice(0, 6)
              .map((dim, index) => (
                <tr key={index}>
                  <td className="a">
                    <a href={`/nodes/${dim.name}`}>{dim.name}</a>
                  </td>
                  <td className="a">{dim.indegree}</td>
                  <td className="a">{dim.cube_count}</td>
                </tr>
              ))}
          </tbody>
        </table>
      </div>
    </>
  );
};
