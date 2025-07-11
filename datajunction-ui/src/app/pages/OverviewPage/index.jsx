import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { OverviewPanel } from './OverviewPanel';
import { NodesByTypeChart } from './NodesByTypeChart';
import { ByStatusPanel } from './ByStatusPanel';
import { WithoutDescriptionPanel } from './WithoutDescriptionPanel';
import { TrendsPanel } from './TrendsPanel';
import {
  Legend,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  XAxis,
  YAxis,
  Bar,
} from 'recharts';
import { NodeMetadataPanel } from './NodeMetadataPanel';

export function OverviewPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  const [nodesByUser, setNodesByUser] = useState(null);
  const [dimensionNodes, setDimensionNodes] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      setNodesByUser(await djClient.analytics.node_counts_by_user());
      setDimensionNodes(await djClient.analytics.dimensions());
    };
    fetchData().catch(console.error);
  }, [djClient]);

  const COLOR_MAPPING = {
    source: '#00C49F',
    dimension: '#FFBB28', //'#FF8042',
    transform: '#0088FE',
    metric: '#ff91a3', //'#FFBB28',
    cube: '#AA46BE',
    valid: '#00b368',
    invalid: '#b34b00',
  };

  return (
    <div className="mid">
      <div className="chart-container">
        <OverviewPanel />
        <NodesByTypeChart />
        {/* <ByStatusPanel /> */}
        <WithoutDescriptionPanel />
      </div>

      <div className="chart-container">
        <TrendsPanel />
      </div>

      <div className="chart-container">
        <div className="chart-box">
          <div className="chart-title">Dimension Node Usage</div>
          <table className="card-inner-table table">
            <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
              <tr>
                <th className="text-start">Dimension</th>
                <th className="a">Links</th>
                <th className="a">Cubes</th>
              </tr>
            </thead>
            <tbody>
              {console.log('dimnodes', dimensionNodes)}
              {dimensionNodes
                ?.sort(
                  (a, b) =>
                    b.cube_count + b.indegree - (a.cube_count + a.indegree),
                )
                .slice(0, 10)
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
        <NodeMetadataPanel />
      </div>
    </div>
  );
}
