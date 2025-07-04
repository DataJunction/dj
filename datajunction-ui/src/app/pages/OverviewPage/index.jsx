import * as React from 'react';
import { useParams } from 'react-router-dom';
import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import ValidIcon from '../../icons/ValidIcon';
import InvalidIcon from '../../icons/InvalidIcon';
import { OverviewChart } from './OverviewChart';
import { NodesByTypeChart } from './NodesByTypeChart';
import { ByStatusChart } from './ByStatusChart';
import { WithoutDescriptionPanel } from './WithoutDescriptionPanel';
import { TrendsPanel } from './TrendsPanel';
import {
  PieChart,
  Pie,
  Legend,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Bar,
  Cell,
} from 'recharts';
import InvalidIcon from 'app/icons/InvalidIcon';

export function OverviewPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  const [nodesByUser, setNodesByUser] = useState(null);
  const [dimensionNodes, setDimensionNodes] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      setNodeTrends(await djClient.analytics.node_trends());
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
        <OverviewChart />
        <NodesByTypeChart />
        <TrendsPanel />
      </div>

      <div className="chart-container">
        <ByStatusChart />
        <WithoutDescriptionPanel />
      </div>

      <div className="chart-container">
        <div className="chart-box">
          <div className="chart-title">Top 15 Node Creators</div>
          {console.log('nodesByUser', nodesByUser)}
          {nodesByUser ? (
            <ResponsiveContainer width="100%" height={400}>
              <BarChart
                layout="vertical"
                width={1000}
                height={400}
                data={nodesByUser.slice(0, 15)}
                margin={{
                  top: 20,
                  right: 30,
                  left: 20,
                  bottom: 5,
                }}
              >
                <CartesianGrid strokeDasharray="3 3" />
                <YAxis dataKey="username" type="category" />
                <XAxis type="number" />
                <Tooltip />
                <Legend />
                {Object.entries(COLOR_MAPPING).map(([key, color]) => (
                  <Bar
                    key={key}
                    dataKey={key}
                    stackId="nodeCount"
                    fill={color}
                    name={key}
                  />
                ))}
              </BarChart>
            </ResponsiveContainer>
          ) : (
            ''
          )}
        </div>
        <div className="chart-box">
          <div className="chart-title">Links per Dimension Node</div>
          <table className="card-inner-table table">
            <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
              <tr>
                <th className="text-start">Dimension</th>
                <th className="a">Link Count</th>
              </tr>
            </thead>
            <tbody>
              {dimensionNodes
                ?.map(dim => {
                  return {
                    name: dim.name,
                    value: dim.indegree,
                  };
                })
                .sort((a, b) => b.value - a.value) // assuming `value` is indegree
                .slice(0, 10)
                .map((dim, index) => (
                  <tr key={index}>
                    <td className="a">{dim.name}</td>
                    <td className="a">{dim.value}</td>
                  </tr>
                ))}
            </tbody>
          </table>
        </div>
        <div className="chart-box">
          <div className="chart-title">Orphaned Dimension Nodes</div>
          {
            dimensionNodes?.filter(
              dim => dim.indegree === 0 || dim.cube_count === 0,
            ).length
          }
        </div>
      </div>
    </div>
  );
}
