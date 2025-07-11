import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import {
  Legend,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Bar,
} from 'recharts';

const COLOR_MAPPING = {
  source: '#00C49F',
  dimension: '#FFBB28',
  transform: '#0088FE',
  metric: '#FF91A3',
  cube: '#AA46BE',
};

export const TrendsPanel = () => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [nodeTrends, setNodeTrends] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      setNodeTrends(await djClient.analytics.node_trends());
    };
    fetchData().catch(console.error);
  }, [djClient]);

  return (
    <div className="chart-box" style={{ maxWidth: '60%', flex: '1 1 20%' }}>
      <div className="chart-title">Trends</div>
      <ResponsiveContainer width="100%" height={400}>
        <BarChart
          width={1000}
          height={400}
          data={nodeTrends}
          margin={{
            top: 20,
            right: 30,
            left: 20,
            bottom: 5,
          }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="date" />
          <YAxis />
          <Tooltip />
          <Legend />
          {Object.entries(COLOR_MAPPING).map(([key, color]) => (
            <Bar
              key={key}
              dataKey={key}
              stackId="nodeCount"
              fill={color}
              name={key.charAt(0).toUpperCase() + key.slice(1)}
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};
