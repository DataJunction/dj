import { useEffect, useState } from 'react';
import * as React from 'react';

export default function NodesWithDimension({ node, djClient }) {
  const [availableNodes, setAvailableNodes] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      const data = await djClient.nodesWithDimension(node.name);
      setAvailableNodes(data);
    };
    fetchData().catch(console.error);
  }, [djClient, node]);
  return (
    <div className="table-responsive">
      <table className="card-inner-table table">
        <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
          <tr>
            <th className="text-start">Name</th>
            <th>Type</th>
          </tr>
        </thead>
        <tbody>
          {availableNodes.map(node => (
            <tr>
              <td>
                <a href={`/nodes/${node.name}`}>{node.display_name}</a>
              </td>
              <td>
                <span
                  className={'node_type__' + node.type + ' badge node_type'}
                >
                  {node.type}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
