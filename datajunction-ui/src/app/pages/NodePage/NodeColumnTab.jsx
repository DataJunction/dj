import { useEffect, useState } from 'react';
import ClientCodePopover from './ClientCodePopover';

export default function NodeColumnTab({ node, djClient }) {
  const [columns, setColumns] = useState([]);
  useEffect(() => {
    const fetchData = async () => {
      setColumns(await djClient.columns(node));
    };
    fetchData().catch(console.error);
  }, [djClient, node]);

  const columnList = columns => {
    return columns.map(col => (
      <tr>
        <td className="text-start">{col.name}</td>
        <td>
          <span className="node_type__transform badge node_type">
            {col.type}
          </span>
        </td>
        <td>
          {col.dimension !== undefined && col.dimension !== null ? (
            <>
              <a href={`/nodes/${col.dimension.name}`}>{col.dimension.name}</a>
              <ClientCodePopover code={col.clientCode} />
            </>
          ) : (
            ''
          )}{' '}
        </td>
        <td>
          {col.attributes.find(
            attr => attr.attribute_type.name === 'dimension',
          ) ? (
            <span className="node_type__dimension badge node_type">
              dimensional
            </span>
          ) : (
            ''
          )}
        </td>
      </tr>
    ));
  };

  return (
    <div className="table-responsive">
      <table className="card-inner-table table">
        <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
          <th className="text-start">Column</th>
          <th>Type</th>
          <th>Dimension</th>
          <th>Attributes</th>
        </thead>
        {columnList(columns)}
      </table>
    </div>
  );
}
