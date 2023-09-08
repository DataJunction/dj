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

  const showColumnAttributes = col => {
    return col.attributes.map((attr, idx) => (
      <span
        className="node_type__dimension badge node_type"
        key={`col-attr-${col.name}-${idx}`}
      >
        {attr.attribute_type.name.replace(/_/, ' ')}
      </span>
    ));
  };

  const columnList = columns => {
    return columns.map(col => (
      <tr key={col.name}>
        <td
          className="text-start"
          role="columnheader"
          aria-label="ColumnName"
          aria-hidden="false"
        >
          {col.name}
        </td>
        <td>
          <span
            className="node_type__transform badge node_type"
            role="columnheader"
            aria-label="ColumnType"
            aria-hidden="false"
          >
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
        <td>{showColumnAttributes(col)}</td>
      </tr>
    ));
  };

  return (
    <div className="table-responsive">
      <table className="card-inner-table table">
        <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
          <tr>
            <th className="text-start">Column</th>
            <th>Type</th>
            <th>Dimension</th>
            <th>Attributes</th>
          </tr>
        </thead>
        <tbody>{columnList(columns)}</tbody>
      </table>
    </div>
  );
}
