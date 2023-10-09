import { useEffect, useState } from 'react';
import ClientCodePopover from './ClientCodePopover';
import * as React from 'react';
import EditColumnPopover from './EditColumnPopover';
import LinkDimensionPopover from './LinkDimensionPopover';
import { labelize } from '../../../utils/form';
import PartitionColumnPopover from './PartitionColumnPopover';

export default function NodeColumnTab({ node, djClient }) {
  const [attributes, setAttributes] = useState([]);
  const [dimensions, setDimensions] = useState([]);
  const [columns, setColumns] = useState([]);
  useEffect(() => {
    const fetchData = async () => {
      setColumns(await djClient.columns(node));
    };
    fetchData().catch(console.error);
  }, [djClient, node]);

  useEffect(() => {
    const fetchData = async () => {
      const attributes = await djClient.attributes();
      const options = attributes.map(attr => {
        return { value: attr.name, label: labelize(attr.name) };
      });
      setAttributes(options);
    };
    fetchData().catch(console.error);
  }, [djClient]);

  useEffect(() => {
    const fetchData = async () => {
      const dimensions = await djClient.dimensions();
      const options = dimensions.map(name => {
        return { value: name, label: name };
      });
      setDimensions(options);
    };
    fetchData().catch(console.error);
  }, [djClient]);

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

  const showColumnPartition = col => {
    if (col.partition) {
      return (
        <span
          className="node_type__dimension badge node_type"
          key={`col-attr-${col.partition.type_}`}
        >
          <b>Type:</b> {col.partition.type_}
          <br />
          <b>Expr:</b> <code>{col.partition.expression}</code>
        </span>
      );
    }
    return '';
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
            className=""
            role="columnheader"
            aria-label="ColumnDisplayName"
            aria-hidden="false"
          >
            {col.display_name}
          </span>
        </td>
        <td>
          <span
            className={`node_type__${
              node.type === 'cube' ? col.type : 'transform'
            } badge node_type`}
            role="columnheader"
            aria-label="ColumnType"
            aria-hidden="false"
          >
            {col.type}
          </span>
        </td>
        {node.type !== 'cube' ? (
          <td>
            {col.dimension !== undefined && col.dimension !== null ? (
              <>
                <a href={`/nodes/${col.dimension.name}`}>
                  {col.dimension.name}
                </a>
                <ClientCodePopover code={col.clientCode} />
              </>
            ) : (
              ''
            )}{' '}
            <LinkDimensionPopover
              column={col}
              node={node}
              options={dimensions}
              onSubmit={async () => {
                const res = await djClient.node(node.name);
                setColumns(res.columns);
              }}
            />
          </td>
        ) : (
          ''
        )}
        {node.type !== 'cube' ? (
          <td>
            {showColumnAttributes(col)}
            <EditColumnPopover
              column={col}
              node={node}
              options={attributes}
              onSubmit={async () => {
                const res = await djClient.node(node.name);
                setColumns(res.columns);
              }}
            />
          </td>
        ) : (
          ''
        )}
        <td>
          {showColumnPartition(col)}
          <PartitionColumnPopover
            column={col}
            node={node}
            onSubmit={async () => {
              const res = await djClient.node(node.name);
              setColumns(res.columns);
            }}
          />
        </td>
      </tr>
    ));
  };

  return (
    <div className="table-responsive">
      <table className="card-inner-table table">
        <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
          <tr>
            <th className="text-start">Column</th>
            <th>Display Name</th>
            <th>Type</th>
            {node?.type !== 'cube' ? (
              <>
                <th>Linked Dimension</th>
                <th>Attributes</th>
              </>
            ) : (
              ''
            )}
            <th>Partition</th>
          </tr>
        </thead>
        <tbody>{columnList(columns)}</tbody>
      </table>
    </div>
  );
}
