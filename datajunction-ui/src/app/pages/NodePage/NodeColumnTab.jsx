import { useEffect, useState } from 'react';
import * as React from 'react';
import EditColumnPopover from './EditColumnPopover';
import LinkDimensionPopover from './LinkDimensionPopover';
import { labelize } from '../../../utils/form';
import PartitionColumnPopover from './PartitionColumnPopover';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';

export default function NodeColumnTab({ node, djClient }) {
  const [attributes, setAttributes] = useState([]);
  const [dimensions, setDimensions] = useState([]);
  const [columns, setColumns] = useState([]);
  const [links, setLinks] = useState([]);

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
      const options = dimensions.map(dim => {
        return {
          value: dim.name,
          label: `${dim.name} (${dim.indegree} links)`,
        };
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
        <>
          <span
            className="node_type badge node_type__blank"
            key={`col-attr-partition-type`}
          >
            <span
              className="partition_value badge"
              key={`col-attr-partition-type`}
            >
              <b>Type:</b> {col.partition.type_}
            </span>
            <br />
            <span
              className="partition_value badge"
              key={`col-attr-partition-type`}
            >
              <b>Format:</b> <code>{col.partition.format}</code>
            </span>
            <br />
            <span
              className="partition_value badge"
              key={`col-attr-partition-type`}
            >
              <b>Granularity:</b> <code>{col.partition.granularity}</code>
            </span>
          </span>
        </>
      );
    }
    return '';
  };

  const columnList = columns => {
    return columns?.map(col => {
      const dimensionLinks = (links.length > 0 ? links : node?.dimension_links)
        .map(link => [
          link.dimension.name,
          Object.entries(link.foreign_keys).filter(
            entry => entry[0] === node.name + '.' + col.name,
          ),
        ])
        .filter(keys => keys[1].length >= 1);
      const referencedDimensionNode =
        dimensionLinks.length > 0 ? dimensionLinks[0][0] : null;
      return (
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
              {referencedDimensionNode !== null ? (
                <a href={`/nodes/${referencedDimensionNode}`}>
                  {referencedDimensionNode}
                </a>
              ) : (
                ''
              )}
              <LinkDimensionPopover
                column={col}
                referencedDimensionNode={referencedDimensionNode}
                node={node}
                options={dimensions}
                onSubmit={async () => {
                  const res = await djClient.node(node.name);
                  setLinks(res.dimension_links);
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
      );
    });
  };

  return (
    <>
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
      <div>
        <h3>Linked Dimensions (Custom Join SQL)</h3>
        <table className="card-inner-table table">
          <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
            <tr>
              <th className="text-start">Dimension Node</th>
              <th>Join Type</th>
              <th>Join SQL</th>
              <th>Role</th>
            </tr>
          </thead>
          <tbody>
            {node?.dimension_links.map(link => {
              return (
                <tr>
                  <td>
                    <a href={'/nodes/' + link.dimension.name}>
                      {link.dimension.name}
                    </a>
                  </td>
                  <td>{link.join_type.toUpperCase()}</td>
                  <td style={{ width: '25rem', maxWidth: 'none' }}>
                    <SyntaxHighlighter
                      language="sql"
                      style={foundation}
                      wrapLongLines={true}
                    >
                      {link.join_sql}
                    </SyntaxHighlighter>
                  </td>
                  <td>{link.role}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </>
  );
}
