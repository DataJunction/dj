import { useEffect, useState } from 'react';
import * as React from 'react';
import EditColumnPopover from './EditColumnPopover';
import EditColumnDescriptionPopover from './EditColumnDescriptionPopover';
import ManageDimensionLinksDialog from './ManageDimensionLinksDialog';
import AddComplexDimensionLinkPopover from './AddComplexDimensionLinkPopover';
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
      if (node) {
        setColumns(await djClient.columns(node));
      }
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
          <span className="node_type badge node_type__blank">
            <span className="partition_value badge">
              <b>Type:</b> {col.partition.type_}
            </span>
            <br />
            <span className="partition_value badge">
              <b>Format:</b> <code>{col.partition.format}</code>
            </span>
            <br />
            <span className="partition_value badge">
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
      // FK Links: Only show links that specifically reference THIS column's foreign keys
      // Filter out complex dimension links that may join on multiple columns
      const fkLinksForColumn = (
        links.length > 0 ? links : node?.dimension_links
      )
        .filter(link => {
          // Check if this link has a foreign key entry for this specific column
          const foreignKeys = Object.keys(link.foreign_keys || {});
          const columnKey = `${node.name}.${col.name}`;
          return foreignKeys.includes(columnKey);
        })
        .map(link => link.dimension.name);

      // Check if this column has a reference dimension link
      const referenceLink = col.dimension
        ? {
            dimension: col.dimension.name,
            dimension_column: col.dimension_column,
            role: col.dimension.role,
          }
        : null;
      return (
        <tr key={col.name} className="column-row">
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
              className=""
              role="columnheader"
              aria-label="ColumnDescription"
              aria-hidden="false"
            >
              {col.description || ''}
              <EditColumnDescriptionPopover
                column={col}
                node={node}
                onSubmit={async () => {
                  const res = await djClient.node(node.name);
                  setColumns(res.columns);
                }}
              />
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
            <td className="dimension-links-cell">
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  flexWrap: 'wrap',
                  gap: '0.25rem',
                  position: 'relative',
                }}
              >
                {fkLinksForColumn.length > 0 && (
                  <div
                    style={{
                      display: 'flex',
                      flexWrap: 'wrap',
                      gap: '0.25rem',
                    }}
                  >
                    {fkLinksForColumn.map(dimName => (
                      <span
                        className="rounded-pill badge bg-secondary-soft dimension-badge"
                        style={{ fontSize: '14px', position: 'relative' }}
                        key={dimName}
                        title="FK Link (via primary key)"
                      >
                        <a href={`/nodes/${dimName}`}>{dimName}</a>
                      </span>
                    ))}
                  </div>
                )}
                {referenceLink && (
                  <span
                    className="rounded-pill badge bg-info dimension-badge"
                    style={{ fontSize: '14px', position: 'relative' }}
                    title={`Reference Link: ${referenceLink.dimension}.${referenceLink.dimension_column}`}
                  >
                    <a href={`/nodes/${referenceLink.dimension}`}>
                      {referenceLink.dimension}.{referenceLink.dimension_column}
                    </a>
                  </span>
                )}
                <ManageDimensionLinksDialog
                  column={col}
                  node={node}
                  dimensions={dimensions}
                  fkLinks={fkLinksForColumn}
                  referenceLink={referenceLink}
                  onSubmit={async () => {
                    const res = await djClient.node(node.name);
                    setLinks(res.dimension_links);
                    setColumns(res.columns);
                  }}
                />
              </div>
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
      <style>
        {`
          .dimension-link-edit:hover {
            opacity: 1 !important;
            color: #007bff !important;
          }
          .dimension-badge a {
            max-width: 300px;
            display: inline-block;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            vertical-align: bottom;
            direction: rtl;
            text-align: left;
          }
        `}
      </style>
      <div className="table-responsive">
        <table className="card-inner-table table">
          <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
            <tr>
              <th className="text-start">Column</th>
              <th>Display Name</th>
              <th>Description</th>
              <th>Type</th>
              {node?.type !== 'cube' ? (
                <>
                  <th>Dimension Links</th>
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
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            marginBottom: '1rem',
          }}
        >
          <h3 style={{ margin: 0 }}>
            Complex Dimension Links (Custom Join SQL)
          </h3>
          <AddComplexDimensionLinkPopover
            node={node}
            dimensions={dimensions}
            onSubmit={async () => {
              const res = await djClient.node(node.name);
              setLinks(res.dimension_links);
            }}
          />
        </div>
        <table className="card-inner-table table">
          <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
            <tr>
              <th className="text-start">Dimension Node</th>
              <th>Join Type</th>
              <th>Join SQL</th>
              <th>Join Cardinality</th>
              <th>Role</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {node?.dimension_links && node.dimension_links.length > 0 ? (
              node.dimension_links.map((link, idx) => {
                return (
                  <tr key={`${link.dimension.name}-${link.role || idx}`}>
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
                    <td>
                      {link.join_cardinality
                        ? link.join_cardinality.replace(/_/g, ' ').toUpperCase()
                        : 'N/A'}
                    </td>
                    <td>{link.role || '-'}</td>
                    <td>
                      <button
                        onClick={async () => {
                          if (
                            window.confirm(
                              `Remove link to ${link.dimension.name}?`,
                            )
                          ) {
                            const response =
                              await djClient.removeComplexDimensionLink(
                                node.name,
                                link.dimension.name,
                                link.role,
                              );
                            if (
                              response.status === 200 ||
                              response.status === 201
                            ) {
                              const res = await djClient.node(node.name);
                              setLinks(res.dimension_links);
                            } else {
                              alert(
                                response.json?.message ||
                                  'Failed to remove link',
                              );
                            }
                          }
                        }}
                        style={{
                          padding: '0.25rem 0.5rem',
                          fontSize: '0.75rem',
                          background: '#dc3545',
                          color: 'white',
                          border: 'none',
                          borderRadius: '4px',
                          cursor: 'pointer',
                        }}
                      >
                        Remove
                      </button>
                    </td>
                  </tr>
                );
              })
            ) : (
              <tr>
                <td
                  colSpan="6"
                  style={{
                    textAlign: 'center',
                    padding: '2rem',
                    color: '#6c757d',
                  }}
                >
                  No complex dimension links. Click the + button above to add
                  one.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </>
  );
}
