import { useEffect, useState } from 'react';
import ClientCodePopover from './ClientCodePopover';
import TableIcon from '../../icons/TableIcon';
import AddMaterializationPopover from './AddMaterializationPopover';
import * as React from 'react';
import AddBackfillPopover from './AddBackfillPopover';

const cronstrue = require('cronstrue');

export default function NodeMaterializationTab({ node, djClient }) {
  const [materializations, setMaterializations] = useState([]);
  useEffect(() => {
    const fetchData = async () => {
      if (node) {
        const data = await djClient.materializations(node.name);
        setMaterializations(data);
      }
    };
    fetchData().catch(console.error);
  }, [djClient, node]);

  const rangePartition = partition => {
    return (
      <div>
        <span className="badge partition_value">
          <span className="badge partition_value">{partition.range[0]}</span>to
          <span className="badge partition_value">{partition.range[1]}</span>
        </span>
      </div>
    );
  };

  const cron = materialization => {
    var parsedCron = '';
    try {
      parsedCron = cronstrue.toString(materialization.schedule);
    } catch (e) {}
    return parsedCron;
  };

  const materializationRows = materializations => {
    return materializations.map(materialization => (
      <tr key={materialization.name}>
        {/*<td className="text-start node_name">*/}
          {/*<a href={materialization.urls[0]}>{materialization.name}</a>*/}
          {/*<ClientCodePopover code={materialization.clientCode} />*/}
        {/*</td>*/}
        <td className="text-start node_name">
          <span className={`badge cron`}>{materialization.schedule}</span>
          <div className={`cron-description`}>{cron(materialization)} </div>
        </td>
        <td>
          {materialization.engine.name}
          <br />
          {materialization.engine.version}
          <ClientCodePopover code={materialization.clientCode} />
        </td>
        <td>
          {(node.type === 'cube' ? node.cube_elements : node.columns)
            .filter(col => col.partition !== null)
            .map(column => {
              return (
                <div className="partition__full" key={column.name}>
                  <div className="partition__header">
                    {column.display_name}
                  </div>
                  <div className="partition__body">
                    {/*<span*/}
                    {/*  className={`badge partition_value_highlight`}*/}
                    {/*  style={{ fontSize: '100%' }}*/}
                    {/*>*/}
                    <code>{column.name}</code>
                    <span className="badge partition_value">{column.partition.type_}</span>
                    {/*</span>*/}
                  </div>
                </div>
              );
            })}
          {materialization.config.partitions ? (
            materialization.config.partitions.map(partition =>
              partition.type_ === 'categorical' ? (
                <div className="partition__full" key={partition.name}>
                  <div className="partition__header">{partition.name}</div>
                  <div className="partition__body">
                    {partition.values !== null && partition.values.length > 0
                      ? partition.values.map(val => (
                          <span
                            className="badge partition_value"
                            key={`partition-value-${val}`}
                          >
                            {val}
                          </span>
                        ))
                      : null}
                    {partition.range !== null && partition.range.length > 0
                      ? rangePartition(partition)
                      : null}
                    {(partition.range === null && partition.values === null) ||
                    (partition.range?.length === 0 &&
                      partition.values?.length === 0) ? (
                      <span className={`badge partition_value_highlight`}>
                        ALL
                      </span>
                    ) : null}
                  </div>
                </div>
              ) : (
                <span
                  className={`badge partition_value_highlight`}
                  style={{ fontSize: '100%' }}
                >
                  {partition.name}
                </span>
              ),
            )
          ) : (
            <br />
          )}
        </td>
        <td>
          {materialization.output_tables.map(table => (
            <div className={`table__full`} key={table}>
              <div className="table__header">
                <TableIcon />{' '}
                <span className={`entity-info`}>
                  {table.split('.')[0] + '.' + table.split('.')[1]}
                </span>
              </div>
              <div className={`table__body upstream_tables`}>
                {table.split('.')[2]}
              </div>
            </div>
          ))}
        </td>

        <td>
          {/*{materialization.config.partitions ? (*/}
          {/*  materialization.config.partitions.map(partition =>*/}
          {/*    partition.type_ === 'temporal' ? (*/}
          {/*      <div className="partition__full" key={partition.name}>*/}
          {/*        <div className="partition__header">{partition.name}</div>*/}
          {/*        <div className="partition__body">*/}
          {/*          {partition.values !== null && partition.values.length > 0*/}
          {/*            ? partition.values.map(val => (*/}
          {/*                <span className="badge partition_value">{val}</span>*/}
          {/*              ))*/}
          {/*            : null}*/}
          {/*          {partition.range !== null && partition.range.length > 0*/}
          {/*            ? rangePartition(partition)*/}
          {/*            : null}*/}
          {/*        </div>*/}
          {/*      </div>*/}
          {/*    ) : null,*/}
          {/*  )*/}
          {/*) : (*/}
          {/*  <br />*/}
          {/*)}*/}
          <AddBackfillPopover node={node} materialization={materialization} />
        </td>
        <td>
          {materialization.urls.map((url, idx) => (
            <a href={url} key={`url-${idx}`}>
              [{idx + 1}]
            </a>
          ))}
        </td>
      </tr>
    ));
  };
  return (
    <>
      <div className="table-vertical">
        <div>
          <h2>Materializations</h2>
          <AddMaterializationPopover node={node} />
          {materializations.length > 0 ? (
            <table
              className="card-inner-table table"
              aria-label="Materializations"
              aria-hidden="false"
            >
              <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
                <tr>
                  {/*<th className="text-start">Name</th>*/}
                  <th className="text-start">Schedule</th>
                  <th>Engine</th>
                  <th>Partitions</th>
                  <th>Output Tables</th>
                  <th>Backfills</th>
                  <th>URLs</th>
                </tr>
              </thead>
              <tbody>
                {materializationRows(
                  materializations.filter(
                    materialization =>
                      !(
                        materialization.name === 'default' &&
                        node.type === 'cube'
                      ),
                  ),
                )}
              </tbody>
            </table>
          ) : (
            <div className="message alert" style={{ marginTop: '10px' }}>
              No materialization workflows configured for this node.
            </div>
          )}
        </div>
        <div>
          <h2>Materialized Datasets</h2>
          {node && node.availability !== null ? (
            <table
              className="card-inner-table table"
              aria-label="Availability"
              aria-hidden="false"
            >
              <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
                <tr>
                  <th className="text-start">Catalog</th>
                  <th>Schema</th>
                  <th>Table</th>
                  <th>Valid Through</th>
                  <th>Partitions</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>{node.availability.schema_}</td>
                  <td>
                    {
                      <div
                        className={`table__full`}
                        key={node.availability.table}
                      >
                        <div className="table__header">
                          <TableIcon />{' '}
                          <span className={`entity-info`}>
                            {node.availability.catalog +
                              '.' +
                              node.availability.schema_}
                          </span>
                        </div>
                        <div className={`table__body upstream_tables`}>
                          <a href={node.availability.url}>
                            {node.availability.table}
                          </a>
                        </div>
                      </div>
                    }
                  </td>
                  <td>{node.availability.valid_through_ts}</td>
                  <td>
                    <span
                      className={`badge partition_value`}
                      style={{ fontSize: '100%' }}
                    >
                      <span className={`badge partition_value_highlight`}>
                        {node.availability.min_temporal_partition}
                      </span>
                      to
                      <span className={`badge partition_value_highlight`}>
                        {node.availability.max_temporal_partition}
                      </span>
                    </span>
                  </td>
                </tr>
              </tbody>
            </table>
          ) : (
            <div className="message alert" style={{ marginTop: '10px' }}>
              No materialized datasets available for this node.
            </div>
          )}
        </div>
      </div>
    </>
  );
}
