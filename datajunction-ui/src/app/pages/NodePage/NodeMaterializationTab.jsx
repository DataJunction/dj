import { useEffect, useState } from 'react';
import TableIcon from '../../icons/TableIcon';
import AddMaterializationPopover from './AddMaterializationPopover';
import * as React from 'react';
import AddBackfillPopover from './AddBackfillPopover';
import { labelize } from '../../../utils/form';
import NodeMaterializationDelete from '../../components/NodeMaterializationDelete';

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

  const partitionColumnsMap = node
    ? Object.fromEntries(
        node?.columns
          .filter(col => col.partition !== null)
          .map(col => [col.name, col.display_name]),
      )
    : {};
  const cron = materialization => {
    var parsedCron = '';
    try {
      parsedCron = cronstrue.toString(materialization.schedule);
    } catch (e) {}
    return parsedCron;
  };

  const materializationRows = materializations => {
    return materializations.map(materialization => (
      <>
        <div className="tr">
          <div key={materialization.name} style={{ fontSize: 'large' }}>
            <div
              className="text-start node_name td"
              style={{ fontWeight: '600' }}
            >
              {materialization.job
                ?.replace('MaterializationJob', '')
                .match(/[A-Z][a-z]+/g)
                .join(' ')}
            </div>
            <div className="td">
              <NodeMaterializationDelete
                nodeName={node.name}
                materializationName={materialization.name}
              />
            </div>
            <div className="td">
              <span className={`badge cron`}>{materialization.schedule}</span>
              <div className={`cron-description`}>{cron(materialization)} </div>
            </div>
            <div className="td">
              <span className={`badge strategy`}>
                {labelize(materialization.strategy)}
              </span>
            </div>
          </div>
        </div>
        <div style={{ display: 'table-row' }}>
          <div style={{ display: 'inline-flex' }}>
            <ul className="backfills">
              <li className="backfill">
                <div className="backfills_header">Output Tables</div>{' '}
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
              </li>
            </ul>
          </div>

          <div style={{ display: 'inline-flex' }}>
            <ul className="backfills">
              <li>
                <div className="backfills_header">Workflows</div>{' '}
                <ul>
                  {materialization.urls.map((url, idx) => (
                    <li style={{ listStyle: 'none' }} key={idx}>
                      <div
                        className="partitionLink"
                        style={{ fontSize: 'revert' }}
                      >
                        <a
                          href={url}
                          key={`url-${idx}`}
                          className=""
                          target="blank"
                        >
                          {idx === 0 ? 'main' : 'backfill'}
                        </a>
                      </div>
                    </li>
                  ))}
                </ul>
              </li>
            </ul>
          </div>

          <div style={{ display: 'inline-flex' }}>
            <ul className="backfills">
              <li className="backfill">
                <details open>
                  <summary>
                    <span className="backfills_header">Backfills</span>{' '}
                  </summary>
                  {materialization.strategy === 'incremental_time' ? (
                    <ul>
                      <li>
                        <AddBackfillPopover
                          node={node}
                          materialization={materialization}
                        />
                      </li>
                      {materialization.backfills.map(backfill => (
                        <li className="backfill">
                          <div className="partitionLink">
                            <a href={backfill.urls[0]}>
                              {backfill.spec.map(partition => {
                                const partitionBody =
                                  'range' in partition &&
                                  partition['range'] !== null ? (
                                    <>
                                      <span className="badge partition_value">
                                        {partition.range[0]}
                                      </span>
                                      to
                                      <span className="badge partition_value">
                                        {partition.range[1]}
                                      </span>
                                    </>
                                  ) : (
                                    <span className="badge partition_value">
                                      {partition.values.join(', ')}
                                    </span>
                                  );
                                return (
                                  <>
                                    <div>
                                      {
                                        partitionColumnsMap[
                                          partition.column_name.replaceAll(
                                            '_DOT_',
                                            '.',
                                          )
                                        ]
                                      }{' '}
                                      {partitionBody}
                                    </div>
                                  </>
                                );
                              })}
                            </a>
                          </div>
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <ul>
                      <li>N/A</li>
                    </ul>
                  )}
                </details>
              </li>
            </ul>
          </div>
          <div className="td">
            <ul className="backfills">
              <li className="backfill">
                <div className="backfills_header">Partitions</div>{' '}
                <ul>
                  {node.columns
                    .filter(col => col.partition !== null)
                    .map(column => {
                      return (
                        <li>
                          <div className="partitionLink">
                            {column.display_name}
                            <span className="badge partition_value">
                              {column.partition.type_}
                            </span>
                          </div>
                        </li>
                      );
                    })}
                </ul>
              </li>
            </ul>
          </div>
        </div>
      </>
    ));
  };
  return (
    <>
      <div
        className="table-vertical"
        role="table"
        aria-label="Materializations"
      >
        <div>
          <h2>Materializations</h2>
          {node ? <AddMaterializationPopover node={node} /> : <></>}
          {materializations.length > 0 ? (
            <div
              className="card-inner-table table"
              aria-label="Materializations"
              aria-hidden="false"
            >
              <div style={{ display: 'table' }}>
                {materializationRows(
                  materializations.filter(
                    materialization =>
                      !(
                        materialization.name === 'default' &&
                        node.type === 'cube'
                      ),
                  ),
                )}
              </div>
            </div>
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
                  <th className="text-start">Output Dataset</th>
                  <th>Valid Through</th>
                  <th>Partitions</th>
                  <th>Links</th>
                </tr>
              </thead>
              <tbody>
                <tr>
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
                  <td>
                    {new Date(node.availability.valid_through_ts).toISOString()}
                  </td>
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
                  <td>
                    {node.availability.links !== null ? (
                      Object.entries(node.availability.links).map(
                        ([key, value]) => (
                          <div key={key}>
                            <a href={value} target="_blank" rel="noreferrer">
                              {key}
                            </a>
                          </div>
                        ),
                      )
                    ) : (
                      <></>
                    )}
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
