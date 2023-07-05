import { useEffect, useState } from 'react';
import ClientCodePopover from './ClientCodePopover';
import TableIcon from '../../icons/TableIcon';

const cronstrue = require('cronstrue');

export default function NodeMaterializationTab({ node, djClient }) {
  const [materializations, setMaterializations] = useState([]);
  useEffect(() => {
    const fetchData = async () => {
      const data = await djClient.materializations(node.name);
      setMaterializations(data);
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
      <tr>
        <td className="text-start node_name">
          <a href={materialization.urls[0]}>{materialization.name}</a>
          <ClientCodePopover code={materialization.clientCode} />
        </td>
        <td>
          <span className={`badge cron`}>{materialization.schedule}</span>
          <div className={`cron-description`}>{cron(materialization)} </div>
        </td>
        <td>
          {materialization.engine.name}
          <br />
          {materialization.engine.version}
        </td>
        <td>
          {materialization.config.partitions ? (
            materialization.config.partitions.map(partition =>
              partition.type_ === 'categorical' ? (
                <div className="partition__full">
                  <div className="partition__header">{partition.name}</div>
                  <div className="partition__body">
                    {partition.values !== null && partition.values.length > 0
                      ? partition.values.map(val => (
                          <span className="badge partition_value">{val}</span>
                        ))
                      : null}
                    {partition.range !== null && partition.range.length > 0
                      ? rangePartition(partition)
                      : null}
                    {(partition.range === null && partition.values === null) ||
                    (partition.range.length === 0 &&
                      partition.values.length === 0) ? (
                      <span className={`badge partition_value_highlight`}>
                        ALL
                      </span>
                    ) : null}
                  </div>
                </div>
              ) : null,
            )
          ) : (
            <br />
          )}
        </td>
        <td>
          {materialization.output_tables.map(table => (
            <div className={`table__full`}>
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
        {/*<td>{Object.keys(materialization.config.spark).map(key => <li className={`list-group-item`}>{key}: {materialization.config.spark[key]}</li>)}</td>*/}

        <td>
          {materialization.config.partitions ? (
            materialization.config.partitions.map(partition =>
              partition.type_ === 'temporal' ? (
                <div className="partition__full">
                  <div className="partition__header">{partition.name}</div>
                  <div className="partition__body">
                    {partition.values !== null && partition.values.length > 0
                      ? partition.values.map(val => (
                          <span className="badge partition_value">{val}</span>
                        ))
                      : null}
                    {partition.range !== null && partition.range.length > 0
                      ? rangePartition(partition)
                      : null}
                  </div>
                </div>
              ) : null,
            )
          ) : (
            <br />
          )}
        </td>
        <td>
          {materialization.urls.map((url, idx) => (
            <a href={url}>[{idx + 1}]</a>
          ))}
        </td>
      </tr>
    ));
  };
  return (
    <div className="table-responsive">
      <table className="card-inner-table table">
        <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
          <th className="text-start">Name</th>
          <th>Schedule</th>
          <th>Engine</th>
          <th>Partitions</th>
          <th>Output Tables</th>
          <th>Backfills</th>
          <th>URLs</th>
        </thead>
        {materializationRows(
          materializations.filter(
            materialization =>
              !(materialization.name === 'default' && node.type === 'cube'),
          ),
        )}
      </table>
    </div>
  );
}
