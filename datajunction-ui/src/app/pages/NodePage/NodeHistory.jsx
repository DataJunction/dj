import { useEffect, useState } from 'react';

export default function NodeHistory({ node, djClient }) {
  const [history, setHistory] = useState([]);
  const [revisions, setRevisions] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      const data = await djClient.history('node', node.name);
      setHistory(data);
      const revisions = await djClient.revisions(node.name);
      setRevisions(revisions);
    };
    fetchData().catch(console.error);
  }, [djClient, node]);

  const eventData = event => {
    console.log('event', event);
    if (
      event.activity_type === 'set_attribute' &&
      event.entity_type === 'column_attribute'
    ) {
      return event.details.attributes
        .map(attr => (
          <div>
            Set{' '}
            <span className={`badge partition_value`}>{attr.column_name}</span>{' '}
            as{' '}
            <span className={`badge partition_value_highlight`}>
              {attr.attribute_type_name}
            </span>
          </div>
        ))
        .reduce((prev, curr) => [prev, <br />, curr]);
    }
    if (event.activity_type === 'create' && event.entity_type === 'link') {
      return (
        <div>
          Linked{' '}
          <span className={`badge partition_value`}>
            {event.details.column}
          </span>{' '}
          to
          <span className={`badge partition_value_highlight`}>
            {event.details.dimension}
          </span>{' '}
          via
          <span className={`badge partition_value`}>
            {event.details.dimension_column}
          </span>
        </div>
      );
    }
    if (
      event.activity_type === 'create' &&
      event.entity_type === 'materialization'
    ) {
      return (
        <div>
          Initialized materialization{' '}
          <span className={`badge partition_value`}>
            {event.details.materialization}
          </span>
        </div>
      );
    }
    if (
      event.activity_type === 'create' &&
      event.entity_type === 'availability'
    ) {
      return (
        <div>
          Materialized at{' '}
          <span className={`badge partition_value_highlight`}>
            {event.post.catalog}.{event.post.schema_}.{event.post.table}
          </span>
          from{' '}
          <span className={`badge partition_value`}>
            {event.post.min_temporal_partition}
          </span>{' '}
          to
          <span className={`badge partition_value`}>
            {event.post.max_temporal_partition}
          </span>
        </div>
      );
    }
    if (
      event.activity_type === 'status_change' &&
      event.entity_type === 'node'
    ) {
      const expr = (
        <div>
          Caused by a change in upstream{' '}
          <a href={`/nodes/${event.details['upstream_node']}`}>
            {event.details['upstream_node']}
          </a>
        </div>
      );
      return (
        <div>
          Status changed from{' '}
          <span className={`status__${event.pre['status']}`}>
            {event.pre['status']}
          </span>{' '}
          to{' '}
          <span className={`status__${event.post['status']}`}>
            {event.post['status']}
          </span>{' '}
          {event.details['upstream_node'] !== undefined ? expr : ''}
        </div>
      );
    }
    return '';
  };

  const tableData = history => {
    return history.map(event => (
      <tr>
        <td className="text-start">
          <span
            className={`history_type__${event.activity_type} badge node_type`}
          >
            {event.activity_type}
          </span>
        </td>
        <td>{event.entity_type}</td>
        <td>{event.user ? event.user : 'unknown'}</td>
        <td>{event.created_at}</td>
        <td>{eventData(event)}</td>
      </tr>
    ));
  };

  const revisionsTable = revisions => {
    return revisions.map(revision => (
      <tr>
        <td className="text-start">
          <span className={`badge node_type__source`}>{revision.version}</span>
        </td>
        <td>{revision.display_name}</td>
        <td>{revision.description}</td>
        <td>{revision.query}</td>
        <td>{revision.tags}</td>
      </tr>
    ));
  };
  return (
    <div className="table-vertical">
      <table className="card-inner-table table">
        <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
          <th className="text-start">Version</th>
          <th>Display Name</th>
          <th>Description</th>
          <th>Query</th>
          <th>Tags</th>
        </thead>
        {revisionsTable(revisions)}
      </table>
      <table className="card-inner-table table">
        <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
          <th className="text-start">Activity</th>
          <th>Type</th>
          <th>User</th>
          <th>Timestamp</th>
          <th>Details</th>
        </thead>
        {tableData(history)}
      </table>
    </div>
  );
}
