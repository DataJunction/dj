import { useEffect, useState } from 'react';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import * as React from 'react';

export default function NodeHistory({ node, djClient }) {
  const [history, setHistory] = useState([]);
  const [revisions, setRevisions] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      if (node) {
        const data = await djClient.history('node', node.name);
        const revisions = await djClient.revisions(node.name);
        setHistory(data);
        setRevisions(revisions);
      }
    };
    fetchData().catch(console.error);
  }, [djClient, node]);

  const eventData = event => {
    if (
      event.activity_type === 'set_attribute' &&
      event.entity_type === 'column_attribute'
    ) {
      return event.details.attributes
        .map(attr => (
          <div
            key={event.id}
            role="cell"
            aria-label="HistoryAttribute"
            aria-hidden="false"
          >
            Set{' '}
            <span className={`badge partition_value`}>
              {event.details.column}
            </span>{' '}
            as{' '}
            <span className={`badge partition_value_highlight`}>
              {attr.name}
            </span>
          </div>
        ))
        .reduce((prev, curr) => [prev, <br />, curr], []);
    }
    if (event.activity_type === 'create' && event.entity_type === 'link') {
      return (
        <div
          key={event.id}
          role="cell"
          aria-label="HistoryCreateLink"
          aria-hidden="false"
        >
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
        <div
          key={event.id}
          role="cell"
          aria-label="HistoryCreateMaterialization"
          aria-hidden="false"
        >
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
        <div
          key={event.id}
          role="cell"
          aria-label="HistoryCreateAvailability"
          aria-hidden="false"
        >
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
        <div
          key={event.id}
          role="cell"
          aria-label="HistoryNodeStatusChange"
          aria-hidden="false"
        >
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
    return (
      <div key={event.id}>
        {JSON.stringify(event.details) === '{}'
          ? ''
          : JSON.stringify(event.details)}
      </div>
    );
  };

  const tableData = history => {
    return history.map(event => (
      <tr key={`history-row-${event.id}`}>
        <td className="text-start">
          <span
            className={`history_type__${event.activity_type} badge node_type`}
          >
            {event.activity_type}
          </span>
        </td>
        <td>{event.entity_type}</td>
        <td>{event.entity_name}</td>
        <td>{event.user ? event.user : 'unknown'}</td>
        <td>{event.created_at}</td>
        <td>{eventData(event)}</td>
      </tr>
    ));
  };

  const revisionsTable = revisions => {
    return revisions.map(revision => (
      <tr key={revision.version}>
        <td className="text-start">
          <span className={`badge node_type__source`}>{revision.version}</span>
        </td>
        <td>{revision.display_name}</td>
        <td>{revision.description}</td>
        <td>
          <SyntaxHighlighter
            language="sql"
            style={foundation}
            wrapLongLines={true}
          >
            {revision.query}
          </SyntaxHighlighter>
        </td>
        <td>{revision.tags}</td>
      </tr>
    ));
  };
  return (
    <div className="table-vertical">
      <table className="card-inner-table table" aria-label="Revisions">
        <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
          <tr>
            <th className="text-start">Version</th>
            <th>Display Name</th>
            <th>Description</th>
            <th>Query</th>
            <th>Tags</th>
          </tr>
        </thead>
        <tbody>{revisionsTable(revisions)}</tbody>
      </table>
      <table className="card-inner-table table" aria-label="Activity">
        <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
          <tr>
            <th className="text-start">Activity</th>
            <th>Type</th>
            <th>Name</th>
            <th>User</th>
            <th>Timestamp</th>
            <th>Details</th>
          </tr>
        </thead>
        <tbody>{tableData(history)}</tbody>
      </table>
    </div>
  );
}
