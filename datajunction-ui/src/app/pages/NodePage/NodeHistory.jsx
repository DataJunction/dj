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
        <td>{event.entity_name}</td>
        <td>{event.user ? event.user : 'unknown'}</td>
        <td>{event.created_at}</td>
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
    <div className="table-responsive">
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
          <th>Name</th>
          <th>User</th>
          <th>Timestamp</th>
        </thead>
        {tableData(history)}
      </table>
    </div>
  );
}
