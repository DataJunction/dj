import { Component } from 'react';
import { useContext, useEffect, useState } from 'react';

export default function NodeColumnTab({ node, djClient }) {
  const [history, setHistory] = useState([]);
  useEffect(() => {
    const fetchData = async () => {
      const data = await djClient.history('node', node.name);
      console.log(data);
      setHistory(data);
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
        <td>{event.created_at}</td>
      </tr>
    ));
  };
  return (
    <div className="table-responsive">
      <table className="card-inner-table table">
        <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
          <th className="text-start">Activity</th>
          <th>Type</th>
          <th>Name</th>
          <th>Timestamp</th>
        </thead>
        {tableData(history)}
      </table>
    </div>
  );
}
