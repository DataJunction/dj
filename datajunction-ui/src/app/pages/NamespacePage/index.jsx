import * as React from 'react';
import { useParams } from 'react-router-dom';
import { useContext, useEffect, useState } from 'react';
import NamespaceHeader from '../../components/NamespaceHeader';
import NodeStatus from '../NodePage/NodeStatus';
import DJClientContext from '../../providers/djclient';

export function NamespacePage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const { namespace } = useParams();

  const [state, setState] = useState({
    namespace: namespace,
    nodes: [],
  });

  useEffect(() => {
    const fetchData = async () => {
      const djNodes = await djClient.namespace(namespace);
      const nodes = djNodes.map(node => {
        return djClient.node(node);
      });
      const foundNodes = await Promise.all(nodes);
      setState({
        namespace: namespace,
        nodes: foundNodes,
      });
    };
    fetchData().catch(console.error);
  }, [djClient, namespace]);

  const nodesList = state.nodes.map(node => (
    <tr>
      <td>
        <a href={'/namespaces/' + node.namespace}>{node.namespace}</a>
      </td>
      <td>
        <a href={'/nodes/' + node.name} className="link-table">
          {node.display_name}
        </a>
        <span
          className="rounded-pill badge bg-secondary-soft"
          style={{ marginLeft: '0.5rem' }}
        >
          {node.version}
        </span>
      </td>
      <td>
        <span className={'node_type__' + node.type + ' badge node_type'}>
          {node.type}
        </span>
      </td>
      <td>
        <NodeStatus node={node} />
      </td>
      <td>
        <span className="status">{node.mode}</span>
      </td>
    </tr>
  ));

  // @ts-ignore
  return (
    <div className="mid">
      <NamespaceHeader namespace={namespace} />
      <div className="card">
        <div className="card-header">
          <h2>Nodes</h2>
          <div className="table-responsive">
            <table className="card-table table">
              <thead>
                <tr>
                  <th>Namespace</th>
                  <th>Name</th>
                  <th>Type</th>
                  <th>Status</th>
                  <th>Mode</th>
                </tr>
              </thead>
              <tbody>{nodesList}</tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
