import * as React from 'react';
import { useParams } from 'react-router-dom';
import { useContext, useEffect, useState } from 'react';
import NamespaceHeader from '../../components/NamespaceHeader';
import NodeStatus from '../NodePage/NodeStatus';
import DJClientContext from '../../providers/djclient';
import Explorer from '../ListNamespacesPage/Explorer';

export function NamespacePage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const { namespace } = useParams();

  const [state, setState] = useState({
    namespace: namespace,
    nodes: [],
  });

  const [namespaces, setNamespaces] = useState({});

  useEffect(() => {
    const fetchData = async () => {
      const namespaces = await djClient.namespaces();
      var hierarchy = { namespace: 'r', children: [], path: '' };
      namespaces.forEach(namespace => {
        const parts = namespace.namespace.split('.');
        let current = hierarchy;
        parts.forEach(part => {
          const found = current.children.find(
            child => part === child.namespace,
          );
          if (found !== undefined) current = found;
          else
            current.children.push({
              namespace: part,
              children: [],
              path: current.path === '' ? part : current.path + '.' + part,
            });
        });
      });
      setNamespaces(hierarchy);
    };
    fetchData().catch(console.error);
  }, [djClient, djClient.namespaces]);

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
            <div className={`sidebar`}>
              <Explorer parent={namespaces.children} />
            </div>
            <table className="card-table table">
              <thead>
                <tr>
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
