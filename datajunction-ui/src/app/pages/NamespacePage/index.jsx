import * as React from 'react';
import { useParams } from 'react-router-dom';
import { useContext, useEffect, useState } from 'react';
import NodeStatus from '../NodePage/NodeStatus';
import DJClientContext from '../../providers/djclient';
import Explorer from '../NamespacePage/Explorer';

export function NamespacePage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  var { namespace } = useParams();

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
      if (namespace === undefined && namespaces !== undefined) {
        namespace = namespaces.children[0].path;
      }
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
  }, [djClient, namespace, namespaces]);

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
      <td>
        <span className="status">{node.tags}</span>
      </td>
      <td>
        <span className="status">
          {new Date(node.updated_at).toLocaleString('en-us')}
        </span>
      </td>
    </tr>
  ));

  return (
    <div className="mid">
      <div className="card">
        <div className="card-header">
          <h2>Explore</h2>
          <div className="table-responsive">
            <div className={`sidebar`}>
              <span
                style={{
                  textTransform: 'uppercase',
                  fontSize: '0.8125rem',
                  fontWeight: '600',
                  color: '#95aac9',
                  padding: '1rem 1rem 1rem 0',
                }}
              >
                Namespaces
              </span>
              {namespaces.children
                ? namespaces.children.map(child => (
                    <Explorer
                      item={child}
                      current={state.namespace}
                      defaultExpand={true}
                    />
                  ))
                : null}
            </div>
            <table className="card-table table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Type</th>
                  <th>Status</th>
                  <th>Mode</th>
                  <th>Tags</th>
                  <th>Last Updated</th>
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
