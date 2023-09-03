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

  const [namespaceHierarchy, setNamespaceHierarchy] = useState([]);

  const createNamespaceHierarchy = namespaceList => {
    const hierarchy = [];

    for (const item of namespaceList) {
      const namespaces = item.namespace.split('.');
      let currentLevel = hierarchy;

      let path = '';
      for (const ns of namespaces) {
        path += ns;

        let existingNamespace = currentLevel.find(el => el.namespace === ns);
        if (!existingNamespace) {
          existingNamespace = {
            namespace: ns,
            children: [],
            path: path,
          };
          currentLevel.push(existingNamespace);
        }

        currentLevel = existingNamespace.children;
        path += '.';
      }
    }
    return hierarchy;
  };

  useEffect(() => {
    const fetchData = async () => {
      const namespaces = await djClient.namespaces();
      const hierarchy = createNamespaceHierarchy(namespaces);
      setNamespaceHierarchy(hierarchy);
    };
    fetchData().catch(console.error);
  }, [djClient, djClient.namespaces]);

  useEffect(() => {
    const fetchData = async () => {
      if (namespace === undefined && namespaceHierarchy !== undefined) {
        namespace = namespaceHierarchy.children[0].path;
      }
      const nodes = await djClient.namespace(namespace);
      const foundNodes = await Promise.all(nodes);
      setState({
        namespace: namespace,
        nodes: foundNodes,
      });
    };
    fetchData().catch(console.error);
  }, [djClient, namespace, namespaceHierarchy]);

  const nodesList = state.nodes.map(node => (
    <tr>
      <td>
        <a href={'/nodes/' + node.name} className="link-table">
          {node.name}
        </a>
        <span
          className="rounded-pill badge bg-secondary-soft"
          style={{ marginLeft: '0.5rem' }}
        >
          {node.version}
        </span>
      </td>
      <td>
        <a href={'/nodes/' + node.name} className="link-table">
          {node.display_name}
        </a>
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

          <span className="menu-link">
            <span className="menu-title">
              <div className="dropdown">
                <span className="add_node">+ Add Node</span>
                <div className="dropdown-content">
                  <a href={`/create/transform/${namespace}`}>
                    <div className="node_type__transform node_type_creation_heading">
                      Transform
                    </div>
                  </a>
                  <a href={`/create/metric/${namespace}`}>
                    <div className="node_type__metric node_type_creation_heading">
                      Metric
                    </div>
                  </a>
                  <a href={`/create/dimension/${namespace}`}>
                    <div className="node_type__dimension node_type_creation_heading">
                      Dimension
                    </div>
                  </a>
                </div>
              </div>
            </span>
          </span>
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
              {namespaceHierarchy
                ? namespaceHierarchy.map(child => (
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
                  <th>Display Name</th>
                  <th>Type</th>
                  <th>Status</th>
                  <th>Mode</th>
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
