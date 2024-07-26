import * as React from 'react';
import { useParams } from 'react-router-dom';
import { useContext, useEffect, useState } from 'react';
import NodeStatus from '../NodePage/NodeStatus';
import DJClientContext from '../../providers/djclient';
import Explorer from '../NamespacePage/Explorer';
import AddNodeDropdown from '../../components/AddNodeDropdown';
import NodeListActions from '../../components/NodeListActions';
import AddNamespacePopover from './AddNamespacePopover';
import 'styles/node-list.css';
import 'styles/sorted-table.css';

export function NamespacePage() {
  const ASC = 'ascending';
  const DESC = 'descending';

  const fields = ['name', 'display_name', 'type', 'status', 'updated_at'];

  const djClient = useContext(DJClientContext).DataJunctionAPI;
  var { namespace } = useParams();

  const [state, setState] = useState({
    namespace: namespace,
    nodes: [],
  });

  const [namespaceHierarchy, setNamespaceHierarchy] = useState([]);

  const [sortConfig, setSortConfig] = useState({ key: 'updated_at', direction: DESC });
  const sortedNodes = React.useMemo(() => {
    let sortableData = [...Object.values(state.nodes)];
    if (sortConfig !== null) {
      sortableData.sort((a, b) => {
        if (a[sortConfig.key] < b[sortConfig.key]) {
          return sortConfig.direction === ASC ? -1 : 1;
        }
        if (a[sortConfig.key] > b[sortConfig.key]) {
          return sortConfig.direction === ASC ? 1 : -1;
        }
        return 0;
      });
    }
    return sortableData;
  }, [state.nodes, sortConfig]);

  const requestSort = (key) => {
    let direction = ASC;
    if (sortConfig.key === key && sortConfig.direction === ASC) {
      direction = DESC;
    }
    setSortConfig({ key, direction });
  };

  const getClassNamesFor = (name) => {
    if (sortConfig.key === name) {
      return sortConfig.direction;
    }
    return undefined;
  };

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
        namespace = namespaceHierarchy[0].namespace;
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

  const nodesList = sortedNodes.map(node => (
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
          {node.type !== 'source' ? node.display_name : ''}
        </a>
      </td>
      <td>
        <span className={'node_type__' + node.type + ' badge node_type'}>
          {node.type}
        </span>
      </td>
      <td>
        <NodeStatus node={node} revalidate={false} />
      </td>
      <td>
        <span className="status">
          {new Date(node.updated_at).toLocaleString('en-us')}
        </span>
      </td>
      <td>
        <NodeListActions nodeName={node?.name} />
      </td>
    </tr>
  ));

  return (
    <div className="mid">
      <div className="card">
        <div className="card-header">
          <h2>Explore</h2>
          <AddNodeDropdown namespace={namespace} />
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
                Namespaces <AddNamespacePopover namespace={namespace}/>
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
                  {fields.map(field => {
                    return (
                      <th>
                        <button type="button" onClick={() => requestSort(field)} className={'sortable ' + getClassNamesFor(field)}>
                          {field.replace('_', ' ')}
                        </button>
                      </th>
                    );
                  })}
                  <th>Actions</th>
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
