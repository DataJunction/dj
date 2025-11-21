import * as React from 'react';
import { useParams } from 'react-router-dom';
import { useContext, useEffect, useState } from 'react';
import NodeStatus from '../NodePage/NodeStatus';
import DJClientContext from '../../providers/djclient';
import Explorer from '../NamespacePage/Explorer';
import AddNodeDropdown from '../../components/AddNodeDropdown';
import NodeListActions from '../../components/NodeListActions';
import AddNamespacePopover from './AddNamespacePopover';
import FilterIcon from '../../icons/FilterIcon';
import LoadingIcon from '../../icons/LoadingIcon';
import UserSelect from './UserSelect';
import NodeTypeSelect from './NodeTypeSelect';
import TagSelect from './TagSelect';

import 'styles/node-list.css';
import 'styles/sorted-table.css';

export function NamespacePage() {
  const ASC = 'ascending';
  const DESC = 'descending';

  const fields = ['name', 'displayName', 'type', 'status', 'updatedAt'];

  const djClient = useContext(DJClientContext).DataJunctionAPI;
  var { namespace } = useParams();

  const [state, setState] = useState({
    namespace: namespace ? namespace : '',
    nodes: [],
  });
  const [retrieved, setRetrieved] = useState(false);
  const [currentUser, setCurrentUser] = useState(null);

  const [filters, setFilters] = useState({
    tags: [],
    node_type: '',
    edited_by: '',
  });

  const [namespaceHierarchy, setNamespaceHierarchy] = useState([]);

  const [sortConfig, setSortConfig] = useState({
    key: 'updatedAt',
    direction: DESC,
  });

  const [before, setBefore] = useState(null);
  const [after, setAfter] = useState(null);
  const [prevCursor, setPrevCursor] = useState(true);
  const [nextCursor, setNextCursor] = useState(true);

  const [hasNextPage, setHasNextPage] = useState(true);
  const [hasPrevPage, setHasPrevPage] = useState(true);

  const requestSort = key => {
    let direction = ASC;
    if (sortConfig.key === key && sortConfig.direction === ASC) {
      direction = DESC;
    }
    if (sortConfig.key !== key || sortConfig.direction !== direction) {
      setSortConfig({ key, direction });
    }
  };

  const getClassNamesFor = name => {
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
      const currentUser = await djClient.whoami();
      // setFilters({...filters, edited_by: currentUser?.username});
      setCurrentUser(currentUser);
    };
    fetchData().catch(console.error);
  }, [djClient, djClient.namespaces]);

  useEffect(() => {
    const fetchData = async () => {
      setRetrieved(false);
      const nodes = await djClient.listNodesForLanding(
        namespace,
        filters.node_type ? [filters.node_type.toUpperCase()] : [],
        filters.tags,
        filters.edited_by,
        before,
        after,
        50,
        sortConfig,
      );

      setState({
        namespace: namespace,
        nodes: nodes.data
          ? nodes.data.findNodesPaginated.edges.map(n => n.node)
          : [],
      });
      if (nodes.data) {
        setPrevCursor(
          nodes.data ? nodes.data.findNodesPaginated.pageInfo.startCursor : '',
        );
        setNextCursor(
          nodes.data ? nodes.data.findNodesPaginated.pageInfo.endCursor : '',
        );
        setHasPrevPage(
          nodes.data
            ? nodes.data.findNodesPaginated.pageInfo.hasPrevPage
            : false,
        );
        setHasNextPage(
          nodes.data
            ? nodes.data.findNodesPaginated.pageInfo.hasNextPage
            : false,
        );
      }
      setRetrieved(true);
    };
    fetchData().catch(console.error);
  }, [djClient, filters, before, after, sortConfig.key, sortConfig.direction]);

  const loadNext = () => {
    if (nextCursor) {
      setAfter(nextCursor);
      setBefore(null);
    }
  };
  const loadPrev = () => {
    if (prevCursor) {
      setAfter(null);
      setBefore(prevCursor);
    }
  };

  const nodesList = retrieved ? (
    state.nodes.length > 0 ? (
      state.nodes.map(node => (
        <tr key={node.name}>
          <td>
            <a href={'/nodes/' + node.name} className="link-table">
              {node.name}
            </a>
            <span
              className="rounded-pill badge bg-secondary-soft"
              style={{ marginLeft: '0.5rem' }}
            >
              {node.currentVersion}
            </span>
          </td>
          <td>
            <a href={'/nodes/' + node.name} className="link-table">
              {node.type !== 'source' ? node.current.displayName : ''}
            </a>
          </td>
          <td>
            <span
              className={
                'node_type__' + node.type.toLowerCase() + ' badge node_type'
              }
            >
              {node.type}
            </span>
          </td>
          <td>
            <NodeStatus node={node} revalidate={false} />
          </td>
          <td>
            <span className="status">
              {new Date(node.current.updatedAt).toLocaleString('en-us')}
            </span>
          </td>
          <td>
            <NodeListActions nodeName={node?.name} />
          </td>
        </tr>
      ))
    ) : (
      <tr>
        <td>
          <span
            style={{
              display: 'block',
              marginTop: '2rem',
              marginLeft: '2rem',
              fontSize: '16px',
            }}
          >
            There are no nodes in{' '}
            <a href={`/namespaces/${namespace}`}>{namespace}</a> with the above
            filters!
          </span>
        </td>
      </tr>
    )
  ) : (
    <tr>
      <td>
        <span style={{ display: 'block', marginTop: '2rem' }}>
          <LoadingIcon />
        </span>
      </td>
    </tr>
  );

  return (
    <div className="mid">
      <div className="card">
        <div className="card-header">
          <h2>Explore</h2>
          <div className="menu" style={{ margin: '0 0 20px 0' }}>
            <div
              className="menu-link"
              style={{
                marginTop: '0.7em',
                color: '#777',
                fontFamily: "'Jost'",
                fontSize: '18px',
                marginRight: '10px',
                marginLeft: '15px',
              }}
            >
              <FilterIcon />
            </div>
            <div
              className="menu-link"
              style={{
                marginTop: '0.6em',
                color: '#777',
                fontFamily: "'Jost'",
                fontSize: '18px',
                marginRight: '10px',
              }}
            >
              Filter By
            </div>
            <NodeTypeSelect
              onChange={entry =>
                setFilters({ ...filters, node_type: entry ? entry.value : '' })
              }
            />
            <TagSelect
              onChange={entry =>
                setFilters({
                  ...filters,
                  tags: entry ? entry.map(tag => tag.value) : [],
                })
              }
            />
            <UserSelect
              onChange={entry =>
                setFilters({ ...filters, edited_by: entry ? entry.value : '' })
              }
              currentUser={currentUser?.username}
            />
            <AddNodeDropdown namespace={namespace} />
          </div>
          <div className="table-responsive">
            <div className={`sidebar`}>
              <div
                style={{
                  padding: '1rem 1rem 1rem 0',
                }}
              >
                <span
                  style={{
                    textTransform: 'uppercase',
                    fontSize: '0.8125rem',
                    fontWeight: '600',
                    color: '#95aac9',
                  }}
                >
                  Namespaces
                </span>
              </div>
              {namespaceHierarchy
                ? namespaceHierarchy.map(child => (
                    <Explorer
                      item={child}
                      current={state.namespace}
                      defaultExpand={true}
                      isTopLevel={true}
                      key={child.namespace}
                    />
                  ))
                : null}
            </div>
            <table className="card-table table">
              <thead>
                <tr>
                  {fields.map(field => {
                    return (
                      <th key={field}>
                        <button
                          type="button"
                          onClick={() => requestSort(field)}
                          className={'sortable ' + getClassNamesFor(field)}
                        >
                          {field.replace(/([a-z](?=[A-Z]))/g, '$1 ')}
                        </button>
                      </th>
                    );
                  })}
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>{nodesList}</tbody>
              <tfoot>
                <tr>
                  <td>
                    {retrieved && hasPrevPage ? (
                      <a
                        onClick={loadPrev}
                        className="previous round pagination"
                      >
                        ← Previous
                      </a>
                    ) : (
                      ''
                    )}
                    {retrieved && hasNextPage ? (
                      <a onClick={loadNext} className="next round pagination">
                        Next →
                      </a>
                    ) : (
                      ''
                    )}
                  </td>
                </tr>
              </tfoot>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
