import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import NodeStatus from '../NodePage/NodeStatus';
import NodeListActions from '../../components/NodeListActions';
import LoadingIcon from '../../icons/LoadingIcon';

export default function CubeGrouping({
    cubeName,
    nodes,
}) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  const [retrieved, setRetrieved] = useState(false);
  const [references, setReferences] = useState({});

  useEffect(() => {
    const fetchData = async () => {
      console.log('fetch for', cubeName);
      const dag = await djClient.node_dag(cubeName);
      setReferences(
        dag
        .filter(related => related.name !== cubeName)
        .sort((a, b) => a.type.localeCompare(b.type))
        .map(related => related.name)
      );
      setRetrieved(true);
    };
    fetchData().catch(console.error);
  }, [djClient, cubeName]);

  return (
      <div className="grouping" style={{marginBottom: '1rem'}}>
          <div className="partitionLink" style={{ padding: '12px', margin: '0'}}>
              <span className={'node_type__' + nodes[cubeName].type + ' node_type'} style={{
              borderRadius: '0.375rem 0 0 0',
              fontFamily: 'monospace',
              padding: '1rem 1rem',
              marginLeft: '-12px',
              marginRight: '1rem',
              zIndex: '100',
              position: 'relative',
              }}>
              <b>{nodes[cubeName].type}</b>
              </span>
              <a href={'/nodes/' + nodes[cubeName].name} className="link-table">
              {nodes[cubeName].display_name.slice(0, 1000) + (nodes[cubeName].display_name.length > 1000 ? '...' : '')}
              </a>
          </div>
          <div className="table__body card-table" style={{width: '-webkit-fill-available', minWidth: '80%'}}>
              <table className="card-table table">
              <tbody>
              {retrieved ? (
              references?.filter(nodeName => nodeName in nodes).map(nodeName => {
                  const node = nodes[nodeName];
                  return (
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
                  {/* <td>
                  <span className="status">{node.mode}</span>
                  </td> */}
                  <td>
                  <span className="status">
                      {new Date(node.updated_at).toLocaleString('en-us')}
                  </span>
                  </td>
                  <td style={{minWidth: '100px'}}>
                  <NodeListActions nodeName={node?.name} />
                  </td>
              </tr>
                  );})

              ) : (
                <span style={{ display: 'inline-block', padding: '20px' }}>
                  <LoadingIcon />
                </span>
              )}
              </tbody>
              </table>
          </div>
      </div>
  );
};