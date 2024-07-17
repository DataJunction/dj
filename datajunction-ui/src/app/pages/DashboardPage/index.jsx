import * as React from 'react';
import Select from 'react-select';
import { Form, Formik } from 'formik';
import Explorer from '../NamespacePage/Explorer';
import AddNodeDropDown from '../../components/AddNodeDropDown';
import Select, {
  components,
  ControlProps,
  Props,
  StylesConfig,
} from 'react-select';

import { useParams } from 'react-router-dom';
import { useContext, useEffect, useState } from 'react';
import NodeStatus from '../NodePage/NodeStatus';
import CubeGrouping from './CubeGrouping';
import OwnerSelect from './OwnerSelect';
import GroupBySelect from './GroupBySelect';
import NodeTypeSelect from './NodeTypeSelect';
import DJClientContext from '../../providers/djclient';
import NodeListActions from '../../components/NodeListActions';
import NamespaceHierarchy from '../../components/NamespaceHierarchy';
import LoadingIcon from '../../icons/LoadingIcon';

import 'styles/node-list.css';

export function DashboardPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  var { namespace } = useParams();
  const [retrieved, setRetrieved] = useState(false);
  const [currentUser, setCurrentUser] = useState(null);

  const [nodes, setNodes] = useState({});
  const [groupHeaders, setGroupHeaders] = useState([]);
  const initialValues = {
    groupby: 'namespace',
    node_type: [],
    owner: currentUser?.username,
  };
  
  useEffect(() => {
    const fetchData = async () => {
      const currentUser = await djClient.me();
      setCurrentUser(currentUser);

      const nodes = await djClient.userNodes(namespace);
      const foundNodes = await Promise.all(nodes);

      const groups = (
        foundNodes.filter(node => node.type === 'cube')
        .sort((a, b) => new Date(b.updated_at).getTime() - new Date(a.updated_at).getTime())
        .map(node => node.name)
      );

      const nodesLookup = {};
      for (const node of foundNodes) {
        nodesLookup[node.name] = node;
      }
      setNodes(nodesLookup);
      setGroupHeaders(groups);
      setRetrieved(true);
    };
    fetchData().catch(console.error);
  }, [djClient, namespace]);

  return (
    <div className="mid">
      <div className="card">
        <div className="card-header">
          <h2>Explore</h2>
          <div class="menu" style={{margin: '0 0 20px 0'}}>
            {/* style={{margin: '0 0 0 250px'}}> */}
            <Formik initialValues={initialValues} 
              // onSubmit={handleSubmit}
              >
              {function Render({ isSubmitting, status, setFieldValue }) {
                return (
                  <Form style={{display: 'flex'}}>
                    <GroupBySelect />
                    <NodeTypeSelect />
                    <OwnerSelect />
                    <AddNodeDropDown />
                  </Form>
                );
              }}
            </Formik>
          </div>

          <div className="table-responsive">
            <div className={`sidebar`} style={{width: '200px', marginRight: '1rem'}}>
              {retrieved ? <NamespaceHierarchy nodes={Object.keys(nodes).map(node => node)}/> : ''}
            </div>
            <div className="cards">
              {/* {groupHeaders.map(header => {
                return header in nodes ? (
                  <CubeGrouping nodes={nodes} cubeName={header} />
                ) : '';
              })} */}
              {<div className="table__body card-table" style={{width: '-webkit-fill-available', minWidth: '80%'}}>
              <table className="card-table table">
              <tbody>
              {retrieved ? (
              Object.values(nodes)?.map(node => {
                  // const node = nodes[nodeName];
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
          </div>}
              {/* <div className="grouping">
                <h4 className="level2">
                  Gropuing 2
                </h4>
                <div className="table__body">
                  {nodesList}
                </div>
              </div> */}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
