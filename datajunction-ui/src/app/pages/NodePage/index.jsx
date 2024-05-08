import * as React from 'react';
import { useParams } from 'react-router-dom';
import { useContext, useEffect, useState } from 'react';
import Tab from '../../components/Tab';
import NamespaceHeader from '../../components/NamespaceHeader';
import NodeInfoTab from './NodeInfoTab';
import NodeColumnTab from './NodeColumnTab';
import NodeLineage from './NodeGraphTab';
import NodeHistory from './NodeHistory';
import DJClientContext from '../../providers/djclient';
import NodeSQLTab from './NodeSQLTab';
import NodeMaterializationTab from './NodeMaterializationTab';
import ClientCodePopover from './ClientCodePopover';
import NodesWithDimension from './NodesWithDimension';
import NodeColumnLineage from './NodeLineageTab';
import EditIcon from '../../icons/EditIcon';
import AlertIcon from '../../icons/AlertIcon';
import NodeDimensionsTab from './NodeDimensionsTab';
import { useNavigate } from 'react-router-dom';

export function NodePage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const navigate = useNavigate();

  const { name, tab } = useParams();

  const [state, setState] = useState({
    selectedTab: tab || 'info',
  });

  const [node, setNode] = useState();

  const onClickTab = id => () => {
    navigate(`/nodes/${name}/${id}`);
    setState({ selectedTab: id });
  };

  const buildTabs = tab => {
    return tab.display ? (
      <Tab
        key={tab.id}
        id={tab.id}
        name={tab.name}
        onClick={onClickTab(tab.id)}
        selectedTab={state.selectedTab}
      />
    ) : null;
  };

  useEffect(() => {
    const fetchData = async () => {
      const data = await djClient.node(name);
      data.createNodeClientCode = await djClient.clientCode(name);
      if (data.type === 'metric') {
        const metric = await djClient.metric(name);
        data.dimensions = metric.dimensions;
        data.metric_metadata = metric.metric_metadata;
        data.required_dimensions = metric.required_dimensions;
        data.upstream_node = metric.upstream_node;
        data.expression = metric.expression;
        data.incompatible_druid_functions = metric.incompatible_druid_functions;
      }
      if (data.type === 'cube') {
        const cube = await djClient.cube(name);
        data.cube_elements = cube.cube_elements;
      }
      setNode(data);
    };
    fetchData().catch(console.error);
  }, [djClient, name]);

  const tabsList = node => {
    return [
      {
        id: 'info',
        name: 'Info',
        display: true,
      },
      {
        id: 'columns',
        name: 'Columns',
        display: true,
      },
      {
        id: 'graph',
        name: 'Graph',
        display: true,
      },
      {
        id: 'history',
        name: 'History',
        display: true,
      },
      {
        id: 'sql',
        name: 'Run',
        display: node?.type !== 'source',
      },
      {
        id: 'materializations',
        name: 'Materializations',
        display: node?.type !== 'source',
      },
      {
        id: 'linked',
        name: 'Linked Nodes',
        display: node?.type === 'dimension',
      },
      {
        id: 'lineage',
        name: 'Lineage',
        display: node?.type === 'metric',
      },
      {
        id: 'dimensions',
        name: 'Dimensions',
        display: node?.type !== 'cube',
      },
    ];
  };
  let tabToDisplay = null;

  switch (state.selectedTab) {
    case 'info':
      tabToDisplay =
        node && node.message === undefined ? <NodeInfoTab node={node} /> : '';
      break;
    case 'columns':
      tabToDisplay = <NodeColumnTab node={node} djClient={djClient} />;
      break;
    case 'graph':
      tabToDisplay = <NodeLineage djNode={node} djClient={djClient} />;
      break;
    case 'history':
      tabToDisplay = <NodeHistory node={node} djClient={djClient} />;
      break;
    case 'sql':
      tabToDisplay = <NodeSQLTab node={node} djClient={djClient} />;
      break;
    case 'materializations':
      tabToDisplay = <NodeMaterializationTab node={node} djClient={djClient} />;
      break;
    case 'linked':
      tabToDisplay = <NodesWithDimension node={node} djClient={djClient} />;
      break;
    case 'lineage':
      tabToDisplay = <NodeColumnLineage djNode={node} djClient={djClient} />;
      break;
    case 'dimensions':
      tabToDisplay = <NodeDimensionsTab node={node} djClient={djClient} />;
      break;
    default: /* istanbul ignore next */
      tabToDisplay = <NodeInfoTab node={node} />;
  }
  // @ts-ignore
  return (
    <div className="node__header">
      <NamespaceHeader namespace={name.split('.').slice(0, -1).join('.')} />
      <div className="card">
        {node?.message === undefined ? (
          <div className="card-header">
            <h3
              className="card-title align-items-start flex-column"
              style={{ display: 'inline-block' }}
            >
              <span
                className="card-label fw-bold text-gray-800"
                role="dialog"
                aria-hidden="false"
                aria-label="DisplayName"
              >
                {node?.display_name}{' '}
                <span
                  className={'node_type__' + node?.type + ' badge node_type'}
                  role="dialog"
                  aria-hidden="false"
                  aria-label="NodeType"
                >
                  {node?.type}
                </span>
              </span>
            </h3>
            <a
              href={`/nodes/${node?.name}/edit`}
              style={{ marginLeft: '0.5rem' }}
            >
              <EditIcon />
            </a>
            <ClientCodePopover code={node?.createNodeClientCode} />
            <div>
              <a
                href={'/nodes/' + node?.name}
                className="link-table"
                role="dialog"
                aria-hidden="false"
                aria-label="NodeName"
              >
                {node?.name}
              </a>
              <span
                className="rounded-pill badge bg-secondary-soft"
                style={{ marginLeft: '0.5rem' }}
              >
                {node?.version}
              </span>
            </div>
            <div className="align-items-center row">
              {tabsList(node).map(buildTabs)}
            </div>
            {tabToDisplay}
          </div>
        ) : node?.message !== undefined ? (
          <div className="message alert" style={{ margin: '20px' }}>
            <AlertIcon />
            Node `{name}` does not exist!
          </div>
        ) : (
          ''
        )}
      </div>
    </div>
  );
}
