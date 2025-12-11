import * as React from 'react';
import { useParams } from 'react-router-dom';
import { useContext, useEffect, useState } from 'react';
import Tab from '../../components/Tab';
import NamespaceHeader from '../../components/NamespaceHeader';
import NodeInfoTab from './NodeInfoTab';
import NodeColumnTab from './NodeColumnTab';
import NodeGraphTab from './NodeGraphTab';
import NodeHistory from './NodeHistory';
import NotebookDownload from './NotebookDownload';
import DJClientContext from '../../providers/djclient';
import NodeValidateTab from './NodeValidateTab';
import NodeMaterializationTab from './NodeMaterializationTab';
import ClientCodePopover from './ClientCodePopover';
import WatchButton from './WatchNodeButton';
import NodesWithDimension from './NodesWithDimension';
import NodeColumnLineage from './NodeLineageTab';
import EditIcon from '../../icons/EditIcon';
import AlertIcon from '../../icons/AlertIcon';
import LoadingIcon from '../../icons/LoadingIcon';
import NodeDependenciesTab from './NodeDependenciesTab';
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
      // Fetch node data first and display it immediately
      const data = await djClient.node(name);
      if (data.message !== undefined) {
        // Error response - set node to show error message
        setNode(data);
        return;
      }

      // Set node immediately so the page can render
      setNode({ ...data });

      // Fetch additional data in parallel and update as they arrive
      const additionalFetches = [];

      // Always fetch client code
      additionalFetches.push(
        djClient.clientCode(name).then(code => {
          setNode(prev => prev ? { ...prev, createNodeClientCode: code } : prev);
        }).catch(err => console.error('Failed to fetch client code:', err))
      );

      // Fetch metric-specific data
      if (data.type === 'metric') {
        additionalFetches.push(
          djClient.getMetric(name).then(metric => {
            setNode(prev => prev ? {
              ...prev,
              metric_metadata: metric.current.metricMetadata,
              required_dimensions: metric.current.requiredDimensions,
              upstream_node: metric.current.parents[0]?.name,
              expression: metric.current.metricMetadata?.expression,
              incompatible_druid_functions:
                metric.current.metricMetadata?.incompatibleDruidFunctions || [],
            } : prev);
          }).catch(err => console.error('Failed to fetch metric data:', err))
        );
      }

      // Fetch cube-specific data
      if (data.type === 'cube') {
        additionalFetches.push(
          djClient.cube(name).then(cube => {
            setNode(prev => prev ? { ...prev, cube_elements: cube.cube_elements } : prev);
          }).catch(err => console.error('Failed to fetch cube data:', err))
        );
      }

      // Wait for all additional fetches (they update state individually)
      await Promise.allSettled(additionalFetches);
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
        id: 'validate',
        name: '► Validate',
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
        id: 'dependencies',
        name: 'Dependencies',
        display: node?.type !== 'cube',
      },
    ];
  };
  let tabToDisplay = null;

  switch (state.selectedTab) {
    case 'info':
      tabToDisplay = node ? <NodeInfoTab node={node} /> : '';
      break;
    case 'columns':
      tabToDisplay = <NodeColumnTab node={node} djClient={djClient} />;
      break;
    case 'graph':
      tabToDisplay = <NodeGraphTab djNode={node} djClient={djClient} />;
      break;
    case 'history':
      tabToDisplay = <NodeHistory node={node} djClient={djClient} />;
      break;
    case 'validate':
      tabToDisplay = <NodeValidateTab node={node} djClient={djClient} />;
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
    case 'dependencies':
      tabToDisplay = <NodeDependenciesTab node={node} djClient={djClient} />;
      break;
    default:
      /* istanbul ignore next */
      tabToDisplay = <NodeInfoTab node={node} />;
  }

  const NodeButtons = () => {
    return (
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
        <button
          className="button-3"
          onClick={() => navigate(`/nodes/${node?.name}/edit`)}
        >
          <EditIcon /> Edit
        </button>

        <WatchButton node={node} />

        <ClientCodePopover code={node?.createNodeClientCode} />
        {node?.type === 'cube' && <NotebookDownload node={node} />}
      </div>
    );
  };

  // @ts-ignore
  return (
    <div className="node__header">
      <NamespaceHeader namespace={name.split('.').slice(0, -1).join('.')} />
      <div className="card">
        {node === undefined ? (
          <div style={{ padding: '2rem', textAlign: 'center' }}>
            <LoadingIcon />
          </div>
        ) : node?.message === undefined ? (
          <div className="card-header" style={{}}>
            <div
              style={{
                display: 'flex',
                flexDirection: 'row',
                justifyContent: 'space-between',
              }}
            >
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
              <NodeButtons />
            </div>
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
