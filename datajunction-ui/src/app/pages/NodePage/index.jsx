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
import InfoIcon from '../../icons/InfoIcon';
import ColumnsIcon from '../../icons/ColumnsIcon';
import GraphIcon from '../../icons/GraphIcon';
import HistoryIcon from '../../icons/HistoryIcon';
import PlayIcon from '../../icons/PlayIcon';
import LayersIcon from '../../icons/LayersIcon';
import LinkIcon from '../../icons/LinkIcon';
import GitBranchIcon from '../../icons/GitBranchIcon';
import DependenciesIcon from '../../icons/DependenciesIcon';
import { useNavigate } from 'react-router-dom';

export function NodePage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const navigate = useNavigate();

  const { name, tab } = useParams();

  const [state, setState] = useState({
    selectedTab: tab || 'info',
  });

  const [node, setNode] = useState(null);

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
        icon={tab.icon}
        onClick={onClickTab(tab.id)}
        selectedTab={state.selectedTab}
      />
    ) : null;
  };

  useEffect(() => {
    const fetchData = async () => {
      const data = await djClient.node(name);
      if (data.message !== undefined) {
        // Error response
        setNode(data);
        return;
      }
      setNode({ ...data });
    };
    fetchData().catch(console.error);
  }, [djClient, name]);

  const tabsList = node => {
    return [
      {
        id: 'info',
        name: 'Info',
        icon: <InfoIcon />,
        display: true,
      },
      {
        id: 'columns',
        name: 'Columns',
        icon: <ColumnsIcon />,
        display: true,
      },
      {
        id: 'graph',
        name: 'Graph',
        icon: <GraphIcon />,
        display: true,
      },
      {
        id: 'history',
        name: 'History',
        icon: <HistoryIcon />,
        display: true,
      },
      {
        id: 'validate',
        name: 'Validate',
        icon: <PlayIcon />,
        display: node?.type !== 'source',
      },
      {
        id: 'materializations',
        name: 'Materializations',
        icon: <LayersIcon />,
        display: node?.type !== 'source',
      },
      {
        id: 'linked',
        name: 'Linked Nodes',
        icon: <LinkIcon />,
        display: node?.type === 'dimension',
      },
      {
        id: 'lineage',
        name: 'Lineage',
        icon: <GitBranchIcon />,
        display: node?.type === 'metric',
      },
      {
        id: 'dependencies',
        name: 'Dependencies',
        icon: <DependenciesIcon />,
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
          className="action-btn"
          onClick={() => navigate(`/nodes/${node?.name}/edit`)}
        >
          <EditIcon /> Edit
        </button>

        <WatchButton node={node} />

        <ClientCodePopover nodeName={name} />
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
          <>
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
              <div style={{ marginBottom: '16px' }}>
                <a
                  href={'/nodes/' + node?.name}
                  className="link-table"
                  role="dialog"
                  aria-hidden="false"
                  aria-label="NodeName"
                >
                  {node?.name}
                </a>
              </div>
            <div className="underline-tabs" role="tablist">
              {tabsList(node).map(buildTabs)}
            </div>
            </div>
            <div className="card-body" style={{ padding: '1.5rem 0.2rem' }}>
              {tabToDisplay}
            </div>
          </>
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
