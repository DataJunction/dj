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

export function NodePage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [state, setState] = useState({
    selectedTab: 0,
  });

  const [node, setNode] = useState();

  const onClickTab = id => () => {
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

  const { name } = useParams();

  useEffect(() => {
    const fetchData = async () => {
      const data = await djClient.node(name);
      data.createNodeClientCode = await djClient.clientCode(name);
      setNode(data);
      if (data.type === 'metric') {
        const metric = await djClient.metric(name);
        data.dimensions = metric.dimensions;
        setNode(data);
      }
      if (data.type === 'cube') {
        const cube = await djClient.cube(name);
        data.cube_elements = cube.cube_elements;
        setNode(data);
      }
    };
    fetchData().catch(console.error);
  }, [djClient, name]);

  const tabsList = node => {
    return [
      {
        id: 0,
        name: 'Info',
        display: true,
      },
      {
        id: 1,
        name: 'Columns',
        display: true,
      },
      {
        id: 2,
        name: 'Graph',
        display: true,
      },
      {
        id: 3,
        name: 'History',
        display: true,
      },
      {
        id: 4,
        name: 'SQL',
        display: node?.type !== 'dimension' && node?.type !== 'source',
      },
      {
        id: 5,
        name: 'Materializations',
        display: node?.type !== 'source',
      },
      {
        id: 6,
        name: 'Available',
        display: node?.type === 'dimension',
      },
    ];
  };

  //
  //
  let tabToDisplay = null;
  switch (state.selectedTab) {
    case 0:
      tabToDisplay = node ? <NodeInfoTab node={node} /> : '';
      break;
    case 1:
      tabToDisplay = <NodeColumnTab node={node} djClient={djClient} />;
      break;
    case 2:
      tabToDisplay = <NodeLineage djNode={node} djClient={djClient} />;
      break;
    case 3:
      tabToDisplay = <NodeHistory node={node} djClient={djClient} />;
      break;
    case 4:
      tabToDisplay =
        node.type === 'metric' ? <NodeSQLTab djNode={node} /> : <br />;
      break;
    case 5:
      tabToDisplay = <NodeMaterializationTab node={node} djClient={djClient} />;
      break;
    case 6:
      tabToDisplay = <NodesWithDimension node={node} djClient={djClient} />;
      break;
    default:
      tabToDisplay = <NodeInfoTab node={node} />;
  }

  // @ts-ignore
  return (
    <div className="node__header">
      <NamespaceHeader namespace={name.split('.').slice(0, -1).join('.')} />
      <div className="card">
        <div className="card-header">
          <h3
            className="card-title align-items-start flex-column"
            style={{ display: 'inline-block' }}
          >
            <span className="card-label fw-bold text-gray-800">
              {node?.display_name}
            </span>
          </h3>
          <ClientCodePopover code={node?.createNodeClientCode} />
          <div className="align-items-center row">
            {tabsList(node).map(buildTabs)}
          </div>
          {tabToDisplay}
        </div>
      </div>
    </div>
  );
}
