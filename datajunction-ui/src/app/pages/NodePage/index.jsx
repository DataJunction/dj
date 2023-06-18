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
    return (
      <Tab
        key={tab.id}
        id={tab.id}
        name={tab.name}
        onClick={onClickTab(tab.id)}
        selectedTab={state.selectedTab}
      />
    );
  };

  const { name } = useParams();

  useEffect(() => {
    const fetchData = async () => {
      const data = await djClient.node(name);
      setNode(data);
      if (data.type === 'metric') {
        const metric = await djClient.metric(name);
        data.dimensions = metric.dimensions;
        setNode(data);
      }
    };
    fetchData().catch(console.error);
  }, [djClient, name]);

  const TabsJson = [
    {
      id: 0,
      name: 'Info',
    },
    {
      id: 1,
      name: 'Columns',
    },
    {
      id: 2,
      name: 'Graph',
    },
    {
      id: 3,
      name: 'History',
    },
    {
      id: 4,
      name: 'SQL',
    },
    {
      id: 5,
      name: 'Materializations',
    },
  ];
  //
  //
  let tabToDisplay = null;
  switch (state.selectedTab) {
    case 0:
      tabToDisplay = node ? <NodeInfoTab node={node} /> : '';
      break;
    case 1:
      tabToDisplay = <NodeColumnTab node={node} />;
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
    default:
      tabToDisplay = <NodeInfoTab node={node} />;
  }

  // @ts-ignore
  return (
    <div className="node__header">
      <NamespaceHeader namespace={name.split('.').slice(0, -1).join('.')} />
      <div className="card">
        <div className="card-header">
          <h3 className="card-title align-items-start flex-column">
            <span className="card-label fw-bold text-gray-800">
              {node?.display_name}
            </span>
          </h3>
          <span
            className="fs-6 fw-semibold text-gray-400"
            style={{ marginTop: '-4rem' }}
          >
            Updated {new Date(node?.updated_at).toDateString()}
          </span>
          <div className="align-items-center row">
            {TabsJson.map(buildTabs)}
          </div>
          {tabToDisplay}
        </div>
      </div>
    </div>
  );
}
