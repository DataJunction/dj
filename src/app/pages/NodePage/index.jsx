import * as React from "react";
import {useParams} from "react-router-dom";
import {useEffect, useState} from "react";
import {DataJunctionAPI} from "../../services/DJService";
import Tab from "../../components/Tab";
import NamespaceHeader from "../../components/NamespaceHeader";
import NodeInfoTab from "./NodeInfoTab";
import NodeColumnTab from "./NodeColumnTab";
import NodeLineage from "./NodeGraphTab";
// import NodeLineage from "./NodeGraphTab";

export function NodePage() {
  const [state, setState] = useState({
     selectedTab: 0
  });

  const [node, setNode] = useState();


  const onClickTab = id => () => {
     setState({ selectedTab: id });
  }

  const buildTabs = (tab) => {
    return(
      <Tab
        key={tab.id}
        id={tab.id}
        name={tab.name}
        onClick={onClickTab(tab.id)}
        selectedTab={state.selectedTab}
      />
    );
  }

  const { name } = useParams();

  useEffect(() => {
    const fetchData = async () => {
      const data = await DataJunctionAPI.node(name);
      console.log("Data", data);
      setNode(data);
    }
    fetchData().catch(console.error);
  }, [name]);

  const TabsJson = [
    {
      "id": 0,
      "name": "Info"
    },
    {
      "id": 1,
      "name": "Columns"
    },
    {
      "id": 2,
      "name": "Graph"
    }
  ]
  //
  //
  let tabToDisplay = null;
  switch(state.selectedTab) {
    case 0: tabToDisplay = (node ? <NodeInfoTab node={node}  /> : ""); break;
    case 1: tabToDisplay = (<NodeColumnTab node={node} />); break;
    case 2: tabToDisplay = (<NodeLineage djNode={node} />); break;
    default: tabToDisplay = <NodeInfoTab node={node} />;
  }

  // @ts-ignore
  return (
    <div className="node__header">
      <NamespaceHeader namespace={ name }/>
      <div className="card">
      <div className="card-header">
        <h3 className="card-title align-items-start flex-column">
          <span className="card-label fw-bold text-gray-800">{ node?.display_name }</span>
        </h3>
        <div className="align-items-center row">
        {TabsJson.map(buildTabs)}
        </div>
        {tabToDisplay}
      </div>
      </div>
    </div>
  );
}
