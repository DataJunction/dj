import { useContext } from 'react';
import { MarkerType } from 'reactflow';

import '../../../styles/dag.css';
import 'reactflow/dist/style.css';
import DJClientContext from '../../providers/djclient';
import LayoutFlow from '../../components/djgraph/LayoutFlow';

const createDJNode = node => {
  return {
    id: node.node_name,
    type: 'DJNode',
    data: {
      label: node.node_name,
      name: node.node_name,
      type: node.node_type,
      table: node.node_type === 'source' ? node.node_name : '',
      display_name:
        node.node_type === 'source' ? node.node_name : node.display_name,
      column_names: [{ name: node.column_name, type: '' }],
      primary_key: [],
    },
  };
};

const NodeColumnLineage = djNode => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const dagFetch = async (getLayoutedElements, setNodes, setEdges) => {
    let relatedNodes = await djClient.node_lineage(djNode.djNode.name);
    let nodesMapping = {};
    let edgesMapping = {};
    let processing = relatedNodes;
    while (processing.length > 0) {
      let current = processing.pop();
      let node = createDJNode(current);
      if (node.id in nodesMapping) {
        nodesMapping[node.id].data.column_names = Array.from(
          new Set([
            ...nodesMapping[node.id].data.column_names.map(x => x.name),
            ...node.data.column_names.map(x => x.name),
          ]),
        ).map(x => {
          return { name: x, type: '' };
        });
      } else {
        nodesMapping[node.id] = node;
      }

      current.lineage.forEach(lineageColumn => {
        const sourceHandle =
          lineageColumn.node_name + '.' + lineageColumn.column_name;
        const targetHandle = current.node_name + '.' + current.column_name;
        edgesMapping[sourceHandle + '->' + targetHandle] = {
          id: sourceHandle + '->' + targetHandle,
          source: lineageColumn.node_name,
          sourceHandle: sourceHandle,
          target: current.node_name,
          targetHandle: targetHandle,
          animated: true,
          markerEnd: {
            type: MarkerType.Arrow,
          },
          style: {
            strokeWidth: 3,
            stroke: '#b0b9c2',
          },
        };
        processing.push(lineageColumn);
      });
    }

    // use dagre to determine the position of the parents (the DJ nodes)
    // the positions of the columns are relative to each DJ node
    const elements = getLayoutedElements(
      Object.keys(nodesMapping).map(key => nodesMapping[key]),
      Object.keys(edgesMapping).map(key => edgesMapping[key]),
    );

    setNodes(elements.nodes);
    setEdges(elements.edges);
  };
  return LayoutFlow(djNode, dagFetch);
};
export default NodeColumnLineage;
