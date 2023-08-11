import React, { useContext } from 'react';
import { MarkerType } from 'reactflow';

import '../../../styles/dag.css';
import 'reactflow/dist/style.css';
import DJNode from '../../components/djgraph/DJNode';
import DJClientContext from '../../providers/djclient';
import LayoutFlow from '../../components/djgraph/LayoutFlow';

const createDJNode = node => {
  return {
    id: String(node.name),
    type: 'DJNode',
    data: {
      label: node.name,
      name: node.name,
      type: node.type,
      table: node.type === 'source' ? node.name : '',
      display_name: node.name,
      column_names: node.columns.map(col => {
        return { name: col.name, type: '' };
      }),
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
    let processing = [relatedNodes];
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
      current.columns.forEach(col => {
        if (col.node !== null) {
          edgesMapping[current.name + '-' + col.node.name] = {
            id: current.name + '-' + col.node.name,
            source: col.node.name,
            sourceHandle: col.node.name,
            target: current.name,
            targetHandle: current.name,
            animated: true,
            markerEnd: {
              type: MarkerType.Arrow,
            },
            style: {
              strokeWidth: 3,
              stroke: '#b0b9c2',
            },
          };
          processing.push(col.node);
        }
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
