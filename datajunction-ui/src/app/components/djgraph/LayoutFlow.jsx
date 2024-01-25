import React, { useCallback, useEffect, useMemo } from 'react';
import ReactFlow, {
  addEdge,
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
} from 'reactflow';

import '../../../styles/dag.css';
import 'reactflow/dist/style.css';
import DJNode from '../../components/djgraph/DJNode';
import dagre from 'dagre';

const getLayoutedElements = (
  nodes,
  edges,
  direction = 'LR',
  nodeWidth = 600,
) => {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));

  const isHorizontal = direction === 'TB';
  dagreGraph.setGraph({
    rankdir: direction,
    nodesep: 40,
    ranksep: 10,
    ranker: 'longest-path',
  });
  const nodeHeightTracker = {};

  nodes.forEach(node => {
    const minColumnsLength = node.data.column_names.filter(
      col => col.order > 0,
    ).length;
    nodeHeightTracker[node.id] = Math.min(minColumnsLength, 5) * 40 + 250;
    dagreGraph.setNode(node.id, {
      width: nodeWidth,
      height: nodeHeightTracker[node.id],
    });
  });

  edges.forEach(edge => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  nodes.forEach(node => {
    const nodeWithPosition = dagreGraph.node(node.id);
    node.targetPosition = isHorizontal ? 'left' : 'top';
    node.sourcePosition = isHorizontal ? 'right' : 'bottom';
    node.position = {
      x: nodeWithPosition.x - nodeWidth / 2,
      y: nodeWithPosition.y - nodeHeightTracker[node.id] / 3,
    };
    node.width = nodeWidth;
    node.height = nodeHeightTracker[node.id];
    return node;
  });

  return { nodes: nodes, edges: edges };
};

const LayoutFlow = (djNode, saveGraph) => {
  const nodeTypes = useMemo(() => ({ DJNode: DJNode }), []);

  // These are used internally by ReactFlow (to update the nodes on the ReactFlow pane)
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  const minimapStyle = {
    height: 100,
    width: 150,
  };

  useEffect(() => {
    saveGraph(getLayoutedElements, setNodes, setEdges).catch(console.error);
  }, [djNode]);

  const onConnect = useCallback(
    params => setEdges(eds => addEdge(params, eds)),
    [setEdges],
  );
  return (
    <div style={{ height: '800px' }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        snapToGrid={true}
        fitView
      >
        <MiniMap style={minimapStyle} zoomable pannable />
        <Controls />
        <Background color="#aaa" gap={16} />
      </ReactFlow>
    </div>
  );
};
export default LayoutFlow;
