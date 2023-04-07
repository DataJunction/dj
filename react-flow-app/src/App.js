import React, {useCallback, useEffect, useMemo} from 'react';
import dagre from 'dagre';

import ReactFlow, {
  addEdge,
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  MarkerType,
} from 'reactflow';

import { edges as initialEdges } from './initial-elements';

import 'reactflow/dist/style.css';
import './overview.css';
import DJNode from "./DJNode";

const minimapStyle = {
  height: 120,
};

const DJ_URL = process.env.REACT_APP_DJ_URL;

const onInit = (reactFlowInstance) => console.log('flow loaded:', reactFlowInstance);

const dagreGraph = new dagre.graphlib.Graph();
dagreGraph.setDefaultEdgeLabel(() => ({}));

const getLayoutedElements = (
  nodes,
  edges,
  direction = 'TB',
  nodeWidth = 150,
  nodeHeight = 50,
) => {
  const isHorizontal = direction === 'LR';
  dagreGraph.setGraph({ rankdir: direction });

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
  });

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  nodes.forEach((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);
    node.targetPosition = isHorizontal ? 'left' : 'top';
    node.sourcePosition = isHorizontal ? 'right' : 'bottom';
    node.position = {
      x: nodeWithPosition.x - nodeWidth / 2,
      y: nodeWithPosition.y - nodeHeight / 2,
    };
    return node;
  });

  return { nodes, edges };
};


const Flow = () => {
  const nodeTypes = useMemo(() => ({ DJNode: DJNode }), []);

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(
    nodes,
    edges
  );

  useEffect(() => {
    const dagFetch = async () => {
      const data = await (
        await fetch(
          DJ_URL + "/nodes/"
        )
      ).json();

      const edges = [];
      data.forEach(
        (obj) => {
          obj.parents.forEach((parent) => {
            if (parent.name) {
              edges.push({
                id: obj.name + "-" + parent.name,
                target: obj.name,
                source: parent.name,
                markerEnd: {
                  type: MarkerType.Arrow,
                },
              });
            }
          });

          obj.columns.forEach((col) => {
            if (col.dimension) {
              edges.push({
                id: obj.name + "-" + col.dimension.name,
                target: obj.name,
                source: col.dimension.name,
                animated: true,
                draggable: true,
              });
            }
          });
      });

      setEdges(edges);

      setNodes(data.map((node, index) => {
        return {
            id: String(node.name),
            type: "DJNode",
            data: {label: node.display_name, type: node.type},
        }
      }));
    };
    dagFetch();
  }, []);

  const onConnect = useCallback((params) => setEdges((eds) => addEdge(params, eds)), []);

  return (
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
  );
};

export default Flow;
