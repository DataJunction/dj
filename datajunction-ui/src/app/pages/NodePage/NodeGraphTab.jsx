import React, { useCallback, useContext, useEffect, useMemo } from 'react';
import ReactFlow, {
  addEdge,
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  MarkerType,
} from 'reactflow';

import '../../../styles/dag.css';
import 'reactflow/dist/style.css';
import DJNode from '../../components/djgraph/DJNode';
import { DJColumn } from '../../components/djgraph/DJColumn';
import dagre from 'dagre';
import DJClientContext from '../../providers/djclient';

const NodeLineage = djNode => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const nodeTypes = useMemo(() => ({ DJNode: DJNode, DJColumn: DJColumn }), []);

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  const minimapStyle = {
    height: 100,
    width: 150,
  };

  const dagreGraph = useMemo(() => new dagre.graphlib.Graph(), []);
  dagreGraph.setDefaultEdgeLabel(() => ({}));

  useEffect(() => {
    const setElementsLayout = (
      nodes,
      edges,
      direction = 'LR',
      nodeWidth = 800,
    ) => {
      const isHorizontal = direction === 'TB';
      dagreGraph.setGraph({ rankdir: direction });
      const nodeHeightTracker = {};

      nodes.forEach(node => {
        console.log(node);
        nodeHeightTracker[node.id] = node.data.column_names.length * 37 + 250;
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
          y: nodeWithPosition.y - nodeHeightTracker[node.id] / 2,
        };
        node.width = nodeWidth;
        node.height = nodeHeightTracker[node.id];
        return node;
      });
      return { nodes, edges };
    };

    const dagFetch = async () => {
      let related_nodes = await djClient.node_dag(djNode.djNode.name);
      var djNodes = [djNode.djNode];
      for (const iterable of [related_nodes]) {
        for (const item of iterable) {
          djNodes.push(item);
        }
      }
      let edges = [];
      djNodes.forEach(obj => {
        obj.parents.forEach(parent => {
          if (parent.name) {
            edges.push({
              id: obj.name + '-' + parent.name,
              source: parent.name,
              sourceHandle: parent.name,
              target: obj.name,
              targetHandle: obj.name,
              animated: true,
              markerEnd: {
                type: MarkerType.Arrow,
              },
              style: {
                strokeWidth: 3,
                stroke: '#b0b9c2',
              },
            });
          }
        });

        obj.columns.forEach(col => {
          if (col.dimension) {
            const edge = {
              id: col.dimension.name + '->' + obj.name + '.' + col.name,
              source: col.dimension.name,
              sourceHandle: col.dimension.name,
              target: obj.name,
              targetHandle: obj.name + '.' + col.name,
              draggable: true,
              markerStart: {
                type: MarkerType.Arrow,
                width: 20,
                height: 20,
                color: '#b0b9c2',
              },
              style: {
                strokeWidth: 3,
                stroke: '#b0b9c2',
              },
            };
            edges.push(edge);
            console.log('Edge added:', edge);
          }
        });
      });
      const nodes = djNodes.map(node => {
        const primary_key = node.columns
          .filter(col =>
            col.attributes.some(
              attr => attr.attribute_type.name === 'primary_key',
            ),
          )
          .map(col => col.name);
        const column_names = node.columns.map(col => {
          return { name: col.name, type: col.type };
        });
        return {
          id: String(node.name),
          type: 'DJNode',
          data: {
            label:
              node.table !== null
                ? String(node.schema_ + '.' + node.table)
                : 'default.' + node.name,
            table: node.table,
            name: String(node.name),
            display_name: String(node.display_name),
            type: node.type,
            primary_key: primary_key,
            column_names: column_names,
          },
        };
      });
      setNodes(nodes);
      setEdges(edges);

      // use dagre to determine the position of the parents (the DJ nodes)
      // the positions of the columns are relative to each DJ node
      setElementsLayout(nodes, edges);
    };

    dagFetch();
  }, [dagreGraph, djClient, djNode.djNode, setEdges, setNodes]);

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
export default NodeLineage;
