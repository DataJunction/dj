import React, {useCallback, useEffect, useMemo} from 'react';
import ReactFlow, {
  addEdge,
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState, MarkerType,
} from 'reactflow';
import 'reactflow/dist/style.css';
import DJNode from "../../components/djgraph/DJNode";
import {DataJunctionAPI} from "../../services/DJService";
import dagre from "dagre";


const NodeLineage = (djNode) => {
  const nodeTypes = useMemo(() => ({ DJNode: DJNode }), []);

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  const minimapStyle = {
    height: 100,
    width: 150,
  };
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));

  const setElementsLayout = (
    nodes,
    edges,
    direction = 'LR',
    nodeWidth = 500,
    nodeHeight = 60,
  ) => {
    const isHorizontal = direction === 'TB';
    dagreGraph.setGraph({rankdir: direction});

    nodes.forEach((node) => {
      dagreGraph.setNode(node.id, {width: nodeWidth, height: nodeHeight});
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

    return {nodes, edges};
  };

  useEffect(() => {
    const dagFetch = async () => {
      let upstreams = await DataJunctionAPI.upstreams(djNode.djNode.name);
      let downstreams = await DataJunctionAPI.downstreams(djNode.djNode.name);
      let djNodes = [...new Set([...upstreams, ...downstreams, djNode.djNode])];
      let edges = [];
      djNodes.forEach(
        (obj) => {
          obj.parents.forEach((parent) => {
            if (parent.name) {
              edges.push({
                id: obj.name + "-" + parent.name,
                target: obj.name,
                source: parent.name,
                animated: true,
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
                draggable: true,
              });
            }
          });
        }
      );
      const nodes = djNodes.map((node) => {
        const primary_key = node.columns.filter(
          (col) => col.attributes.some((attr) => attr.attribute_type.name === "primary_key")
        ).map(col => col.name);
        console.log("Primary key", primary_key)
        const column_names = node.columns.map((col) => {
          return {name: col.name, type: col.type};
        });
          return {
            id: String(node.name),
            type: "DJNode",
            data: {
              label: node.table !== null ? String(node.schema_ + "." + node.table) : String(node.name),
              table: node.table,
              name: String(node.name),
              display_name: String(node.display_name),
              type: node.type,
              primary_key: primary_key,
              column_names: column_names,
              // dimensions: dimensions,
            },
            // parentNode: [node.name.split(".").slice(-2, -1)],
            // extent: 'parent',
          }
        }
      );

      setNodes(nodes);
      setEdges(edges);
      setElementsLayout(
        nodes,
        edges,
      );
    };

    dagFetch();
  }, []);

  const onConnect = useCallback(
    (params) => setEdges((eds) => addEdge(params, eds)), []
  );

  return (
    <div style={{height: "600px"}}>
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