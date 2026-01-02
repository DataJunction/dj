import { useMemo, useEffect, useCallback } from 'react';
import ReactFlow, {
  Background,
  Controls,
  MarkerType,
  useNodesState,
  useEdgesState,
  Handle,
  Position,
} from 'reactflow';
import dagre from 'dagre';
import 'reactflow/dist/style.css';

/**
 * Compact Pre-aggregation node - clickable, shows minimal info
 */
function PreAggNode({ data, selected }) {
  const componentCount = data.components?.length || 0;

  return (
    <div
      className={`compact-node compact-node-preagg ${
        selected ? 'selected' : ''
      }`}
    >
      <div className="compact-node-icon">◫</div>
      <div className="compact-node-content">
        <div className="compact-node-name">{data.name}</div>
        <div className="compact-node-meta">
          <span className="meta-item">{componentCount} components</span>
          {data.grain?.length > 0 && (
            <span className="meta-item grain-count">
              {data.grain.length} grain cols
            </span>
          )}
        </div>
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

/**
 * Compact Metric node - clickable, shows minimal info
 */
function MetricNode({ data, selected }) {
  return (
    <div
      className={`compact-node compact-node-metric ${
        data.isDerived ? 'compact-node-derived' : ''
      } ${selected ? 'selected' : ''}`}
    >
      <Handle type="target" position={Position.Left} />
      <div className="compact-node-icon">{data.isDerived ? '◇' : '◈'}</div>
      <div className="compact-node-content">
        <div className="compact-node-name">{data.shortName}</div>
        {data.isDerived && <div className="compact-node-badge">Derived</div>}
      </div>
    </div>
  );
}

const nodeTypes = {
  preagg: PreAggNode,
  metric: MetricNode,
};

// Node dimensions for dagre layout
const NODE_WIDTH = 200;
const NODE_HEIGHT = 50;

/**
 * Use dagre to automatically layout nodes
 */
function getLayoutedElements(nodes, edges) {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));

  // Configure the layout
  dagreGraph.setGraph({
    rankdir: 'LR', // Left to right
    nodesep: 60, // Vertical spacing between nodes
    ranksep: 150, // Horizontal spacing between columns
    marginx: 40,
    marginy: 40,
  });

  // Add nodes to dagre
  nodes.forEach(node => {
    dagreGraph.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
  });

  // Add edges to dagre
  edges.forEach(edge => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  // Run the layout
  dagre.layout(dagreGraph);

  // Apply the calculated positions back to nodes
  const layoutedNodes = nodes.map(node => {
    const nodeWithPosition = dagreGraph.node(node.id);
    return {
      ...node,
      position: {
        x: nodeWithPosition.x - NODE_WIDTH / 2,
        y: nodeWithPosition.y - NODE_HEIGHT / 2,
      },
    };
  });

  return { nodes: layoutedNodes, edges };
}

/**
 * MetricFlowGraph - Uses dagre for automatic layout
 */
export function MetricFlowGraph({
  grainGroups,
  metricFormulas,
  selectedNode,
  onNodeSelect,
}) {
  const { nodes, edges } = useMemo(() => {
    if (!grainGroups?.length || !metricFormulas?.length) {
      return { nodes: [], edges: [] };
    }

    const rawNodes = [];
    const rawEdges = [];

    // Track mappings
    const preAggNodesMap = new Map();
    const componentToPreAgg = new Map();

    let nodeId = 0;
    const getNextId = () => `node-${nodeId++}`;

    // Build component -> preAgg mapping
    grainGroups.forEach((gg, idx) => {
      gg.components?.forEach(comp => {
        componentToPreAgg.set(comp.name, idx);
      });
    });

    // Create pre-aggregation nodes
    grainGroups.forEach((gg, idx) => {
      const id = getNextId();
      preAggNodesMap.set(idx, id);

      const shortName = gg.parent_name?.split('.').pop() || `preagg_${idx}`;

      rawNodes.push({
        id,
        type: 'preagg',
        position: { x: 0, y: 0 }, // Will be set by dagre
        data: {
          name: shortName,
          fullName: gg.parent_name,
          grain: gg.grain || [],
          components: gg.components || [],
          grainGroupIndex: idx,
        },
        selected:
          selectedNode?.type === 'preagg' && selectedNode?.index === idx,
      });
    });

    // Create metric nodes
    const metricNodeIds = new Map();

    metricFormulas.forEach((metric, idx) => {
      const id = getNextId();
      metricNodeIds.set(metric.name, id);

      rawNodes.push({
        id,
        type: 'metric',
        position: { x: 0, y: 0 }, // Will be set by dagre
        data: {
          name: metric.name,
          shortName: metric.short_name,
          combiner: metric.combiner,
          isDerived: metric.is_derived,
          components: metric.components,
          metricIndex: idx,
        },
        selected:
          selectedNode?.type === 'metric' && selectedNode?.index === idx,
      });
    });

    // Create edges
    metricFormulas.forEach(metric => {
      const metricId = metricNodeIds.get(metric.name);
      const connectedPreAggs = new Set();

      metric.components?.forEach(compName => {
        const preAggIdx = componentToPreAgg.get(compName);
        if (preAggIdx !== undefined) {
          connectedPreAggs.add(preAggIdx);
        }
      });

      connectedPreAggs.forEach(preAggIdx => {
        const preAggId = preAggNodesMap.get(preAggIdx);
        if (preAggId && metricId) {
          rawEdges.push({
            id: `edge-${preAggId}-${metricId}`,
            source: preAggId,
            target: metricId,
            type: 'default', // Straight/bezier edges
            style: { stroke: '#64748b', strokeWidth: 2 },
            markerEnd: {
              type: MarkerType.ArrowClosed,
              color: '#64748b',
              width: 16,
              height: 16,
            },
          });
        }
      });
    });

    // Apply dagre layout
    return getLayoutedElements(rawNodes, rawEdges);
  }, [grainGroups, metricFormulas, selectedNode]);

  const [flowNodes, setNodes, onNodesChange] = useNodesState(nodes);
  const [flowEdges, setEdges, onEdgesChange] = useEdgesState(edges);

  // Update nodes/edges when data changes
  useEffect(() => {
    setNodes(nodes);
    setEdges(edges);
  }, [nodes, edges, setNodes, setEdges]);

  const handleNodeClick = useCallback(
    (event, node) => {
      if (node.type === 'preagg') {
        onNodeSelect?.({
          type: 'preagg',
          index: node.data.grainGroupIndex,
          data: grainGroups[node.data.grainGroupIndex],
        });
      } else if (node.type === 'metric') {
        onNodeSelect?.({
          type: 'metric',
          index: node.data.metricIndex,
          data: metricFormulas[node.data.metricIndex],
        });
      }
    },
    [onNodeSelect, grainGroups, metricFormulas],
  );

  const handlePaneClick = useCallback(() => {
    onNodeSelect?.(null);
  }, [onNodeSelect]);

  if (!grainGroups?.length || !metricFormulas?.length) {
    return (
      <div className="graph-empty-state">
        <div className="empty-icon">◎</div>
        <p>Select metrics and dimensions above to visualize the data flow</p>
      </div>
    );
  }

  return (
    <div className="compact-flow-container">
      <ReactFlow
        nodes={flowNodes}
        edges={flowEdges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        onNodeClick={handleNodeClick}
        onPaneClick={handlePaneClick}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        minZoom={0.5}
        maxZoom={1.5}
        attributionPosition="bottom-left"
        proOptions={{ hideAttribution: true }}
      >
        <Background color="#cbd5e1" gap={20} size={1} />
        <Controls showInteractive={false} />
      </ReactFlow>

      {/* Legend */}
      <div className="graph-legend">
        <div className="legend-item">
          <span className="legend-dot preagg"></span>
          <span>Pre-agg</span>
        </div>
        <div className="legend-item">
          <span className="legend-dot metric"></span>
          <span>Metric</span>
        </div>
        <div className="legend-item">
          <span className="legend-dot derived"></span>
          <span>Derived</span>
        </div>
      </div>
    </div>
  );
}

export default MetricFlowGraph;
