import { useCallback, useContext, useEffect, useMemo, useState } from 'react';
import * as React from 'react';
import ReactFlow, {
  addEdge,
  useEdgesState,
  useNodesState,
  Handle,
  Position,
} from 'reactflow';
import { useNavigate } from 'react-router-dom';
import dagre from 'dagre';
import 'reactflow/dist/style.css';
import DJClientContext from '../../providers/djclient';

const TYPE_COLORS = {
  source: { border: '#00b368', text: '#00b368', bg: '#ccf7e5' },
  transform: { border: '#0063b4', text: '#0063b4', bg: '#ccefff' },
  metric: { border: '#a2283e', text: '#a2283e', bg: '#fad7dd' },
  dimension: { border: '#a96621', text: '#a96621', bg: '#ffefd0' },
  cube: { border: '#580076', text: '#580076', bg: '#dbafff' },
};

const NODE_WIDTH = 220;
const NODE_HEIGHT = 72;
const HANDLE_BASE = {
  width: 1,
  height: 1,
  opacity: 0,
  border: 'none',
  background: 'transparent',
  pointerEvents: 'none',
};
// ReactFlow connects Position.Right edges to handle.x + handle.width (right edge of handle).
// Setting right:0 puts the right edge flush with the node's right border → edge is flush.
// Similarly, left:0 puts the left edge flush with the node's left border.
const TARGET_HANDLE = { ...HANDLE_BASE, left: 0 };
const SOURCE_HANDLE = { ...HANDLE_BASE, right: 0 };

const BASE_EDGE = { strokeWidth: 1, stroke: '#94a3b8' };
const BASE_MARKER = undefined;

function DimNode({ data }) {
  const navigate = useNavigate();
  const c = TYPE_COLORS[data.type] ?? {
    border: '#cbd5e1',
    text: '#64748b',
    bg: '#f8fafc',
  };
  const label = (data.display_name || data.name || '').split('.').pop();
  const namespace = (data.name || '').split('.').slice(0, -1).join('.');

  const borderStyle = data.isCurrent
    ? {
        border: `2px solid ${c.border}`,
        boxShadow: `inset 0 0 0 3px ${c.bg}, 0 2px 8px rgba(0,0,0,0.08)`,
      }
    : {
        border: `1px solid ${c.border}66`,
        opacity: data.dimmed ? 0.3 : 1,
        boxShadow: '0 1px 4px rgba(0,0,0,0.06)',
      };

  return (
    <>
      <Handle type="target" position={Position.Left} style={TARGET_HANDLE} />
      <div
        onClick={() => navigate('/nodes/' + data.name)}
        style={{
          width: NODE_WIDTH,
          padding: '12px 16px',
          background: '#fff',
          borderRadius: 12,
          cursor: 'pointer',
          transition: 'opacity 0.15s',
          ...borderStyle,
        }}
      >
        <div
          style={{
            display: 'flex',
            alignItems: 'flex-start',
            justifyContent: 'space-between',
            gap: 8,
          }}
        >
          <div
            style={{
              fontSize: 13,
              fontWeight: 700,
              color: '#0f172a',
              lineHeight: 1.3,
              flex: 1,
              minWidth: 0,
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            {label}
          </div>
          <span
            style={{
              fontSize: 10,
              fontWeight: 600,
              color: c.text,
              background: c.bg,
              borderRadius: 5,
              padding: '2px 6px',
              textTransform: 'uppercase',
              letterSpacing: '0.05em',
              whiteSpace: 'nowrap',
              flexShrink: 0,
            }}
          >
            {data.type}
          </span>
        </div>
        {namespace && (
          <div
            style={{
              fontSize: 11,
              color: '#94a3b8',
              marginTop: 5,
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            {namespace}
          </div>
        )}
      </div>
      <Handle type="source" position={Position.Right} style={SOURCE_HANDLE} />
    </>
  );
}

const getLayoutedElements = (nodes, edges) => {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({
    rankdir: 'LR',
    nodesep: 24,
    ranksep: 80,
    ranker: 'longest-path',
  });
  nodes.forEach(n =>
    g.setNode(n.id, { width: NODE_WIDTH, height: NODE_HEIGHT }),
  );
  edges.forEach(e => g.setEdge(e.source, e.target));
  dagre.layout(g);
  nodes.forEach(n => {
    const pos = g.node(n.id);
    n.position = { x: pos.x - NODE_WIDTH / 2, y: pos.y - NODE_HEIGHT / 2 };
    n.targetPosition = 'left';
    n.sourcePosition = 'right';
  });
  return { nodes, edges };
};

const buildRFNode = (n, currentName) => ({
  id: n.name,
  type: 'DimNode',
  data: {
    name: n.name,
    display_name: n.display_name,
    type: n.type,
    isCurrent: n.name === currentName,
    dimmed: false,
  },
});

export default function NodeDimensionsTab({ djNode }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const nodeTypes = useMemo(() => ({ DimNode }), []);
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    if (!djNode?.name) return;

    djClient
      .node_dag(djNode.name)
      .then(dagNodes => {
        const allNodes = [djNode, ...(dagNodes || [])];
        const byName = Object.fromEntries(allNodes.map(n => [n.name, n]));

        const dimNodeNames = new Set();
        allNodes.forEach(n => {
          (n.dimension_links || []).forEach(link => {
            if (link.dimension?.name) dimNodeNames.add(link.dimension.name);
          });
        });
        if (djNode.type === 'cube') {
          (djNode.dimensions || []).forEach(d => {
            const parts = (d.value || '').split('.');
            if (parts.length > 1)
              dimNodeNames.add(parts.slice(0, -1).join('.'));
          });
        }

        if (dimNodeNames.size === 0) {
          setLoaded(true);
          return;
        }

        const missing = Array.from(dimNodeNames).filter(name => !byName[name]);
        Promise.all(missing.map(name => djClient.node(name).catch(() => null)))
          .then(fetched => {
            fetched.filter(Boolean).forEach(n => {
              byName[n.name] = n;
            });

            const graphNodeNames = [
              // All DAG nodes (metric + its upstream transforms/sources)
              ...allNodes.filter(n => n.type !== 'dimension').map(n => n.name),
              // Plus all dimension nodes
              ...Array.from(dimNodeNames).filter(name => byName[name]),
            ];
            const rfNodes = graphNodeNames.map(name =>
              buildRFNode(byName[name], djNode.name),
            );

            const graphNodeSet = new Set(graphNodeNames);
            const edgeSeen = new Set();
            const addEdge = (source, target) => {
              const id = `${source}->${target}`;
              if (edgeSeen.has(id)) return null;
              edgeSeen.add(id);
              return {
                id,
                source,
                target,
                style: BASE_EDGE,
                markerEnd: BASE_MARKER,
              };
            };

            // Dimension-link edges: dimension → node-with-that-link
            const dimLinkEdges = allNodes.flatMap(n =>
              (n.dimension_links || [])
                .filter(link => graphNodeSet.has(link.dimension?.name))
                .map(link => addEdge(link.dimension.name, n.name))
                .filter(Boolean),
            );

            // DAG edges: parent → child, for nodes already in the graph
            const dagEdges = allNodes.flatMap(n =>
              graphNodeSet.has(n.name)
                ? (n.parents || [])
                    .filter(p => graphNodeSet.has(p.name))
                    .map(p => addEdge(p.name, n.name))
                    .filter(Boolean)
                : [],
            );

            const rfEdges = [...dimLinkEdges, ...dagEdges];

            const { nodes: ln, edges: le } = getLayoutedElements(
              rfNodes,
              rfEdges,
            );
            setNodes(ln);
            setEdges(le);
            setLoaded(true);
          })
          .catch(console.error);
      })
      .catch(err => {
        console.error(err);
        setLoaded(true);
      });
  }, [djNode, djClient]);

  const onNodeMouseEnter = useCallback(
    (_, node) => {
      const connectedIds = new Set([node.id]);
      setEdges(eds => {
        eds.forEach(e => {
          if (e.source === node.id) connectedIds.add(e.target);
          if (e.target === node.id) connectedIds.add(e.source);
        });
        return eds.map(e => {
          const isConn = e.source === node.id || e.target === node.id;
          return {
            ...e,
            style: isConn
              ? { strokeWidth: 2, stroke: '#475569' }
              : { strokeWidth: 1, stroke: '#cbd5e1' },
            markerEnd: undefined,
          };
        });
      });
      setNodes(ns =>
        ns.map(n => ({
          ...n,
          data: { ...n.data, dimmed: !connectedIds.has(n.id) },
        })),
      );
    },
    [setEdges, setNodes],
  );

  const onNodeMouseLeave = useCallback(() => {
    setEdges(eds =>
      eds.map(e => ({ ...e, style: BASE_EDGE, markerEnd: BASE_MARKER })),
    );
    setNodes(ns => ns.map(n => ({ ...n, data: { ...n.data, dimmed: false } })));
  }, [setEdges, setNodes]);

  const onConnect = useCallback(
    params => setEdges(eds => addEdge(params, eds)),
    [setEdges],
  );

  if (loaded && nodes.length === 0) {
    return (
      <div style={{ padding: '2rem', color: '#64748b', fontSize: 14 }}>
        No dimension links found for this node.
      </div>
    );
  }

  return (
    <div
      style={{
        height: 'calc(100vh - 280px)',
        minHeight: 400,
        background: '#fff',
      }}
    >
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeMouseEnter={onNodeMouseEnter}
        onNodeMouseLeave={onNodeMouseLeave}
        fitView
        proOptions={{ hideAttribution: true }}
      />
    </div>
  );
}
