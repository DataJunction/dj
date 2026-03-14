import {
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react'; // useRef kept for hasFitRef
import * as React from 'react';
import ReactFlow, {
  addEdge,
  useEdgesState,
  useNodesState,
  Handle,
  MarkerType,
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

const BASE_EDGE = { strokeWidth: 1, stroke: '#dde3ec' };
const BASE_MARKER = {
  type: MarkerType.ArrowClosed,
  color: '#dde3ec',
  width: 12,
  height: 12,
};

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
        border: `3px solid ${c.border}`,
        boxShadow: `0 0 0 1px ${c.border}33, 0 4px 12px rgba(0,0,0,0.12)`,
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

const getLayoutedElements = (nodes, edges, currentName) => {
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

  // Translate so the current node's center sits at the origin (0, 0).
  // fitView then centers on it regardless of how asymmetric the rest of the graph is.
  const current = nodes.find(n => n.id === currentName);
  if (current) {
    const dx = -(current.position.x + NODE_WIDTH / 2);
    const dy = -(current.position.y + NODE_HEIGHT / 2);
    nodes.forEach(n => {
      n.position = { x: n.position.x + dx, y: n.position.y + dy };
    });
  }

  return { nodes, edges };
};

const buildRFNode = (n, currentName) => ({
  id: n.name,
  type: 'DimNode',
  zIndex: 10,
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
  const [rfInstance, setRfInstance] = useState(null);
  const [currentNodeName, setCurrentNodeName] = useState(null);
  const hasFitRef = useRef(false);

  // Reset fit-view state whenever the node changes so a fresh fitView fires on load.
  useEffect(() => {
    hasFitRef.current = false;
    setCurrentNodeName(null);
  }, [djNode?.name]);

  // Center viewport on the current node (which is placed at the origin by getLayoutedElements).
  // rfInstance is state (not ref) so this re-runs if onInit fires after nodes are set.
  useEffect(() => {
    if (
      nodes.length > 0 &&
      rfInstance &&
      currentNodeName &&
      !hasFitRef.current
    ) {
      hasFitRef.current = true;
      rfInstance.setCenter(0, 0, { zoom: 0.9 });
    }
  }, [nodes, rfInstance, currentNodeName]);

  useEffect(() => {
    if (!djNode?.name) return;

    const makeEdge = (() => {
      const seen = new Set();
      return (source, target) => {
        const id = `${source}->${target}`;
        if (seen.has(id)) return null;
        seen.add(id);
        return { id, source, target, style: BASE_EDGE, markerEnd: BASE_MARKER };
      };
    })();

    const applyLayout = (rfNodes, rfEdges) => {
      const { nodes: ln, edges: le } = getLayoutedElements(
        rfNodes,
        rfEdges,
        djNode.name,
      );
      setCurrentNodeName(djNode.name);
      setNodes(ln);
      setEdges(le);
      setLoaded(true);
    };

    djClient
      .dimensionDag(djNode.name)
      .then(
        ({
          inbound = [],
          inbound_edges = [],
          outbound = [],
          outbound_edges = [],
        }) => {
          if (inbound.length === 0 && outbound.length === 0) {
            setLoaded(true);
            return;
          }
          const allRelated = [...inbound, ...outbound];
          const rfNodes = [
            buildRFNode(djNode, djNode.name),
            ...allRelated.map(n => buildRFNode(n, djNode.name)),
          ];
          const rfEdges = [
            ...inbound_edges.map(e => makeEdge(e.source, e.target)),
            ...outbound_edges.map(e => makeEdge(e.source, e.target)),
          ].filter(Boolean);
          applyLayout(rfNodes, rfEdges);
        },
      )
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
          const hoverColor = isConn ? '#475569' : '#cbd5e1';
          return {
            ...e,
            style: { strokeWidth: isConn ? 2 : 1, stroke: hoverColor },
            markerEnd: {
              type: MarkerType.ArrowClosed,
              color: hoverColor,
              width: 12,
              height: 12,
            },
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
      {/* Ensure node HTML layer always renders above the edge SVG layer */}
      <style>{`.react-flow__nodes { z-index: 10 !important; }`}</style>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeMouseEnter={onNodeMouseEnter}
        onNodeMouseLeave={onNodeMouseLeave}
        onInit={setRfInstance}
        proOptions={{ hideAttribution: true }}
      />
    </div>
  );
}
