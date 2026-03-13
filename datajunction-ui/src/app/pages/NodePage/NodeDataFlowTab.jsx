import { useContext, useEffect, useRef, useState } from 'react';
import * as React from 'react';
import { Sankey, Tooltip } from 'recharts';
import { useNavigate } from 'react-router-dom';
import DJClientContext from '../../providers/djclient';
import LoadingIcon from '../../icons/LoadingIcon';

// Match badge background colors from index.css .node_type__* classes
const TYPE_COLORS = {
  source: '#ccf7e5',
  transform: '#ccefff',
  metric: '#fad7dd',
  dimension: '#ffefd0',
  cube: '#dbafff',
};

const TYPE_BORDER_COLORS = {
  source: '#00b368',
  transform: '#0063b4',
  metric: '#a2283e',
  dimension: '#a96621',
  cube: '#580076',
};

const TYPE_LAYER_ORDER = ['source', 'transform', 'metric', 'cube'];

// Returns a slightly darker version of the pastel fill for the node border
const DARKER_FILL = {
  source: '#8de8c3',
  transform: '#8ed6f7',
  metric: '#f0a3b0',
  dimension: '#ffd08a',
  cube: '#bc80f5',
};

function SankeyNode({
  x,
  y,
  width,
  height,
  payload,
  currentNodeName,
  rightmostType,
  onNavigate,
  hoveredNodeName,
  onNodeHover,
}) {
  if (!payload) return null;
  if (payload.type === 'phantom') return <g />;
  const isHovered = hoveredNodeName === payload.name;
  const isDimmed = hoveredNodeName && !isHovered;
  const baseFill = TYPE_COLORS[payload.type] ?? '#f1f5f9';
  const hoverFill = DARKER_FILL[payload.type] ?? '#cbd5e1';
  const borderColor = DARKER_FILL[payload.type] ?? '#cbd5e1';
  const isCurrent = payload.name === currentNodeName;
  const label = (payload.display_name || payload.name || '').split('.').pop();
  const isRightmost = payload.type === rightmostType;
  const labelX = isRightmost ? x + width + 8 : x - 8;
  const labelAnchor = isRightmost ? 'start' : 'end';

  return (
    <g
      style={{
        cursor: payload.name ? 'pointer' : 'default',
        opacity: isDimmed ? 0.55 : 1,
        transition: 'opacity 0.15s',
      }}
      onMouseEnter={() => onNodeHover && onNodeHover(payload.name)}
      onMouseLeave={() => onNodeHover && onNodeHover(null)}
      onClick={() =>
        payload.name && onNavigate && onNavigate('/nodes/' + payload.name)
      }
    >
      <rect
        x={x}
        y={y}
        width={width}
        height={height}
        fill={isHovered ? hoverFill : baseFill}
        fillOpacity={1}
        stroke={isCurrent || isHovered ? borderColor : 'none'}
        strokeWidth={1.5}
        rx={2}
      />
      <text
        x={labelX}
        y={y + height / 2}
        textAnchor={labelAnchor}
        dominantBaseline="middle"
        fontSize={11}
        fill="#374151"
        style={{ userSelect: 'none', pointerEvents: 'none' }}
      >
        {label}
      </text>
    </g>
  );
}

function SankeyLink({
  sourceX,
  targetX,
  sourceY,
  targetY,
  sourceControlX,
  targetControlX,
  linkWidth,
  index,
  payload,
  hoveredNodeName,
}) {
  const [linkHovered, setLinkHovered] = useState(false);
  const hw = Math.max(linkWidth, 1);
  const d = `
    M${sourceX},${sourceY - hw / 2}
    C${sourceControlX},${sourceY - hw / 2} ${targetControlX},${
    targetY - hw / 2
  } ${targetX},${targetY - hw / 2}
    L${targetX},${targetY + hw / 2}
    C${targetControlX},${targetY + hw / 2} ${sourceControlX},${
    sourceY + hw / 2
  } ${sourceX},${sourceY + hw / 2}
    Z
  `;
  const targetType = payload?.target?.type;
  if (targetType === 'phantom') return <g />;
  const sourceType = payload?.source?.type;
  const fromColor = TYPE_COLORS[sourceType] ?? '#e2e8f0';
  const toColor = TYPE_COLORS[targetType] ?? '#e2e8f0';
  const gradientId = `link-grad-${index}`;

  const isConnected =
    hoveredNodeName &&
    (payload?.source?.name === hoveredNodeName ||
      payload?.target?.name === hoveredNodeName);
  const opacity = linkHovered
    ? 0.85
    : hoveredNodeName
    ? isConnected
      ? 0.8
      : 0.15
    : 0.38;

  return (
    <g>
      <defs>
        <linearGradient id={gradientId} x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor={fromColor} stopOpacity={opacity} />
          <stop offset="100%" stopColor={toColor} stopOpacity={opacity} />
        </linearGradient>
      </defs>
      <path
        d={d}
        fill={`url(#${gradientId})`}
        stroke="none"
        style={{ cursor: 'pointer', transition: 'opacity 0.15s' }}
        onMouseEnter={() => setLinkHovered(true)}
        onMouseLeave={() => setLinkHovered(false)}
      />
    </g>
  );
}

export default function NodeDataFlowTab({ djNode }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const navigate = useNavigate();
  const [sankeyData, setSankeyData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [containerWidth, setContainerWidth] = useState(0);
  const [hoveredNodeName, setHoveredNodeName] = useState(null);
  const containerRef = useRef(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    // Read initial width synchronously so the chart fills the container on first paint
    setContainerWidth(el.getBoundingClientRect().width);
    const observer = new ResizeObserver(entries => {
      setContainerWidth(entries[0].contentRect.width);
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    if (!djNode?.name) return;
    setLoading(true);
    Promise.all([
      djClient.node_dag(djNode.name),
      djClient.downstreamsGQL(djNode.name),
    ])
      .then(async ([dagNodes, downstreamNodes]) => {
        // Fetch downstream cubes in one batch call
        const cubeNames = (downstreamNodes || [])
          .filter(n => n.type === 'CUBE' || n.type === 'cube')
          .map(n => n.name);
        const cubeNodes = await djClient.findCubesWithMetrics(cubeNames);

        const allNodes = [djNode, ...(dagNodes || []), ...cubeNodes];

        // Deduplicate and exclude dimension nodes (they belong in the Dimensions tab)
        const seen = new Set();
        const nodes = [];
        allNodes.forEach(n => {
          if (n && !seen.has(n.name) && n.type !== 'dimension') {
            seen.add(n.name);
            nodes.push(n);
          }
        });

        // Sort so seed node is first within its type group — it will appear at the top
        // of its column when sort={false} is used.
        const seedName = djNode?.name;
        nodes.sort((a, b) => {
          const aLayer = TYPE_LAYER_ORDER.indexOf(a.type);
          const bLayer = TYPE_LAYER_ORDER.indexOf(b.type);
          if (aLayer !== bLayer) return aLayer - bLayer;
          if (a.name === seedName) return -1;
          if (b.name === seedName) return 1;
          return 0;
        });

        const nodeIndex = {};
        nodes.forEach((n, i) => {
          nodeIndex[n.name] = i;
        });

        const links = [];
        nodes.forEach(node => {
          (node.parents || []).forEach(parent => {
            if (
              parent.name &&
              nodeIndex[parent.name] !== undefined &&
              nodeIndex[node.name] !== undefined
            ) {
              links.push({
                source: nodeIndex[parent.name],
                target: nodeIndex[node.name],
                value: 1,
              });
            }
          });
        });

        // recharts forces any node with no outgoing links to maxDepth (the cube column).
        // Fix: give free-floating metrics a tiny phantom outgoing link so they stay in
        // the metric column. The phantom node renders as invisible.
        const hasOutgoing = new Set(links.map(l => l.source));
        const phantomLinks = [];
        nodes.forEach((node, i) => {
          if (node.type === 'metric' && !hasOutgoing.has(i)) {
            phantomLinks.push({ source: i, target: nodes.length, value: 0.01 });
          }
        });
        if (phantomLinks.length > 0) {
          nodes.push({
            name: '__phantom__',
            type: 'phantom',
            display_name: '',
          });
          links.push(...phantomLinks);
        }

        setSankeyData({ nodes, links });
        setLoading(false);
      })
      .catch(err => {
        console.error(err);
        setLoading(false);
      });
  }, [djNode, djClient]);

  // Always render the sentinel div so containerRef is mounted before data loads
  if (loading || !sankeyData || sankeyData.links.length === 0) {
    return (
      <div>
        {/* Sentinel must be in DOM so ResizeObserver fires even during loading */}
        <div ref={containerRef} style={{ width: '100%', height: 0 }} />
        {loading ? (
          <div style={{ padding: '2rem' }}>
            <LoadingIcon />
          </div>
        ) : (
          <div style={{ padding: '2rem', color: '#64748b', fontSize: 14 }}>
            No data flow relationships found for this node.
          </div>
        )}
      </div>
    );
  }

  const counts = sankeyData.nodes.reduce((acc, n) => {
    if (n.type !== 'phantom') acc[n.type] = (acc[n.type] || 0) + 1;
    return acc;
  }, {});

  const summaryParts = TYPE_LAYER_ORDER.filter(t => counts[t]).map(
    t => `${counts[t]} ${t}${counts[t] > 1 ? 's' : ''}`,
  );

  // Height driven by the tallest column, not total node count
  const colDepths = {};
  sankeyData.nodes.forEach(n => {
    if (n.type !== 'phantom') {
      const col = TYPE_LAYER_ORDER.indexOf(n.type);
      colDepths[col] = (colDepths[col] || 0) + 1;
    }
  });
  const maxColNodes = Math.max(...Object.values(colDepths), 1);
  // Derive chart height so each node is at least MIN_NODE_HEIGHT px tall.
  // recharts fills: chartHeight - top - bottom = maxNodes * nodeHeight + (maxNodes - 1) * nodePadding
  // Solving for chartHeight given a desired minimum nodeHeight:
  const MIN_NODE_HEIGHT = 24;
  const NODE_PADDING = 12;
  const MARGIN_V = 20; // top + bottom margin
  const chartHeight = Math.max(
    280,
    maxColNodes * (MIN_NODE_HEIGHT + NODE_PADDING) - NODE_PADDING + MARGIN_V,
  );

  // Rightmost column determines label side (right); everything else labels left
  const rightmostType = counts.cube > 0 ? 'cube' : 'metric';

  // Measure label widths per side to set margins
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');
  ctx.font = '11px system-ui, sans-serif';
  const measureLabel = n =>
    ctx.measureText((n.display_name || n.name || '').split('.').pop()).width;
  const rightNodes = sankeyData.nodes.filter(n => n.type === rightmostType);
  const leftNodes = sankeyData.nodes.filter(
    n => n.type !== rightmostType && n.type !== 'phantom',
  );
  const rightMargin =
    Math.ceil(Math.max(0, ...rightNodes.map(measureLabel))) + 16;
  const leftMargin =
    Math.ceil(Math.max(0, ...leftNodes.map(measureLabel))) + 16;

  const nodeEl = (
    <SankeyNode
      currentNodeName={djNode?.name}
      rightmostType={rightmostType}
      onNavigate={navigate}
      hoveredNodeName={hoveredNodeName}
      onNodeHover={setHoveredNodeName}
    />
  );
  const linkEl = <SankeyLink hoveredNodeName={hoveredNodeName} />;

  return (
    <div style={{ padding: '1.5rem 0.75rem' }}>
      <div
        style={{
          fontSize: 13,
          fontWeight: 700,
          color: '#374151',
          textTransform: 'uppercase',
          letterSpacing: '0.06em',
          marginBottom: '0.75rem',
        }}
      >
        {summaryParts.join(' → ')}
      </div>
      <div
        style={{
          display: 'flex',
          gap: '1rem',
          marginBottom: '1.25rem',
          flexWrap: 'wrap',
        }}
      >
        {TYPE_LAYER_ORDER.filter(t => counts[t]).map(type => (
          <span
            key={type}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 5,
              fontSize: 12,
              color: '#64748b',
            }}
          >
            <span
              style={{
                width: 12,
                height: 12,
                background: TYPE_COLORS[type],
                border: `1.5px solid ${TYPE_BORDER_COLORS[type]}`,
                borderRadius: 2,
                display: 'inline-block',
              }}
            />
            {type}
          </span>
        ))}
      </div>
      {/* Sentinel div: measures true available width without the Sankey influencing it */}
      <div ref={containerRef} style={{ width: '100%', height: 0 }} />
      {containerWidth > 0 && (
        <Sankey
          width={containerWidth}
          height={chartHeight}
          data={sankeyData}
          nodePadding={NODE_PADDING}
          nodeWidth={16}
          margin={{ top: 10, right: rightMargin, bottom: 10, left: leftMargin }}
          node={nodeEl}
          link={linkEl}
          sort={false}
        >
          <Tooltip
            content={({ active, payload }) => {
              if (!active || !payload?.length) return null;
              const item = payload[0]?.payload;
              if (!item) return null;
              const name =
                item.name ||
                `${item.source?.display_name || item.source?.name} → ${
                  item.target?.display_name || item.target?.name
                }`;
              return (
                <div
                  style={{
                    background: 'white',
                    border: '1px solid #e2e8f0',
                    padding: '8px 12px',
                    borderRadius: 4,
                    fontSize: 12,
                    boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
                  }}
                >
                  <div style={{ fontWeight: 600 }}>{name}</div>
                  {item.type && (
                    <div
                      style={{
                        color: TYPE_COLORS[item.type] || '#64748b',
                        marginTop: 2,
                      }}
                    >
                      {item.type}
                    </div>
                  )}
                </div>
              );
            }}
          />
        </Sankey>
      )}
    </div>
  );
}
