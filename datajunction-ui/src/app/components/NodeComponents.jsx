import * as React from 'react';

/**
 * Reusable node type badge component
 *
 * @param {Object} props
 * @param {string} props.type - Node type (e.g., "METRIC", "DIMENSION")
 * @param {('small'|'medium'|'large')} [props.size='medium'] - Badge size
 * @param {boolean} [props.abbreviated=false] - Show only first character
 * @param {Object} [props.style] - Additional styles
 */
export function NodeBadge({
  type,
  size = 'medium',
  abbreviated = false,
  style = {},
}) {
  if (!type) return null;

  const sizeMap = {
    small: { fontSize: '7px', padding: '0.44em' },
    medium: { fontSize: '9px', padding: '0.44em' },
    large: { fontSize: '11px', padding: '0.44em' },
  };

  const sizeStyles = sizeMap[size] || sizeMap.medium;
  const displayText = abbreviated ? type.charAt(0) : type;

  return (
    <span
      className={`node_type__${type.toLowerCase()} badge node_type`}
      style={{
        ...sizeStyles,
        flexShrink: 0,
        ...style,
      }}
    >
      {displayText}
    </span>
  );
}

/**
 * Reusable node link component
 *
 * @param {Object} props
 * @param {Object} props.node - Node object with name and current.displayName
 * @param {('small'|'medium'|'large')} [props.size='medium'] - Link size
 * @param {boolean} [props.showFullName=false] - Show full node name instead of display name
 * @param {boolean} [props.ellipsis=false] - Enable text overflow ellipsis
 * @param {Object} [props.style] - Additional styles
 */
export function NodeLink({
  node,
  size = 'medium',
  showFullName = false,
  ellipsis = false,
  style = {},
}) {
  if (!node?.name) return null;

  const sizeMap = {
    small: { fontSize: '10px', fontWeight: '500' },
    medium: { fontSize: '12px', fontWeight: '500' },
    large: { fontSize: '13px', fontWeight: '500' },
  };

  const sizeStyles = sizeMap[size] || sizeMap.medium;
  const displayName = showFullName
    ? node.name
    : node.current?.displayName || node.name.split('.').pop();

  const ellipsisStyles = ellipsis
    ? {
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
      }
    : {};

  return (
    <a
      href={`/nodes/${node.name}`}
      style={{
        ...sizeStyles,
        textDecoration: 'none',
        ...ellipsisStyles,
        ...style,
      }}
    >
      {displayName}
    </a>
  );
}

/**
 * Combined node display with link and badge
 *
 * @param {Object} props
 * @param {Object} props.node - Node object
 * @param {('small'|'medium'|'large')} [props.size='medium'] - Overall size
 * @param {boolean} [props.showBadge=true] - Show type badge
 * @param {boolean} [props.abbreviatedBadge=false] - Show abbreviated badge
 * @param {boolean} [props.ellipsis=false] - Enable text overflow ellipsis
 * @param {string} [props.gap='6px'] - Gap between link and badge
 * @param {Object} [props.containerStyle] - Additional container styles
 */
export function NodeDisplay({
  node,
  size = 'medium',
  showBadge = true,
  abbreviatedBadge = false,
  ellipsis = false,
  gap = '6px',
  containerStyle = {},
}) {
  if (!node) return null;

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        gap,
        ...containerStyle,
      }}
    >
      <NodeLink node={node} size={size} ellipsis={ellipsis} />
      {showBadge && node.type && (
        <NodeBadge
          type={node.type}
          size={size}
          abbreviated={abbreviatedBadge}
        />
      )}
    </div>
  );
}

/**
 * Node chip component - compact display with border
 * Used in NeedsAttentionSection
 *
 * @param {Object} props
 * @param {Object} props.node - Node object
 * @param {boolean} [props.abbreviatedBadge=true] - Show abbreviated badge
 */
export function NodeChip({ node }) {
  if (!node) return null;

  return (
    <a
      href={`/nodes/${node.name}`}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: '3px',
        padding: '2px 6px',
        fontSize: '10px',
        border: '1px solid var(--border-color, #ddd)',
        borderRadius: '3px',
        textDecoration: 'none',
        color: 'inherit',
        backgroundColor: 'var(--card-bg, #f8f9fa)',
        whiteSpace: 'nowrap',
        flexShrink: 0,
      }}
    >
      <NodeBadge type={node.type} size="small" abbreviated={true} />
      {node.current?.displayName || node.name.split('.').pop()}
    </a>
  );
}
