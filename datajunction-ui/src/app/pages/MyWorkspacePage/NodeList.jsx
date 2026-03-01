import * as React from 'react';
import NodeListActions from '../../components/NodeListActions';
import { NodeDisplay } from '../../components/NodeComponents';

// Node List Component
export function NodeList({ nodes, showUpdatedAt }) {
  const formatDateTime = dateStr => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  return (
    <div className="node-list">
      {nodes.map(node => (
        <div key={node.name} className="subscription-item node-list-item">
          <div className="node-list-item-content">
            <div className="node-list-item-display">
              <NodeDisplay
                node={node}
                size="large"
                ellipsis={true}
                gap="0.5rem"
              />
            </div>
            <div className="node-list-item-name">{node.name}</div>
          </div>
          <div className="node-list-item-actions">
            {showUpdatedAt && node.current?.updatedAt && (
              <span className="node-list-item-updated">
                {formatDateTime(node.current.updatedAt)}
              </span>
            )}
            <div className="node-list-item-actions-wrapper">
              <NodeListActions nodeName={node.name} />
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
