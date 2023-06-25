import React, { memo } from 'react';
import { Handle, Position } from 'reactflow';
import { DJNodeDimensions } from './DJNodeDimensions';

function capitalize(string) {
  return string.charAt(0).toUpperCase() + string.slice(1);
}

export function DJNode({ id, data }) {
  const handleWrapperStyle = {
    display: 'flex',
    position: 'absolute',
    height: '100%',
    flexDirection: 'column',
    top: '50%',
    justifyContent: 'space-between',
  };
  const handleWrapperStyleRight = { ...handleWrapperStyle, ...{ right: 0 } };

  const handleStyle = {
    width: '12px',
    height: '12px',
    borderRadius: '12px',
    background: 'transparent',
    border: '4px solid transparent',
    cursor: 'pointer',
    position: 'absolute',
    top: '0px',
    left: 0,
  };
  const handleStyleLeft = percentage => {
    return {
      ...handleStyle,
      ...{
        transform: 'translate(-' + percentage + '%, -50%)',
      },
    };
  };
  const columnsRenderer = data =>
    data.column_names.map(col => (
      <div className={'custom-node-subheader node_type__' + data.type}>
        <div style={handleWrapperStyle}>
          <Handle
            type="target"
            position="left"
            id={data.name + '.' + col.name}
            style={handleStyleLeft(100)}
          />
        </div>
        <div
          className="custom-node-port"
          id={data.name + '.' + col.name}
          key={'i-' + data.name + '.' + col.name}
        >
          {data.primary_key.includes(col.name) ? (
            <b>{col.name} (PK)</b>
          ) : (
            <>{col.name}</>
          )}
          <span style={{ marginLeft: '0.25rem' }} className={'badge'}>
            {col.type}
          </span>
        </div>
        <div style={handleWrapperStyleRight}>
          <Handle
            type="source"
            position="right"
            id={data.name + '.' + col.name}
            style={handleStyle}
          />
        </div>
      </div>
    ));

  return (
    <>
      <div className={'dj-node__full node_type__' + data.type}>
        <div style={handleWrapperStyle}>
          <Handle
            type="target"
            id={data.name}
            position={Position.Left}
            style={handleStyleLeft(100)}
          />
        </div>
        <div className="dj-node__header">
          <div className="serif">
            {data.name
              .split('.')
              .slice(0, data.name.split('.').length - 1)
              .join(' \u25B6 ')}
          </div>
        </div>
        <div className="dj-node__body">
          <b>{capitalize(data.type)}</b>:{' '}
          <a href={`/nodes/${data.name}`}>
            {data.type === 'source' ? data.table : data.display_name}
          </a>
          {data.type !== 'metric'
            ? columnsRenderer(data)
            : DJNodeDimensions(data)}
        </div>
        <div style={handleWrapperStyleRight}>
          <Handle
            type="source"
            id={data.name}
            position={Position.Right}
            style={handleStyleLeft(90)}
          />
        </div>
      </div>
    </>
  );
}

export default memo(DJNode);
