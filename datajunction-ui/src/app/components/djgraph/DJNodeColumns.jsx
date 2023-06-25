import { Handle } from 'reactflow';
import React from 'react';

export function DJNodeColumns({ data, limit }) {
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
  return data.column_names.slice(0, limit).map(col => (
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
}
