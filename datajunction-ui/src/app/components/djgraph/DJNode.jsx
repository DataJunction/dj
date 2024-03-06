import { memo } from 'react';
import { Handle, Position } from 'reactflow';
import Collapse from './Collapse';

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
  const highlightNodeClass =
    data.is_current === true ? ' dj-node_highlight' : '';
  return (
    <>
      <div
        className={'dj-node__full node_type__' + data.type + highlightNodeClass}
        key={data.name}
        style={{ width: '450px' }}
      >
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
          <b>{capitalize(data.type)}</b>
          <br />{' '}
          <a href={`/nodes/${data.name}`}>
            {data.type === 'source' ? data.table : data.display_name}
          </a>
          <Collapse
            collapsed={data.is_current ? false : true}
            text={data.type !== 'metric' ? 'columns' : 'dimensions'}
            data={data}
          />
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
