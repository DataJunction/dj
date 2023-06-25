import React, { memo } from 'react';
import { Handle, Position } from 'reactflow';
import DJClientContext from '../../providers/djclient';
import { useContext, useEffect, useState } from 'react';

function renderBasedOnDJNodeType(param) {
  switch (param) {
    case 'source':
      return { backgroundColor: '#7EB46150', color: '#7EB461' };
    case 'transform':
      return { backgroundColor: '#6DAAA750', color: '#6DAAA7' };
    case 'dimension':
      return { backgroundColor: '#CF7D2950', color: '#CF7D29' };
    case 'metric':
      return { backgroundColor: '#A27E8650', color: '#A27E86' };
    case 'cube':
      return { backgroundColor: '#C2180750', color: '#C21807' };
    default:
      return {};
  }
}

function capitalize(string) {
  return string.charAt(0).toUpperCase() + string.slice(1);
}

const Collapse = ({ collapsed, text, children }) => {
  const [isCollapsed, setIsCollapsed] = React.useState(collapsed);

  return (
    <>
      <div className="collapse">
        <button
          className="collapse-button"
          onClick={() => setIsCollapsed(!isCollapsed)}
        >
          {isCollapsed ? '\u25B6 Show' : '\u25BC Hide'} {text}
        </button>
        <div
          className={`collapse-content ${
            isCollapsed ? 'collapsed' : 'expanded'
          }`}
          aria-expanded={isCollapsed}
        >
          {children}
        </div>
      </div>
    </>
  );
};

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
          <span
            style={{ marginLeft: '0.25rem' }}
            className={'badge node_type__' + data.type}
          >
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

  const [dimensions, setDimensions] = useState([]);
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  useEffect(() => {
    if (data.type === 'metric') {
      async function getDimensions() {
        try {
          const metricData = await djClient.metric(data.name);
          setDimensions(metricData.dimensions);
        } catch (err) {
          console.log(err);
        }
      }
      getDimensions();
    }
  }, [data, djClient]);
  const dimensionsToObject = dimensions => {
    return dimensions.map(dim => {
      const [attribute, ...nodeName] = dim.name.split('.').reverse();
      return {
        dimension: nodeName.reverse().join('.'),
        path: dim.path,
        column: attribute,
      };
    });
  };
  const groupedDimensions = dims =>
    dims.reduce((acc, current) => {
      const dimKey = current.dimension + ' via ' + current.path.slice(-1);
      if (dimKey === 'column') {
        console.log('found', dimKey, current);
      }
      acc[dimKey] = acc[dimKey] || {
        dimension: current.dimension,
        path: current.path.slice(-1),
        columns: [],
      };
      acc[dimKey].columns.push(current.column);
      return acc;
    }, {});
  const dimensionsRenderer = grouped =>
    Object.entries(grouped).map(([dimKey, dimValue]) => {
      if (Array.isArray(dimValue.columns)) {
        const attributes = dimValue.columns.map(col => {
          return (
            <span className={'badge node_type__metric white_badge'}>{col}</span>
          );
        });
        return (
          <div className={'custom-node-subheader node_type__' + data.type}>
            <div className="custom-node-port">
              {dimValue.dimension}{' '}
              <div className={'badge node_type__metric'}>{dimValue.path}</div>
            </div>
            <div className={'dimension_attributes'}>{attributes}</div>
          </div>
        );
      }
      return <></>;
    });

  return (
    <>
      <div className="dj-node__full" style={renderBasedOnDJNodeType(data.type)}>
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
          {data.type !== 'metric' ? (
            columnsRenderer(data)
          ) : (
            <Collapse
              collapsed={true}
              text={data.type !== 'metric' ? 'columns' : 'dimensions'}
            >
              {dimensions.length <= 0
                ? ''
                : dimensionsRenderer(
                    groupedDimensions(dimensionsToObject(dimensions)),
                  )}
            </Collapse>
          )}
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
