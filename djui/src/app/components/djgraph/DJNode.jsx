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
  const columnsRenderer = data =>
    data.column_names.map(col => (
      <tr>
        <td>
          {data.primary_key.includes(col.name) ? (
            <b>{col.name} (PK)</b>
          ) : (
            <>{col.name}</>
          )}
        </td>
        <td style={{ textAlign: 'right' }}>{col.type}</td>
      </tr>
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

  const dimensionsRenderer = dimensions =>
    dimensions.map(dim => (
      <tr>
        <a href={`/nodes/${dim.substring(0, dim.lastIndexOf('.'))}`}>
          <td>{dim}</td>
        </a>
      </tr>
    ));

  return (
    <>
      <Handle
        type="target"
        position={Position.Left}
        style={{ backgroundColor: '#ccc' }}
      />
      <div className="dj-node__full" style={renderBasedOnDJNodeType(data.type)}>
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
          <Collapse
            collapsed={true}
            text={data.type !== 'metric' ? 'columns' : 'dimensions'}
          >
            <div className="dj-node__metadata">
              {data.type !== 'metric'
                ? columnsRenderer(data)
                : dimensionsRenderer(dimensions)}
            </div>
          </Collapse>
        </div>
      </div>
      <Handle
        type="source"
        position={Position.Right}
        style={{ backgroundColor: '#ccc' }}
      />
    </>
  );
}

export default memo(DJNode);
