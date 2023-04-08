import React, { memo } from 'react';
import { Handle, Position } from 'reactflow';

function renderBasedOnDJNodeType(param) {
  switch(param) {
    case 'source':
      return {"backgroundColor": "#7EB46150", "color": "#7EB461"};
    case 'transform':
      return {"backgroundColor": "#6DAAA750", "color": "#6DAAA7"};
    case 'dimension':
      return {"backgroundColor": "#CF7D2950", "color": "#CF7D29"};
    case 'metric':
      return {"backgroundColor": "#A27E8650", "color": "#A27E86"};
    default:
      return {};
  }
}


function DJNode({ id, data }) {
  return (
    <>
        <Handle type="target" position={Position.Top} style={{"backgroundColor": "#ccc"}}/>
        <div className="dj-node__full" style={renderBasedOnDJNodeType(data.type)}>
          <div className="dj-node__header">
            {data.type}
          </div>
          <div className="dj-node__body">
            {data.label}
          </div>
        </div>
        <Handle type="source" position={Position.Bottom} style={{"backgroundColor": "#ccc"}}/>
    </>
  );
}

export default memo(DJNode);
