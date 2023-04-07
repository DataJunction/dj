import React, { memo } from 'react';
import { Handle, Position } from 'reactflow';

function renderBasedOnDJNodeType(param) {
  switch(param) {
    case 'source':
      return {"background-color": "#7EB46150", "color": "#7EB461"};
    case 'transform':
      return {"background-color": "#6DAAA750", "color": "#6DAAA7"};
    case 'dimension':
      return {"background-color": "#CF7D2950", "color": "#CF7D29"};
    case 'metric':
      return {"background-color": "#A27E8650", "color": "#A27E86"};
    default:
      return 'foo';
  }
}


function DJNode({ id, data }) {
  return (
    <>
        <Handle type="target" position={Position.Top} style={{"background-color": "#ccc"}}/>
        <div className="custom-node__full" style={renderBasedOnDJNodeType(data.type)}>
          <div className="custom-node__header">
            {data.type}
          </div>
          <div className="custom-node__body">
            {data.label}
          </div>
        </div>
        <Handle type="source" position={Position.Bottom} style={{"background-color": "#ccc"}}/>
    </>
  );
}

export default memo(DJNode);
