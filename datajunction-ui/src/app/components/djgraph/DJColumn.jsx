import React, { memo } from 'react';
import { Handle, Position } from 'reactflow';

export function DJColumn({ id, data }) {
  const nodeClass = 'node_type__' + data.type;
  return (
    <>
      <div className={nodeClass + ' larger'}>
        <Handle
          type="target"
          position={Position.Left}
          style={{ backgroundColor: '#ccc' }}
        />
        {data.label}
        <Handle
          type="source"
          position={Position.Right}
          style={{ backgroundColor: '#ccc' }}
          isConnectable={true}
        />
      </div>
    </>
  );
}

export default memo(DJColumn);
