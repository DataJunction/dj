import React from 'react';
import { DJNodeDimensions } from './DJNodeDimensions';
import { DJNodeColumns } from './DJNodeColumns';

export default function Collapse({ collapsed, text, data }) {
  const [isCollapsed, setIsCollapsed] = React.useState(collapsed);

  const limit = 5;
  return (
    <>
      <div className="collapse">
        {data.type === 'metric' ? (
          <button
            className="collapse-button"
            onClick={() => setIsCollapsed(!isCollapsed)}
          >
            {isCollapsed ? '\u25B6 Show' : '\u25BC Hide'} {text}
          </button>
        ) : (
          ''
        )}
        <div
          className={`collapse-content ${
            isCollapsed && data.type === 'metric' ? 'collapsed' : 'expanded'
          }`}
          aria-expanded={isCollapsed}
        >
          {data.type !== 'metric'
            ? isCollapsed
              ? DJNodeColumns({ data: data, limit: limit })
              : DJNodeColumns({ data: data, limit: 100 })
            : DJNodeDimensions(data)}
        </div>
        {data.type !== 'metric' && data.column_names.length > limit ? (
          <button
            className="collapse-button"
            onClick={() => setIsCollapsed(!isCollapsed)}
          >
            {isCollapsed ? '\u25B6 More' : '\u25BC Less'} {text}
          </button>
        ) : (
          ''
        )}
      </div>
    </>
  );
}
