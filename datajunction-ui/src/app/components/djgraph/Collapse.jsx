import React from 'react';

export default function Collapse({ collapsed, text, children }) {
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
}
