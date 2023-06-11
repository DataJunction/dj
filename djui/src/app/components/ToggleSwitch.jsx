import React from 'react';

const ToggleSwitch = ({ checked, onChange, toggleName }) => (
  <div>
    <input
      id="show-compiled-sql-toggle"
      type="checkbox"
      className="checkbox"
      checked={checked}
      onChange={e => onChange(e.target.checked)}
    />
    <label htmlFor="show-compiled-sql-toggle" className="switch"></label>{' '}
    {toggleName}
  </div>
);

export default ToggleSwitch;
