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
    <label for="show-compiled-sql-toggle" class="switch"></label> {toggleName}
  </div>
);

export default ToggleSwitch;
