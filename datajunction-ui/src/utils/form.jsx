import ValidIcon from '../app/icons/ValidIcon';
import AlertIcon from '../app/icons/AlertIcon';
import * as React from 'react';

export const displayMessageAfterSubmit = status => {
  return status?.success !== undefined ? (
    <div className="message success" data-testid="success">
      <ValidIcon />
      {status?.success}
    </div>
  ) : status?.failure !== undefined ? (
    <div className="message alert" data-testid="failure">
      <AlertIcon />
      {status?.failure}
    </div>
  ) : (
    ''
  );
};

export const labelize = name => {
  return name.replace(/_/g, ' ').replace(/\b(\w)/g, char => char.toUpperCase());
};
