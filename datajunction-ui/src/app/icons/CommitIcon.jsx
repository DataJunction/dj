import * as React from 'react';

const CommitIcon = props => (
  <svg
    width="2em"
    height="2em"
    viewBox="0 0 256 256"
    xmlns="http://www.w3.org/2000/svg"
  >
    <rect fill="none" height="256" width="256" />
    <circle
      cx="128"
      cy="128"
      fill="none"
      r="52"
      stroke="#000"
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth="12"
    />
    <line
      fill="none"
      stroke="#000"
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth="12"
      x1="8"
      x2="76"
      y1="128"
      y2="128"
    />
    <line
      fill="none"
      stroke="#000"
      strokeLinecap="round"
      strokeLinejoin="round"
      strokeWidth="12"
      x1="180"
      x2="248"
      y1="128"
      y2="128"
    />
  </svg>
);
export default CommitIcon;
