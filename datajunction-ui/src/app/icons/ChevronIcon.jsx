import React from 'react';

// A clean chevron that points right when collapsed and rotates down when open.
// Shared by the rail folder tree and the rail's collapsible group headings so
// expand/collapse looks the same everywhere. Color comes from `currentColor`.
export default function ChevronIcon({ open = false, size = 12 }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="3"
      strokeLinecap="round"
      strokeLinejoin="round"
      style={{
        transform: open ? 'rotate(90deg)' : 'none',
        transition: 'transform 0.12s ease',
      }}
    >
      <polyline points="9 6 15 12 9 18" />
    </svg>
  );
}
