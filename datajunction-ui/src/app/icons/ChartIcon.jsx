const ChartIcon = ({ size = 16, ...props }) => (
  <svg
    width={size}
    height={size}
    viewBox="0 0 24 24"
    fill="currentColor"
    stroke="none"
    {...props}
  >
    {/* Bars */}
    <rect x="4" y="11" width="5" height="8" rx="1" />
    <rect x="10" y="5" width="5" height="14" rx="1" />
    <rect x="16" y="8" width="5" height="11" rx="1" />
    {/* X axis */}
    <line x1="2" y1="21" x2="23" y2="21" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" opacity="0.35" />
  </svg>
);

export default ChartIcon;
