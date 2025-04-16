const JupyterExportIcon = props => (
  <svg
    className="feather feather-jupyter-export"
    fill="none"
    height="24"
    width="24"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    xmlns="http://www.w3.org/2000/svg"
    {...props}
  >
    {/* Notebook outline */}
    <rect x="3" y="2" width="14" height="20" rx="2" ry="2" />
    
    {/* Notebook lines */}
    <line x1="7" y1="6" x2="13" y2="6" />
    <line x1="7" y1="10" x2="13" y2="10" />
    <line x1="7" y1="14" x2="13" y2="14" />
  </svg>
);

export default JupyterExportIcon;
