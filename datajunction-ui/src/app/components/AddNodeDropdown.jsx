export default function AddNodeDropdown({ namespace }) {
  return (
    <span className="menu-link" style={{ margin: '0.5em 0 0 1em', width: '130px' }}>
      <span className="menu-title">
        <div className="dropdown">
          <span className="add_node">+ Add Node</span>
          <div className="dropdown-content">
            <a href={`/create/source`}>
              <div className="node_type__source node_type_creation_heading">
                Register Table
              </div>
            </a>
            <a href={`/create/transform/${namespace}`}>
              <div className="node_type__transform node_type_creation_heading">
                Transform
              </div>
            </a>
            <a href={`/create/metric/${namespace}`}>
              <div className="node_type__metric node_type_creation_heading">
                Metric
              </div>
            </a>
            <a href={`/create/dimension/${namespace}`}>
              <div className="node_type__dimension node_type_creation_heading">
                Dimension
              </div>
            </a>
            <a href={`/create/tag`}>
              <div className="entity__tag node_type_creation_heading">Tag</div>
            </a>
            <a href={`/create/cube/${namespace}`}>
              <div className="node_type__cube node_type_creation_heading">
                Cube
              </div>
            </a>
          </div>
        </div>
      </span>
    </span>
  );
}
