import { useContext } from 'react';
import { useNavigate } from 'react-router-dom';
import { useCurrentUser } from '../providers/UserProvider';
import DJClientContext from '../providers/djclient';

const PERSONAL_NS_PREFIX =
  process.env.REACT_APP_PERSONAL_NAMESPACE_PREFIX || 'users';

function resolvePersonalNamespace(namespace, username) {
  if (namespace && namespace !== 'default') return namespace;
  if (!username) return 'default';
  const handle = username
    .split('@')[0]
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_');
  return `${PERSONAL_NS_PREFIX}.${handle}`;
}

export default function AddNodeDropdown({ namespace }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const { currentUser } = useCurrentUser();
  const navigate = useNavigate();
  const ns = resolvePersonalNamespace(namespace, currentUser?.username);
  const isPersonalFallback =
    (!namespace || namespace === 'default') && currentUser?.username;

  const goTo = path => async e => {
    e.preventDefault();
    if (isPersonalFallback) {
      // Create the personal namespace if it doesn't exist yet (idempotent on the server)
      try {
        await djClient.addNamespace(ns);
      } catch (err) {
        console.error('Failed to ensure namespace exists:', err);
      }
    }
    navigate(path);
  };

  return (
    <span
      className="menu-link"
      style={{ margin: '0.5em 0 0 1em', width: '130px' }}
    >
      <span className="menu-title">
        <div className="dropdown">
          <span className="add_node">+ Add Node</span>
          <div className="dropdown-content">
            <a href={`/create/source`}>
              <div className="node_type__source node_type_creation_heading">
                Register Table
              </div>
            </a>
            <a
              href={`/create/transform/${ns}`}
              onClick={goTo(`/create/transform/${ns}`)}
            >
              <div className="node_type__transform node_type_creation_heading">
                Transform
              </div>
            </a>
            <a
              href={`/create/metric/${ns}`}
              onClick={goTo(`/create/metric/${ns}`)}
            >
              <div className="node_type__metric node_type_creation_heading">
                Metric
              </div>
            </a>
            <a
              href={`/create/dimension/${ns}`}
              onClick={goTo(`/create/dimension/${ns}`)}
            >
              <div className="node_type__dimension node_type_creation_heading">
                Dimension
              </div>
            </a>
            <a href={`/create/tag`}>
              <div className="entity__tag node_type_creation_heading">Tag</div>
            </a>
            <a href={`/create/cube/${ns}`} onClick={goTo(`/create/cube/${ns}`)}>
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
