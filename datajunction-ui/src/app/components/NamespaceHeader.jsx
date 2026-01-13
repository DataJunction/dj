import { useContext, useEffect, useState } from 'react';
import HorizontalHierarchyIcon from '../icons/HorizontalHierarchyIcon';
import DJClientContext from '../providers/djclient';

export default function NamespaceHeader({ namespace }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [sources, setSources] = useState(null);

  useEffect(() => {
    const fetchSources = async () => {
      if (namespace) {
        try {
          const data = await djClient.namespaceSources(namespace);
          setSources(data);
        } catch (e) {
          // Silently fail - badge just won't show
        }
      }
    };
    fetchSources();
  }, [djClient, namespace]);

  const namespaceParts = namespace ? namespace.split('.') : [];
  const namespaceList = namespaceParts.map((piece, index) => {
    return (
      <li className="breadcrumb-item" key={index}>
        <a
          className="link-body-emphasis"
          href={'/namespaces/' + namespaceParts.slice(0, index + 1).join('.')}
        >
          {piece}
        </a>
      </li>
    );
  });

  // Render source badge
  const renderSourceBadge = () => {
    if (!sources || sources.total_deployments === 0) {
      return null;
    }

    const isGit = sources.primary_source?.type === 'git';

    return (
      <li
        className="breadcrumb-item"
        style={{ display: 'flex', alignItems: 'center' }}
      >
        <span
          title={
            isGit
              ? `CI-managed: ${sources.primary_source.repository}${
                  sources.primary_source.branch
                    ? ` (${sources.primary_source.branch})`
                    : ''
                }`
              : 'Local/adhoc deployment'
          }
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: '4px',
            padding: '2px 8px',
            fontSize: '11px',
            borderRadius: '12px',
            backgroundColor: isGit ? '#d4edda' : '#e2e3e5',
            color: isGit ? '#155724' : '#383d41',
            cursor: 'help',
          }}
        >
          {isGit ? '🔗' : '📁'}
          {isGit ? 'CI' : 'Local'}
        </span>
      </li>
    );
  };

  return (
    <ol className="breadcrumb breadcrumb-chevron p-3 bg-body-tertiary rounded-3">
      <li className="breadcrumb-item">
        <a href="/">
          <HorizontalHierarchyIcon />
        </a>
      </li>
      {namespaceList}
      {renderSourceBadge()}
    </ol>
  );
}
