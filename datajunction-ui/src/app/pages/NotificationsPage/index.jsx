import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import LoadingIcon from '../../icons/LoadingIcon';
import { formatRelativeTime, groupByDate } from '../../utils/date';

// Enrich history entries with node info from GraphQL
const enrichWithNodeInfo = (entries, nodes) => {
  const nodeMap = new Map(nodes.map(n => [n.name, n]));
  return entries.map(entry => {
    const node = nodeMap.get(entry.entity_name);
    return {
      ...entry,
      node_type: node?.type,
      display_name: node?.current?.displayName,
    };
  });
};

export function NotificationsPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [notifications, setNotifications] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchNotifications() {
      try {
        const history = (await djClient.getSubscribedHistory(50)) || [];

        // Get unique entity names and fetch their info via GraphQL
        const nodeNames = Array.from(new Set(history.map(h => h.entity_name)));
        const nodes = nodeNames.length
          ? await djClient.getNodesByNames(nodeNames)
          : [];

        const enriched = enrichWithNodeInfo(history, nodes);
        setNotifications(enriched);
      } catch (error) {
        console.error('Error fetching notifications:', error);
      } finally {
        setLoading(false);
      }
    }
    fetchNotifications();
  }, [djClient]);

  const groupedNotifications = groupByDate(notifications);

  return (
    <div className="mid">
      <div className="card">
        <div className="card-header">
          <h2>Notifications</h2>
        </div>
        <div className="card-body">
          <div className="notifications-list">
            {loading ? (
              <div style={{ padding: '2rem', textAlign: 'center' }}>
                <LoadingIcon />
              </div>
            ) : notifications.length === 0 ? (
              <div
                style={{
                  padding: '2rem 1rem',
                  color: '#666',
                  textAlign: 'center',
                }}
              >
                No notifications yet. Watch nodes to receive updates when they
                change.
              </div>
            ) : (
              groupedNotifications.map(group => (
                <div key={group.label} className="notification-group">
                  <div
                    style={{
                      padding: '0.5rem 0.75rem',
                      backgroundColor: '#f8f9fa',
                      borderBottom: '1px solid #eee',
                      fontSize: '12px',
                      fontWeight: 600,
                      color: '#666',
                      textTransform: 'uppercase',
                      letterSpacing: '0.5px',
                    }}
                  >
                    {group.label}
                  </div>
                  {group.items.map(entry => {
                    const version = entry.details?.version;
                    const href = version
                      ? `/nodes/${entry.entity_name}/revisions/${version}`
                      : `/nodes/${entry.entity_name}/history`;

                    return (
                      <a
                        key={entry.id}
                        className="notification-item"
                        href={href}
                      >
                        <span className="notification-node">
                          <span className="notification-title">
                            {entry.display_name || entry.entity_name}
                            {version && (
                              <span className="badge version">{version}</span>
                            )}
                          </span>
                          {entry.display_name && (
                            <span className="notification-entity">
                              {entry.entity_name}
                            </span>
                          )}
                        </span>
                        <span className="notification-meta">
                          {entry.node_type && (
                            <span
                              className={`node_type__${entry.node_type} badge node_type`}
                            >
                              {entry.node_type.toUpperCase()}
                            </span>
                          )}
                          {entry.activity_type}d by{' '}
                          <span style={{ color: '#333' }}>{entry.user}</span> ·{' '}
                          {formatRelativeTime(entry.created_at)}
                        </span>
                      </a>
                    );
                  })}
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
