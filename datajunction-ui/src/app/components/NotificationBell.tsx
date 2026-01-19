import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../providers/djclient';
import { useCurrentUser } from '../providers/UserProvider';
import NotificationIcon from '../icons/NotificationIcon';
import SettingsIcon from '../icons/SettingsIcon';
import LoadingIcon from '../icons/LoadingIcon';
import { formatRelativeTime } from '../utils/date';

interface HistoryEntry {
  id: number;
  entity_type: string;
  entity_name: string;
  node: string;
  activity_type: string;
  user: string;
  created_at: string;
  details?: {
    version?: string;
    [key: string]: any;
  };
}

interface EnrichedHistoryEntry extends HistoryEntry {
  node_type?: string;
  display_name?: string;
}

interface NodeInfo {
  name: string;
  type: string;
  current?: {
    displayName?: string | null;
  };
}

// Calculate unread count based on last_viewed_notifications_at
const calculateUnreadCount = (
  notifs: HistoryEntry[],
  lastViewed: string | null | undefined,
): number => {
  if (!lastViewed) return notifs.length;
  const lastViewedDate = new Date(lastViewed);
  return notifs.filter(n => new Date(n.created_at) > lastViewedDate).length;
};

// Enrich history entries with node info from GraphQL
const enrichWithNodeInfo = (
  entries: HistoryEntry[],
  nodes: NodeInfo[],
): EnrichedHistoryEntry[] => {
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

interface NotificationBellProps {
  onDropdownToggle?: (isOpen: boolean) => void;
  forceClose?: boolean;
}

export default function NotificationBell({
  onDropdownToggle,
  forceClose,
}: NotificationBellProps) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const { currentUser, loading: userLoading } = useCurrentUser();
  const [showDropdown, setShowDropdown] = useState(false);

  // Close when forceClose becomes true
  useEffect(() => {
    if (forceClose && showDropdown) {
      setShowDropdown(false);
    }
  }, [forceClose, showDropdown]);
  const [notifications, setNotifications] = useState<EnrichedHistoryEntry[]>(
    [],
  );
  const [loading, setLoading] = useState(false);
  const [unreadCount, setUnreadCount] = useState(0);

  // Fetch notifications when user data is available
  useEffect(() => {
    if (userLoading) return;

    async function fetchNotifications() {
      setLoading(true);
      try {
        const history: HistoryEntry[] =
          (await djClient.getSubscribedHistory(5)) || [];

        // Get unique entity names and fetch their info via GraphQL
        // (some may not be nodes, but GraphQL will just not return them)
        const nodeNames = Array.from(new Set(history.map(h => h.entity_name)));
        const nodes: NodeInfo[] = nodeNames.length
          ? await djClient.getNodesByNames(nodeNames)
          : [];

        const enriched = enrichWithNodeInfo(history, nodes);
        setNotifications(enriched);
        setUnreadCount(
          calculateUnreadCount(
            history,
            currentUser?.last_viewed_notifications_at,
          ),
        );
      } catch (error) {
        console.error('Error fetching notifications:', error);
      } finally {
        setLoading(false);
      }
    }
    fetchNotifications();
  }, [djClient, currentUser, userLoading]);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      const target = event.target as HTMLElement;
      if (!target.closest('.notification-bell-dropdown')) {
        setShowDropdown(false);
        onDropdownToggle?.(false);
      }
    };
    document.addEventListener('click', handleClickOutside);
    return () => document.removeEventListener('click', handleClickOutside);
  }, [onDropdownToggle]);

  const handleToggle = (e: React.MouseEvent) => {
    e.stopPropagation();
    const willOpen = !showDropdown;

    // Mark as read when opening
    if (willOpen && unreadCount > 0) {
      djClient.markNotificationsRead();
      setUnreadCount(0);
    }

    setShowDropdown(willOpen);
    onDropdownToggle?.(willOpen);
  };

  return (
    <div className="nav-dropdown notification-bell-dropdown">
      <button className="nav-icon-button" onClick={handleToggle}>
        <NotificationIcon />
        {unreadCount > 0 && (
          <span className="notification-badge">{unreadCount}</span>
        )}
      </button>
      {showDropdown && (
        <div className="nav-dropdown-menu notifications-menu">
          <div className="dropdown-header">
            <span className="header-left">
              <NotificationIcon /> Updates
            </span>
            <a
              href="/settings#notifications"
              className="header-settings"
              title="Manage subscriptions"
            >
              <SettingsIcon />
            </a>
          </div>
          <div className="notifications-list">
            {loading ? (
              <div className="dropdown-item">
                <LoadingIcon centered={false} />
              </div>
            ) : notifications.length === 0 ? (
              <div className="dropdown-item text-muted">
                No updates on watched nodes
              </div>
            ) : (
              notifications.slice(0, 5).map(entry => {
                const version = entry.details?.version;
                const href = version
                  ? `/nodes/${entry.entity_name}/revisions/${version}`
                  : `/nodes/${entry.entity_name}/history`;
                return (
                  <a key={entry.id} className="notification-item" href={href}>
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
              })
            )}
          </div>
          {notifications.length > 0 && (
            <>
              <hr className="dropdown-divider" />
              <a className="dropdown-item view-all" href="/notifications">
                View all
              </a>
            </>
          )}
        </div>
      )}
    </div>
  );
}
