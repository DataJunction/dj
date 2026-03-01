import * as React from 'react';
import { Link } from 'react-router-dom';
import { formatRelativeTime } from '../../utils/date';
import DashboardCard, {
  DashboardCardEmpty,
} from '../../components/DashboardCard';
import { NodeBadge } from '../../components/NodeComponents';

// Notifications Section - matches NotificationBell dropdown styling
export function NotificationsSection({ notifications, username, loading }) {
  // Group notifications by node
  const groupedByNode = {};
  notifications.forEach(entry => {
    if (!groupedByNode[entry.entity_name]) {
      groupedByNode[entry.entity_name] = [];
    }
    groupedByNode[entry.entity_name].push(entry);
  });

  // Convert to array and sort by most recent activity
  const grouped = Object.entries(groupedByNode)
    .map(([nodeName, entries]) => ({
      nodeName,
      entries,
      mostRecent: entries[0], // Assuming already sorted by date
      count: entries.length,
    }))
    .slice(0, 15);

  const notificationsList = grouped.map(
    ({ nodeName, entries, mostRecent, count }) => {
      const version = mostRecent.details?.version;
      const href = version
        ? `/nodes/${mostRecent.entity_name}/revisions/${version}`
        : `/nodes/${mostRecent.entity_name}/history`;

      // Get unique users who made updates
      const allUsers = entries
        .map(e => e.user)
        .filter(u => u != null && u !== '');
      const users = [...new Set(allUsers)];

      // If no users found, fall back to mostRecent.user
      const userText =
        users.length === 0
          ? mostRecent.user === username
            ? 'you'
            : mostRecent.user?.split('@')[0] || 'unknown'
          : users.length === 1
          ? users[0] === username
            ? 'you'
            : users[0]?.split('@')[0]
          : users.includes(username)
          ? `you + ${users.length - 1} other${users.length - 1 > 1 ? 's' : ''}`
          : `${users.length} users`;

      return (
        <div
          key={nodeName}
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '0.5rem',
            padding: '0.5rem 0',
            borderBottom: '1px solid var(--border-color, #eee)',
            fontSize: '12px',
          }}
        >
          <a
            href={href}
            style={{
              fontSize: '13px',
              fontWeight: '500',
              textDecoration: 'none',
              flexShrink: 0,
            }}
          >
            {mostRecent.display_name || mostRecent.entity_name.split('.').pop()}
          </a>
          {mostRecent.node_type && (
            <NodeBadge
              type={mostRecent.node_type.toUpperCase()}
              size="medium"
            />
          )}
          <span style={{ color: '#888', whiteSpace: 'nowrap' }}>
            {count > 1
              ? `${count} updates`
              : mostRecent.activity_type?.charAt(0).toUpperCase() +
                mostRecent.activity_type?.slice(1) +
                'd'}{' '}
            by {userText}
            {' · '}
            {formatRelativeTime(mostRecent.created_at)}
          </span>
        </div>
      );
    },
  );

  return (
    <DashboardCard
      title="Notifications"
      actionLink="/notifications"
      loading={loading}
      cardStyle={{ padding: '0', maxHeight: '300px', overflowY: 'auto' }}
      emptyState={
        <DashboardCardEmpty
          icon="🔔"
          message="No notifications yet."
          action={
            <p style={{ fontSize: '11px', marginTop: '0.5rem' }}>
              Watch nodes to get notified of changes.
            </p>
          }
        />
      }
    >
      {notifications.length > 0 && (
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            gap: '0.5rem',
            padding: '0.5rem',
          }}
        >
          {notificationsList}
        </div>
      )}
    </DashboardCard>
  );
}
