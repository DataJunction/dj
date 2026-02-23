import * as React from 'react';
import { useContext, useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import DJClientContext from '../../providers/djclient';
import NodeStatus from '../NodePage/NodeStatus';
import NodeListActions from '../../components/NodeListActions';
import LoadingIcon from '../../icons/LoadingIcon';
import { formatRelativeTime } from '../../utils/date';
import DashboardCard, {
  DashboardCardEmpty,
} from '../../components/DashboardCard';
import {
  NodeBadge,
  NodeLink,
  NodeDisplay,
  NodeChip,
} from '../../components/NodeComponents';
import {
  useCurrentUser,
  useWorkspaceOwnedNodes,
  useWorkspaceRecentlyEdited,
  useWorkspaceWatchedNodes,
  useWorkspaceCollections,
  useWorkspaceNotifications,
  useWorkspaceMaterializations,
  useWorkspaceNeedsAttention,
  usePersonalNamespace,
} from '../../hooks/useWorkspaceData';

import 'styles/settings.css';

export function MyWorkspacePage() {
  // Use custom hooks for all data fetching
  const { data: currentUser, loading: userLoading } = useCurrentUser();
  const { data: ownedNodes, loading: ownedLoading } = useWorkspaceOwnedNodes(
    currentUser?.username,
  );
  const { data: recentlyEdited, loading: editedLoading } =
    useWorkspaceRecentlyEdited(currentUser?.username);
  const { data: watchedNodes, loading: watchedLoading } =
    useWorkspaceWatchedNodes(currentUser?.username);
  const { data: collections, loading: collectionsLoading } =
    useWorkspaceCollections(currentUser?.username);
  const { data: notifications, loading: notificationsLoading } =
    useWorkspaceNotifications(currentUser?.username);
  const { data: materializedNodes, loading: materializationsLoading } =
    useWorkspaceMaterializations(currentUser?.username);
  const { data: needsAttentionData, loading: needsAttentionLoading } =
    useWorkspaceNeedsAttention(currentUser?.username);
  const { exists: hasPersonalNamespace, loading: namespaceLoading } =
    usePersonalNamespace(currentUser?.username);

  // Extract needs attention data
  const {
    nodesMissingDescription = [],
    invalidNodes = [],
    staleDrafts = [],
    orphanedDimensions = [],
  } = needsAttentionData || {};

  // Combine loading states for "My Nodes" section
  const myNodesLoading = ownedLoading || editedLoading || watchedLoading;

  // Filter stale materializations (> 72 hours old)
  const staleMaterializations = materializedNodes.filter(node => {
    const validThroughTs = node.current?.availability?.validThroughTs;
    if (!validThroughTs) return false; // Pending ones aren't "stale"
    const hoursSinceUpdate = (Date.now() - validThroughTs) / (1000 * 60 * 60);
    return hoursSinceUpdate > 72;
  });

  const hasActionableItems =
    nodesMissingDescription.length > 0 ||
    invalidNodes.length > 0 ||
    staleDrafts.length > 0 ||
    staleMaterializations.length > 0 ||
    orphanedDimensions.length > 0;

  // Personal namespace for the user
  const usernameForNamespace = currentUser?.username?.split('@')[0] || '';
  const personalNamespace = `users.${usernameForNamespace}`;

  if (userLoading) {
    return (
      <div className="settings-page" style={{ padding: '1.5rem 2rem' }}>
        <h1 className="settings-title">Dashboard</h1>
        <div style={{ textAlign: 'center', padding: '3rem' }}>
          <LoadingIcon />
        </div>
      </div>
    );
  }

  // Calculate stats
  return (
    <div className="settings-page" style={{ padding: '1.5rem 2rem' }}>
      {/* Two Column Layout: Collections/Organization (left) + Activity (right) */}
      <div
        style={{
          display: 'flex',
          gap: '1.5rem',
          alignItems: 'stretch',
        }}
      >
        {/* Left Column: Organization (65%) */}
        <div
          style={{
            width: 'calc(65% - 0.75rem)',
            display: 'flex',
            flexDirection: 'column',
            gap: '1.5rem',
          }}
        >
          {/* Collections (My + Featured) */}
          <CollectionsSection
            collections={collections}
            loading={collectionsLoading}
            currentUser={currentUser}
          />

          {/* My Nodes */}
          <MyNodesSection
            ownedNodes={ownedNodes}
            watchedNodes={watchedNodes}
            recentlyEdited={recentlyEdited}
            username={currentUser?.username}
            loading={myNodesLoading}
          />
        </div>

        {/* Right Column: Activity (35%) */}
        <div
          style={{
            width: 'calc(35% - 0.75rem)',
            display: 'flex',
            flexDirection: 'column',
            gap: '1.5rem',
          }}
        >
          {/* Notifications */}
          <NotificationsSection
            notifications={notifications}
            username={currentUser?.username}
            loading={notificationsLoading}
          />

          {/* Active Branches */}
          <ActiveBranchesSection
            ownedNodes={ownedNodes}
            recentlyEdited={recentlyEdited}
            loading={myNodesLoading}
          />

          {/* Materializations */}
          <MaterializationsSection
            nodes={materializedNodes}
            loading={materializationsLoading}
          />

          {/* Needs Attention */}
          <NeedsAttentionSection
            nodesMissingDescription={nodesMissingDescription}
            invalidNodes={invalidNodes}
            staleDrafts={staleDrafts}
            staleMaterializations={staleMaterializations}
            orphanedDimensions={orphanedDimensions}
            username={currentUser?.username}
            hasItems={hasActionableItems}
            loading={needsAttentionLoading || materializationsLoading}
            personalNamespace={personalNamespace}
            hasPersonalNamespace={hasPersonalNamespace}
            namespaceLoading={namespaceLoading}
          />
        </div>
      </div>
    </div>
  );
}

// Notifications Section - matches NotificationBell dropdown styling
function NotificationsSection({ notifications, username, loading }) {
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

// Needs Attention Section with single-line categories
function NeedsAttentionSection({
  nodesMissingDescription,
  invalidNodes,
  staleDrafts,
  staleMaterializations,
  orphanedDimensions,
  username,
  hasItems,
  loading,
  personalNamespace,
  hasPersonalNamespace,
  namespaceLoading,
}) {
  const categories = [
    {
      id: 'invalid',
      icon: '❌',
      label: 'Invalid',
      nodes: invalidNodes,
      viewAllLink: `/?ownedBy=${username}&statuses=INVALID`,
    },
    {
      id: 'stale-drafts',
      icon: '⏰',
      label: 'Stale Drafts',
      nodes: staleDrafts,
      viewAllLink: `/?ownedBy=${username}&mode=DRAFT`,
    },
    {
      id: 'stale-materializations',
      icon: '📦',
      label: 'Stale Materializations',
      nodes: staleMaterializations,
      viewAllLink: `/?ownedBy=${username}&hasMaterialization=true`,
    },
    {
      id: 'no-description',
      icon: '📝',
      label: 'No Description',
      nodes: nodesMissingDescription,
      viewAllLink: `/?ownedBy=${username}&missingDescription=true`,
    },
    {
      id: 'orphaned-dimensions',
      icon: '🔗',
      label: 'Orphaned Dimensions',
      nodes: orphanedDimensions,
      viewAllLink: `/?ownedBy=${username}&orphanedDimension=true`,
    },
  ];

  const categoriesList = categories.map(cat => (
    <div
      key={cat.id}
      className="settings-card"
      style={{ padding: '0.5rem 0.75rem', minWidth: 0 }}
    >
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: '0.3rem',
        }}
      >
        <span style={{ fontSize: '11px', fontWeight: '600', color: '#555' }}>
          {cat.icon} {cat.label}
          <span
            style={{
              color: cat.nodes.length > 0 ? '#dc3545' : '#666',
              marginLeft: '4px',
            }}
          >
            ({cat.nodes.length})
          </span>
        </span>
        {cat.nodes.length > 0 && (
          <Link to={cat.viewAllLink} style={{ fontSize: '10px' }}>
            View all →
          </Link>
        )}
      </div>
      {cat.nodes.length > 0 ? (
        <div style={{ display: 'flex', gap: '0.3rem', overflow: 'hidden' }}>
          {cat.nodes.slice(0, 10).map(node => (
            <NodeChip key={node.name} node={node} />
          ))}
        </div>
      ) : (
        <div style={{ fontSize: '10px', color: '#28a745' }}>✓ All good!</div>
      )}
    </div>
  ));

  return (
    <section style={{ minWidth: 0, width: '100%' }}>
      <h2 className="settings-section-title">Needs Attention</h2>
      <div style={{ maxHeight: '300px', overflowY: 'auto' }}>
        {loading ? (
          <div
            className="settings-card"
            style={{ textAlign: 'center', padding: '1rem' }}
          >
            <LoadingIcon />
          </div>
        ) : (
          <div
            style={{
              display: 'flex',
              flexDirection: 'column',
              width: '100%',
              gap: '0.5rem',
            }}
          >
            {categoriesList}
            {/* Personal namespace prompt if missing */}
            {!namespaceLoading && !hasPersonalNamespace && (
              <div
                style={{
                  padding: '0.75rem',
                  backgroundColor: 'var(--card-bg, #f8f9fa)',
                  border: '1px dashed var(--border-color, #dee2e6)',
                  borderRadius: '6px',
                  textAlign: 'center',
                }}
              >
                <div style={{ fontSize: '16px', marginBottom: '0.25rem' }}>
                  📁
                </div>
                <div
                  style={{
                    fontSize: '11px',
                    fontWeight: '500',
                    marginBottom: '0.25rem',
                  }}
                >
                  Set up your namespace
                </div>
                <p
                  style={{
                    fontSize: '10px',
                    color: '#666',
                    marginBottom: '0.5rem',
                  }}
                >
                  Create{' '}
                  <code
                    style={{
                      backgroundColor: '#e9ecef',
                      padding: '1px 4px',
                      borderRadius: '3px',
                      fontSize: '9px',
                    }}
                  >
                    {personalNamespace}
                  </code>
                </p>
                <Link
                  to={`/namespaces/${personalNamespace}`}
                  style={{
                    display: 'inline-block',
                    padding: '3px 8px',
                    fontSize: '10px',
                    backgroundColor: '#28a745',
                    color: '#fff',
                    borderRadius: '4px',
                    textDecoration: 'none',
                  }}
                >
                  Create →
                </Link>
              </div>
            )}
          </div>
        )}
      </div>
    </section>
  );
}

// My Nodes Section (owned + watched, with tabs)
function MyNodesSection({
  ownedNodes,
  watchedNodes,
  recentlyEdited,
  username,
  loading,
}) {
  const [activeTab, setActiveTab] = React.useState('owned');

  const ownedNames = new Set(ownedNodes.map(n => n.name));
  const watchedOnly = watchedNodes.filter(n => !ownedNames.has(n.name));

  const allMyNodeNames = new Set([
    ...ownedNames,
    ...watchedNodes.map(n => n.name),
  ]);
  const editedOnly = recentlyEdited.filter(n => !allMyNodeNames.has(n.name));

  const getDisplayNodes = () => {
    switch (activeTab) {
      case 'owned':
        return ownedNodes;
      case 'watched':
        return watchedOnly;
      case 'edited':
        return recentlyEdited;
      default:
        return ownedNodes;
    }
  };
  const displayNodes = getDisplayNodes();

  const hasAnyContent =
    ownedNodes.length > 0 ||
    watchedOnly.length > 0 ||
    recentlyEdited.length > 0;
  const maxDisplay = 8;

  return (
    <DashboardCard
      title="My Nodes"
      actionLink={`/?ownedBy=${username}`}
      loading={loading}
      cardStyle={{
        padding: '0.25rem 0.75rem',
        maxHeight: '400px',
        overflowY: 'auto',
      }}
      emptyState={
        <div style={{ padding: '0.75rem 0' }}>
          <p
            style={{ fontSize: '12px', color: '#666', marginBottom: '0.75rem' }}
          >
            No nodes yet.
          </p>
          <div style={{ display: 'flex', gap: '0.75rem' }}>
            <div
              style={{
                flex: 1,
                padding: '0.75rem',
                backgroundColor: 'var(--card-bg, #f8f9fa)',
                border: '1px dashed var(--border-color, #dee2e6)',
                borderRadius: '6px',
                textAlign: 'center',
              }}
            >
              <div style={{ fontSize: '16px', marginBottom: '0.25rem' }}>
                ➕
              </div>
              <div
                style={{
                  fontSize: '11px',
                  fontWeight: '500',
                  marginBottom: '0.25rem',
                }}
              >
                Create a node
              </div>
              <p
                style={{
                  fontSize: '10px',
                  color: '#666',
                  marginBottom: '0.5rem',
                }}
              >
                Build your data model
              </p>
              <Link
                to="/create/source"
                style={{
                  display: 'inline-block',
                  padding: '3px 8px',
                  fontSize: '10px',
                  backgroundColor: 'var(--primary-color, #4a90d9)',
                  color: '#fff',
                  borderRadius: '4px',
                  textDecoration: 'none',
                }}
              >
                Create →
              </Link>
            </div>
            <div
              style={{
                flex: 1,
                padding: '0.75rem',
                backgroundColor: 'var(--card-bg, #f8f9fa)',
                border: '1px dashed var(--border-color, #dee2e6)',
                borderRadius: '6px',
                textAlign: 'center',
              }}
            >
              <div style={{ fontSize: '16px', marginBottom: '0.25rem' }}>
                👤
              </div>
              <div
                style={{
                  fontSize: '11px',
                  fontWeight: '500',
                  marginBottom: '0.25rem',
                }}
              >
                Claim ownership
              </div>
              <p
                style={{
                  fontSize: '10px',
                  color: '#666',
                  marginBottom: '0.5rem',
                }}
              >
                Add yourself as owner
              </p>
              <Link
                to="/"
                style={{
                  display: 'inline-block',
                  padding: '3px 8px',
                  fontSize: '10px',
                  backgroundColor: '#6c757d',
                  color: '#fff',
                  borderRadius: '4px',
                  textDecoration: 'none',
                }}
              >
                Browse →
              </Link>
            </div>
          </div>
        </div>
      }
    >
      {hasAnyContent && (
        <>
          {/* Tabs */}
          <div
            style={{
              display: 'flex',
              gap: '0.5rem',
              marginBottom: '0.5rem',
              paddingTop: '0.5rem',
            }}
          >
            <button
              onClick={() => setActiveTab('owned')}
              style={{
                padding: '4px 10px',
                fontSize: '11px',
                border: 'none',
                borderRadius: '4px',
                cursor: 'pointer',
                backgroundColor:
                  activeTab === 'owned'
                    ? 'var(--primary-color, #4a90d9)'
                    : '#e9ecef',
                color: activeTab === 'owned' ? '#fff' : '#495057',
              }}
            >
              Owned ({ownedNodes.length})
            </button>
            <button
              onClick={() => setActiveTab('watched')}
              style={{
                padding: '4px 10px',
                fontSize: '11px',
                border: 'none',
                borderRadius: '4px',
                cursor: 'pointer',
                backgroundColor:
                  activeTab === 'watched'
                    ? 'var(--primary-color, #4a90d9)'
                    : '#e9ecef',
                color: activeTab === 'watched' ? '#fff' : '#495057',
              }}
            >
              Watched ({watchedOnly.length})
            </button>
            <button
              onClick={() => setActiveTab('edited')}
              style={{
                padding: '4px 10px',
                fontSize: '11px',
                border: 'none',
                borderRadius: '4px',
                cursor: 'pointer',
                backgroundColor:
                  activeTab === 'edited'
                    ? 'var(--primary-color, #4a90d9)'
                    : '#e9ecef',
                color: activeTab === 'edited' ? '#fff' : '#495057',
              }}
            >
              Recent Edits ({recentlyEdited.length})
            </button>
          </div>
          {displayNodes.length > 0 ? (
            <>
              <NodeList
                nodes={displayNodes.slice(0, maxDisplay)}
                showUpdatedAt={true}
              />
              {displayNodes.length > maxDisplay && (
                <div
                  style={{
                    textAlign: 'center',
                    padding: '0.5rem',
                    fontSize: '12px',
                    color: '#666',
                  }}
                >
                  +{displayNodes.length - maxDisplay} more
                </div>
              )}
            </>
          ) : (
            <div
              style={{
                padding: '1rem',
                textAlign: 'center',
                color: '#666',
                fontSize: '12px',
              }}
            >
              {activeTab === 'owned' && 'No owned nodes'}
              {activeTab === 'watched' && 'No watched nodes'}
              {activeTab === 'edited' && 'No recent edits'}
            </div>
          )}
        </>
      )}
    </DashboardCard>
  );
}

// Collections Section (includes featured + my collections)
function CollectionsSection({ collections, loading, currentUser }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [allCollections, setAllCollections] = useState([]);
  const [allLoading, setAllLoading] = useState(true);

  useEffect(() => {
    const fetchAll = async () => {
      try {
        const response = await djClient.listAllCollections();
        console.log('All collections response:', response);
        const all = response?.data?.listCollections || [];
        setAllCollections(all);
      } catch (error) {
        console.error('Error fetching all collections:', error);
        // Fall back to user's collections if fetching all fails
        setAllCollections(collections);
      }
      setAllLoading(false);
    };
    fetchAll();
  }, [djClient, collections]);

  // Sort: user's collections first, then others
  // If allCollections is empty, fall back to the collections prop
  const collectionsToUse =
    allCollections.length > 0 ? allCollections : collections;
  const myCollections = collectionsToUse.filter(
    c => c.createdBy === currentUser?.username || !c.createdBy,
  );
  const otherCollections = collectionsToUse.filter(
    c => c.createdBy && c.createdBy !== currentUser?.username,
  );
  const allToShow = [...myCollections, ...otherCollections].slice(0, 8);

  const collectionsGrid = allToShow.map(collection => {
    const isOwner = collection.createdBy === currentUser?.username;
    const ownerDisplay = isOwner
      ? 'you'
      : collection.createdBy?.split('@')[0] || 'unknown';

    return (
      <a
        key={collection.name}
        href={`/collections/${collection.name}`}
        style={{
          display: 'flex',
          flexDirection: 'column',
          padding: '1rem',
          border: '1px solid var(--border-color, #e0e0e0)',
          borderRadius: '8px',
          textDecoration: 'none',
          color: 'inherit',
          transition: 'all 0.15s ease',
          backgroundColor: 'var(--card-bg, #fff)',
          cursor: 'pointer',
        }}
        onMouseEnter={e => {
          e.currentTarget.style.borderColor = 'var(--primary-color, #007bff)';
          e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.1)';
          e.currentTarget.style.transform = 'translateY(-2px)';
        }}
        onMouseLeave={e => {
          e.currentTarget.style.borderColor = 'var(--border-color, #e0e0e0)';
          e.currentTarget.style.boxShadow = 'none';
          e.currentTarget.style.transform = 'translateY(0)';
        }}
      >
        <div
          style={{
            fontWeight: '600',
            fontSize: '14px',
            marginBottom: '0.5rem',
            lineHeight: '1.3',
          }}
        >
          {collection.name}
        </div>
        {collection.description && (
          <div
            style={{
              fontSize: '12px',
              color: '#666',
              lineHeight: '1.4',
              marginBottom: '0.75rem',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              display: '-webkit-box',
              WebkitLineClamp: 2,
              WebkitBoxOrient: 'vertical',
              flex: 1,
            }}
          >
            {collection.description}
          </div>
        )}
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            fontSize: '11px',
            color: '#888',
            marginTop: 'auto',
          }}
        >
          <span>
            {collection.nodeCount}{' '}
            {collection.nodeCount === 1 ? 'node' : 'nodes'}
          </span>
          <span
            style={{
              color: isOwner ? 'var(--primary-color, #4a90d9)' : '#888',
            }}
          >
            by {ownerDisplay}
          </span>
        </div>
      </a>
    );
  });

  return (
    <DashboardCard
      title="Collections"
      actionLink="/collections"
      actionText="+ Create"
      loading={loading || allLoading}
      cardStyle={{ padding: '0.75rem', minHeight: '200px' }}
      emptyState={
        <div style={{ padding: '1rem', textAlign: 'center' }}>
          <div
            style={{ fontSize: '48px', marginBottom: '0.5rem', opacity: 0.3 }}
          >
            📁
          </div>
          <p style={{ fontSize: '13px', color: '#666', marginBottom: '1rem' }}>
            No collections yet
          </p>
          <p
            style={{
              fontSize: '11px',
              color: '#999',
              marginBottom: '1rem',
              lineHeight: '1.4',
            }}
          >
            Group related metrics and dimensions together for easier discovery
          </p>
          <Link
            to="/collections"
            style={{
              display: 'inline-block',
              padding: '6px 12px',
              fontSize: '11px',
              backgroundColor: 'var(--primary-color, #4a90d9)',
              color: '#fff',
              borderRadius: '6px',
              textDecoration: 'none',
              fontWeight: '500',
            }}
          >
            + Create Collection
          </Link>
        </div>
      }
    >
      {allToShow.length > 0 && (
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))',
            gap: '0.75rem',
          }}
        >
          {collectionsGrid}
        </div>
      )}
    </DashboardCard>
  );
}

// Materializations Section
function MaterializationsSection({ nodes, loading }) {
  // Ensure nodes is always an array
  const sortedNodes = (nodes || []).slice().sort((a, b) => {
    const aTs = a.current?.availability?.validThroughTs;
    const bTs = b.current?.availability?.validThroughTs;
    if (!aTs && !bTs) return 0;
    if (!aTs) return 1;
    if (!bTs) return -1;
    return bTs - aTs;
  });

  const getAvailabilityStatus = availability => {
    if (!availability) {
      return { icon: '⏳', text: 'Pending', color: '#6c757d' };
    }
    const validThrough = availability.validThroughTs
      ? new Date(availability.validThroughTs)
      : null;
    const now = new Date();
    const hoursSinceUpdate = validThrough
      ? (now - validThrough) / (1000 * 60 * 60)
      : null;
    if (!validThrough) {
      return { icon: '⏳', text: 'Pending', color: '#6c757d' };
    } else if (hoursSinceUpdate <= 24) {
      return {
        icon: '🟢',
        text: formatTimeAgo(validThrough),
        color: '#28a745',
      };
    } else if (hoursSinceUpdate <= 72) {
      return {
        icon: '🟡',
        text: formatTimeAgo(validThrough),
        color: '#ffc107',
      };
    } else {
      return {
        icon: '🔴',
        text: formatTimeAgo(validThrough),
        color: '#dc3545',
      };
    }
  };

  const formatTimeAgo = date => {
    const now = new Date();
    const diffMs = now - date;
    const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
    const diffDays = Math.floor(diffHours / 24);
    if (diffHours < 1) return 'just now';
    if (diffHours < 24) return `${diffHours}h ago`;
    if (diffDays === 1) return 'yesterday';
    return `${diffDays}d ago`;
  };

  const maxDisplay = 5;

  const materializationsList = sortedNodes.slice(0, maxDisplay).map(node => {
    const status = getAvailabilityStatus(node.current?.availability);
    return (
      <div
        key={node.name}
        style={{
          padding: '0.5rem',
          border: '1px solid var(--border-color, #e0e0e0)',
          borderRadius: '4px',
          backgroundColor: 'var(--card-bg, #fff)',
        }}
      >
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            marginBottom: '4px',
          }}
        >
          <NodeDisplay node={node} size="medium" />
          <span style={{ fontSize: '10px', color: status.color }}>
            {status.icon} {status.text}
          </span>
        </div>
        <div style={{ fontSize: '10px', color: '#666' }}>
          {node.current?.materializations?.map(mat => (
            <span key={mat.name} style={{ marginRight: '8px' }}>
              🕐 {mat.schedule || 'No schedule'}
            </span>
          ))}
          {node.current?.availability?.table && (
            <span style={{ color: '#888' }}>
              → {node.current.availability.table}
            </span>
          )}
        </div>
      </div>
    );
  });

  return (
    <DashboardCard
      title="Materializations"
      actionLink="/?hasMaterialization=true"
      loading={loading}
      cardStyle={{
        padding: '0.75rem 1rem',
        maxHeight: '300px',
        overflowY: 'auto',
      }}
      emptyState={
        <div style={{ padding: '0' }}>
          <p
            style={{ fontSize: '12px', color: '#666', marginBottom: '0.75rem' }}
          >
            No materializations configured.
          </p>
          <div
            style={{
              padding: '0.75rem',
              backgroundColor: 'var(--card-bg, #f8f9fa)',
              border: '1px dashed var(--border-color, #dee2e6)',
              borderRadius: '6px',
              textAlign: 'center',
            }}
          >
            <div style={{ fontSize: '16px', marginBottom: '0.25rem' }}>📦</div>
            <div
              style={{
                fontSize: '11px',
                fontWeight: '500',
                marginBottom: '0.25rem',
              }}
            >
              Materialize a node
            </div>
            <p
              style={{
                fontSize: '10px',
                color: '#666',
                marginBottom: '0.5rem',
              }}
            >
              Speed up queries with cached data
            </p>
            <Link
              to="/"
              style={{
                display: 'inline-block',
                padding: '3px 8px',
                fontSize: '10px',
                backgroundColor: 'var(--primary-color, #4a90d9)',
                color: '#fff',
                borderRadius: '4px',
                textDecoration: 'none',
              }}
            >
              Browse nodes →
            </Link>
          </div>
        </div>
      }
    >
      {sortedNodes.length > 0 && (
        <div
          style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}
        >
          {materializationsList}
          {sortedNodes.length > maxDisplay && (
            <div
              style={{
                textAlign: 'center',
                padding: '0.5rem',
                fontSize: '12px',
                color: '#666',
              }}
            >
              +{sortedNodes.length - maxDisplay} more
            </div>
          )}
        </div>
      )}
    </DashboardCard>
  );
}

// Git Namespaces Section - shows git-managed namespaces with their branches
function ActiveBranchesSection({ ownedNodes, recentlyEdited, loading }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [branchCounts, setBranchCounts] = useState({});
  const [countsLoading, setCountsLoading] = useState(true);

  // Combine owned and edited nodes to get all user's nodes
  const allNodes = [...ownedNodes, ...recentlyEdited];

  // Group nodes by base git namespace (the root namespace that's git-managed)
  const gitNamespaceMap = new Map();
  allNodes.forEach(node => {
    if (node.gitInfo && node.gitInfo.repo) {
      // Extract the base namespace (everything before the branch part)
      const fullNamespace = node.name.split('.').slice(0, -1).join('.');

      // For git-managed namespaces, the base is the parent namespace
      // If not available, we derive it by removing the branch from the full namespace
      let baseNamespace;
      if (node.gitInfo.parentNamespace) {
        baseNamespace = node.gitInfo.parentNamespace;
      } else {
        // Fallback: remove the branch part from fullNamespace
        // fullNamespace is like "myproject.main", we want "myproject"
        const parts = fullNamespace.split('.');
        baseNamespace = parts.slice(0, -1).join('.');
      }

      if (!gitNamespaceMap.has(baseNamespace)) {
        gitNamespaceMap.set(baseNamespace, {
          baseNamespace,
          repo: node.gitInfo.repo,
          defaultBranch: node.gitInfo.defaultBranch,
          branches: new Map(),
        });
      }

      const gitNs = gitNamespaceMap.get(baseNamespace);
      const branchKey = node.gitInfo.branch;

      if (!gitNs.branches.has(branchKey)) {
        gitNs.branches.set(branchKey, {
          branch: node.gitInfo.branch,
          isDefault: node.gitInfo.isDefaultBranch,
          namespace: fullNamespace,
          nodes: [],
          lastActivity: node.current?.updatedAt,
        });
      }

      const branchInfo = gitNs.branches.get(branchKey);
      branchInfo.nodes.push(node);
      // Track most recent activity
      if (
        node.current?.updatedAt &&
        (!branchInfo.lastActivity ||
          node.current.updatedAt > branchInfo.lastActivity)
      ) {
        branchInfo.lastActivity = node.current.updatedAt;
      }
    }
  });

  // Ensure default branch is always present (even if user has no nodes there)
  gitNamespaceMap.forEach(gitNs => {
    if (gitNs.defaultBranch && !gitNs.branches.has(gitNs.defaultBranch)) {
      gitNs.branches.set(gitNs.defaultBranch, {
        branch: gitNs.defaultBranch,
        isDefault: true,
        namespace: `${gitNs.baseNamespace}.${gitNs.defaultBranch}`,
        nodes: [],
        lastActivity: null,
      });
    }
  });

  // Convert to array and sort
  const gitNamespaces = Array.from(gitNamespaceMap.values())
    .map(ns => ({
      ...ns,
      branches: Array.from(ns.branches.values()).sort((a, b) => {
        // Default branch first
        if (a.isDefault && !b.isDefault) return -1;
        if (!a.isDefault && b.isDefault) return 1;
        // Then by last activity
        if (!a.lastActivity && !b.lastActivity) return 0;
        if (!a.lastActivity) return 1;
        if (!b.lastActivity) return -1;
        return new Date(b.lastActivity) - new Date(a.lastActivity);
      }),
    }))
    .sort((a, b) => {
      // Sort git namespaces by most recent activity across all branches
      const aLatest = Math.max(
        ...a.branches.map(b =>
          b.lastActivity ? new Date(b.lastActivity).getTime() : 0,
        ),
      );
      const bLatest = Math.max(
        ...b.branches.map(b =>
          b.lastActivity ? new Date(b.lastActivity).getTime() : 0,
        ),
      );
      return bLatest - aLatest;
    });

  const maxDisplay = 3; // Show top 3 git namespaces

  // Fetch total node counts for each branch
  useEffect(() => {
    if (loading || gitNamespaces.length === 0) return;

    const fetchCounts = async () => {
      const counts = {};

      // Collect all branch namespaces to query
      const branchNamespaces = [];
      gitNamespaces.forEach(gitNs => {
        gitNs.branches.forEach(branch => {
          branchNamespaces.push(branch.namespace);
        });
      });

      // Fetch counts for all branches in parallel
      const countPromises = branchNamespaces.map(async namespace => {
        try {
          const result = await djClient.listNodesForLanding(
            namespace,
            ['TRANSFORM', 'METRIC', 'DIMENSION', 'CUBE'], // Exclude SOURCE nodes
            null, // tags
            null, // editedBy
            null, // before
            null, // after
            5000, // high limit to get accurate count
            { key: 'name', direction: 'ascending' }, // sortConfig (required, but doesn't matter for counting)
            null, // mode
          );
          const count = result?.data?.findNodesPaginated?.edges?.length || 0;
          counts[namespace] = count;
        } catch (error) {
          console.error(`Error fetching count for ${namespace}:`, error);
          counts[namespace] = 0;
        }
      });

      await Promise.all(countPromises);
      setBranchCounts(counts);
      setCountsLoading(false);
    };

    fetchCounts();
  }, [djClient, loading, gitNamespaces.length]);

  const gitNamespacesList = gitNamespaces
    .slice(0, maxDisplay)
    .map((gitNs, gitNsIdx) => {
      const isLastGitNs =
        gitNsIdx === gitNamespaces.slice(0, maxDisplay).length - 1;
      return (
        <div key={gitNs.baseNamespace}>
          {/* Base namespace and repository header */}
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              padding: '0.75rem 0 0.5rem 0',
              marginTop: gitNsIdx === 0 ? '0' : '0',
            }}
          >
            <a
              href={`/namespaces/${gitNs.baseNamespace}`}
              style={{
                fontSize: '12px',
                fontWeight: '600',
                textDecoration: 'none',
              }}
            >
              {gitNs.baseNamespace}
            </a>
            <span
              style={{
                fontSize: '9px',
                padding: '2px 6px',
                backgroundColor: '#f0f0f0',
                color: '#666',
                borderRadius: '3px',
                fontWeight: '500',
              }}
            >
              {gitNs.repo}
            </span>
          </div>

          {/* Branch list */}
          {gitNs.branches.map(branchInfo => {
            const totalNodes = branchCounts[branchInfo.namespace] ?? 0;
            return (
              <div
                key={branchInfo.branch}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  padding: '0px 1em 0.4em 1em',
                }}
              >
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '6px',
                    minWidth: 0,
                    flex: 1,
                  }}
                >
                  <a
                    href={`/namespaces/${branchInfo.namespace}`}
                    style={{
                      fontSize: '12px',
                      fontWeight: '500',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {branchInfo.branch}
                  </a>
                  {branchInfo.isDefault && (
                    <span style={{ fontSize: '10px' }}>⭐</span>
                  )}
                </div>
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '6px',
                    fontSize: '10px',
                    color: '#666',
                    whiteSpace: 'nowrap',
                  }}
                >
                  <span>
                    {totalNodes} node{totalNodes !== 1 ? 's' : ''}
                  </span>
                  {branchInfo.lastActivity && (
                    <>
                      <span>•</span>
                      <span style={{ color: '#888' }}>
                        updated {formatRelativeTime(branchInfo.lastActivity)}
                      </span>
                    </>
                  )}
                </div>
              </div>
            );
          })}

          {/* Horizontal line between namespaces */}
          {!isLastGitNs && (
            <div
              style={{
                borderBottom: '1px solid var(--border-color, #ddd)',
                margin: '0.5rem 0',
              }}
            />
          )}
        </div>
      );
    });

  return (
    <DashboardCard
      title="Git Namespaces"
      loading={loading || countsLoading}
      cardStyle={{
        padding: '0.25rem 0.25rem 0.5em 0.75rem',
        maxHeight: '300px',
        overflowY: 'auto',
      }}
      emptyState={
        <div style={{ padding: '0', textAlign: 'center' }}>
          <p
            style={{ fontSize: '12px', color: '#666', marginBottom: '0.75rem' }}
          >
            No git-managed namespaces.
          </p>
          <div
            style={{
              padding: '0.75rem',
              backgroundColor: 'var(--card-bg, #f8f9fa)',
              border: '1px dashed var(--border-color, #dee2e6)',
              borderRadius: '6px',
            }}
          >
            <div style={{ fontSize: '16px', marginBottom: '0.25rem' }}>🌿</div>
            <div
              style={{
                fontSize: '11px',
                fontWeight: '500',
                marginBottom: '0.25rem',
              }}
            >
              Set up git-backed namespaces
            </div>
            <p style={{ fontSize: '10px', color: '#666', marginBottom: '0' }}>
              Manage your nodes with version control
            </p>
          </div>
        </div>
      }
    >
      {gitNamespaces.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column' }}>
          {gitNamespacesList}
          {gitNamespaces.length > maxDisplay && (
            <div
              style={{
                textAlign: 'center',
                padding: '0.5rem',
                fontSize: '12px',
                color: '#666',
              }}
            >
              +{gitNamespaces.length - maxDisplay} more git namespace
              {gitNamespaces.length - maxDisplay !== 1 ? 's' : ''}
            </div>
          )}
        </div>
      )}
    </DashboardCard>
  );
}

// Node List Component
function NodeList({ nodes, showUpdatedAt }) {
  const formatDateTime = dateStr => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
      {nodes.map(node => (
        <div
          key={node.name}
          className="subscription-item"
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            padding: '0.5rem 0',
            borderBottom: '1px solid var(--border-color, #eee)',
            gap: '0.5rem',
          }}
        >
          <div style={{ flex: 1, minWidth: 0, overflow: 'hidden' }}>
            <div style={{ marginBottom: '0.25rem' }}>
              <NodeDisplay
                node={node}
                size="large"
                ellipsis={true}
                gap="0.5rem"
              />
            </div>
            <div
              style={{
                fontSize: '11px',
                color: '#888',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
            >
              {node.name}
            </div>
          </div>
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem',
              flexShrink: 0,
              whiteSpace: 'nowrap',
            }}
          >
            {showUpdatedAt && node.current?.updatedAt && (
              <span style={{ fontSize: '11px', color: '#888' }}>
                {formatDateTime(node.current.updatedAt)}
              </span>
            )}
            <div
              style={{
                display: 'inline-flex',
                transform: 'scale(0.85)',
                transformOrigin: 'right center',
              }}
            >
              <NodeListActions nodeName={node.name} />
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
