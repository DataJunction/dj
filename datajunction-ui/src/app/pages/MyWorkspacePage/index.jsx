import * as React from 'react';
import LoadingIcon from '../../icons/LoadingIcon';
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
import { NotificationsSection } from './NotificationsSection';
import { NeedsAttentionSection } from './NeedsAttentionSection';
import { MyNodesSection } from './MyNodesSection';
import { CollectionsSection } from './CollectionsSection';
import { MaterializationsSection } from './MaterializationsSection';
import { ActiveBranchesSection } from './ActiveBranchesSection';

import 'styles/settings.css';
import './MyWorkspacePage.css';

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
      <div className="workspace-layout">
        {/* Left Column: Organization (65%) */}
        <div className="workspace-left-column">
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
        <div className="workspace-right-column">
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
