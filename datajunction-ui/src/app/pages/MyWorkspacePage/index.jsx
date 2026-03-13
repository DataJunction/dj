import * as React from 'react';
import LoadingIcon from '../../icons/LoadingIcon';
import { useCurrentUser } from '../../providers/UserProvider';
import { useWorkspaceDashboardData } from '../../hooks/useWorkspaceData';
import { NotificationsSection } from './NotificationsSection';
import { NeedsAttentionSection } from './NeedsAttentionSection';
import { MyNodesSection } from './MyNodesSection';
import { MaterializationsSection } from './MaterializationsSection';
import { ActiveBranchesSection } from './ActiveBranchesSection';

import 'styles/settings.css';
import './MyWorkspacePage.css';

export function MyWorkspacePage() {
  const { currentUser, loading: userLoading } = useCurrentUser();
  const { data: workspaceData, loadingStates } = useWorkspaceDashboardData(
    currentUser?.username,
  );

  const {
    ownedNodes = [],
    ownedHasMore = {},
    recentlyEdited = [],
    editedHasMore = {},
    watchedNodes = [],
    notifications = [],
    materializedNodes = [],
    needsAttention: {
      nodesMissingDescription = [],
      invalidNodes = [],
      staleDrafts = [],
      orphanedDimensions = [],
    } = {},
    hasPersonalNamespace = null,
  } = workspaceData;

  const myNodesLoading = loadingStates.myNodes;
  const notificationsLoading = loadingStates.notifications;
  const materializationsLoading = loadingStates.materializations;
  const needsAttentionLoading = loadingStates.needsAttention;
  const namespaceLoading = loadingStates.namespace;

  // Filter stale materializations (> 72 hours old)
  const staleMaterializations = materializedNodes.filter(node => {
    const validThroughTs = node.current?.availability?.validThroughTs;
    if (!validThroughTs) return false; // Pending ones aren't "stale"
    const hoursSinceUpdate = (Date.now() - validThroughTs) / (1000 * 60 * 60);
    return hoursSinceUpdate > 72;
  });

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
      <div className="workspace-layout">
        {/* Left Column (65%): My Nodes */}
        <div className="workspace-left-column">
          <MyNodesSection
            ownedNodes={ownedNodes}
            ownedHasMore={ownedHasMore}
            watchedNodes={watchedNodes}
            recentlyEdited={recentlyEdited}
            editedHasMore={editedHasMore}
            username={currentUser?.username}
            loading={myNodesLoading}
          />
        </div>

        {/* Right Column (35%): action items first, then activity */}
        <div className="workspace-right-column">
          <NeedsAttentionSection
            nodesMissingDescription={nodesMissingDescription}
            invalidNodes={invalidNodes}
            staleDrafts={staleDrafts}
            staleMaterializations={staleMaterializations}
            orphanedDimensions={orphanedDimensions}
            username={currentUser?.username}
            loading={needsAttentionLoading || materializationsLoading}
            personalNamespace={personalNamespace}
            hasPersonalNamespace={hasPersonalNamespace}
            namespaceLoading={namespaceLoading}
          />

          <NotificationsSection
            notifications={notifications}
            username={currentUser?.username}
            loading={notificationsLoading}
          />

          <ActiveBranchesSection
            ownedNodes={ownedNodes}
            recentlyEdited={recentlyEdited}
            loading={myNodesLoading}
          />

          <MaterializationsSection
            nodes={materializedNodes}
            loading={materializationsLoading}
          />
        </div>
      </div>
    </div>
  );
}
