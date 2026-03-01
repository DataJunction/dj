import * as React from 'react';
import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import DashboardCard from '../../components/DashboardCard';
import { formatRelativeTime } from '../../utils/date';

// Git Namespaces Section - shows git-managed namespaces with their branches
export function ActiveBranchesSection({ ownedNodes, recentlyEdited, loading }) {
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
    if (loading) return;

    if (gitNamespaces.length === 0) {
      setCountsLoading(false);
      return;
    }

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
