import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../providers/djclient';

/**
 * Hook to fetch the current user
 */
export function useCurrentUser() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchUser = async () => {
      try {
        const user = await djClient.whoami();
        setData(user);
      } catch (err) {
        console.error('Error fetching user:', err);
        setError(err);
      }
      setLoading(false);
    };
    fetchUser();
  }, [djClient]);

  return { data, loading, error };
}

/**
 * Hook to fetch workspace owned nodes
 */
export function useWorkspaceOwnedNodes(username, limit = 5000) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!username) {
      setLoading(false);
      return;
    }

    const fetchData = async () => {
      try {
        const result = await djClient.getWorkspaceOwnedNodes(username, limit);
        setData(
          result?.data?.findNodesPaginated?.edges?.map(e => e.node) || [],
        );
      } catch (err) {
        console.error('Error fetching owned nodes:', err);
        setError(err);
      }
      setLoading(false);
    };
    fetchData();
  }, [djClient, username, limit]);

  return { data, loading, error };
}

/**
 * Hook to fetch workspace recently edited nodes
 */
export function useWorkspaceRecentlyEdited(username, limit = 5000) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!username) {
      setLoading(false);
      return;
    }

    const fetchData = async () => {
      try {
        const result = await djClient.getWorkspaceRecentlyEdited(
          username,
          limit,
        );
        setData(
          result?.data?.findNodesPaginated?.edges?.map(e => e.node) || [],
        );
      } catch (err) {
        console.error('Error fetching recently edited nodes:', err);
        setError(err);
      }
      setLoading(false);
    };
    fetchData();
  }, [djClient, username, limit]);

  return { data, loading, error };
}

/**
 * Hook to fetch watched nodes (notification subscriptions)
 */
export function useWorkspaceWatchedNodes(username) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!username) {
      setLoading(false);
      return;
    }

    const fetchData = async () => {
      try {
        const subscriptions = await djClient.getNotificationPreferences({
          entity_type: 'node',
        });
        const watchedNodeNames = (subscriptions || []).map(s => s.entity_name);

        if (watchedNodeNames.length > 0) {
          const nodes = await djClient.getNodesByNames(watchedNodeNames);
          setData(nodes || []);
        } else {
          setData([]);
        }
      } catch (err) {
        console.error('Error fetching watched nodes:', err);
        setError(err);
      }
      setLoading(false);
    };
    fetchData();
  }, [djClient, username]);

  return { data, loading, error };
}

/**
 * Hook to fetch workspace collections
 */
export function useWorkspaceCollections(username) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!username) {
      setLoading(false);
      return;
    }

    const fetchData = async () => {
      try {
        const result = await djClient.getWorkspaceCollections(username);
        setData(result?.data?.listCollections || []);
      } catch (err) {
        console.error('Error fetching collections:', err);
        setError(err);
      }
      setLoading(false);
    };
    fetchData();
  }, [djClient, username]);

  return { data, loading, error };
}

/**
 * Hook to fetch workspace notifications
 */
export function useWorkspaceNotifications(username, limit = 50) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!username) {
      setLoading(false);
      return;
    }

    const fetchData = async () => {
      try {
        const history = await djClient.getSubscribedHistory(limit);

        // Enrich with node info
        const nodeNames = Array.from(
          new Set((history || []).map(h => h.entity_name)),
        );
        let nodeInfoMap = {};
        if (nodeNames.length > 0) {
          const nodes = await djClient.getNodesByNames(nodeNames);
          nodeInfoMap = Object.fromEntries((nodes || []).map(n => [n.name, n]));
        }

        const enriched = (history || []).map(entry => ({
          ...entry,
          node_type: nodeInfoMap[entry.entity_name]?.type,
          display_name: nodeInfoMap[entry.entity_name]?.current?.displayName,
        }));

        setData(enriched);
      } catch (err) {
        console.error('Error fetching notifications:', err);
        setError(err);
      }
      setLoading(false);
    };
    fetchData();
  }, [djClient, username, limit]);

  return { data, loading, error };
}

/**
 * Hook to fetch workspace materializations
 */
export function useWorkspaceMaterializations(username, limit = 5000) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!username) {
      setLoading(false);
      return;
    }

    const fetchData = async () => {
      try {
        const result = await djClient.getWorkspaceMaterializations(
          username,
          limit,
        );
        setData(
          result?.data?.findNodesPaginated?.edges?.map(e => e.node) || [],
        );
      } catch (err) {
        console.error('Error fetching materializations:', err);
        setError(err);
      }
      setLoading(false);
    };
    fetchData();
  }, [djClient, username, limit]);

  return { data, loading, error };
}

/**
 * Hook to fetch all "Needs Attention" items
 * Returns an object with all actionable item categories
 */
export function useWorkspaceNeedsAttention(username) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [data, setData] = useState({
    nodesMissingDescription: [],
    invalidNodes: [],
    staleDrafts: [],
    orphanedDimensions: [],
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!username) {
      setLoading(false);
      return;
    }

    const fetchData = async () => {
      try {
        const [missingDescData, invalidData, draftData, orphanedData] =
          await Promise.all([
            djClient.getWorkspaceNodesMissingDescription(username, 5000),
            djClient.getWorkspaceInvalidNodes(username, 5000),
            djClient.getWorkspaceDraftNodes(username, 5000),
            djClient.getWorkspaceOrphanedDimensions(username, 5000),
          ]);

        const nodesMissingDescription =
          missingDescData?.data?.findNodesPaginated?.edges?.map(e => e.node) ||
          [];
        const invalidNodes =
          invalidData?.data?.findNodesPaginated?.edges?.map(e => e.node) || [];
        const orphanedDimensions =
          orphanedData?.data?.findNodesPaginated?.edges?.map(e => e.node) || [];

        // Filter drafts older than 7 days
        const sevenDaysAgo = new Date();
        sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
        const allDrafts =
          draftData?.data?.findNodesPaginated?.edges?.map(e => e.node) || [];
        const staleDrafts = allDrafts.filter(node => {
          const updatedAt = new Date(node.current?.updatedAt);
          return updatedAt < sevenDaysAgo;
        });

        setData({
          nodesMissingDescription,
          invalidNodes,
          staleDrafts,
          orphanedDimensions,
        });
      } catch (err) {
        console.error('Error fetching needs attention items:', err);
        setError(err);
      }
      setLoading(false);
    };
    fetchData();
  }, [djClient, username]);

  return { data, loading, error };
}

const toNodes = r => r?.data?.findNodesPaginated?.edges?.map(e => e.node) || [];

/**
 * Fetches all workspace dashboard data in independent parallel groups so that
 * fast sections (My Nodes, Collections) render immediately while slower ones
 * (Needs Attention, namespace check) finish in the background.
 */
export function useWorkspaceDashboardData(username) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  const [data, setData] = useState({
    ownedNodes: [],
    ownedHasMore: {},
    recentlyEdited: [],
    editedHasMore: {},
    watchedNodes: [],
    collections: [],
    notifications: [],
    materializedNodes: [],
    needsAttention: {
      nodesMissingDescription: [],
      invalidNodes: [],
      staleDrafts: [],
      orphanedDimensions: [],
    },
    hasPersonalNamespace: null,
  });

  const [loadingStates, setLoadingStates] = useState({
    myNodes: true,
    collections: true,
    notifications: true,
    materializations: true,
    needsAttention: true,
    namespace: true,
  });

  useEffect(() => {
    if (!username) {
      setLoadingStates({
        myNodes: false,
        collections: false,
        notifications: false,
        materializations: false,
        needsAttention: false,
        namespace: false,
      });
      return;
    }

    // Reset all loading flags whenever the username changes (e.g. after initial login)
    setLoadingStates({
      myNodes: true,
      collections: true,
      notifications: true,
      materializations: true,
      needsAttention: true,
      namespace: true,
    });

    // Group 1: Owned nodes + recently edited + collections
    // Each type is queried independently with limit=11 so hasNextPage tells us if there are more.
    const NODE_TYPES = ['METRIC', 'TRANSFORM', 'DIMENSION', 'CUBE'];
    const hasNextPage = r =>
      r?.data?.findNodesPaginated?.pageInfo?.hasNextPage ?? false;

    const fetchMyNodesAndCollections = async () => {
      try {
        const [ownedByType, editedByType, collectionsResult] =
          await Promise.all([
            Promise.all(
              NODE_TYPES.map(t =>
                djClient.getWorkspaceOwnedNodes(username, 11, t),
              ),
            ),
            Promise.all(
              NODE_TYPES.map(t =>
                djClient.getWorkspaceRecentlyEdited(username, 11, t),
              ),
            ),
            djClient.getWorkspaceCollections(username),
          ]);

        const ownedHasMore = Object.fromEntries(
          NODE_TYPES.map((t, i) => [
            t.toLowerCase(),
            hasNextPage(ownedByType[i]),
          ]),
        );
        const editedHasMore = Object.fromEntries(
          NODE_TYPES.map((t, i) => [
            t.toLowerCase(),
            hasNextPage(editedByType[i]),
          ]),
        );

        setData(prev => ({
          ...prev,
          ownedNodes: ownedByType.flatMap(r => toNodes(r)),
          ownedHasMore,
          recentlyEdited: editedByType.flatMap(r => toNodes(r)),
          editedHasMore,
          collections: collectionsResult?.data?.listCollections || [],
        }));
      } catch (err) {
        console.error('Error fetching nodes/collections:', err);
      }
      setLoadingStates(prev => ({
        ...prev,
        myNodes: false,
        collections: false,
      }));
    };

    // Group 2: Watched nodes + notifications (2 round-trips each, run in parallel)
    const fetchWatchedAndNotifications = async () => {
      try {
        const [subscriptions, historyResult] = await Promise.all([
          djClient.getNotificationPreferences({ entity_type: 'node' }),
          djClient.getSubscribedHistory(50),
        ]);

        const watchedNodeNames = (subscriptions || []).map(s => s.entity_name);
        const historyEntries = historyResult || [];
        const notifNodeNames = Array.from(
          new Set(historyEntries.map(h => h.entity_name)),
        );

        const [watchedNodes, notifNodes] = await Promise.all([
          watchedNodeNames.length > 0
            ? djClient.getNodesByNames(watchedNodeNames)
            : Promise.resolve([]),
          notifNodeNames.length > 0
            ? djClient.getNodesByNames(notifNodeNames)
            : Promise.resolve([]),
        ]);

        const notifNodeMap = Object.fromEntries(
          (notifNodes || []).map(n => [n.name, n]),
        );
        const notifications = historyEntries.map(entry => ({
          ...entry,
          node_type: notifNodeMap[entry.entity_name]?.type,
          display_name: notifNodeMap[entry.entity_name]?.current?.displayName,
        }));

        setData(prev => ({
          ...prev,
          watchedNodes: watchedNodes || [],
          notifications,
        }));
      } catch (err) {
        console.error('Error fetching watched/notifications:', err);
      }
      setLoadingStates(prev => ({ ...prev, notifications: false }));
    };

    // Group 3: Materializations (small limit — we only display 5)
    const fetchMaterializations = async () => {
      try {
        const result = await djClient.getWorkspaceMaterializations(username, 6);
        setData(prev => ({ ...prev, materializedNodes: toNodes(result) }));
      } catch (err) {
        console.error('Error fetching materializations:', err);
      }
      setLoadingStates(prev => ({ ...prev, materializations: false }));
    };

    // Group 4: Needs Attention + personal namespace check (slower, loads last)
    const fetchNeedsAttentionAndNamespace = async () => {
      try {
        const usernameForNamespace = username.split('@')[0];
        const personalNamespace = `users.${usernameForNamespace}`;

        const [
          missingDescResult,
          invalidResult,
          draftResult,
          orphanedResult,
          namespacesList,
        ] = await Promise.all([
          djClient.getWorkspaceNodesMissingDescription(username, 100),
          djClient.getWorkspaceInvalidNodes(username, 100),
          djClient.getWorkspaceDraftNodes(username, 100),
          djClient.getWorkspaceOrphanedDimensions(username, 100),
          djClient.namespaces(),
        ]);

        const sevenDaysAgo = new Date();
        sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
        const allDrafts = toNodes(draftResult);

        setData(prev => ({
          ...prev,
          needsAttention: {
            nodesMissingDescription: toNodes(missingDescResult),
            invalidNodes: toNodes(invalidResult),
            staleDrafts: allDrafts.filter(
              n => new Date(n.current?.updatedAt) < sevenDaysAgo,
            ),
            orphanedDimensions: toNodes(orphanedResult),
          },
          hasPersonalNamespace: (namespacesList || []).some(
            ns => ns.namespace === personalNamespace,
          ),
        }));
      } catch (err) {
        console.error('Error fetching needs attention:', err);
      }
      setLoadingStates(prev => ({
        ...prev,
        needsAttention: false,
        namespace: false,
      }));
    };

    // All four groups fire independently — each resolves its own loading flag
    fetchMyNodesAndCollections();
    fetchWatchedAndNotifications();
    fetchMaterializations();
    fetchNeedsAttentionAndNamespace();
  }, [djClient, username]);

  return { data, loadingStates };
}

/**
 * Hook to check if personal namespace exists
 */
export function usePersonalNamespace(username) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [exists, setExists] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!username) {
      setLoading(false);
      return;
    }

    const checkNamespace = async () => {
      try {
        // Extract username without domain for namespace
        const usernameForNamespace = username.split('@')[0];
        const personalNamespace = `users.${usernameForNamespace}`;
        const namespaces = await djClient.namespaces();
        const namespaceExists = namespaces.some(
          ns => ns.namespace === personalNamespace,
        );
        setExists(namespaceExists);
      } catch (err) {
        console.error('Error checking namespace:', err);
        setError(err);
        setExists(false);
      }
      setLoading(false);
    };
    checkNamespace();
  }, [djClient, username]);

  return { exists, loading, error };
}
