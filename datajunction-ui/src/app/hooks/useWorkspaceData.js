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
