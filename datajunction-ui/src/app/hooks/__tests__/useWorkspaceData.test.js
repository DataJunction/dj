import { renderHook, waitFor } from '@testing-library/react';
import { useContext } from 'react';
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
} from '../useWorkspaceData';

jest.mock('react', () => ({
  ...jest.requireActual('react'),
  useContext: jest.fn(),
}));

const mockDjClient = {
  whoami: jest.fn(),
  getWorkspaceOwnedNodes: jest.fn(),
  getWorkspaceRecentlyEdited: jest.fn(),
  getNotificationPreferences: jest.fn(),
  getNodesByNames: jest.fn(),
  getWorkspaceCollections: jest.fn(),
  getSubscribedHistory: jest.fn(),
  getWorkspaceMaterializations: jest.fn(),
  getWorkspaceNodesMissingDescription: jest.fn(),
  getWorkspaceInvalidNodes: jest.fn(),
  getWorkspaceDraftNodes: jest.fn(),
  getWorkspaceOrphanedDimensions: jest.fn(),
  namespaces: jest.fn(),
};

describe('useWorkspaceData hooks', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    useContext.mockReturnValue({ DataJunctionAPI: mockDjClient });
  });

  describe('useCurrentUser', () => {
    it('should fetch current user successfully', async () => {
      const mockUser = { username: 'test@example.com', id: 1 };
      mockDjClient.whoami.mockResolvedValue(mockUser);

      const { result } = renderHook(() => useCurrentUser());

      expect(result.current.loading).toBe(true);

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(result.current.data).toEqual(mockUser);
      expect(result.current.error).toBe(null);
    });

    it('should handle errors when fetching user', async () => {
      const mockError = new Error('API error');
      mockDjClient.whoami.mockRejectedValue(mockError);
      const consoleErrorSpy = jest
        .spyOn(console, 'error')
        .mockImplementation(() => {});

      const { result } = renderHook(() => useCurrentUser());

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(result.current.data).toBe(null);
      expect(result.current.error).toEqual(mockError);
      consoleErrorSpy.mockRestore();
    });
  });

  describe('useWorkspaceOwnedNodes', () => {
    it('should fetch owned nodes successfully', async () => {
      const mockNodes = [
        { name: 'node1', type: 'metric' },
        { name: 'node2', type: 'dimension' },
      ];
      mockDjClient.getWorkspaceOwnedNodes.mockResolvedValue({
        data: {
          findNodesPaginated: {
            edges: mockNodes.map(node => ({ node })),
          },
        },
      });

      const { result } = renderHook(() =>
        useWorkspaceOwnedNodes('test@example.com'),
      );

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(result.current.data).toEqual(mockNodes);
      expect(result.current.error).toBe(null);
    });

    it('should handle empty username', async () => {
      const { result } = renderHook(() => useWorkspaceOwnedNodes(null));

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(result.current.data).toEqual([]);
      expect(mockDjClient.getWorkspaceOwnedNodes).not.toHaveBeenCalled();
    });

    it('should handle errors', async () => {
      const mockError = new Error('API error');
      mockDjClient.getWorkspaceOwnedNodes.mockRejectedValue(mockError);
      const consoleErrorSpy = jest
        .spyOn(console, 'error')
        .mockImplementation(() => {});

      const { result } = renderHook(() =>
        useWorkspaceOwnedNodes('test@example.com'),
      );

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(result.current.error).toEqual(mockError);
      consoleErrorSpy.mockRestore();
    });
  });

  describe('useWorkspaceRecentlyEdited', () => {
    it('should fetch recently edited nodes', async () => {
      const mockNodes = [{ name: 'node1', type: 'metric' }];
      mockDjClient.getWorkspaceRecentlyEdited.mockResolvedValue({
        data: {
          findNodesPaginated: {
            edges: mockNodes.map(node => ({ node })),
          },
        },
      });

      const { result } = renderHook(() =>
        useWorkspaceRecentlyEdited('test@example.com'),
      );

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(result.current.data).toEqual(mockNodes);
    });

    it('should handle empty username', async () => {
      const { result } = renderHook(() => useWorkspaceRecentlyEdited(null));

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(mockDjClient.getWorkspaceRecentlyEdited).not.toHaveBeenCalled();
    });
  });

  describe('useWorkspaceWatchedNodes', () => {
    it('should fetch watched nodes successfully', async () => {
      const mockSubscriptions = [
        { entity_name: 'node1' },
        { entity_name: 'node2' },
      ];
      const mockNodes = [
        { name: 'node1', type: 'metric' },
        { name: 'node2', type: 'dimension' },
      ];

      mockDjClient.getNotificationPreferences.mockResolvedValue(
        mockSubscriptions,
      );
      mockDjClient.getNodesByNames.mockResolvedValue(mockNodes);

      const { result } = renderHook(() =>
        useWorkspaceWatchedNodes('test@example.com'),
      );

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(result.current.data).toEqual(mockNodes);
      expect(mockDjClient.getNodesByNames).toHaveBeenCalledWith([
        'node1',
        'node2',
      ]);
    });

    it('should handle no watched nodes', async () => {
      mockDjClient.getNotificationPreferences.mockResolvedValue([]);

      const { result } = renderHook(() =>
        useWorkspaceWatchedNodes('test@example.com'),
      );

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(result.current.data).toEqual([]);
      expect(mockDjClient.getNodesByNames).not.toHaveBeenCalled();
    });

    it('should handle empty username', async () => {
      const { result } = renderHook(() => useWorkspaceWatchedNodes(null));

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(mockDjClient.getNotificationPreferences).not.toHaveBeenCalled();
    });
  });

  describe('useWorkspaceCollections', () => {
    it('should fetch collections successfully', async () => {
      const mockCollections = [
        { name: 'collection1', nodeCount: 5 },
        { name: 'collection2', nodeCount: 10 },
      ];
      mockDjClient.getWorkspaceCollections.mockResolvedValue({
        data: { listCollections: mockCollections },
      });

      const { result } = renderHook(() =>
        useWorkspaceCollections('test@example.com'),
      );

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(result.current.data).toEqual(mockCollections);
    });

    it('should handle empty username', async () => {
      const { result } = renderHook(() => useWorkspaceCollections(null));

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(mockDjClient.getWorkspaceCollections).not.toHaveBeenCalled();
    });
  });

  describe('useWorkspaceNotifications', () => {
    it('should fetch and enrich notifications', async () => {
      const mockHistory = [
        { entity_name: 'node1', activity: 'updated' },
        { entity_name: 'node2', activity: 'created' },
      ];
      const mockNodes = [
        {
          name: 'node1',
          type: 'metric',
          current: { displayName: 'Node 1' },
        },
        {
          name: 'node2',
          type: 'dimension',
          current: { displayName: 'Node 2' },
        },
      ];

      mockDjClient.getSubscribedHistory.mockResolvedValue(mockHistory);
      mockDjClient.getNodesByNames.mockResolvedValue(mockNodes);

      const { result } = renderHook(() =>
        useWorkspaceNotifications('test@example.com'),
      );

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(result.current.data).toEqual([
        {
          entity_name: 'node1',
          activity: 'updated',
          node_type: 'metric',
          display_name: 'Node 1',
        },
        {
          entity_name: 'node2',
          activity: 'created',
          node_type: 'dimension',
          display_name: 'Node 2',
        },
      ]);
    });

    it('should handle empty notifications', async () => {
      mockDjClient.getSubscribedHistory.mockResolvedValue([]);

      const { result } = renderHook(() =>
        useWorkspaceNotifications('test@example.com'),
      );

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(result.current.data).toEqual([]);
    });

    it('should handle empty username', async () => {
      const { result } = renderHook(() => useWorkspaceNotifications(null));

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(mockDjClient.getSubscribedHistory).not.toHaveBeenCalled();
    });
  });

  describe('useWorkspaceMaterializations', () => {
    it('should fetch materializations successfully', async () => {
      const mockNodes = [{ name: 'node1', type: 'metric' }];
      mockDjClient.getWorkspaceMaterializations.mockResolvedValue({
        data: {
          findNodesPaginated: {
            edges: mockNodes.map(node => ({ node })),
          },
        },
      });

      const { result } = renderHook(() =>
        useWorkspaceMaterializations('test@example.com'),
      );

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(result.current.data).toEqual(mockNodes);
    });

    it('should handle empty username', async () => {
      const { result } = renderHook(() => useWorkspaceMaterializations(null));

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(mockDjClient.getWorkspaceMaterializations).not.toHaveBeenCalled();
    });
  });

  describe('useWorkspaceNeedsAttention', () => {
    it('should fetch all needs attention items', async () => {
      const mockMissingDesc = [{ name: 'node1', type: 'metric' }];
      const mockInvalid = [{ name: 'node2', type: 'dimension' }];
      const mockDrafts = [
        {
          name: 'node3',
          type: 'transform',
          current: {
            updatedAt: new Date(Date.now() - 10 * 24 * 60 * 60 * 1000), // 10 days ago
          },
        },
      ];
      const mockOrphaned = [{ name: 'node4', type: 'dimension' }];

      mockDjClient.getWorkspaceNodesMissingDescription.mockResolvedValue({
        data: {
          findNodesPaginated: {
            edges: mockMissingDesc.map(node => ({ node })),
          },
        },
      });
      mockDjClient.getWorkspaceInvalidNodes.mockResolvedValue({
        data: {
          findNodesPaginated: { edges: mockInvalid.map(node => ({ node })) },
        },
      });
      mockDjClient.getWorkspaceDraftNodes.mockResolvedValue({
        data: {
          findNodesPaginated: { edges: mockDrafts.map(node => ({ node })) },
        },
      });
      mockDjClient.getWorkspaceOrphanedDimensions.mockResolvedValue({
        data: {
          findNodesPaginated: { edges: mockOrphaned.map(node => ({ node })) },
        },
      });

      const { result } = renderHook(() =>
        useWorkspaceNeedsAttention('test@example.com'),
      );

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(result.current.data.nodesMissingDescription).toEqual(
        mockMissingDesc,
      );
      expect(result.current.data.invalidNodes).toEqual(mockInvalid);
      expect(result.current.data.staleDrafts).toEqual(mockDrafts);
      expect(result.current.data.orphanedDimensions).toEqual(mockOrphaned);
    });

    it('should filter out recent drafts', async () => {
      const recentDraft = {
        name: 'recent',
        current: { updatedAt: new Date().toISOString() },
      };
      const staleDraft = {
        name: 'stale',
        current: {
          updatedAt: new Date(Date.now() - 10 * 24 * 60 * 60 * 1000),
        },
      };

      mockDjClient.getWorkspaceNodesMissingDescription.mockResolvedValue({
        data: { findNodesPaginated: { edges: [] } },
      });
      mockDjClient.getWorkspaceInvalidNodes.mockResolvedValue({
        data: { findNodesPaginated: { edges: [] } },
      });
      mockDjClient.getWorkspaceDraftNodes.mockResolvedValue({
        data: {
          findNodesPaginated: {
            edges: [recentDraft, staleDraft].map(node => ({ node })),
          },
        },
      });
      mockDjClient.getWorkspaceOrphanedDimensions.mockResolvedValue({
        data: { findNodesPaginated: { edges: [] } },
      });

      const { result } = renderHook(() =>
        useWorkspaceNeedsAttention('test@example.com'),
      );

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      // Should only include stale draft (older than 7 days)
      expect(result.current.data.staleDrafts).toEqual([staleDraft]);
    });

    it('should handle empty username', async () => {
      const { result } = renderHook(() => useWorkspaceNeedsAttention(null));

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(
        mockDjClient.getWorkspaceNodesMissingDescription,
      ).not.toHaveBeenCalled();
    });
  });

  describe('usePersonalNamespace', () => {
    it('should check if personal namespace exists', async () => {
      const mockNamespaces = [
        { namespace: 'default' },
        { namespace: 'users.testuser' },
      ];
      mockDjClient.namespaces.mockResolvedValue(mockNamespaces);

      const { result } = renderHook(() =>
        usePersonalNamespace('testuser@example.com'),
      );

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(result.current.exists).toBe(true);
    });

    it('should return false if namespace does not exist', async () => {
      const mockNamespaces = [{ namespace: 'default' }];
      mockDjClient.namespaces.mockResolvedValue(mockNamespaces);

      const { result } = renderHook(() =>
        usePersonalNamespace('testuser@example.com'),
      );

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(result.current.exists).toBe(false);
    });

    it('should handle empty username', async () => {
      const { result } = renderHook(() => usePersonalNamespace(null));

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(mockDjClient.namespaces).not.toHaveBeenCalled();
    });

    it('should handle errors', async () => {
      const mockError = new Error('API error');
      mockDjClient.namespaces.mockRejectedValue(mockError);
      const consoleErrorSpy = jest
        .spyOn(console, 'error')
        .mockImplementation(() => {});

      const { result } = renderHook(() =>
        usePersonalNamespace('testuser@example.com'),
      );

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      expect(result.current.exists).toBe(false);
      expect(result.current.error).toEqual(mockError);
      consoleErrorSpy.mockRestore();
    });
  });
});
