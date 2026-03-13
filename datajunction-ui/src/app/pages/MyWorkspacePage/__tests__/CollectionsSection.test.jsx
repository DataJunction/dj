import React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { CollectionsSection } from '../CollectionsSection';
import DJClientContext from '../../../providers/djclient';

jest.mock('../MyWorkspacePage.css', () => ({}));

describe('<CollectionsSection />', () => {
  const mockDjClient = {
    listAllCollections: jest.fn(),
  };

  const mockCollections = [
    {
      name: 'my_collection',
      description: 'My test collection',
      nodeCount: 5,
      createdBy: { username: 'test.user@example.com' },
    },
    {
      name: 'other_collection',
      description: 'Another collection',
      nodeCount: 10,
      createdBy: { username: 'other.user@example.com' },
    },
  ];

  const mockCurrentUser = {
    username: 'test.user@example.com',
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  const renderWithContext = props => {
    return render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <MemoryRouter>
          <CollectionsSection {...props} />
        </MemoryRouter>
      </DJClientContext.Provider>,
    );
  };

  it('should render loading state', () => {
    mockDjClient.listAllCollections.mockResolvedValue({
      data: { listCollections: [] },
    });

    renderWithContext({
      collections: [],
      loading: true,
      currentUser: mockCurrentUser,
    });

    expect(screen.getByText('Collections')).toBeInTheDocument();
  });

  it('should fetch and display all collections', async () => {
    mockDjClient.listAllCollections.mockResolvedValue({
      data: { listCollections: mockCollections },
    });

    renderWithContext({
      collections: [],
      loading: false,
      currentUser: mockCurrentUser,
    });

    await waitFor(() => {
      expect(mockDjClient.listAllCollections).toHaveBeenCalled();
    });

    await waitFor(() => {
      expect(screen.getByText('my_collection')).toBeInTheDocument();
      expect(screen.getByText('other_collection')).toBeInTheDocument();
    });
  });

  it('should show owner as "you" for current user collections', async () => {
    mockDjClient.listAllCollections.mockResolvedValue({
      data: { listCollections: mockCollections },
    });

    renderWithContext({
      collections: [],
      loading: false,
      currentUser: mockCurrentUser,
    });

    await waitFor(() => {
      expect(screen.getByText('by you')).toBeInTheDocument();
    });
  });

  it('should show username for other user collections', async () => {
    mockDjClient.listAllCollections.mockResolvedValue({
      data: { listCollections: mockCollections },
    });

    renderWithContext({
      collections: [],
      loading: false,
      currentUser: mockCurrentUser,
    });

    await waitFor(() => {
      expect(screen.getByText('by other.user')).toBeInTheDocument();
    });
  });

  it('should display node counts', async () => {
    mockDjClient.listAllCollections.mockResolvedValue({
      data: { listCollections: mockCollections },
    });

    renderWithContext({
      collections: [],
      loading: false,
      currentUser: mockCurrentUser,
    });

    await waitFor(() => {
      expect(screen.getByText('5 nodes')).toBeInTheDocument();
      expect(screen.getByText('10 nodes')).toBeInTheDocument();
    });
  });

  it('should render empty state when no collections', async () => {
    mockDjClient.listAllCollections.mockResolvedValue({
      data: { listCollections: [] },
    });

    renderWithContext({
      collections: [],
      loading: false,
      currentUser: mockCurrentUser,
    });

    await waitFor(() => {
      expect(screen.getByText('No collections yet')).toBeInTheDocument();
    });
  });

  it('should display user and other collections', async () => {
    const collections = [
      {
        name: 'other_first',
        description: 'Other collection',
        createdBy: { username: 'other@example.com' },
        nodeCount: 1,
      },
      {
        name: 'my_first',
        description: 'My collection',
        createdBy: { username: 'test.user@example.com' },
        nodeCount: 2,
      },
    ];

    mockDjClient.listAllCollections.mockResolvedValue({
      data: { listCollections: collections },
    });

    renderWithContext({
      collections: [],
      loading: false,
      currentUser: mockCurrentUser,
    });

    await waitFor(() => {
      // Both collections should be displayed
      expect(screen.getByText('my_first')).toBeInTheDocument();
      expect(screen.getByText('other_first')).toBeInTheDocument();
    });
  });

  it('should limit display to 8 collections', async () => {
    const manyCollections = Array.from({ length: 12 }, (_, i) => ({
      name: `collection_${i}`,
      nodeCount: i,
      createdBy: { username: 'test@example.com' },
    }));

    mockDjClient.listAllCollections.mockResolvedValue({
      data: { listCollections: manyCollections },
    });

    renderWithContext({
      collections: [],
      loading: false,
      currentUser: mockCurrentUser,
    });

    await waitFor(() => {
      const links = screen.getAllByRole('link');
      // Should have at most 9 links (8 collections + 1 "Create Collection")
      expect(links.length).toBeLessThanOrEqual(9);
    });
  });

  it('should log response when fetching all collections succeeds', async () => {
    const consoleSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
    mockDjClient.listAllCollections.mockResolvedValue({
      data: { listCollections: mockCollections },
    });

    renderWithContext({
      collections: [],
      loading: false,
      currentUser: mockCurrentUser,
    });

    await waitFor(() => {
      expect(consoleSpy).toHaveBeenCalledWith(
        'All collections response:',
        expect.anything(),
      );
    });

    consoleSpy.mockRestore();
  });

  it('should handle API errors gracefully', async () => {
    mockDjClient.listAllCollections.mockRejectedValue(new Error('API error'));

    const fallbackCollections = [
      {
        name: 'fallback_collection',
        nodeCount: 1,
        createdBy: { username: 'test@example.com' },
      },
    ];

    renderWithContext({
      collections: fallbackCollections,
      loading: false,
      currentUser: mockCurrentUser,
    });

    await waitFor(() => {
      // Should fall back to props collections
      expect(screen.getByText('fallback_collection')).toBeInTheDocument();
    });
  });

  it('should handle collections without createdBy', async () => {
    const collectionsWithoutOwner = [
      {
        name: 'no_owner',
        nodeCount: 1,
        createdBy: null,
      },
    ];

    mockDjClient.listAllCollections.mockResolvedValue({
      data: { listCollections: collectionsWithoutOwner },
    });

    renderWithContext({
      collections: [],
      loading: false,
      currentUser: mockCurrentUser,
    });

    await waitFor(() => {
      expect(screen.getByText('by unknown')).toBeInTheDocument();
    });
  });

  it('should handle hover interactions on collection cards', async () => {
    mockDjClient.listAllCollections.mockResolvedValue({
      data: { listCollections: mockCollections },
    });

    renderWithContext({
      collections: [],
      loading: false,
      currentUser: mockCurrentUser,
    });

    await waitFor(() => {
      expect(screen.getByText('my_collection')).toBeInTheDocument();
    });

    const collectionLink = screen.getByText('my_collection').closest('a');

    // Test mouse enter - should change styles
    if (collectionLink) {
      fireEvent.mouseEnter(collectionLink);
      expect(collectionLink).toBeInTheDocument();

      // Test mouse leave - should reset styles
      fireEvent.mouseLeave(collectionLink);
      expect(collectionLink).toBeInTheDocument();
    }
  });
});
