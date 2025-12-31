import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import DJClientContext from '../../../providers/djclient';
import UserContext from '../../../providers/UserProvider';
import { NamespacePage } from '../index';
import React from 'react';
import userEvent from '@testing-library/user-event';

const mockDjClient = {
  namespaces: jest.fn(),
  namespace: jest.fn(),
  listNodesForLanding: jest.fn(),
  addNamespace: jest.fn(),
  whoami: jest.fn(),
  users: jest.fn(),
  listTags: jest.fn(),
};

const mockCurrentUser = { username: 'dj', email: 'dj@test.com' };

const renderWithProviders = (ui, { route = '/namespaces/default' } = {}) => {
  return render(
    <UserContext.Provider
      value={{ currentUser: mockCurrentUser, loading: false }}
    >
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <MemoryRouter initialEntries={[route]}>
          <Routes>
            <Route path="namespaces/:namespace" element={ui} />
            <Route path="/" element={ui} />
          </Routes>
        </MemoryRouter>
      </DJClientContext.Provider>
    </UserContext.Provider>,
  );
};

describe('NamespacePage', () => {
  const original = window.location;

  const reloadFn = () => {
    window.location.reload();
  };

  beforeAll(() => {
    Object.defineProperty(window, 'location', {
      configurable: true,
      value: { reload: jest.fn() },
    });
  });

  afterAll(() => {
    Object.defineProperty(window, 'location', {
      configurable: true,
      value: original,
    });
  });

  beforeEach(() => {
    fetch.resetMocks();
    mockDjClient.whoami.mockResolvedValue({ username: 'dj' });
    mockDjClient.users.mockResolvedValue([
      { username: 'dj' },
      { username: 'user1' },
    ]);
    mockDjClient.listTags.mockResolvedValue([
      { name: 'tag1' },
      { name: 'tag2' },
    ]);
    mockDjClient.namespaces.mockResolvedValue([
      {
        namespace: 'common.one',
        num_nodes: 3,
      },
      {
        namespace: 'common.one.a',
        num_nodes: 6,
      },
      {
        namespace: 'common.one.b',
        num_nodes: 17,
      },
      {
        namespace: 'common.one.c',
        num_nodes: 64,
      },
      {
        namespace: 'default',
        num_nodes: 41,
      },
      {
        namespace: 'default.fruits',
        num_nodes: 1,
      },
      {
        namespace: 'default.fruits.citrus.lemons',
        num_nodes: 1,
      },
      {
        namespace: 'default.vegetables',
        num_nodes: 2,
      },
    ]);
    mockDjClient.namespace.mockResolvedValue([
      {
        name: 'testNode',
        display_name: 'Test Node',
        type: 'transform',
        mode: 'active',
        updated_at: new Date(),
        tags: [{ name: 'tag1' }],
        edited_by: ['dj'],
      },
    ]);
    mockDjClient.listNodesForLanding.mockResolvedValue({
      data: {
        findNodesPaginated: {
          pageInfo: {
            hasNextPage: true,
            endCursor:
              'eyJjcmVhdGVkX2F0IjogIjIwMjQtMDQtMTZUMjM6MjI6MjIuNDQxNjg2KzAwOjAwIiwgImlkIjogNjE0fQ==',
            hasPrevPage: true,
            startCursor:
              'eyJjcmVhdGVkX2F0IjogIjIwMjQtMTAtMTZUMTY6MDM6MTcuMDgzMjY3KzAwOjAwIiwgImlkIjogMjQwOX0=',
          },
          edges: [
            {
              node: {
                name: 'default.test_node',
                type: 'DIMENSION',
                currentVersion: 'v4.0',
                tags: [],
                editedBy: ['dj'],
                current: {
                  displayName: 'Test Node',
                  status: 'VALID',
                  mode: 'PUBLISHED',
                  updatedAt: '2024-10-18T15:15:33.532949+00:00',
                },
                createdBy: {
                  username: 'dj',
                },
              },
            },
          ],
        },
      },
    });
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('displays namespaces and renders nodes', async () => {
    reloadFn();
    const element = (
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <NamespacePage />
      </DJClientContext.Provider>
    );
    render(
      <MemoryRouter initialEntries={['/namespaces/test.namespace']}>
        <Routes>
          <Route path="namespaces/:namespace" element={element} />
        </Routes>
      </MemoryRouter>,
    );

    // Wait for initial nodes to load
    await waitFor(() => {
      expect(mockDjClient.listNodesForLanding).toHaveBeenCalled();
      expect(screen.getByText('Namespaces')).toBeInTheDocument();
    });

    // Check that it displays namespaces
    expect(screen.getByText('common')).toBeInTheDocument();
    expect(screen.getByText('one')).toBeInTheDocument();
    expect(screen.getByText('fruits')).toBeInTheDocument();
    expect(screen.getByText('vegetables')).toBeInTheDocument();

    // Check that it renders nodes
    expect(screen.getByText('Test Node')).toBeInTheDocument();

    // --- Sorting ---

    // Track current call count
    const initialCallCount = mockDjClient.listNodesForLanding.mock.calls.length;

    // sort by 'name'
    fireEvent.click(screen.getByText('name'));
    await waitFor(() => {
      expect(
        mockDjClient.listNodesForLanding.mock.calls.length,
      ).toBeGreaterThan(initialCallCount);
    });

    const afterFirstSort = mockDjClient.listNodesForLanding.mock.calls.length;

    // flip direction
    fireEvent.click(screen.getByText('name'));
    await waitFor(() => {
      expect(
        mockDjClient.listNodesForLanding.mock.calls.length,
      ).toBeGreaterThan(afterFirstSort);
    });

    const afterSecondSort = mockDjClient.listNodesForLanding.mock.calls.length;

    // sort by 'displayName'
    fireEvent.click(screen.getByText('display Name'));
    await waitFor(() => {
      expect(
        mockDjClient.listNodesForLanding.mock.calls.length,
      ).toBeGreaterThan(afterSecondSort);
    });

    // --- Filters ---

    // Node type - use react-select properly
    const selectNodeType = screen.getAllByTestId('select-node-type')[0];
    const typeInput = selectNodeType.querySelector('input');
    if (typeInput) {
      fireEvent.focus(typeInput);
      fireEvent.keyDown(typeInput, { key: 'ArrowDown' });
      await waitFor(() => {
        const sourceOption = screen.queryByText('Source');
        if (sourceOption) {
          fireEvent.click(sourceOption);
        }
      });
    }

    // Tag filter
    const selectTag = screen.getAllByTestId('select-tag')[0];
    const tagInput = selectTag.querySelector('input');
    if (tagInput) {
      fireEvent.focus(tagInput);
      fireEvent.keyDown(tagInput, { key: 'ArrowDown' });
    }

    // User filter
    const selectUser = screen.getAllByTestId('select-user')[0];
    const userInput = selectUser.querySelector('input');
    if (userInput) {
      fireEvent.focus(userInput);
      fireEvent.keyDown(userInput, { key: 'ArrowDown' });
    }

    // --- Expand/Collapse Namespace ---
    fireEvent.click(screen.getByText('common'));
    fireEvent.click(screen.getByText('common'));
  });

  it('can add new namespace via inline creation', async () => {
    // Mock window.location to track navigation
    delete window.location;
    window.location = { href: jest.fn() };
    Object.defineProperty(window.location, 'href', {
      set: jest.fn(),
      get: jest.fn(),
    });

    mockDjClient.addNamespace.mockReturnValue({
      status: 201,
      json: {},
    });
    const element = (
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <NamespacePage />
      </DJClientContext.Provider>
    );
    render(
      <MemoryRouter initialEntries={['/namespaces/default']}>
        <Routes>
          <Route path="namespaces/:namespace" element={element} />
        </Routes>
      </MemoryRouter>,
    );

    // Wait for namespaces to load
    await waitFor(() => {
      expect(screen.getByText('default')).toBeInTheDocument();
    });

    // Find the namespace and hover to reveal add button
    const defaultNamespace = screen
      .getByText('default')
      .closest('.select-name');
    fireEvent.mouseEnter(defaultNamespace);

    // Find the add namespace button (it exists but is hidden, so use getAllByTitle)
    const addButtons = screen.getAllByTitle('Add child namespace');
    const defaultAddButton = addButtons.find(btn =>
      btn
        .closest('.namespace-item')
        ?.querySelector('a[href="/namespaces/default"]'),
    );

    expect(defaultAddButton).toBeInTheDocument();
    fireEvent.click(defaultAddButton);

    // Type in the new namespace name
    await waitFor(() => {
      const input = screen.getByPlaceholderText('New namespace name');
      expect(input).toBeInTheDocument();
    });

    const input = screen.getByPlaceholderText('New namespace name');
    await userEvent.type(input, 'new_child');

    // Submit the form
    const submitButton = screen.getByRole('button', { name: '✓' });
    fireEvent.click(submitButton);

    await waitFor(() => {
      expect(mockDjClient.addNamespace).toHaveBeenCalledWith(
        'default.new_child',
      );
    });
  });

  it('can fail to add namespace', async () => {
    mockDjClient.addNamespace.mockReturnValue({
      status: 500,
      json: { message: 'you failed' },
    });
    const element = (
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <NamespacePage />
      </DJClientContext.Provider>
    );
    render(
      <MemoryRouter initialEntries={['/namespaces/default']}>
        <Routes>
          <Route path="namespaces/:namespace" element={element} />
        </Routes>
      </MemoryRouter>,
    );

    // Wait for namespaces to load
    await waitFor(() => {
      expect(screen.getByText('default')).toBeInTheDocument();
    });

    // Find the namespace and hover to reveal add button
    const defaultNamespace = screen
      .getByText('default')
      .closest('.select-name');
    fireEvent.mouseEnter(defaultNamespace);

    // Find the add namespace button (it exists but is hidden, so use getAllByTitle)
    const addButtons = screen.getAllByTitle('Add child namespace');
    const defaultAddButton = addButtons.find(btn =>
      btn
        .closest('.namespace-item')
        ?.querySelector('a[href="/namespaces/default"]'),
    );

    expect(defaultAddButton).toBeInTheDocument();
    fireEvent.click(defaultAddButton);

    // Type in the new namespace name
    await waitFor(() => {
      const input = screen.getByPlaceholderText('New namespace name');
      expect(input).toBeInTheDocument();
    });

    const input = screen.getByPlaceholderText('New namespace name');
    await userEvent.type(input, 'bad_namespace');

    // Submit the form
    const submitButton = screen.getByRole('button', { name: '✓' });
    fireEvent.click(submitButton);

    // Should display failure alert
    await waitFor(() => {
      expect(screen.getByText('you failed')).toBeInTheDocument();
    });
  });

  describe('Filter Bar', () => {
    it('displays quick filter presets', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('Quick:')).toBeInTheDocument();
      });

      // Check that preset buttons are rendered
      expect(screen.getByText('My Nodes')).toBeInTheDocument();
      expect(screen.getByText('Needs Attention')).toBeInTheDocument();
      expect(screen.getByText('Drafts')).toBeInTheDocument();
    });

    it('applies My Nodes preset when clicked', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('My Nodes')).toBeInTheDocument();
      });

      const initialCalls = mockDjClient.listNodesForLanding.mock.calls.length;
      fireEvent.click(screen.getByText('My Nodes'));

      await waitFor(() => {
        // The API should be called again after clicking preset
        expect(
          mockDjClient.listNodesForLanding.mock.calls.length,
        ).toBeGreaterThan(initialCalls);
      });
    });

    it('applies Needs Attention preset when clicked', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('Needs Attention')).toBeInTheDocument();
      });

      const initialCalls = mockDjClient.listNodesForLanding.mock.calls.length;
      fireEvent.click(screen.getByText('Needs Attention'));

      await waitFor(() => {
        expect(
          mockDjClient.listNodesForLanding.mock.calls.length,
        ).toBeGreaterThan(initialCalls);
      });
    });

    it('applies Drafts preset when clicked', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('Drafts')).toBeInTheDocument();
      });

      const initialCalls = mockDjClient.listNodesForLanding.mock.calls.length;
      fireEvent.click(screen.getByText('Drafts'));

      await waitFor(() => {
        expect(
          mockDjClient.listNodesForLanding.mock.calls.length,
        ).toBeGreaterThan(initialCalls);
      });
    });

    it('shows Clear all button when filters are active', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('My Nodes')).toBeInTheDocument();
      });

      // Apply a preset to activate filters
      fireEvent.click(screen.getByText('My Nodes'));

      await waitFor(() => {
        expect(screen.getByText('Clear all ×')).toBeInTheDocument();
      });
    });

    it('clears all filters when Clear all is clicked', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('My Nodes')).toBeInTheDocument();
      });

      // Apply a preset
      fireEvent.click(screen.getByText('My Nodes'));

      await waitFor(() => {
        expect(screen.getByText('Clear all ×')).toBeInTheDocument();
      });

      // Clear all filters
      fireEvent.click(screen.getByText('Clear all ×'));

      await waitFor(() => {
        // Clear all button should disappear
        expect(screen.queryByText('Clear all ×')).not.toBeInTheDocument();
      });
    });

    it('displays filter dropdowns', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        // Check for filter labels
        expect(screen.getByText('Type')).toBeInTheDocument();
        expect(screen.getByText('Tags')).toBeInTheDocument();
        expect(screen.getByText('Edited By')).toBeInTheDocument();
        expect(screen.getByText('Mode')).toBeInTheDocument();
        expect(screen.getByText('Owner')).toBeInTheDocument();
        expect(screen.getByText('Status')).toBeInTheDocument();
        expect(screen.getByText('Quality')).toBeInTheDocument();
      });
    });

    it('opens Quality dropdown when clicked', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('Quality')).toBeInTheDocument();
      });

      // Find and click the Quality button
      const qualityButton = screen.getByText('Issues');
      fireEvent.click(qualityButton);

      await waitFor(() => {
        expect(screen.getByText('Missing Description')).toBeInTheDocument();
        expect(screen.getByText('Orphaned Dimensions')).toBeInTheDocument();
        expect(screen.getByText('Has Materialization')).toBeInTheDocument();
      });
    });

    it('toggles quality filters in dropdown', async () => {
      renderWithProviders(<NamespacePage />);

      await waitFor(() => {
        expect(screen.getByText('Quality')).toBeInTheDocument();
      });

      // Open the Quality dropdown
      const qualityButton = screen.getByText('Issues');
      fireEvent.click(qualityButton);

      await waitFor(() => {
        expect(screen.getByText('Missing Description')).toBeInTheDocument();
      });

      // Toggle the Missing Description checkbox
      const checkbox = screen.getByLabelText('Missing Description');
      const callsBefore = mockDjClient.listNodesForLanding.mock.calls.length;
      fireEvent.click(checkbox);

      await waitFor(() => {
        expect(
          mockDjClient.listNodesForLanding.mock.calls.length,
        ).toBeGreaterThan(callsBefore);
      });
    });

    it('displays no nodes message with clear filter link when no results', async () => {
      mockDjClient.listNodesForLanding.mockResolvedValue({
        data: {
          findNodesPaginated: {
            pageInfo: {
              hasNextPage: false,
              endCursor: null,
              hasPrevPage: false,
              startCursor: null,
            },
            edges: [],
          },
        },
      });

      renderWithProviders(<NamespacePage />);

      // Apply a filter first
      await waitFor(() => {
        expect(screen.getByText('My Nodes')).toBeInTheDocument();
      });
      fireEvent.click(screen.getByText('My Nodes'));

      await waitFor(() => {
        expect(
          screen.getByText('No nodes found with the current filters.'),
        ).toBeInTheDocument();
        expect(screen.getByText('Clear filters')).toBeInTheDocument();
      });
    });
  });

  describe('URL Parameter Sync', () => {
    it('reads filters from URL parameters on load', async () => {
      renderWithProviders(<NamespacePage />, {
        route: '/namespaces/default?type=metric&ownedBy=dj',
      });

      await waitFor(() => {
        expect(mockDjClient.listNodesForLanding).toHaveBeenCalled();
      });
    });

    it('reads status filter from URL', async () => {
      renderWithProviders(<NamespacePage />, {
        route: '/namespaces/default?statuses=INVALID',
      });

      await waitFor(() => {
        expect(mockDjClient.listNodesForLanding).toHaveBeenCalled();
      });
    });

    it('reads mode filter from URL', async () => {
      renderWithProviders(<NamespacePage />, {
        route: '/namespaces/default?mode=draft',
      });

      await waitFor(() => {
        expect(mockDjClient.listNodesForLanding).toHaveBeenCalled();
      });
    });

    it('reads quality filters from URL', async () => {
      renderWithProviders(<NamespacePage />, {
        route:
          '/namespaces/default?missingDescription=true&orphanedDimension=true',
      });

      await waitFor(() => {
        expect(mockDjClient.listNodesForLanding).toHaveBeenCalled();
      });
    });
  });
});
