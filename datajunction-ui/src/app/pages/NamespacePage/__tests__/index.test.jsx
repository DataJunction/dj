import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import DJClientContext from '../../../providers/djclient';
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

    // sort by 'name'
    fireEvent.click(screen.getByText('name'));
    await waitFor(() => {
      expect(mockDjClient.listNodesForLanding).toHaveBeenCalledTimes(2);
    });

    // flip direction
    fireEvent.click(screen.getByText('name'));
    await waitFor(() => {
      expect(mockDjClient.listNodesForLanding).toHaveBeenCalledTimes(3);
    });

    // sort by 'displayName'
    fireEvent.click(screen.getByText('display Name'));
    await waitFor(() => {
      expect(mockDjClient.listNodesForLanding).toHaveBeenCalledTimes(4);
    });

    // --- Filters ---

    // Node type
    const selectNodeType = screen.getAllByTestId('select-node-type')[0];
    fireEvent.keyDown(selectNodeType.firstChild, { key: 'ArrowDown' });
    fireEvent.click(screen.getByText('Source'));

    // Tag filter
    const selectTag = screen.getAllByTestId('select-tag')[0];
    fireEvent.keyDown(selectTag.firstChild, { key: 'ArrowDown' });

    // User filter
    const selectUser = screen.getAllByTestId('select-user')[0];
    fireEvent.keyDown(selectUser.firstChild, { key: 'ArrowDown' });

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
});
