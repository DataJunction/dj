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
    mockDjClient.listNodesForLanding.mockResolvedValue(
      {
        "data": {
          "findNodesPaginated": {
              "pageInfo": {
                  "hasNextPage": false,
                  "endCursor": "eyJjcmVhdGVkX2F0IjogIjIwMjQtMDQtMTZUMjM6MjI6MjIuNDQxNjg2KzAwOjAwIiwgImlkIjogNjE0fQ==",
                  "hasPrevPage": false,
                  "startCursor": "eyJjcmVhdGVkX2F0IjogIjIwMjQtMTAtMTZUMTY6MDM6MTcuMDgzMjY3KzAwOjAwIiwgImlkIjogMjQwOX0="
              },
              "edges": [
                  {
                      "node": {
                          "name": "default.test_node",
                          "type": "DIMENSION",
                          "currentVersion": "v4.0",
                          "tags": [],
                          "editedBy": [
                              "dj",
                          ],
                          "current": {
                              "displayName": "Test Node",
                              "status": "VALID",
                              "updatedAt": "2024-10-18T15:15:33.532949+00:00"
                          },
                          "createdBy": {
                              "username": "dj"
                          }
                      }
                  },
              ]
          }
        }
      }
    );
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

    await waitFor(() => {
      expect(mockDjClient.listNodesForLanding).toHaveBeenCalledTimes(2);
      expect(screen.getByText('Namespaces')).toBeInTheDocument();

      // check that it displays namespaces
      expect(screen.getByText('common')).toBeInTheDocument();
      expect(screen.getByText('one')).toBeInTheDocument();
      expect(screen.getByText('fruits')).toBeInTheDocument();
      expect(screen.getByText('vegetables')).toBeInTheDocument();

      // check that it renders nodes
      expect(screen.getByText('Test Node')).toBeInTheDocument();

      // check that it sorts nodes
      fireEvent.click(screen.getByText('name'));
      fireEvent.click(screen.getByText('display Name'));

      // check that we can filter by node type
      const selectNodeType = screen.getAllByTestId('select-node-type')[0];
      expect(selectNodeType).toBeDefined();
      expect(selectNodeType).not.toBeNull();
      fireEvent.keyDown(selectNodeType.firstChild, { key: 'ArrowDown' });
      fireEvent.click(screen.getByText('Source'));

      // check that we can filter by tag
      const selectTag = screen.getAllByTestId('select-tag')[0];
      expect(selectTag).toBeDefined();
      expect(selectTag).not.toBeNull();
      fireEvent.keyDown(selectTag.firstChild, { key: 'ArrowDown' });

      // check that we can filter by user
      const selectUser = screen.getAllByTestId('select-user')[0];
      expect(selectUser).toBeDefined();
      expect(selectUser).not.toBeNull();
      fireEvent.keyDown(selectUser.firstChild, { key: 'ArrowDown' });
      // fireEvent.click(screen.getByText('dj'));

      // click to open and close tab
      fireEvent.click(screen.getByText('common'));
      fireEvent.click(screen.getByText('common'));
    });
  });

  it('can add new namespace via add namespace popover', async () => {
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
      <MemoryRouter initialEntries={['/namespaces/test.namespace']}>
        <Routes>
          <Route path="namespaces/:namespace" element={element} />
        </Routes>
      </MemoryRouter>,
    );

    // Find the button to toggle the add namespace popover
    const addNamespaceToggle = screen.getByRole('button', {
      name: 'AddNamespaceTogglePopover',
    });
    expect(addNamespaceToggle).toBeInTheDocument();

    // Click the toggle and verify that the popover displays
    fireEvent.click(addNamespaceToggle);
    const addNamespacePopover = screen.getByRole('dialog', {
      name: 'AddNamespacePopover',
    });
    expect(addNamespacePopover).toBeInTheDocument();

    // Type in the new namespace
    await userEvent.type(
      screen.getByLabelText('Namespace'),
      'some.random.namespace',
    );

    // Save
    const saveNamespace = screen.getByRole('button', {
      name: 'SaveNamespace',
    });
    await waitFor(() => {
      fireEvent.click(saveNamespace);
    });
    expect(mockDjClient.addNamespace).toHaveBeenCalled();
    expect(mockDjClient.addNamespace).toHaveBeenCalledWith(
      'test.namespace.some.random.namespace',
    );
    expect(screen.getByText('Saved')).toBeInTheDocument();
    expect(window.location.reload).toHaveBeenCalled();
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
      <MemoryRouter initialEntries={['/namespaces/test.namespace']}>
        <Routes>
          <Route path="namespaces/:namespace" element={element} />
        </Routes>
      </MemoryRouter>,
    );

    // Open the add namespace popover
    const addNamespaceToggle = screen.getByRole('button', {
      name: 'AddNamespaceTogglePopover',
    });
    fireEvent.click(addNamespaceToggle);

    // Type in the new namespace
    await userEvent.type(
      screen.getByLabelText('Namespace'),
      'some.random.namespace',
    );

    // Save
    const saveNamespace = screen.getByRole('button', {
      name: 'SaveNamespace',
    });
    await waitFor(() => {
      fireEvent.click(saveNamespace);
    });

    // Should display failure alert
    expect(screen.getByText('you failed')).toBeInTheDocument();
  });
});
