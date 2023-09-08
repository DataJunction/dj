import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import DJClientContext from '../../../providers/djclient';
import { NamespacePage } from '../index';
import React from 'react';

// Mocking the DataJunctionAPI service methods
const mockDjClient = {
  namespaces: jest.fn(),
  namespace: jest.fn(),
};

describe('NamespacePage', () => {
  beforeEach(() => {
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
      },
    ]);
  });

  it('displays namespaces and renders nodes', async () => {
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
      expect(mockDjClient.namespaces).toHaveBeenCalledTimes(1);
      expect(screen.getByText('Namespaces')).toBeInTheDocument();

      // check that it displays namespaces
      expect(screen.getByText('common')).toBeInTheDocument();
      expect(screen.getByText('one')).toBeInTheDocument();
      expect(screen.getByText('fruits')).toBeInTheDocument();
      expect(screen.getByText('vegetables')).toBeInTheDocument();

      // check that it renders nodes
      expect(screen.getByText('Test Node')).toBeInTheDocument();
    });
  });

  afterEach(() => {
    jest.clearAllMocks();
  });
});
