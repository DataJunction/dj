import * as React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { createRenderer } from 'react-test-renderer/shallow';
import { MemoryRouter } from 'react-router-dom';

import NamespaceHeader from '../NamespaceHeader';
import DJClientContext from '../../providers/djclient';

const renderer = createRenderer();

describe('<NamespaceHeader />', () => {
  it('should render and match the snapshot', () => {
    renderer.render(<NamespaceHeader namespace="shared.dimensions.accounts" />);
    const renderedOutput = renderer.getRenderOutput();
    expect(renderedOutput).toMatchSnapshot();
  });

  it('should render git source badge when source type is git', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 5,
        has_multiple_sources: false,
        primary_source: {
          type: 'git',
          repository: 'github.com/test/repo',
          branch: 'main',
        },
        sources: [
          {
            type: 'git',
            repository: 'github.com/test/repo',
            branch: 'main',
          },
        ],
      }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(mockDjClient.namespaceSources).toHaveBeenCalledWith(
        'test.namespace',
      );
    });

    // Should render CI badge for git source
    expect(screen.getByText(/CI/)).toBeInTheDocument();
  });

  it('should render local source badge when source type is local', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 2,
        has_multiple_sources: false,
        primary_source: {
          type: 'local',
          hostname: 'localhost',
        },
        sources: [
          {
            type: 'local',
            hostname: 'localhost',
          },
        ],
      }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(mockDjClient.namespaceSources).toHaveBeenCalledWith(
        'test.namespace',
      );
    });

    // Should render Local badge for local source
    expect(screen.getByText(/Local/)).toBeInTheDocument();
  });

  it('should render warning badge when multiple sources exist', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 10,
        has_multiple_sources: true,
        primary_source: {
          type: 'git',
          repository: 'github.com/test/repo',
        },
        sources: [
          { type: 'git', repository: 'github.com/test/repo' },
          { type: 'local', hostname: 'localhost' },
        ],
      }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(mockDjClient.namespaceSources).toHaveBeenCalledWith(
        'test.namespace',
      );
    });

    // Should render warning badge for multiple sources
    expect(screen.getByText(/2 sources/)).toBeInTheDocument();
  });

  it('should not render badge when no deployments', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 0,
        has_multiple_sources: false,
        primary_source: null,
        sources: [],
      }),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(mockDjClient.namespaceSources).toHaveBeenCalledWith(
        'test.namespace',
      );
    });

    // Should not render any source badge
    expect(screen.queryByText(/CI/)).not.toBeInTheDocument();
    expect(screen.queryByText(/Local/)).not.toBeInTheDocument();
  });

  it('should handle API error gracefully', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockRejectedValue(new Error('API Error')),
    };

    render(
      <MemoryRouter>
        <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
          <NamespaceHeader namespace="test.namespace" />
        </DJClientContext.Provider>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(mockDjClient.namespaceSources).toHaveBeenCalledWith(
        'test.namespace',
      );
    });

    // Should still render breadcrumb without badge
    expect(screen.getByText('test')).toBeInTheDocument();
    expect(screen.getByText('namespace')).toBeInTheDocument();
    expect(screen.queryByText(/CI/)).not.toBeInTheDocument();
  });
});
