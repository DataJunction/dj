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

  it('should render git source badge when source type is git with branch', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 5,
        primary_source: {
          type: 'git',
          repository: 'github.com/test/repo',
          branch: 'main',
        },
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

  it('should render git source badge when source type is git without branch', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 3,
        primary_source: {
          type: 'git',
          repository: 'github.com/test/repo',
          branch: null,
        },
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

    // Should render CI badge for git source even without branch
    expect(screen.getByText(/CI/)).toBeInTheDocument();
  });

  it('should render local source badge when source type is local', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 2,
        primary_source: {
          type: 'local',
          hostname: 'localhost',
        },
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

  it('should not render badge when no deployments', async () => {
    const mockDjClient = {
      namespaceSources: jest.fn().mockResolvedValue({
        total_deployments: 0,
        primary_source: null,
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
