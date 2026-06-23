import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import NamespaceTypeSummary from '../NamespaceTypeSummary';
import DJClientContext from '../../../providers/djclient';

function makeClient(countsByType) {
  return {
    listNodesForLanding: vi.fn((ns, [TYPE]) =>
      Promise.resolve({
        data: { findNodesPaginated: { totalCount: countsByType[TYPE] ?? 0 } },
      }),
    ),
  };
}

function renderSummary(djClient, namespace = 'member.cds.main') {
  return render(
    <DJClientContext.Provider value={{ DataJunctionAPI: djClient }}>
      <NamespaceTypeSummary namespace={namespace} />
    </DJClientContext.Provider>,
  );
}

beforeEach(() => vi.clearAllMocks());

describe('NamespaceTypeSummary', () => {
  it('lists only non-empty types and deep-links to the filtered list', async () => {
    const djClient = makeClient({ METRIC: 12, DIMENSION: 4 });
    renderSummary(djClient);

    const metrics = await screen.findByText('metrics');
    expect(metrics).toBeInTheDocument();
    expect(screen.getByText('Nodes')).toBeInTheDocument();
    expect(screen.getByText('12')).toBeInTheDocument();
    expect(metrics.closest('a').getAttribute('href')).toBe(
      '/namespaces/member.cds.main?type=metric',
    );

    expect(screen.getByText('dimensions')).toBeInTheDocument();
    expect(screen.getByText('4')).toBeInTheDocument();

    // Zero-count types are omitted.
    expect(screen.queryByText('cubes')).not.toBeInTheDocument();
    expect(screen.queryByText('transforms')).not.toBeInTheDocument();
    expect(screen.queryByText('sources')).not.toBeInTheDocument();
  });

  it('renders nothing when the namespace has no nodes', async () => {
    const djClient = makeClient({});
    const { container } = renderSummary(djClient);
    await waitFor(() => {
      expect(djClient.listNodesForLanding).toHaveBeenCalledTimes(5);
    });
    expect(screen.queryByText('Nodes')).not.toBeInTheDocument();
    expect(container.querySelector('.dj-ns-type-summary')).toBeNull();
  });
});
