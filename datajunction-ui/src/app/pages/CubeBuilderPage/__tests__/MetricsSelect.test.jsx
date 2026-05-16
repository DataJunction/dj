import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { MetricsSelect } from '../MetricsSelect';
import DJClientContext from '../../../providers/djclient';
import React from 'react';

const renderInForm = ({ djClient, cube, onChange = () => {} }) =>
  render(
    <DJClientContext.Provider value={{ DataJunctionAPI: djClient }}>
      <MetricsSelect cube={cube} onChange={onChange} />
    </DJClientContext.Provider>,
  );

describe('MetricsSelect', () => {
  it('renders an empty state with the prompt to start typing', () => {
    const djClient = { searchMetrics: vi.fn(), getMetricsInfo: vi.fn() };
    renderInForm({ djClient });

    expect(screen.getByText('Type to search metrics...')).toBeInTheDocument();
    // No async fetches kick off until the user types.
    expect(djClient.searchMetrics).not.toHaveBeenCalled();
    expect(djClient.getMetricsInfo).not.toHaveBeenCalled();
  });

  it('pre-populates and enriches existing cube metrics on mount', async () => {
    const djClient = {
      searchMetrics: vi.fn(),
      getMetricsInfo: vi.fn().mockResolvedValue([
        {
          value: 'default.revenue',
          label: 'Revenue',
          name: 'default.revenue',
          gitInfo: { branch: 'feat-x', isDefaultBranch: false },
        },
      ]),
    };
    const cube = {
      current: {
        cubeMetrics: [{ name: 'default.revenue', displayName: 'Revenue' }],
      },
    };

    renderInForm({ djClient, cube });

    // The chip uses the displayName from the cube response.
    expect(await screen.findByText('Revenue')).toBeInTheDocument();
    // getMetricsInfo is called with the metric names so chips can render
    // a branch badge.
    await waitFor(() =>
      expect(djClient.getMetricsInfo).toHaveBeenCalledWith(['default.revenue']),
    );
  });

  it('queries searchMetrics and renders namespace-grouped results when the user types', async () => {
    const djClient = {
      searchMetrics: vi.fn().mockResolvedValue([
        {
          value: 'finance.total_revenue',
          label: 'Total Revenue',
          name: 'finance.total_revenue',
          gitInfo: { branch: 'main', isDefaultBranch: true },
        },
        {
          value: 'growth.signups',
          label: 'growth.signups',
          name: 'growth.signups',
          gitInfo: { branch: 'feat-x', isDefaultBranch: false },
        },
      ]),
      getMetricsInfo: vi.fn(),
    };

    const { container } = renderInForm({ djClient });
    const input = container.querySelector('input');
    expect(input).not.toBeNull();

    // Typing kicks off the debounced search.
    fireEvent.change(input, { target: { value: 'rev' } });
    await waitFor(
      () => expect(djClient.searchMetrics).toHaveBeenCalledWith('rev', 50),
      { timeout: 1500 },
    );

    // Both namespaces appear as group headings; the non-default branch
    // shows its branch badge.
    expect(await screen.findByText('finance')).toBeInTheDocument();
    expect(await screen.findByText('growth')).toBeInTheDocument();
    expect(await screen.findByText('feat-x')).toBeInTheDocument();
  });

  it('returns no options for queries shorter than 2 characters', async () => {
    const djClient = {
      searchMetrics: vi.fn(),
      getMetricsInfo: vi.fn(),
    };
    const { container } = renderInForm({ djClient });
    const input = container.querySelector('input');

    fireEvent.change(input, { target: { value: 'a' } });
    // The "type at least 2 characters" message is the noOptionsMessage path.
    expect(
      await screen.findByText('Type at least 2 characters to search'),
    ).toBeInTheDocument();
    expect(djClient.searchMetrics).not.toHaveBeenCalled();
  });

  it('returns [] and logs when searchMetrics throws', async () => {
    const djClient = {
      searchMetrics: vi.fn().mockRejectedValue(new Error('boom')),
      getMetricsInfo: vi.fn(),
    };
    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const { container } = renderInForm({ djClient });
    const input = container.querySelector('input');

    fireEvent.change(input, { target: { value: 'rev' } });
    await waitFor(() => expect(djClient.searchMetrics).toHaveBeenCalled(), {
      timeout: 1500,
    });
    expect(await screen.findByText('No metrics found')).toBeInTheDocument();
    errSpy.mockRestore();
  });

  it('handles a missing displayName by falling back to the metric name', async () => {
    const djClient = {
      searchMetrics: vi.fn(),
      getMetricsInfo: vi.fn().mockResolvedValue([]),
    };
    const cube = {
      current: {
        cubeMetrics: [{ name: 'default.no_display' }],
      },
    };

    renderInForm({ djClient, cube });
    expect(await screen.findByText('default.no_display')).toBeInTheDocument();
  });
});
