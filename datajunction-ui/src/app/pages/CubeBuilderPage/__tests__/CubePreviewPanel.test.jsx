import { render, screen, waitFor } from '@testing-library/react';
import { CubePreviewPanel } from '../CubePreviewPanel';
import DJClientContext from '../../../providers/djclient';
import React from 'react';

const renderPanel = ({ djClient, initialValues }) =>
  render(
    <DJClientContext.Provider value={{ DataJunctionAPI: djClient }}>
      <CubePreviewPanel
        metrics={initialValues.metrics}
        dimensions={initialValues.dimensions}
      />
    </DJClientContext.Provider>,
  );

describe('CubePreviewPanel', () => {
  it('shows the empty state when no metrics or dimensions are selected', () => {
    const djClient = { metricsV3: vi.fn() };
    renderPanel({ djClient, initialValues: { metrics: [], dimensions: [] } });

    expect(
      screen.getByText('Select metrics and dimensions to preview SQL'),
    ).toBeInTheDocument();
    // No SQL fetch should be issued for an empty selection.
    expect(djClient.metricsV3).not.toHaveBeenCalled();
  });

  it('renders the generated SQL once metricsV3 resolves', async () => {
    const djClient = {
      metricsV3: vi.fn().mockResolvedValue({ sql: 'SELECT 1', errors: [] }),
    };
    renderPanel({
      djClient,
      initialValues: {
        metrics: ['default.revenue'],
        dimensions: ['default.date'],
      },
    });

    // The fetch is debounced 500ms — wait for it via real time.
    await waitFor(
      () =>
        expect(djClient.metricsV3).toHaveBeenCalledWith(
          ['default.revenue'],
          ['default.date'],
          '',
        ),
      { timeout: 1500 },
    );
    // Once SQL arrives, the empty / loading / error states all disappear.
    await waitFor(
      () =>
        expect(
          screen.queryByText('Select metrics and dimensions to preview SQL'),
        ).not.toBeInTheDocument(),
      { timeout: 1500 },
    );
    expect(screen.queryByText('Generating SQL...')).not.toBeInTheDocument();
  });

  it('surfaces API errors returned alongside a 200 response', async () => {
    const djClient = {
      metricsV3: vi.fn().mockResolvedValue({ errors: ['boom'] }),
    };
    renderPanel({
      djClient,
      initialValues: {
        metrics: ['default.revenue'],
        dimensions: ['default.date'],
      },
    });
    expect(
      await screen.findByText('boom', {}, { timeout: 1500 }),
    ).toBeInTheDocument();
  });

  it('surfaces the message field when the API returns it instead of sql', async () => {
    const djClient = {
      metricsV3: vi.fn().mockResolvedValue({ message: 'no metrics' }),
    };
    renderPanel({
      djClient,
      initialValues: {
        metrics: ['default.revenue'],
        dimensions: ['default.date'],
      },
    });
    expect(
      await screen.findByText('no metrics', {}, { timeout: 1500 }),
    ).toBeInTheDocument();
  });

  it('surfaces thrown errors from metricsV3', async () => {
    const djClient = {
      metricsV3: vi.fn().mockRejectedValue(new Error('network down')),
    };
    renderPanel({
      djClient,
      initialValues: {
        metrics: ['default.revenue'],
        dimensions: ['default.date'],
      },
    });
    expect(
      await screen.findByText('network down', {}, { timeout: 1500 }),
    ).toBeInTheDocument();
  });
});
