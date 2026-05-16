import { render, screen, waitFor } from '@testing-library/react';
import { DimensionsSelect } from '../DimensionsSelect';
import DJClientContext from '../../../providers/djclient';
import React from 'react';

const renderInForm = ({
  djClient,
  cube,
  initialValues = { metrics: [], dimensions: [] },
  onChange = () => {},
}) =>
  render(
    <DJClientContext.Provider value={{ DataJunctionAPI: djClient }}>
      <DimensionsSelect
        cube={cube}
        metrics={initialValues.metrics}
        onChange={onChange}
      />
    </DJClientContext.Provider>,
  );

describe('DimensionsSelect', () => {
  it('renders nothing when no metrics are selected', () => {
    const djClient = { commonDimensions: vi.fn() };
    const { container } = renderInForm({ djClient });
    expect(container.firstChild).toBeNull();
    expect(djClient.commonDimensions).not.toHaveBeenCalled();
  });

  it('groups dimensions by hop distance', async () => {
    const djClient = {
      commonDimensions: vi.fn().mockResolvedValue([
        // Direct dimension (path length 0)
        {
          name: 'default.event.event_type',
          node_name: 'default.event',
          node_display_name: 'Event',
          attribute: 'event_type',
          properties: [],
          path: [],
        },
        // 2-hop dimension
        {
          name: 'default.user.country',
          node_name: 'default.user',
          node_display_name: 'User',
          attribute: 'country',
          properties: [],
          path: ['default.event.user_id', 'default.user.id'],
        },
      ]),
    };

    renderInForm({
      djClient,
      initialValues: {
        metrics: ['default.events'],
        dimensions: [],
      },
    });

    // Hop labels render once the fetch resolves.
    expect(await screen.findByText('Direct Dimensions')).toBeInTheDocument();
    expect(await screen.findByText('2 Hops Away')).toBeInTheDocument();

    await waitFor(() =>
      expect(djClient.commonDimensions).toHaveBeenCalledWith([
        'default.events',
      ]),
    );
  });

  it('uses singular "1 Hop Away" label for path length 1', async () => {
    const djClient = {
      commonDimensions: vi.fn().mockResolvedValue([
        {
          name: 'default.user.country',
          node_name: 'default.user',
          node_display_name: 'User',
          attribute: 'country',
          properties: [],
          path: ['default.event.user_id'],
        },
      ]),
    };

    renderInForm({
      djClient,
      initialValues: { metrics: ['default.events'], dimensions: [] },
    });

    expect(await screen.findByText('1 Hop Away')).toBeInTheDocument();
  });

  it('pre-fills selected dimensions when editing an existing cube', async () => {
    const djClient = {
      commonDimensions: vi.fn().mockResolvedValue([
        {
          name: 'default.event.event_type',
          node_name: 'default.event',
          node_display_name: 'Event',
          attribute: 'event_type',
          properties: ['primary_key'],
          path: [],
        },
      ]),
    };
    const cube = {
      current: {
        cubeDimensions: [
          {
            name: 'default.event.event_type',
            attribute: 'event_type',
            properties: ['primary_key'],
          },
        ],
      },
    };

    renderInForm({
      djClient,
      cube,
      initialValues: { metrics: ['default.events'], dimensions: [] },
    });

    // The PK suffix is appended to the chip label.
    expect(
      await screen.findByText(content => content.includes('(PK)')),
    ).toBeInTheDocument();
  });

  it('shows the role suffix on chip labels for role-aliased cube dimensions', async () => {
    const djClient = {
      commonDimensions: vi.fn().mockResolvedValue([
        {
          name: 'default.user_dim.country_code[birth_country]',
          node_name: 'default.user_dim',
          node_display_name: 'User Dim',
          attribute: 'country_code',
          properties: [],
          path: [],
        },
      ]),
    };
    const cube = {
      current: {
        cubeDimensions: [
          {
            name: 'default.user_dim.country_code[birth_country]',
            attribute: 'country_code',
            role: 'birth_country',
            properties: [],
          },
        ],
      },
    };

    renderInForm({
      djClient,
      cube,
      initialValues: { metrics: ['default.users'], dimensions: [] },
    });

    // Role surfaces as a "[birth_country]" suffix on the labelized attribute.
    expect(
      await screen.findByText(content =>
        content.includes('Country Code [birth_country]'),
      ),
    ).toBeInTheDocument();
  });

  it('renders the role suffix on the chip even for selected role-aliased options', async () => {
    // Two role-aliased instances of the same attribute show as two distinct
    // chips, distinguishable by the role suffix in the label.
    const djClient = {
      commonDimensions: vi.fn().mockResolvedValue([
        {
          name: 'default.user_dim.country_code[birth_country]',
          node_name: 'default.user_dim',
          node_display_name: 'User Dim',
          attribute: 'country_code',
          properties: [],
          path: [],
        },
        {
          name: 'default.user_dim.country_code[residence_country]',
          node_name: 'default.user_dim',
          node_display_name: 'User Dim',
          attribute: 'country_code',
          properties: [],
          path: [],
        },
      ]),
    };
    const cube = {
      current: {
        cubeDimensions: [
          {
            name: 'default.user_dim.country_code[birth_country]',
            attribute: 'country_code',
            role: 'birth_country',
            properties: [],
          },
          {
            name: 'default.user_dim.country_code[residence_country]',
            attribute: 'country_code',
            role: 'residence_country',
            properties: [],
          },
        ],
      },
    };

    renderInForm({
      djClient,
      cube,
      initialValues: { metrics: ['default.users'], dimensions: [] },
    });

    expect(
      await screen.findByText(c => c.includes('Country Code [birth_country]')),
    ).toBeInTheDocument();
    expect(
      await screen.findByText(c =>
        c.includes('Country Code [residence_country]'),
      ),
    ).toBeInTheDocument();
  });
});
