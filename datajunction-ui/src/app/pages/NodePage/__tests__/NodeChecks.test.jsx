import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import DJClientContext from '../../../providers/djclient';
import NodeChecks from '../NodeChecks';

const node = { name: 'ns.orders' };

const renderWith = results => {
  const djClient = {
    DataJunctionAPI: { nodeChecks: vi.fn().mockResolvedValue(results) },
  };
  return render(
    <DJClientContext.Provider value={djClient}>
      <NodeChecks node={node} />
    </DJClientContext.Provider>,
  );
};

describe('NodeChecks', () => {
  it('says nothing is governing the node when no manifest declares checks', async () => {
    renderWith(null);
    await waitFor(() => {
      expect(screen.getByText('Not governed')).toBeInTheDocument();
    });
  });

  it('shows a tier the node meets', async () => {
    renderWith({
      node: 'ns.orders',
      checks: [
        { check: 'demo.described', verdict: 'passed', gate: 'warn' },
        { check: 'demo.wound_down', verdict: 'skipped', gate: 'block' },
      ],
      rulesets: [
        {
          ruleset: 'baseline',
          verdict: 'passed',
          checks: ['demo.described'],
        },
      ],
    });
    const badge = await screen.findByLabelText('GovernanceChecks');
    // The badge names the level reached, not every tier.
    expect(badge).toHaveTextContent('baseline');
  });

  it('says nothing applied when every check skipped', async () => {
    renderWith({
      node: 'ns.orders',
      checks: [{ check: 'demo.wound_down', verdict: 'skipped', gate: 'block' }],
      rulesets: [
        {
          ruleset: 'baseline',
          verdict: 'not_applicable',
          checks: [],
        },
      ],
    });
    await waitFor(() => {
      expect(screen.getByText('Not governed')).toBeInTheDocument();
    });
  });

  it('lists the failing checks when the badge is opened', async () => {
    renderWith({
      node: 'ns.orders',
      checks: [
        {
          check: 'demo.described',
          verdict: 'failed',
          gate: 'warn',
          description: 'Every node has a description.',
        },
        {
          check: 'demo.primary_key',
          verdict: 'failed',
          gate: 'block',
          description: 'Dimensions declare a primary key.',
        },
        { check: 'demo.tagged', verdict: 'passed', gate: 'warn' },
        { check: 'demo.wound_down', verdict: 'skipped', gate: 'block' },
      ],
      rulesets: [
        {
          ruleset: 'baseline',
          verdict: 'passed',
          checks: ['demo.tagged'],
        },
        {
          ruleset: 'certified',
          verdict: 'failed',
          // Repeats baseline's member: `includes` is expanded server-side.
          checks: ['demo.tagged', 'demo.described', 'demo.primary_key'],
        },
      ],
    });
    // The badge names the level held and the gap to the next one.
    const badge = await screen.findByLabelText('GovernanceChecks');
    expect(badge).toHaveTextContent('baseline');
    expect(badge).toHaveTextContent('2 from certified');

    await userEvent.click(badge);
    // The description leads; the identifier is secondary and unprefixed.
    await waitFor(() => {
      expect(
        screen.getByText('Every node has a description.'),
      ).toBeInTheDocument();
    });
    expect(
      screen.getByText('Dimensions declare a primary key.'),
    ).toBeInTheDocument();
    // The identifier is not display text; it stays as a tooltip for support.
    expect(screen.queryByText('described')).not.toBeInTheDocument();
    // The tier row carries the verdict, so each unmet item is plain text.
    expect(screen.queryByText('Expected:')).not.toBeInTheDocument();
  });
});

describe('NodeChecks tiers', () => {
  it('attributes a check to the first tier that claims it', async () => {
    renderWith({
      node: 'ns.orders',
      checks: [
        {
          check: 'demo.described',
          verdict: 'failed',
          gate: 'warn',
          description: 'Every node has a description.',
        },
        {
          check: 'demo.owners',
          verdict: 'failed',
          gate: 'warn',
          description: 'Certified nodes name two owners.',
        },
      ],
      rulesets: [
        {
          ruleset: 'baseline',
          verdict: 'failed',
          checks: ['demo.described'],
        },
        {
          // Expanded includes, so it repeats baseline's member.
          ruleset: 'certified',
          verdict: 'failed',
          checks: ['demo.described', 'demo.owners'],
        },
      ],
    });
    await userEvent.click(await screen.findByLabelText('GovernanceChecks'));
    await waitFor(() => {
      expect(
        screen.getByText('Every node has a description.'),
      ).toBeInTheDocument();
    });
    // Listed once, under baseline, not repeated under certified.
    expect(screen.getAllByText('Every node has a description.')).toHaveLength(
      1,
    );
    expect(
      screen.getByText('Certified nodes name two owners.'),
    ).toBeInTheDocument();
  });
});

describe('NodeChecks met-check disclosure', () => {
  it('reveals what a tier asked for only when its row is opened', async () => {
    renderWith({
      node: 'ns.orders',
      checks: [
        {
          check: 'demo.described',
          verdict: 'passed',
          gate: 'warn',
          description: 'Every node has a description.',
        },
      ],
      rulesets: [
        { ruleset: 'baseline', verdict: 'passed', checks: ['demo.described'] },
      ],
    });
    await userEvent.click(await screen.findByLabelText('GovernanceChecks'));
    // A met check is not listed until its tier is opened: the card is for
    // what still needs doing.
    expect(
      screen.queryByText('Every node has a description.'),
    ).not.toBeInTheDocument();

    await userEvent.click(screen.getByLabelText('tier baseline'));
    await waitFor(() => {
      expect(
        screen.getByText('Every node has a description.'),
      ).toBeInTheDocument();
    });
  });
});

describe('NodeChecks shape detection', () => {
  const check = (name, verdict) => ({
    check: name,
    verdict,
    gate: 'warn',
    description: `${name} description`,
  });

  it('tallies rulesets that do not contain one another', async () => {
    renderWith({
      node: 'ns.orders',
      checks: [check('demo.a', 'passed'), check('demo.b', 'failed')],
      rulesets: [
        { ruleset: 'pii', verdict: 'passed', checks: ['demo.a'] },
        { ruleset: 'finance', verdict: 'failed', checks: ['demo.b'] },
      ],
    });
    const badge = await screen.findByLabelText('GovernanceChecks');
    // Siblings have no order, so there is no level to name.
    expect(badge).toHaveTextContent('1/2 met');
    expect(badge).not.toHaveTextContent('from');
  });

  it('names a level when each ruleset contains the one before it', async () => {
    renderWith({
      node: 'ns.orders',
      checks: [check('demo.a', 'passed'), check('demo.b', 'failed')],
      rulesets: [
        { ruleset: 'baseline', verdict: 'passed', checks: ['demo.a'] },
        {
          ruleset: 'certified',
          verdict: 'failed',
          checks: ['demo.a', 'demo.b'],
        },
      ],
    });
    const badge = await screen.findByLabelText('GovernanceChecks');
    expect(badge).toHaveTextContent('baseline');
    expect(badge).toHaveTextContent('1 from certified');
  });
});

describe('NodeChecks nesting cue', () => {
  const entry = (name, verdict) => ({
    check: name,
    verdict,
    gate: 'warn',
    description: `${name} description`,
  });

  it('names the tier a building tier builds on', async () => {
    renderWith({
      node: 'ns.orders',
      checks: [entry('demo.a', 'passed'), entry('demo.b', 'passed')],
      rulesets: [
        { ruleset: 'baseline', verdict: 'passed', checks: ['demo.a'] },
        {
          ruleset: 'certified',
          verdict: 'passed',
          checks: ['demo.a', 'demo.b'],
        },
      ],
    });
    await userEvent.click(await screen.findByLabelText('GovernanceChecks'));
    // The row states the relationship, so no separate note is needed.
    await waitFor(() => {
      // Every tier reads met/total; a building tier just prefixes a plus.
      expect(screen.getByLabelText('tier certified')).toHaveTextContent(
        'baseline +1/1',
      );
    });
    expect(screen.getByLabelText('tier baseline')).toHaveTextContent('1/1');
  });

  it('names no predecessor when the rulesets are siblings', async () => {
    renderWith({
      node: 'ns.orders',
      checks: [entry('demo.a', 'passed'), entry('demo.b', 'passed')],
      rulesets: [
        { ruleset: 'pii', verdict: 'passed', checks: ['demo.a'] },
        { ruleset: 'finance', verdict: 'passed', checks: ['demo.b'] },
      ],
    });
    await userEvent.click(await screen.findByLabelText('GovernanceChecks'));
    expect(screen.getByLabelText('tier finance')).not.toHaveTextContent('+');
  });
});

describe('NodeChecks coverage of every check', () => {
  const entry = (name, verdict, gate = 'warn') => ({
    check: name,
    verdict,
    gate,
    description: `${name} description`,
  });

  it('shows a failing check that belongs to no ruleset', async () => {
    renderWith({
      node: 'ns.orders',
      checks: [entry('demo.loose', 'failed')],
      rulesets: [],
    });
    // Membership in a ruleset is optional, so grouping by ruleset alone
    // would drop this one entirely.
    await userEvent.click(await screen.findByLabelText('GovernanceChecks'));
    await waitFor(() => {
      expect(screen.getByText('demo.loose description')).toBeInTheDocument();
    });
  });

  it('colours a tier from its own gates, not another tier’s', async () => {
    renderWith({
      node: 'ns.orders',
      checks: [
        entry('demo.warn_only', 'failed', 'warn'),
        entry('demo.blocker', 'failed', 'block'),
      ],
      rulesets: [
        { ruleset: 'pii', verdict: 'failed', checks: ['demo.warn_only'] },
        { ruleset: 'finance', verdict: 'failed', checks: ['demo.blocker'] },
      ],
    });
    await userEvent.click(await screen.findByLabelText('GovernanceChecks'));
    const warnMark = screen
      .getByLabelText('tier pii')
      .querySelector('span[style*="color"]');
    const blockMark = screen
      .getByLabelText('tier finance')
      .querySelector('span[style*="color"]');
    // A blocking failure elsewhere must not make this tier look blocking.
    expect(warnMark).toHaveStyle({ color: '#8a6d00' });
    expect(blockMark).toHaveStyle({ color: '#b34b00' });
  });
});
