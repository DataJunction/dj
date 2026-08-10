import { afterEach, beforeEach, describe, it, expect, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import {
  toMaterializationState,
  dayPartitionsBetween,
  partitionsBetween,
  coverageSquares,
  strategyBadge,
} from '../materializationState';
import MaterializationStatePanel, {
  summarize,
} from '../MaterializationStatePanel';

// Shapes below are trimmed copies of live responses from
// shared.game_health_metrics.cloud_games_session_success_cube; the config bodies
// (metrics, measures, combiners) are omitted because the adapter reads only
// `cube` and `lookback_window` from them.
const NODE = {
  name: 'shared.game_health_metrics.cloud_games_session_success_cube',
  display_name: 'Cloud Games Session Success',
  type: 'cube',
  version: 'v8.0',
  current_version: 'v8.0',
  columns: [
    { name: 'game_title_id', display_name: 'Game Title Id', partition: null },
    {
      name: 'shared.game_health_metrics.cloud_games_session_success_rolling.utc_date',
      display_name: 'Utc Date',
      partition: {
        type_: 'temporal',
        format: 'yyyyMMdd',
        granularity: 'day',
        expression: null,
      },
    },
  ],
};

const INCREMENTAL = {
  node_revision_id: 648812,
  name: 'druid_cube__incremental_time__shared.game_health_metrics.cloud_games_session_success_rolling.utc_date',
  job: 'DruidCubeMaterializationJob',
  schedule: '59 11 * * *',
  strategy: 'incremental_time',
  deactivated_at: null,
  config: { cube: { version: 'v8.0' }, lookback_window: '3 DAYS' },
  urls: [
    'https://data.netflix.net/maestro/prod/dj.shared.game_health_metrics.cloud_games_session_success_cube.v8.0.main',
    'https://data.netflix.net/maestro/prod/dj.shared.game_health_metrics.cloud_games_session_success_cube.v8.0.backfill',
  ],
  workflow_names: [],
};

const FULL = {
  node_revision_id: 648812,
  name: 'druid_cube__full__shared.game_health_metrics.cloud_games_session_success_rolling.utc_date',
  job: 'DruidCubeMaterializationJob',
  schedule: '0 6 * * *',
  strategy: 'full',
  deactivated_at: '2026-08-01T00:00:00+00:00',
  config: { cube: { version: 'v8.0' }, lookback_window: null },
  urls: [
    'https://data.netflix.net/maestro/prod/dj.shared.game_health_metrics.cloud_games_session_success_cube.v8.0.full',
  ],
  workflow_names: [],
};

const AVAILABILITY = {
  node_revision_id: 648812,
  node_version: 'v8.0',
  catalog: 'druid',
  schema_: 'datajunction',
  table:
    'dj__shared_game_health_metrics_cloud_games_session_success_cube_v8_0_cd70304ced94ac2e',
  valid_through_ts: 1786132800000,
  min_temporal_partition: ['20260806'],
  max_temporal_partition: ['20260807'],
  temporal_partitions: ['utc_date'],
  categorical_partitions: [],
  partitions: [],
  links: {
    'Data Explorer': 'https://explorer.prod.netflix.net/cube/?cube=x',
  },
};

const NOW = new Date('2026-08-09T12:00:00Z');

const NODE_SUMMARY = {
  name: 'shared.game_health_metrics.cloud_games_session_success_cube',
  displayName: 'Cloud Games Session Success',
  type: 'cube',
  version: 'v8.0',
  isCurrentVersion: true,
};

const PARTITION = {
  column: 'Utc Date',
  granularity: 'day',
  format: 'yyyyMMdd',
};

describe('toMaterializationState', () => {
  it('maps two materializations on one revision to distinguishable cards', () => {
    expect(
      toMaterializationState({
        node: NODE,
        materializations: [INCREMENTAL, FULL],
        availabilityStates: [AVAILABILITY],
        now: NOW,
      }),
    ).toEqual({
      node: NODE_SUMMARY,
      // One availability row, so one coverage. A trailing day is only a gap once
      // every materialization has had its last chance at it, so the incremental's
      // 3 DAY lookback keeps both trailing days out of `missing` even though the
      // full materialization on its own would call them behind.
      coverage: {
        target: { from: '20260806', through: '20260809' },
        covered: { from: '20260806', through: '20260807' },
        missing: [],
        notDueYet: ['20260808', '20260809'],
        coverageKnown: true,
      },
      materializations: [
        {
          name: INCREMENTAL.name,
          label: 'Druid Cube (incremental time)',
          engine: 'Druid cube',
          active: true,
          workflows: [
            { label: 'main', url: INCREMENTAL.urls[0] },
            { label: 'backfill', url: INCREMENTAL.urls[1] },
          ],
          intent: {
            schedule: '59 11 * * *',
            scheduleHuman: 'At 11:59 AM',
            timezone: null,
            strategy: 'incremental_time',
            lookbackWindow: '3 DAYS',
            partition: PARTITION,
          },
          outcome: {
            servingTable: AVAILABILITY.table,
            servingCatalog: 'druid.datajunction',
            validThrough: '2026-08-07T20:00:00.000Z',
            target: { from: '20260806', through: '20260809' },
            covered: { from: '20260806', through: '20260807' },
            // The 3 DAY lookback gives each partition two further runs, so
            // neither trailing day is a gap yet.
            missing: [],
            notDueYet: ['20260808', '20260809'],
            coverageKnown: true,
            links: [
              {
                label: 'Data Explorer',
                url: 'https://explorer.prod.netflix.net/cube/?cube=x',
              },
            ],
          },
          execution: null,
        },
        {
          name: FULL.name,
          label: 'Druid Cube (full)',
          engine: 'Druid cube',
          active: false,
          workflows: [{ label: 'full', url: FULL.urls[0] }],
          intent: {
            schedule: '0 6 * * *',
            scheduleHuman: 'At 06:00 AM',
            timezone: null,
            strategy: 'full',
            lookbackWindow: null,
            partition: PARTITION,
          },
          outcome: {
            servingTable: AVAILABILITY.table,
            servingCatalog: 'druid.datajunction',
            validThrough: '2026-08-07T20:00:00.000Z',
            target: { from: '20260806', through: '20260809' },
            covered: { from: '20260806', through: '20260807' },
            // No lookback: both runs have fired and produced nothing.
            missing: ['20260808', '20260809'],
            notDueYet: [],
            coverageKnown: true,
            links: [
              {
                label: 'Data Explorer',
                url: 'https://explorer.prod.netflix.net/cube/?cube=x',
              },
            ],
          },
          execution: null,
        },
      ],
    });
  });

  it('reports coverage as unknown when watermarks are absent', () => {
    const state = toMaterializationState({
      node: NODE,
      materializations: [INCREMENTAL],
      availabilityStates: [
        {
          ...AVAILABILITY,
          min_temporal_partition: [],
          max_temporal_partition: [],
        },
      ],
      now: NOW,
    });

    expect(state.materializations[0].outcome).toEqual({
      servingTable: AVAILABILITY.table,
      servingCatalog: 'druid.datajunction',
      validThrough: '2026-08-07T20:00:00.000Z',
      target: null,
      covered: null,
      missing: [],
      notDueYet: [],
      coverageKnown: false,
      links: [
        {
          label: 'Data Explorer',
          url: 'https://explorer.prod.netflix.net/cube/?cube=x',
        },
      ],
    });
  });

  it('reports an empty outcome when nothing has been built', () => {
    const state = toMaterializationState({
      node: NODE,
      materializations: [INCREMENTAL],
      availabilityStates: [],
      now: NOW,
    });

    expect(state.materializations[0].outcome).toEqual({
      servingTable: null,
      servingCatalog: null,
      validThrough: null,
      target: null,
      covered: null,
      missing: [],
      notDueYet: [],
      coverageKnown: false,
      links: [],
    });
  });

  it('falls back to the cube version when availability carries no revision id', () => {
    const state = toMaterializationState({
      node: NODE,
      materializations: [INCREMENTAL],
      availabilityStates: [{ ...AVAILABILITY, node_revision_id: undefined }],
      now: NOW,
    });

    expect(state.materializations[0].outcome.servingTable).toEqual(
      AVAILABILITY.table,
    );
  });

  it('never invents execution, whatever the workflow records say', () => {
    const state = toMaterializationState({
      node: NODE,
      materializations: [
        {
          ...INCREMENTAL,
          urls: ['https://maestro/main'],
          workflow_names: ['main'],
          workflow_status: 'active',
        },
      ],
      availabilityStates: [AVAILABILITY],
      now: NOW,
    });

    expect(state.materializations[0].execution).toBeNull();
  });
});

describe('dayPartitionsBetween', () => {
  it('steps by calendar day across a month boundary', () => {
    expect(dayPartitionsBetween('20260730', '20260802')).toEqual([
      '20260730',
      '20260731',
      '20260801',
      '20260802',
    ]);
  });

  it('returns nothing for unparseable partitions', () => {
    expect(dayPartitionsBetween('2026-07-30', '20260802')).toEqual([]);
  });
});

const outcome = overrides => ({
  servingTable: 'dj__cube',
  servingCatalog: 'druid.datajunction',
  validThrough: '2026-08-08T06:12:00.000Z',
  target: { from: '20260806', through: '20260808' },
  covered: { from: '20260806', through: '20260808' },
  missing: [],
  notDueYet: [],
  coverageKnown: true,
  links: [],
  ...overrides,
});

const intent = strategy => ({
  schedule: '0 6 * * *',
  scheduleHuman: 'At 06:00 AM',
  timezone: null,
  strategy,
  lookbackWindow: strategy === 'full' ? null : '3 DAYS',
  partition: PARTITION,
});

const execution = overrides => ({
  lastRun: {
    status: 'succeeded',
    startedAt: '2026-08-08T06:00:09.000Z',
    endedAt: '2026-08-08T06:11:52.000Z',
    attempt: 1,
    maxAttempts: 3,
    processingPartition: '20260808',
  },
  nextScheduledAt: '2026-08-09T06:00:00.000Z',
  inFlight: null,
  workflows: [],
  ...overrides,
});

describe('summarize', () => {
  it('calls a materialization healthy when coverage meets target', () => {
    expect(
      summarize({
        intent: intent('incremental_time'),
        outcome: outcome(),
        execution: execution(),
      }),
    ).toEqual({
      verdict: 'healthy',
      headline: 'On target',
      detail: 'Covered through 20260808, matching target.',
    });
  });

  it('stays healthy while the trailing days are still inside the lookback', () => {
    expect(
      summarize({
        intent: intent('incremental_time'),
        outcome: outcome({
          covered: { from: '20260806', through: '20260806' },
          notDueYet: ['20260807', '20260808'],
        }),
        execution: execution(),
      }),
    ).toEqual({
      verdict: 'healthy',
      headline: 'On target',
      detail: 'Covered through 20260806, target 20260808 — 2 days not due yet.',
    });
  });

  it('describes an incremental shortfall as partitions to backfill', () => {
    expect(
      summarize({
        intent: intent('incremental_time'),
        outcome: outcome({
          covered: { from: '20260806', through: '20260806' },
          missing: ['20260807'],
          notDueYet: ['20260808'],
        }),
        execution: execution(),
      }),
    ).toEqual({
      verdict: 'stale',
      headline: '1 partition to backfill',
      detail:
        'Covered through 20260806, target 20260808 — 1 partition to backfill.',
    });
  });

  // A full rebuild replaces the table wholesale, so there is no per-day hole to
  // backfill and the only remedy is another run.
  it('describes a full shortfall as days behind, not partitions', () => {
    expect(
      summarize({
        intent: intent('full'),
        outcome: outcome({
          covered: { from: '20260806', through: '20260806' },
          missing: ['20260807', '20260808'],
        }),
        execution: execution(),
      }),
    ).toEqual({
      verdict: 'stale',
      headline: '2 days behind',
      detail: 'Covered through 20260806, target 20260808 — 2 days behind.',
    });
  });

  it('qualifies the verdict rather than replacing it when no run was reported', () => {
    expect(
      summarize({
        intent: intent('full'),
        outcome: outcome({
          covered: { from: '20260806', through: '20260806' },
          missing: ['20260807', '20260808'],
        }),
        execution: null,
      }),
    ).toEqual({
      verdict: 'stale',
      headline: '2 days behind',
      detail:
        'Covered through 20260806, target 20260808 — 2 days behind. Run status unknown.',
    });
  });

  it('calls it failing when the last run failed, ahead of any coverage gap', () => {
    expect(
      summarize({
        intent: intent('incremental_time'),
        outcome: outcome({ missing: ['20260807'] }),
        execution: execution({
          lastRun: {
            status: 'failed',
            startedAt: '2026-08-07T06:00:12.000Z',
            endedAt: '2026-08-07T06:04:31.000Z',
            attempt: 3,
            maxAttempts: 3,
            processingPartition: '20260807',
          },
        }),
      }),
    ).toEqual({
      verdict: 'failing',
      headline: 'Last run failed',
      detail: 'Last run failed on partition 20260807, attempt 3 of 3.',
    });
  });

  it('calls it unknown when coverage cannot be judged', () => {
    expect(
      summarize({
        intent: intent('incremental_time'),
        outcome: outcome({
          target: null,
          covered: null,
          coverageKnown: false,
        }),
        execution: execution(),
      }),
    ).toEqual({
      verdict: 'unknown',
      headline: 'Coverage unknown',
      detail: "This materialization doesn't report which dates it covers.",
    });
  });

  it('carries the run qualifier through an unknown coverage verdict too', () => {
    expect(
      summarize({
        intent: intent('incremental_time'),
        outcome: outcome({
          target: null,
          covered: null,
          coverageKnown: false,
        }),
        execution: null,
      }),
    ).toEqual({
      verdict: 'unknown',
      headline: 'Coverage unknown',
      detail:
        "This materialization doesn't report which dates it covers. Run status unknown.",
    });
  });

  // The regression that motivated the reordering: with `execution` null on every
  // real materialization, leading with run status made both cards read "Unknown"
  // and discarded the coverage DJ had already computed.
  it('judges real adapter output on coverage despite execution being null', () => {
    const state = toMaterializationState({
      node: NODE,
      materializations: [INCREMENTAL, FULL],
      availabilityStates: [AVAILABILITY],
      now: NOW,
    });

    expect(state.materializations.map(summarize)).toEqual([
      {
        verdict: 'healthy',
        headline: 'On target',
        detail:
          'Covered through 20260807, target 20260809 — 2 days not due yet. Run status unknown.',
      },
      {
        verdict: 'stale',
        headline: '2 days behind',
        detail:
          'Covered through 20260807, target 20260809 — 2 days behind. Run status unknown.',
      },
    ]);
  });
});

describe('partitionsBetween', () => {
  it('steps by hour when the keys carry an hour', () => {
    expect(partitionsBetween('2026073122', '2026080101')).toEqual([
      '2026073122',
      '2026073123',
      '2026080100',
      '2026080101',
    ]);
  });

  it('refuses to enumerate across grains', () => {
    expect(partitionsBetween('20260731', '2026080101')).toEqual([]);
  });
});

/** A coverage outcome whose target spans `from`..`through`, everything else covered. */
const coverage = (from, through, overrides = {}) => ({
  ...outcome({
    target: { from, through },
    covered: { from, through },
    ...overrides,
  }),
});

describe('coverageSquares', () => {
  it('draws one square per partition at the four-day scale the live cube reports', () => {
    expect(
      coverageSquares(
        coverage('20260806', '20260810', {
          covered: { from: '20260806', through: '20260807' },
          missing: ['20260808'],
          notDueYet: ['20260809', '20260810'],
        }),
      ),
    ).toEqual([
      {
        state: 'covered',
        from: '20260806',
        through: '20260806',
        count: 1,
        label: '20260806: 1 covered',
      },
      {
        state: 'covered',
        from: '20260807',
        through: '20260807',
        count: 1,
        label: '20260807: 1 covered',
      },
      {
        state: 'behind',
        from: '20260808',
        through: '20260808',
        count: 1,
        label: '20260808: 1 behind',
      },
      {
        state: 'notDue',
        from: '20260809',
        through: '20260809',
        count: 1,
        label: '20260809: 1 not due yet',
      },
      {
        state: 'notDue',
        from: '20260810',
        through: '20260810',
        count: 1,
        label: '20260810: 1 not due yet',
      },
    ]);
  });

  // 61 is the first day count that will not fit one square per partition.
  it('switches to weekly buckets one partition past the per-partition limit', () => {
    const squares = coverageSquares(coverage('20260601', '20260731'));

    expect(squares).toHaveLength(9);
    expect(squares[0]).toEqual({
      state: 'covered',
      from: '20260601',
      through: '20260607',
      count: 7,
      label: '20260601–20260607: 7 covered',
    });
    // The remainder bucket is short rather than padded; 61 is not a multiple of 7.
    expect(squares[8]).toEqual({
      state: 'covered',
      from: '20260727',
      through: '20260731',
      count: 5,
      label: '20260727–20260731: 5 covered',
    });
  });

  // Worst-state-wins. DJ cannot report an interior hole today -- coverage is always a
  // contiguous prefix -- but the aggregation must not be the reason one stays hidden.
  it('lets a single behind partition colour its whole bucket', () => {
    const squares = coverageSquares(
      coverage('20260601', '20260731', { missing: ['20260610'] }),
    );

    expect(squares[1]).toEqual({
      state: 'behind',
      from: '20260608',
      through: '20260614',
      count: 1,
      label: '20260608–20260614: 1 behind',
    });
  });

  it('keeps a year of daily partitions inside the strip', () => {
    const squares = coverageSquares(
      coverage('20250801', '20260806', {
        covered: { from: '20250801', through: '20260802' },
        missing: ['20260803', '20260804'],
        notDueYet: ['20260805', '20260806'],
      }),
    );

    // 371 days at one square per month: the weekly rung would need 53 squares, which
    // is past the 25 cap.
    expect(squares).toHaveLength(13);
    expect(squares[0]).toEqual({
      state: 'covered',
      from: '20250801',
      through: '20250830',
      count: 30,
      label: '20250801–20250830: 30 covered',
    });
    // The trailing bucket holds nine covered days, two behind and two not due; behind
    // outranks both, and the count reports the behind days rather than the bucket size.
    expect(squares[12]).toEqual({
      state: 'behind',
      from: '20260727',
      through: '20260806',
      count: 2,
      label: '20260727–20260806: 2 behind',
    });
  });

  it('buckets hourly partitions by day', () => {
    const squares = coverageSquares(
      coverage('2026070800', '2026080700', {
        covered: { from: '2026070800', through: '2026080618' },
        missing: ['2026080619'],
        notDueYet: [
          '2026080620',
          '2026080621',
          '2026080622',
          '2026080623',
          '2026080700',
        ],
      }),
    );

    // 721 hours at one square per week: the daily rung needs 31 squares, past the cap.
    expect(squares).toHaveLength(5);
    expect(squares[0]).toEqual({
      state: 'covered',
      from: '2026070800',
      through: '2026071423',
      count: 168,
      label: '2026070800–2026071423: 168 covered',
    });
    // One behind hour out of forty-nine still reads as a behind bucket, which is the
    // property that matters: coarsening can hide a healthy edge, never a problem.
    expect(squares[4]).toEqual({
      state: 'behind',
      from: '2026080500',
      through: '2026080700',
      count: 1,
      label: '2026080500–2026080700: 1 behind',
    });
  });

  // Past the coarsest rung the bucket loses its calendar name, but the strip stays
  // bounded -- which is the property the layout depends on.
  it('stays bounded past the end of the bucket ladder', () => {
    const squares = coverageSquares(coverage('20000101', '20260101'));

    expect(squares).toHaveLength(25);
  });

  it('draws nothing when coverage cannot be judged', () => {
    expect(
      coverageSquares(
        outcome({ target: null, covered: null, coverageKnown: false }),
      ),
    ).toEqual([]);
  });
});

describe('strategyBadge', () => {
  it('folds the lookback into the incremental badge', () => {
    expect(strategyBadge(intent('incremental_time'))).toEqual(
      'incremental · 3d lookback',
    );
  });

  // A lookback is meaningless for a full rebuild, so the badge stays a single word.
  it('renders a full strategy as one word', () => {
    expect(strategyBadge(intent('full'))).toEqual('full');
  });

  it('drops the lookback clause when none is declared', () => {
    expect(
      strategyBadge({ ...intent('incremental_time'), lookbackWindow: null }),
    ).toEqual('incremental');
  });

  it('abbreviates a sub-day lookback', () => {
    expect(
      strategyBadge({
        ...intent('incremental_time'),
        lookbackWindow: '12 HOURS',
      }),
    ).toEqual('incremental · 12h lookback');
  });

  it('has nothing to show when no strategy is declared', () => {
    expect(strategyBadge({ ...intent('full'), strategy: null })).toBeNull();
  });
});

const FULL_NAME = `druid.datajunction.${AVAILABILITY.table}`;
const ELIDED = 'druid.datajunction/dj__…cube_v8_0_cd70304ced94ac2e';

const renderPanel = state =>
  render(
    <MemoryRouter>
      <MaterializationStatePanel state={state} />
    </MemoryRouter>,
  );

const panelState = (materializations, availability) =>
  toMaterializationState({
    node: NODE,
    materializations,
    availabilityStates: availability,
    now: NOW,
  });

describe('MaterializationStatePanel serving line', () => {
  // Freshness is relative, so the clock has to be pinned or the assertion rots.
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    vi.setSystemTime(NOW);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('collapses table, freshness and links onto one line', () => {
    const { container } = renderPanel(
      panelState([INCREMENTAL], [AVAILABILITY]),
    );

    expect(container.querySelector('.mat-serving').textContent).toEqual(
      `${ELIDED}⧉·updated 2d agoPreview →Data Explorer ↗`,
    );
    // Elided on screen, whole in `title` and on the clipboard.
    expect(container.querySelector('.mat-chip').getAttribute('title')).toEqual(
      FULL_NAME,
    );
    expect(screen.getByText('updated 2d ago').getAttribute('title')).toEqual(
      'valid through Fri 07 Aug 2026 20:00 GMT',
    );
  });

  // Preview leads: it is the reason a reader is looking at the serving table at all.
  it('links Preview at the node page’s own planner target, ahead of Data Explorer', () => {
    const { container } = renderPanel(
      panelState([INCREMENTAL], [AVAILABILITY]),
    );

    expect(
      [...container.querySelectorAll('.mat-serving__links a')].map(link => [
        link.textContent,
        link.getAttribute('href'),
      ]),
    ).toEqual([
      [
        'Preview →',
        '/planner?cube=shared.game_health_metrics.cloud_games_session_success_cube',
      ],
      ['Data Explorer ↗', 'https://explorer.prod.netflix.net/cube/?cube=x'],
    ]);
  });

  it('copies the fully qualified name and confirms it', async () => {
    // `userEvent.setup` installs its own `navigator.clipboard` stub, so the
    // component's write is read back rather than spied on.
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    renderPanel(panelState([INCREMENTAL], [AVAILABILITY]));

    await user.click(
      screen.getByRole('button', { name: `Copy table name ${FULL_NAME}` }),
    );

    expect(await navigator.clipboard.readText()).toEqual(FULL_NAME);
    await waitFor(() =>
      expect(screen.getByRole('status').textContent).toEqual('Copied'),
    );
  });

  it('says the update time is unknown when the availability row carries none', () => {
    const { container } = renderPanel(
      panelState([INCREMENTAL], [{ ...AVAILABILITY, valid_through_ts: null }]),
    );

    expect(container.querySelector('.mat-serving').textContent).toEqual(
      `${ELIDED}⧉·update time unknownPreview →Data Explorer ↗`,
    );
    expect(
      screen.getByText('update time unknown').getAttribute('title'),
    ).toBeNull();
  });

  it('says nothing is built when there is no availability row at all', () => {
    const { container } = renderPanel(panelState([INCREMENTAL], []));

    expect(container.querySelector('.mat-serving')).toBeNull();
    expect(container.querySelector('.mat-header__note').textContent).toEqual(
      'nothing built for this revision yet',
    );
  });
});

describe('MaterializationStatePanel copy', () => {
  it('names a table row by engine and strategy badge, not by the folded label', () => {
    const { container } = renderPanel(
      panelState([INCREMENTAL, FULL], [AVAILABILITY]),
    );

    expect(
      [...container.querySelectorAll('.mat-table__label')].map(
        row => row.textContent,
      ),
    ).toEqual([
      'Druid cubeincremental · 3d lookback',
      'Druid cubefullinactive',
    ]);
  });

  // "No watermarks" named an internal concept; a reader only needs to know DJ cannot
  // tell them what is covered.
  it('calls unjudgeable coverage unknown in both A and C', async () => {
    const user = userEvent.setup();
    const { container } = renderPanel(
      panelState(
        [INCREMENTAL],
        [
          {
            ...AVAILABILITY,
            min_temporal_partition: [],
            max_temporal_partition: [],
          },
        ],
      ),
    );

    // A states it once, on the cube's Coverage row, and draws no strip beside it:
    // an empty strip reads as "nothing is covered" rather than "this is not known".
    expect(
      container.querySelector('.mat-verdict--unknown').textContent,
    ).toEqual('○ Coverage unknown');
    expect(container.querySelector('.mat-squares')).toEqual(null);
    await user.click(screen.getByTitle('Stacked rows'));
    expect(container.querySelector('.coverage--unknown').textContent).toEqual(
      'coverage unknown',
    );
    expect(container.querySelector('.mat-stack__verdict').textContent).toEqual(
      "This materialization doesn't report which dates it covers. Run status unknown.",
    );
  });

  it('reports an absent run without mentioning the query service', async () => {
    const user = userEvent.setup();
    const { container } = renderPanel(
      panelState([INCREMENTAL], [AVAILABILITY]),
    );

    await user.click(screen.getByTitle('Master / detail'));

    expect(
      [...container.querySelectorAll('.mat-block')].map(
        block => block.querySelector('h5').textContent,
      ),
    ).toEqual(['Declared', 'Last run', 'Workflows']);
    expect(container.querySelectorAll('.mat-block')[1].textContent).toEqual(
      'Last runno run information',
    );
  });
});
