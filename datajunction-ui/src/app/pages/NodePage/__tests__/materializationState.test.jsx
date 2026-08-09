import { describe, it, expect } from 'vitest';
import {
  toMaterializationState,
  dayPartitionsBetween,
} from '../materializationState';
import { summarize } from '../MaterializationStatePanel';

// Shapes below are trimmed copies of live responses from
// shared.game_health_metrics.cloud_games_session_success_cube; the config bodies
// (metrics, measures, combiners) are omitted because the adapter reads only
// `cube` and `lookback_window` from them.
const NODE = {
  name: 'shared.game_health_metrics.cloud_games_session_success_cube',
  display_name: 'Cloud Games Session Success',
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
};

const FULL = {
  node_revision_id: 648812,
  name: 'druid_cube__full__shared.game_health_metrics.cloud_games_session_success_rolling.utc_date',
  job: 'DruidCubeMaterializationJob',
  schedule: '0 6 * * *',
  strategy: 'full',
  deactivated_at: '2026-08-01T00:00:00+00:00',
  config: { cube: { version: 'v8.0' }, lookback_window: null },
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
      materializations: [
        {
          name: INCREMENTAL.name,
          label: 'Druid Cube (incremental time)',
          active: true,
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
          active: false,
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
      detail:
        'No partition watermarks reported, so coverage cannot be checked against the schedule.',
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
        'No partition watermarks reported, so coverage cannot be checked against the schedule. Run status unknown.',
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
