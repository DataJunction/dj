/**
 * Proposed `MaterializationState` read model.
 *
 * This fixture doubles as the interface proposal. Today the UI makes two calls --
 * `djClient.materializations()` and `djClient.availabilityStates()` -- and stitches
 * them together itself, which is why the tab can show an empty "Output Tables" while
 * an output dataset sits at the bottom of the same page. One endpoint would assemble
 * this instead.
 *
 * Every field is tagged with the layer it comes from, because conflating them is the
 * current bug: `PreAggregation.workflow_status` is DJ's record of what it *asked for*
 * and is rendered as though it were observed reality, so a pre-agg whose every run has
 * failed still reads "active".
 *
 *   intent    -- what the author declared. DJ owns it, already stored.
 *   outcome   -- what exists. DJ owns it, arrives by availability callback.
 *   execution -- what the engine did. DJ never stores this; pulled through DJQS on
 *                demand, and `null` whenever the engine cannot answer.
 *
 * `execution` is the only part that does not exist yet: it needs `MaterializationInfo`
 * extended (it currently carries just output_tables, urls, workflow_names) and a DJQS
 * implementation. Mocked here so the UI can be built and the ask can be reviewed as a
 * concrete shape rather than a description.
 */

// A cube that is behind: the incremental materialization's last run failed, so
// coverage stopped two days short of what the schedule should have produced.
export const materializationStateStale = {
  node: {
    name: 'shared.game_health_metrics.cloud_games_session_success_cube',
    displayName: 'Cloud Games Session Success',
    type: 'cube',
    version: 'v8.0',
    isCurrentVersion: true,
  },
  materializations: [
    {
      // Qualified with strategy: two materializations on one cube both rendering as
      // "Druid Cube" today is why the current page is unreadable.
      name: 'druid_cube__incremental_time__utc_date',
      label: 'Druid cube (incremental)',
      // Engine alone; layout C carries the strategy in a badge instead.
      engine: 'Druid cube',
      active: true,

      intent: {
        schedule: '0 6 * * *',
        scheduleHuman: 'daily at 06:00',
        timezone: 'UTC',
        strategy: 'incremental_time',
        lookbackWindow: '1 DAY',
        partition: {
          column: 'utc_date',
          granularity: 'day',
          format: 'yyyyMMdd',
        },
      },

      outcome: {
        servingTable:
          'dj__shared_game_health_metrics_cloud_games_session_success_cube_v8_0_cd70304ced94ac2e',
        servingCatalog: 'druid.datajunction',
        validThrough: '2026-08-06T22:00:00.000Z',
        // Derived by DJ from intent + logical date, not reported by anyone. This is
        // what makes a bare "20260806" interpretable.
        target: { from: '20260726', through: '20260808' },
        covered: { from: '20260726', through: '20260806' },
        // Partition-level, so the UI can render gaps spatially rather than implying
        // a contiguous range that may not be one.
        missing: ['20260807'],
        notDueYet: ['20260808'],
        coverageKnown: true,
        links: [{ label: 'Data Explorer', url: 'https://example/explore' }],
      },

      execution: {
        lastRun: {
          status: 'failed',
          startedAt: '2026-08-07T06:00:12.000Z',
          endedAt: '2026-08-07T06:04:31.000Z',
          attempt: 3,
          maxAttempts: 3,
          processingPartition: '20260807',
        },
        nextScheduledAt: '2026-08-08T06:00:00.000Z',
        inFlight: null,
        workflows: [
          { label: 'main', url: 'https://example/maestro/main' },
          { label: 'backfill', url: 'https://example/maestro/backfill' },
        ],
      },
    },
    {
      // The vestigial second materialization from the real screenshot. DJ believes it
      // is configured; the engine reports nothing for it. Showing that honestly is the
      // point -- today it renders identically to a healthy one.
      name: 'druid_cube__full',
      label: 'Druid cube (full)',
      engine: 'Druid cube',
      active: false,
      intent: {
        schedule: '0 6 * * *',
        scheduleHuman: 'daily at 06:00',
        timezone: 'UTC',
        strategy: 'full',
        lookbackWindow: null,
        partition: {
          column: 'utc_date',
          granularity: 'day',
          format: 'yyyyMMdd',
        },
      },
      outcome: {
        servingTable: null,
        servingCatalog: null,
        validThrough: null,
        target: null,
        covered: null,
        missing: [],
        notDueYet: [],
        coverageKnown: false,
        links: [],
      },
      execution: null, // engine reported nothing -- render as unknown, never as active
    },
  ],
};

// Same cube, healthy: coverage meets target and the last run succeeded.
export const materializationStateHealthy = {
  ...materializationStateStale,
  materializations: [
    {
      ...materializationStateStale.materializations[0],
      outcome: {
        ...materializationStateStale.materializations[0].outcome,
        validThrough: '2026-08-08T06:12:00.000Z',
        covered: { from: '20260726', through: '20260808' },
        missing: [],
        notDueYet: [],
      },
      execution: {
        ...materializationStateStale.materializations[0].execution,
        lastRun: {
          status: 'succeeded',
          startedAt: '2026-08-08T06:00:09.000Z',
          endedAt: '2026-08-08T06:11:52.000Z',
          attempt: 1,
          maxAttempts: 3,
          processingPartition: '20260808',
        },
      },
    },
  ],
};

// DJQS unreachable. The semantic half must still render in full: this is the case
// that decides whether the view is worth building before the shim work lands.
export const materializationStateEngineUnknown = {
  ...materializationStateStale,
  materializations: [
    {
      ...materializationStateStale.materializations[0],
      execution: null,
    },
  ],
};

// Registered externally, or a workflow that reports no watermarks: coverage cannot be
// judged. Must say so rather than implying full coverage -- the same blind spot that
// makes the freshness gate pass unwatermarked pre-aggs (AIE-3218).
export const materializationStateCoverageUnknown = {
  ...materializationStateStale,
  materializations: [
    {
      ...materializationStateStale.materializations[0],
      outcome: {
        ...materializationStateStale.materializations[0].outcome,
        target: null,
        covered: null,
        missing: [],
        notDueYet: [],
        coverageKnown: false,
      },
    },
  ],
};

/**
 * Ranges past anything the live cube can show, so layout C's coverage strip can be
 * checked where it actually has to bucket. Real availability rows are day-grain and a
 * fortnight long; the strip has to survive a year of them, and hour-grain partitions
 * that DJ will report once `computeCoverage` handles sub-day granularity.
 */
function syntheticMat({
  name,
  strategy,
  lookbackWindow,
  granularity,
  outcome,
}) {
  return {
    name,
    label: `Druid cube (${strategy})`,
    engine: 'Druid cube',
    active: true,
    intent: {
      schedule: granularity === 'hour' ? '0 * * * *' : '0 6 * * *',
      scheduleHuman: granularity === 'hour' ? 'hourly' : 'daily at 06:00',
      timezone: 'UTC',
      strategy,
      lookbackWindow,
      partition: {
        column: 'utc_date',
        granularity,
        format: granularity === 'hour' ? 'yyyyMMddHH' : 'yyyyMMdd',
      },
    },
    outcome: {
      servingTable: 'dj__synthetic_cube',
      servingCatalog: 'druid.datajunction',
      validThrough: null,
      coverageKnown: true,
      links: [],
      ...outcome,
    },
    execution: null,
  };
}

// 371 daily partitions -> weekly buckets. The behind days sit inside one bucket
// alongside covered days, which must still render the bucket as behind.
export const materializationStateYearOfDays = {
  ...materializationStateStale,
  materializations: [
    syntheticMat({
      name: 'druid_cube__incremental_time__year',
      strategy: 'incremental_time',
      lookbackWindow: '3 DAYS',
      granularity: 'day',
      outcome: {
        target: { from: '20250801', through: '20260806' },
        covered: { from: '20250801', through: '20260802' },
        missing: ['20260803', '20260804'],
        notDueYet: ['20260805', '20260806'],
      },
    }),
  ],
};

// 721 hourly partitions -> daily buckets. A single behind hour has to survive being
// bucketed with 23 covered ones.
export const materializationStateHourly = {
  ...materializationStateStale,
  materializations: [
    syntheticMat({
      name: 'druid_cube__incremental_time__hourly',
      strategy: 'incremental_time',
      lookbackWindow: '6 HOURS',
      granularity: 'hour',
      outcome: {
        target: { from: '2026070800', through: '2026080700' },
        covered: { from: '2026070800', through: '2026080618' },
        missing: ['2026080619'],
        notDueYet: [
          '2026080620',
          '2026080621',
          '2026080622',
          '2026080623',
          '2026080700',
        ],
      },
    }),
  ],
};

export default materializationStateStale;
