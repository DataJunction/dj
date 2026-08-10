/**
 * Adapter from today's two API responses to the proposed `MaterializationState`
 * read model (see src/mocks/materializationState.js for the shape and the reasons
 * behind it).
 *
 * The layers are kept strictly separate here because merging them is the bug the
 * read model exists to fix:
 *
 *   intent    -- the materialization record. What the author declared.
 *   outcome   -- the availability state. What exists.
 *   execution -- always null. DJ has no engine-reported run data, and its own
 *                request-time record (workflow_status, urls) is not evidence that
 *                anything ran. See EXECUTION_UNAVAILABLE below.
 */
import cronstrue from 'cronstrue';

/**
 * `execution` needs `MaterializationInfo` extended with last-run status, timestamps,
 * attempt, in-flight partition and next fire time, plus a query-service implementation
 * to populate them. Neither exists, so every materialization reports `null` and the
 * panel renders `unknown`. That gap is the argument for the DJQS work; filling it from
 * DJ's own records would erase the argument and lie about what ran.
 */
const EXECUTION_UNAVAILABLE = null;

const MS_PER_DAY = 86400000;
const MS_PER_HOUR = 3600000;

/** Cron minute/hour, when both are plain numbers. Anything else fires at midnight. */
function scheduleTimeOfDay(schedule) {
  const [minute, hour] = String(schedule || '').split(/\s+/);
  if (/^\d+$/.test(hour) && /^\d+$/.test(minute)) {
    return { hour: Number(hour), minute: Number(minute) };
  }
  return { hour: 0, minute: 0 };
}

/** `"3 DAYS"` -> 3. Anything not expressed in days yields 1, the no-op window. */
function lookbackDays(lookbackWindow) {
  const match = /^(\d+)\s*DAYS?$/i.exec(String(lookbackWindow || '').trim());
  return match ? Number(match[1]) : 1;
}

function parseDayPartition(value) {
  if (!/^\d{8}$/.test(String(value))) {
    return null;
  }
  const text = String(value);
  return Date.UTC(
    Number(text.slice(0, 4)),
    Number(text.slice(4, 6)) - 1,
    Number(text.slice(6, 8)),
  );
}

function formatDayPartition(timestamp) {
  return new Date(timestamp).toISOString().slice(0, 10).replace(/-/g, '');
}

/**
 * Inclusive list of day partitions, stepping by calendar day.
 *
 * Exported because the coverage strip needs the same enumeration; incrementing the
 * yyyyMMdd integer instead invents partitions like 20260732 whenever a range crosses
 * a month boundary, which real availability ranges routinely do.
 */
export function dayPartitionsBetween(from, through) {
  const start = parseDayPartition(from);
  const end = parseDayPartition(through);
  if (start === null || end === null) {
    return [];
  }
  const days = [];
  for (let day = start; day <= end; day += MS_PER_DAY) {
    days.push(formatDayPartition(day));
  }
  return days;
}

/**
 * Partition keys are `yyyyMMdd` at day grain and `yyyyMMddHH` at hour grain, and the
 * coverage strip has to enumerate either. Grain is read off the key length rather than
 * off `intent.partition.granularity` so the strip cannot disagree with the watermarks
 * it is drawing.
 */
const GRAINS = {
  8: {
    grain: 'day',
    step: MS_PER_DAY,
    format: ms => new Date(ms).toISOString().slice(0, 10).replace(/-/g, ''),
  },
  10: {
    grain: 'hour',
    step: MS_PER_HOUR,
    format: ms => new Date(ms).toISOString().slice(0, 13).replace(/[-T]/g, ''),
  },
};

function parsePartition(value) {
  const text = String(value);
  const spec = GRAINS[text.length];
  if (!spec || !/^\d+$/.test(text)) {
    return null;
  }
  return {
    spec,
    ms: Date.UTC(
      Number(text.slice(0, 4)),
      Number(text.slice(4, 6)) - 1,
      Number(text.slice(6, 8)),
      Number(text.slice(8, 10) || 0),
    ),
  };
}

/** Inclusive list of partition keys at whichever grain `from` and `through` share. */
export function partitionsBetween(from, through) {
  const start = parsePartition(from);
  const end = parsePartition(through);
  if (!start || !end || start.spec !== end.spec) {
    return [];
  }
  const keys = [];
  for (let ms = start.ms; ms <= end.ms; ms += start.spec.step) {
    keys.push(start.spec.format(ms));
  }
  return keys;
}

/**
 * Squares per strip. Above this the strip stops being scannable and starts being a bar.
 *
 * 25 follows Airflow's grid, which shows the last 25 runs by default. It is a cap on
 * the count, not a window onto the recent tail: the strip still spans the whole target
 * range and buckets to fit, so a year of days reads as months rather than as the last
 * 25 days. Windowing instead would be the closer analogue, but coverage here is a
 * contiguous prefix derived from two watermarks, so the left of the strip is green by
 * construction and there is nothing in the tail that the range does not already say.
 */
const MAX_SQUARES = 25;

/**
 * Bucket sizes to try, in partitions, coarsest-last. Day grain steps partition -> week
 * -> month; hour grain gets quarter-day and day rungs first so a fortnight of hourly
 * partitions does not collapse straight to a fortnight of squares.
 */
const BUCKET_LADDER = {
  day: [1, 7, 30],
  hour: [1, 6, 24, 24 * 7, 24 * 30],
};

/**
 * Smallest rung that fits the strip. Past the coarsest rung — decades of days — the
 * count is divided down directly, because an unbounded strip is worse than a bucket
 * with no calendar meaning.
 *
 * Counts only dip below ~30 immediately after a rung change (61 days becomes 9 weekly
 * squares). That is the price of buckets a reader can name; a size chosen purely to
 * land in 30–60 would label squares "5.1 days".
 */
function bucketSize(count, grain) {
  const ladder = BUCKET_LADDER[grain] || BUCKET_LADDER.day;
  return (
    ladder.find(size => Math.ceil(count / size) <= MAX_SQUARES) ??
    Math.ceil(count / MAX_SQUARES)
  );
}

const STATE_WORD = {
  covered: 'covered',
  behind: 'behind',
  notDue: 'not due yet',
};

/**
 * Coverage as a bounded run of discrete squares.
 *
 * A bucket takes the worst state it holds — one behind partition makes the whole bucket
 * behind — so aggregation can hide a healthy trailing edge but never a problem.
 *
 * What the squares cannot show: an interior hole. Availability rows carry only
 * `min_temporal_partition` and `max_temporal_partition` (`partitions` comes back empty),
 * so coverage is by construction a contiguous prefix and nothing in DJ reports a gap
 * inside it. The squares are honest about covered/behind/not-due; do not read the
 * absence of an interior gap as evidence that there is none.
 */
export function coverageSquares(outcome) {
  if (!outcome?.coverageKnown || !outcome.target) {
    return [];
  }
  const keys = partitionsBetween(outcome.target.from, outcome.target.through);
  if (!keys.length) {
    return [];
  }
  const behind = new Set(outcome.missing);
  const notDue = new Set(outcome.notDueYet);
  const size = bucketSize(keys.length, parsePartition(keys[0]).spec.grain);

  const squares = [];
  for (let start = 0; start < keys.length; start += size) {
    const bucket = keys.slice(start, start + size);
    const counts = { covered: 0, behind: 0, notDue: 0 };
    for (const key of bucket) {
      counts[
        behind.has(key) ? 'behind' : notDue.has(key) ? 'notDue' : 'covered'
      ] += 1;
    }
    const state = counts.behind
      ? 'behind'
      : counts.notDue
      ? 'notDue'
      : 'covered';
    const from = bucket[0];
    const through = bucket[bucket.length - 1];
    squares.push({
      state,
      from,
      through,
      count: counts[state],
      label: `${from === through ? from : `${from}–${through}`}: ${
        counts[state]
      } ${STATE_WORD[state]}`,
    });
  }
  return squares;
}

/** `"3 DAYS"` -> `"3d"`. The badge has no room for the word. */
function shortLookback(lookbackWindow) {
  const match = /^(\d+)\s*(SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)S?$/i.exec(
    String(lookbackWindow || '').trim(),
  );
  if (!match) {
    return String(lookbackWindow || '').toLowerCase();
  }
  const unit = match[2].toLowerCase();
  return `${match[1]}${unit === 'month' ? 'mo' : unit[0]}`;
}

/**
 * Strategy as a short neutral chip: `full`, or `incremental · 3d lookback`.
 *
 * The lookback folds in here because it only means anything for an incremental
 * strategy, and a `lookback 3 DAYS` line sitting apart from `incremental_time` made a
 * reader join two facts that are one fact.
 */
export function strategyBadge(intent) {
  const strategy = String(intent?.strategy || '').trim();
  if (!strategy) {
    return null;
  }
  const label = strategy.replace(/_time$/, '').replace(/_/g, ' ');
  const lookback = intent.lookbackWindow
    ? ` · ${shortLookback(intent.lookbackWindow)} lookback`
    : '';
  return strategy === 'full' ? label : `${label}${lookback}`;
}

/**
 * Partition-level coverage against what the schedule should have produced by `now`.
 *
 * Availability reports only min and max watermarks -- `partitions` comes back empty
 * in practice -- so everything between them is taken as covered. A hole inside the
 * range is invisible to DJ today and this cannot invent it.
 *
 * A partition is only counted missing once the last run that would have written it
 * has fired. With an N-day lookback each run rewrites the trailing N partitions, so
 * the partition for day D still has chances on days D+1 .. D+N-1; until those pass it
 * is `notDueYet`, not a gap.
 */
function computeCoverage({
  min,
  max,
  partition,
  schedule,
  lookbackWindow,
  now,
}) {
  const from = parseDayPartition(min);
  const through = parseDayPartition(max);
  if (from === null || through === null || partition?.granularity !== 'day') {
    return {
      target: null,
      covered: null,
      missing: [],
      notDueYet: [],
      coverageKnown: false,
    };
  }

  const { hour, minute } = scheduleTimeOfDay(schedule);
  const grace = (lookbackDays(lookbackWindow) - 1) * MS_PER_DAY;
  const nowMs = now.getTime();
  const targetThrough = Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate(),
  );

  const missing = [];
  const notDueYet = [];
  for (
    let day = through + MS_PER_DAY;
    day <= targetThrough;
    day += MS_PER_DAY
  ) {
    const lastChance = day + hour * 3600000 + minute * 60000 + grace;
    (nowMs >= lastChance ? missing : notDueYet).push(formatDayPartition(day));
  }

  return {
    target: {
      from: formatDayPartition(from),
      through: formatDayPartition(Math.max(targetThrough, through)),
    },
    covered: {
      from: formatDayPartition(from),
      through: formatDayPartition(through),
    },
    missing,
    notDueYet,
    coverageKnown: true,
  };
}

const EMPTY_OUTCOME = {
  servingTable: null,
  servingCatalog: null,
  validThrough: null,
  target: null,
  covered: null,
  missing: [],
  notDueYet: [],
  coverageKnown: false,
  links: [],
};

function toOutcome({ availability, partition, schedule, lookbackWindow, now }) {
  if (!availability) {
    return { ...EMPTY_OUTCOME };
  }
  return {
    servingTable: availability.table ?? null,
    servingCatalog: [availability.catalog, availability.schema_]
      .filter(Boolean)
      .join('.'),
    validThrough: availability.valid_through_ts
      ? new Date(availability.valid_through_ts).toISOString()
      : null,
    ...computeCoverage({
      min: availability.min_temporal_partition?.[0],
      max: availability.max_temporal_partition?.[0],
      partition,
      schedule,
      lookbackWindow,
      now,
    }),
    links: Object.entries(availability.links || {}).map(([label, url]) => ({
      label,
      url,
    })),
  };
}

/**
 * Distinguishes the two materializations a cube routinely carries. Both come back
 * from the API as `DruidCubeMaterializationJob` and render identically today; the
 * strategy is what actually differs.
 */
function toLabel(materialization) {
  const strategy = String(materialization.strategy || '').replace(/_/g, ' ');
  const engine = titleCaseEngine(materialization);
  return strategy ? `${engine} (${strategy})` : engine;
}

function titleCaseEngine(materialization) {
  return String(materialization.job || 'Materialization')
    .replace('MaterializationJob', '')
    .replace(/([a-z])([A-Z])/g, '$1 $2');
}

/**
 * The engine alone, sentence-cased: `"Druid cube"`. Kept apart from `label` because the
 * strategy now travels as a badge, and "Druid Cube (incremental time)" then repeated it.
 */
function toEngine(materialization) {
  const [first, ...rest] = titleCaseEngine(materialization).split(' ');
  return [first, ...rest.map(word => word.toLowerCase())].join(' ');
}

function humanizeSchedule(schedule) {
  try {
    return cronstrue.toString(schedule);
  } catch (e) {
    return null;
  }
}

function toIntent({ materialization, partitionColumn }) {
  return {
    schedule: materialization.schedule ?? null,
    scheduleHuman: humanizeSchedule(materialization.schedule),
    // DJ stores no timezone alongside the cron expression.
    timezone: null,
    strategy: materialization.strategy ?? null,
    lookbackWindow: materialization.config?.lookback_window ?? null,
    partition: partitionColumn,
  };
}

/**
 * The node's temporal partition column, which is where granularity and format live --
 * neither the materialization record nor the availability state carries them.
 */
function temporalPartition(node, availability) {
  const columns = node?.columns || [];
  const named = availability?.temporal_partitions?.[0];
  const column =
    columns.find(
      col =>
        col.partition &&
        (!named || col.name.endsWith(`.${named}`) || col.name === named),
    ) || columns.find(col => col.partition);
  if (!column) {
    return null;
  }
  return {
    column: column.display_name || column.name,
    granularity: column.partition.granularity ?? null,
    format: column.partition.format ?? null,
  };
}

/**
 * Workflow deep links, labelled by the trailing segment of the workflow id:
 * `.../maestro/prod/dj.<node>.v8.0.main` -> `main`.
 *
 * `workflow_names` comes back empty in practice, so the URL is the only place the
 * label exists. A link here is DJ's record of a workflow it asked the query service
 * to create -- it says the workflow exists, not that it ran.
 */
function workflowLinks(materialization) {
  return (materialization.urls || []).map((url, index) => {
    const text = String(url);
    const tail = text.split('?')[0].replace(/\/+$/, '').split('/').pop() || '';
    const segment = tail.split('.').pop();
    return {
      label: /^[a-z][a-z0-9_-]*$/i.test(segment)
        ? segment
        : `workflow ${index + 1}`,
      url: text,
    };
  });
}

/**
 * One cube, one coverage.
 *
 * Availability is recorded per node revision, so every materialization on a revision
 * is handed the same watermarks and reports the same covered range. Rendering the
 * squares once per row therefore multiplied a single gap into one apparent problem
 * per materialization, and the rows then disagreed with each other -- a cube two days
 * short read as "3 days behind" on one row and "1 partition to backfill" on the next,
 * from identical input.
 *
 * They disagree only about which trailing partitions are still due, because that
 * follows each materialization's lookback. A partition is behind only once every
 * configured materialization has had its last chance to write it, so the not-due sets
 * union and what remains is the real shortfall.
 */
export function mergeCoverage(mats) {
  const known = mats.filter(mat => mat.outcome.coverageKnown);
  if (!known.length) {
    return {
      target: null,
      covered: null,
      missing: [],
      notDueYet: [],
      coverageKnown: false,
    };
  }
  const notDueYet = new Set();
  const trailing = new Set();
  for (const { outcome } of known) {
    for (const day of outcome.notDueYet) {
      notDueYet.add(day);
      trailing.add(day);
    }
    for (const day of outcome.missing) {
      trailing.add(day);
    }
  }
  const target = known
    .map(mat => mat.outcome.target)
    .reduce((a, b) => (b.through > a.through ? b : a));
  return {
    target,
    covered: known[0].outcome.covered,
    missing: [...trailing].filter(day => !notDueYet.has(day)).sort(),
    notDueYet: [...notDueYet].sort(),
    coverageKnown: true,
  };
}

/**
 * Map the two responses the tab already fetches into a `MaterializationState`.
 *
 * Pure: `now` is injected so coverage is testable and so two calls in the same render
 * cannot disagree about what "today" is.
 *
 * @param {object} args
 * @param {object} args.node - node as returned by `/nodes/{name}`; supplies partition columns
 * @param {Array} args.materializations - `djClient.materializations()` response
 * @param {Array} args.availabilityStates - `djClient.availabilityStates()` response
 * @param {Date} [args.now]
 * @returns {{node: object, materializations: Array}}
 */
export function toMaterializationState({
  node,
  materializations = [],
  availabilityStates = [],
  now = new Date(),
}) {
  // Availability is recorded per node revision, not per materialization, so a revision
  // carrying both a full and an incremental materialization attributes the same outcome
  // to both. DJ cannot currently tell them apart.
  const availabilityByRevision = new Map();
  for (const availability of availabilityStates) {
    const key = availability.node_revision_id ?? availability.node_version;
    if (key !== undefined && key !== null && !availabilityByRevision.has(key)) {
      availabilityByRevision.set(key, availability);
    }
  }

  const mats = materializations.map(materialization => {
    const availability =
      availabilityByRevision.get(materialization.node_revision_id) ??
      availabilityByRevision.get(materialization.config?.cube?.version) ??
      null;
    const partition = temporalPartition(node, availability);
    return {
      name: materialization.name,
      label: toLabel(materialization),
      engine: toEngine(materialization),
      active: !materialization.deactivated_at,
      // Neither declared intent nor observed execution: DJ's own record of the
      // workflows it asked for. Kept at the top level so it still renders when
      // `execution` is null, which is every materialization today.
      workflows: workflowLinks(materialization),
      intent: toIntent({ materialization, partitionColumn: partition }),
      outcome: toOutcome({
        availability,
        partition,
        schedule: materialization.schedule,
        lookbackWindow: materialization.config?.lookback_window,
        now,
      }),
      execution: EXECUTION_UNAVAILABLE,
    };
  });

  return {
    node: {
      name: node?.name ?? null,
      displayName: node?.display_name ?? null,
      // Carried because the header's Preview link targets the query planner, and
      // which planner parameter to use depends on the node type.
      type: node?.type ?? null,
      version: node?.version ?? null,
      isCurrentVersion: node?.version === node?.current_version,
    },
    // Cube-scoped, alongside the serving table it is a property of.
    coverage: mergeCoverage(mats),
    materializations: mats,
  };
}

export default toMaterializationState;
