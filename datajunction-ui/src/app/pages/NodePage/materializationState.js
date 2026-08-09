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
  const engine = String(materialization.job || 'Materialization')
    .replace('MaterializationJob', '')
    .replace(/([a-z])([A-Z])/g, '$1 $2');
  const strategy = String(materialization.strategy || '').replace(/_/g, ' ');
  return strategy ? `${engine} (${strategy})` : engine;
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

  return {
    node: {
      name: node?.name ?? null,
      displayName: node?.display_name ?? null,
      version: node?.version ?? null,
      isCurrentVersion: node?.version === node?.current_version,
    },
    materializations: materializations.map(materialization => {
      const availability =
        availabilityByRevision.get(materialization.node_revision_id) ??
        availabilityByRevision.get(materialization.config?.cube?.version) ??
        null;
      const partition = temporalPartition(node, availability);
      return {
        name: materialization.name,
        label: toLabel(materialization),
        active: !materialization.deactivated_at,
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
    }),
  };
}

export default toMaterializationState;
