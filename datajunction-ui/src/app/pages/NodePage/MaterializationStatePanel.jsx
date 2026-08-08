/**
 * Mockup A: health first.
 *
 * Renders a `MaterializationState` (see src/mocks/materializationState.js) rather
 * than stitching `materializations()` and `availabilityStates()` together in the
 * component, which is why the current tab can show an empty "Output Tables" while
 * an output dataset sits at the bottom of the same page.
 *
 * Three rules this layout exists to enforce:
 *
 *  - Coverage is always shown against a target. A bare "20260806 to 20260806" is
 *    uninterpretable; "covered through 20260806, target 20260808" is not.
 *  - Anything the engine reports is labelled as such, and absent engine data reads
 *    `unknown` rather than falling back to what DJ asked for. DJ's own record of
 *    what it requested is not evidence that it happened.
 *  - Per-run detail is a link out, never mirrored into DJ.
 */
import TableIcon from '../../icons/TableIcon';

const VERDICT = {
  healthy: { label: 'Healthy', tone: '#1a7f37', glyph: '●' },
  stale: { label: 'Stale', tone: '#9a6700', glyph: '▲' },
  failing: { label: 'Failing', tone: '#cf222e', glyph: '■' },
  unknown: { label: 'Unknown', tone: '#57606a', glyph: '○' },
};

/**
 * One honest sentence about a materialization.
 *
 * Ordered by what the reader most needs to act on: a failed run outranks a
 * coverage gap, because the gap is usually the failure's consequence.
 */
export function summarize(mat) {
  const run = mat.execution?.lastRun;
  const { covered, target, coverageKnown, missing } = mat.outcome;

  if (!mat.execution) {
    return {
      verdict: 'unknown',
      detail: 'The query service did not report on this materialization.',
    };
  }
  if (run?.status === 'failed') {
    return {
      verdict: 'failing',
      detail: `Last run failed on partition ${run.processingPartition}, attempt ${run.attempt} of ${run.maxAttempts}.`,
    };
  }
  if (!coverageKnown) {
    return {
      verdict: 'unknown',
      detail:
        'No watermarks reported, so coverage cannot be checked against the schedule.',
    };
  }
  if (missing?.length) {
    return {
      verdict: 'stale',
      detail: `Covered through ${covered.through}, target ${target.through} — ${missing.length} partition(s) missing.`,
    };
  }
  return {
    verdict: 'healthy',
    detail: `Covered through ${covered.through}, matching target.`,
  };
}

/**
 * Coverage as a row of partition cells.
 *
 * Deliberately partition-level rather than a min-to-max bar: a backfill hole is not
 * contiguous, and a bar would render it as though it were.
 */
function CoverageStrip({ outcome }) {
  if (!outcome.coverageKnown) {
    return (
      <div className="coverage coverage--unknown" aria-label="Coverage unknown">
        <span className="text-gray-400">
          Coverage unknown — this materialization reports no partition watermarks.
        </span>
      </div>
    );
  }

  const missing = new Set(outcome.missing);
  const notDue = new Set(outcome.notDueYet);
  const cells = [];
  for (
    let day = Number(outcome.target.from);
    day <= Number(outcome.target.through);
    day++
  ) {
    const key = String(day);
    const state = missing.has(key)
      ? 'missing'
      : notDue.has(key)
        ? 'not-due'
        : 'covered';
    cells.push({ key, state });
  }

  return (
    <div className="coverage" aria-label="Coverage">
      <div className="coverage__scale">
        <span>{outcome.target.from}</span>
        <span>{outcome.target.through}</span>
      </div>
      <div className="coverage__cells" role="list">
        {cells.map(cell => (
          <span
            key={cell.key}
            role="listitem"
            aria-label={`${cell.key}: ${cell.state}`}
            title={`${cell.key} — ${cell.state}`}
            className={`coverage__cell coverage__cell--${cell.state}`}
          />
        ))}
      </div>
      <div className="coverage__legend text-gray-400">
        covered · missing · not due yet
      </div>
    </div>
  );
}

/** Declared intent. DJ owns this outright, so it is never `unknown`. */
function DeclaredColumn({ intent }) {
  return (
    <div className="mat-col">
      <h5>Declared</h5>
      <div>
        {intent.scheduleHuman} {intent.timezone}
      </div>
      <div className="text-gray-400">{intent.schedule}</div>
      <div>{intent.strategy}</div>
      {intent.lookbackWindow ? (
        <div>lookback {intent.lookbackWindow}</div>
      ) : null}
      <div className="text-gray-400">
        partition {intent.partition.column} ({intent.partition.granularity})
      </div>
    </div>
  );
}

/** Engine-reported execution. Absent means absent — never backfilled from intent. */
function LastRunColumn({ execution }) {
  if (!execution) {
    return (
      <div className="mat-col">
        <h5>Last run</h5>
        <div className="text-gray-400">
          unknown — the query service reported nothing
        </div>
      </div>
    );
  }
  const run = execution.lastRun;
  return (
    <div className="mat-col">
      <h5>Last run</h5>
      {run ? (
        <>
          <div>
            {run.status} {new Date(run.endedAt).toUTCString()}
          </div>
          <div className="text-gray-400">
            attempt {run.attempt} of {run.maxAttempts}
          </div>
          <div className="text-gray-400">
            processing {run.processingPartition}
          </div>
        </>
      ) : (
        <div className="text-gray-400">no runs recorded</div>
      )}
      {execution.nextScheduledAt ? (
        <div className="text-gray-400">
          next {new Date(execution.nextScheduledAt).toUTCString()}
        </div>
      ) : null}
      {execution.workflows?.map(wf => (
        <div key={wf.label}>
          <a href={wf.url} target="_blank" rel="noreferrer">
            {wf.label} workflow ↗
          </a>
        </div>
      ))}
    </div>
  );
}

/** What consumers actually read from. */
function ServingColumn({ outcome }) {
  if (!outcome.servingTable) {
    return (
      <div className="mat-col">
        <h5>Serving</h5>
        <div className="text-gray-400">nothing built yet</div>
      </div>
    );
  }
  return (
    <div className="mat-col">
      <h5>Serving</h5>
      <div className="table__header">
        <TableIcon /> <span className="entity-info">{outcome.servingCatalog}</span>
      </div>
      <div className="table__body upstream_tables">{outcome.servingTable}</div>
      {outcome.validThrough ? (
        <div className="text-gray-400">
          valid through {new Date(outcome.validThrough).toUTCString()}
        </div>
      ) : null}
      {outcome.links?.map(link => (
        <div key={link.label}>
          <a href={link.url} target="_blank" rel="noreferrer">
            {link.label} ↗
          </a>
        </div>
      ))}
    </div>
  );
}

function MaterializationCard({ mat }) {
  const { verdict, detail } = summarize(mat);
  const tone = VERDICT[verdict];

  return (
    <div className="mat-card" aria-label={`Materialization ${mat.name}`}>
      <div className="mat-card__head">
        <span className="mat-card__verdict" style={{ color: tone.tone }}>
          {tone.glyph} {tone.label}
        </span>
        <span className="mat-card__label">{mat.label}</span>
        {!mat.active ? (
          <span className="badge partition_value">inactive</span>
        ) : null}
      </div>
      <div className="mat-card__detail">{detail}</div>

      <CoverageStrip outcome={mat.outcome} />

      <div className="mat-card__cols">
        <DeclaredColumn intent={mat.intent} />
        <LastRunColumn execution={mat.execution} />
        <ServingColumn outcome={mat.outcome} />
      </div>
    </div>
  );
}

export default function MaterializationStatePanel({ state }) {
  const mats = state.materializations || [];
  return (
    <div className="mat-panel">
      <div className="mat-panel__head">
        <span>
          {mats.length} materialization{mats.length === 1 ? '' : 's'} configured
        </span>
      </div>
      {mats.map(mat => (
        <MaterializationCard key={mat.name} mat={mat} />
      ))}
    </div>
  );
}
