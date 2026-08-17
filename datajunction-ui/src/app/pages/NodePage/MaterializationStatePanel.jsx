/**
 * Materialization panel.
 *
 * Renders a `MaterializationState` (built by ./materializationState.js) rather than
 * stitching `materializations()` and `availabilityStates()` together in the component.
 * Stitching them here is what lets the tab show an empty "Output Tables" while an
 * output dataset sits at the bottom of the same page.
 *
 * Rules the layout enforces:
 *
 *  - Coverage is always shown against a target. A bare "20260806 to 20260806" is
 *    uninterpretable; "covered through 20260806, target 20260808" is not.
 *  - Serving and coverage are properties of the cube revision, not of a
 *    materialization, so both are rendered once above them rather than repeated
 *    (and misattributed) per card. Availability is keyed to the revision, so a
 *    per-materialization verdict would be the cube's verdict wearing another name.
 *  - Anything the engine reports is labelled as such. Absent run data qualifies the
 *    verdict; it does not replace it, because DJ still knows the coverage.
 */
import { Fragment, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { coverageSquares, strategyBadge } from './materializationState';

// Glyphs and semantic classes follow the planner's `getStatusInfo`
// (QueryPlannerPage/PreAggDetailsPanel.jsx), so the two panels read as one product.
const VERDICT = {
  healthy: { glyph: '●', className: 'mat-verdict--healthy' },
  stale: { glyph: '◐', className: 'mat-verdict--stale' },
  failing: { glyph: '■', className: 'mat-verdict--failing' },
  unknown: { glyph: '○', className: 'mat-verdict--unknown' },
};

/** Run data is absent for every materialization today; say so without burying coverage. */
const RUN_UNKNOWN = 'Run status unknown.';

/** Engine run states, mapped onto the panel's four verdict tones. */
const RUN_VERDICT = {
  succeeded: 'healthy',
  running: 'healthy',
  failed: 'failing',
};

function plural(count, noun) {
  return `${count} ${noun}${count === 1 ? '' : 's'}`;
}

/**
 * One honest sentence about a materialization, plus a two-or-three word headline.
 *
 * Ordered by what DJ actually knows. Coverage comes from availability watermarks and
 * is available today; run data comes from the query service and is not. Leading with
 * the run therefore rendered every materialization `unknown` and threw away the one
 * judgement DJ can make. A *failed* run still outranks coverage, because the gap is
 * then the failure's consequence — but only when a run was actually reported.
 */
export function summarize(mat) {
  const run = mat.execution?.lastRun;
  const { covered, target, coverageKnown, missing, notDueYet } = mat.outcome;
  // `full` rebuilds the table wholesale, so there is no per-day hole to backfill;
  // the shortfall is how far behind the whole table is and the fix is another run.
  const isFull = mat.intent?.strategy === 'full';
  const qualifier = mat.execution ? '' : ` ${RUN_UNKNOWN}`;

  if (run?.status === 'failed') {
    return {
      verdict: 'failing',
      headline: 'Last run failed',
      detail: `Last run failed on partition ${run.processingPartition}, attempt ${run.attempt} of ${run.maxAttempts}.`,
    };
  }
  if (!coverageKnown) {
    return {
      verdict: 'unknown',
      headline: 'Coverage unknown',
      detail: `This materialization doesn't report which dates it covers.${qualifier}`,
    };
  }
  if (missing?.length) {
    const shortfall = isFull
      ? `${plural(missing.length, 'day')} behind`
      : `${plural(missing.length, 'partition')} to backfill`;
    return {
      verdict: 'stale',
      headline: shortfall,
      detail: `Covered through ${covered.through}, target ${target.through} — ${shortfall}.${qualifier}`,
    };
  }
  if (notDueYet?.length) {
    return {
      verdict: 'healthy',
      headline: 'On target',
      detail: `Covered through ${covered.through}, target ${
        target.through
      } — ${plural(notDueYet.length, 'day')} not due yet.${qualifier}`,
    };
  }
  return {
    verdict: 'healthy',
    headline: 'On target',
    detail: `Covered through ${covered.through}, matching target.${qualifier}`,
  };
}

/**
 * The cube's coverage in three words, for the serving line.
 *
 * Deliberately not `summarize`: that judges a materialization and mixes in run status.
 * This judges the built table, which is the thing coverage is actually about, and says
 * nothing about how many materializations feed it or which of them is behind -- DJ
 * cannot attribute a watermark to a materialization, so neither can this.
 */
function coverageVerdict(coverage) {
  if (!coverage?.coverageKnown) {
    return { verdict: 'unknown', headline: 'Coverage unknown' };
  }
  if (coverage.missing?.length) {
    return {
      verdict: 'stale',
      headline: `${plural(coverage.missing.length, 'day')} behind`,
    };
  }
  return { verdict: 'healthy', headline: 'On target' };
}

/** "2026-08-07T20:00:00.000Z" -> "Fri 07 Aug 2026 20:00 GMT". Seconds are noise here. */
function formatUtc(iso) {
  return new Date(iso)
    .toUTCString()
    .replace(',', '')
    .replace(/:\d{2} GMT$/, ' GMT');
}

/**
 * "2d ago". Thresholds follow moment's `fromNow`, which rounds rather than floors, so
 * a 41-hour-old build reads "2d ago" instead of understating it as "1d ago".
 */
function relativeTime(iso, now = new Date()) {
  const ms = new Date(iso).getTime();
  if (!Number.isFinite(ms)) {
    return null;
  }
  const seconds = Math.abs(now.getTime() - ms) / 1000;
  const suffix = now.getTime() >= ms ? ' ago' : ' from now';
  const [value, unit] =
    seconds < 45
      ? [0, '']
      : seconds < 2700
      ? [seconds / 60, 'm']
      : seconds < 79200
      ? [seconds / 3600, 'h']
      : seconds < 2246400
      ? [seconds / 86400, 'd']
      : seconds < 28512000
      ? [seconds / 2629800, 'mo']
      : [seconds / 31557600, 'y'];
  return unit ? `${Math.round(value)}${unit}${suffix}` : 'just now';
}

/**
 * The serving table as a chip you copy rather than read. Nobody parses
 * `dj__shared_game_health_metrics_cloud_games_session_success_cube_v8_0_cd70304ced94ac2e`
 * off the screen; they paste it into a query, so the full name lives in `title` and on
 * the clipboard and only the ends are rendered.
 */
function TableChip({ catalog, table }) {
  const [copied, setCopied] = useState(false);
  const fullName = [catalog, table].filter(Boolean).join('.');

  const copy = () => {
    // Absent outside a secure context; the chip still shows the name via `title`.
    navigator.clipboard?.writeText(fullName).then(
      () => {
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      },
      () => {},
    );
  };

  return (
    <span className="mat-chip" title={fullName}>
      {catalog ? (
        <>
          <span className="mat-chip__catalog">{catalog}</span>
          <span className="mat-chip__sep">/</span>
        </>
      ) : null}
      <span className="mat-chip__name mat-mono">{table}</span>
      <button
        type="button"
        className="mat-chip__copy"
        aria-label={`Copy table name ${fullName}`}
        onClick={copy}
      >
        {copied ? '✓' : '⧉'}
      </button>
      {/* Announced as well as shown: the glyph swap alone is invisible to a reader
          who is not looking at the button. */}
      <span role="status" className="mat-chip__flash">
        {copied ? 'Copied' : ''}
      </span>
    </span>
  );
}

/**
 * The node page's Preview tab renders nothing of its own -- `onClickTab` in
 * NodePage/index.jsx navigates to the query planner instead -- so the link reproduces
 * that target. `/nodes/<name>/preview` would fall through to the info tab.
 */
const PLANNER_PARAM = { cube: 'cube', metric: 'metrics' };

function previewUrl(node) {
  const param = PLANNER_PARAM[node?.type];
  return param && node.name
    ? `/planner?${param}=${encodeURIComponent(node.name)}`
    : null;
}

/**
 * Serving and coverage side by side.
 *
 * They answer two different questions from two different systems -- "where is the
 * data" and "how much of it is there" -- so they sit apart rather than as two rows of
 * one block. Unboxed, though: the materializations below are the objects on this page
 * and are what carry a border, and boxing this too gave four panels of identical
 * weight with nothing to say which was the summary.
 *
 * Both are cube-scoped. Availability is keyed to the node revision, so every
 * materialization on it reports the same table and the same watermarks; stating either
 * per materialization would print one fact once per row and invite reading two rows as
 * two problems.
 */
function SummaryCards({ node, serving, coverage }) {
  return (
    <div className="mat-summary">
      <section className="mat-summary__item">
        <h5 className="mat-summary__label">Serving</h5>
        {serving ? (
          <ServingCard node={node} serving={serving} />
        ) : (
          <div className="mat-dim">nothing built for this revision yet</div>
        )}
      </section>
      <section className="mat-summary__item">
        <h5 className="mat-summary__label">Coverage</h5>
        {serving ? (
          <CoverageCard coverage={coverage} />
        ) : (
          <div className="mat-dim">nothing built yet</div>
        )}
      </section>
    </div>
  );
}

function ServingCard({ node, serving }) {
  const preview = previewUrl(node);
  return (
    <>
      <TableChip
        catalog={serving.servingCatalog}
        table={serving.servingTable}
      />
      <div
        className="mat-dim"
        title={
          serving.validThrough
            ? `valid through ${formatUtc(serving.validThrough)}`
            : undefined
        }
      >
        {serving.validThrough
          ? `updated ${relativeTime(serving.validThrough)}`
          : 'update time unknown'}
      </div>
      {/* On their own line: alongside the freshness they read as belonging to it
          rather than to the cube. */}
      <div className="mat-summary__links">
        {preview ? (
          <Link className="mat-header__link" to={preview}>
            Preview →
          </Link>
        ) : null}
        {serving.links?.map(link => (
          <a
            key={link.label}
            href={link.url}
            target="_blank"
            rel="noreferrer"
            className="mat-header__link"
          >
            {link.label} ↗
          </a>
        ))}
      </div>
    </>
  );
}

/**
 * The strip with its range written under it.
 *
 * The dates are what make the squares mean anything -- a run of colour says "behind"
 * without saying since when. Only the ends are named: every bucket carries its own
 * range in `title`, and 25 dates written out would be a table, not an axis.
 */
function LabelledSquares({ coverage }) {
  // A year of hourly partitions is ~8800 keys enumerated and bucketed; the panel
  // re-renders on every copy-button flash, and none of that work depends on it.
  const squares = useMemo(() => coverageSquares(coverage), [coverage]);
  if (!squares.length) {
    return null;
  }
  const first = squares[0].from;
  const last = squares[squares.length - 1].through;
  return (
    <div className="mat-squares-axis">
      <SquaresRun squares={squares} />
      <span className="mat-squares-axis__ends">
        <span>{first}</span>
        {first === last ? null : (
          <>
            <span>→</span>
            <span>{last}</span>
          </>
        )}
      </span>
    </div>
  );
}

function CoverageCard({ coverage }) {
  const { verdict, headline } = coverageVerdict(coverage);
  const tone = VERDICT[verdict];
  return (
    <>
      <div className={tone.className}>
        <span className="mat-glyph">{tone.glyph}</span> {headline}
      </div>
      {coverage?.coverageKnown ? <LabelledSquares coverage={coverage} /> : null}
    </>
  );
}

/**
 * The cube's output, above the builds that produce it.
 *
 * The panel used to be headed `Materializations` with serving and coverage directly
 * beneath it -- naming the wrong group, since neither is a materialization, and
 * leaving the actual materializations below with no heading at all. The heading is
 * also redundant: this panel only ever renders inside a tab called Materializations.
 */
function CubeHeader({ state, serving, coverage }) {
  return (
    <div className="mat-header">
      <h4 className="mat-section">Output</h4>
      <SummaryCards node={state.node} serving={serving} coverage={coverage} />
    </div>
  );
}

/**
 * What the workflows did, one row each.
 *
 * `main` and `backfill` are separate workflows with separate histories -- `main` fires
 * daily, `backfill` may not have run since March -- so a single "last run" for the
 * materialization would have to pick one and silently drop the other. Run state
 * belongs on the workflow it describes.
 *
 * The link is the label, because the workflow is the thing you go and look at. Run
 * state is absent for every workflow today: it needs `MaterializationInfo` extended
 * and a query-service implementation, and inferring it from DJ's own records would
 * report a workflow that was *requested* as one that had *succeeded*.
 */
function WorkflowActivity({ workflows }) {
  return (
    <div className="mat-activity">
      {/* The left column's items label themselves -- `Schedule`, `Partition` -- but
          `main` and `backfill` do not say what kind of thing they are, so this side
          needs the heading that side does not. */}
      <div className="mat-activity__label">Workflows</div>
      <WorkflowRuns workflows={workflows} />
    </div>
  );
}

function WorkflowRuns({ workflows }) {
  if (!workflows?.length) {
    return <div className="mat-dim">none</div>;
  }
  return (
    <dl className="mat-facts">
      {workflows.map(wf => {
        const run = wf.lastRun ?? null;
        const tone = VERDICT[(run && RUN_VERDICT[run.status]) || 'unknown'];
        return (
          <Fragment key={wf.url}>
            <dt className="mat-activity__workflow">
              <a
                href={wf.url}
                target="_blank"
                rel="noreferrer"
                className="mat-workflow-link"
                title={wf.url}
              >
                {wf.label} ↗
              </a>
            </dt>
            <dd className={run ? tone.className : 'mat-dim'}>
              <span className="mat-glyph">{tone.glyph}</span>{' '}
              {run
                ? `${run.status} ${relativeTime(run.endedAt)}`
                : 'no runs reported'}
            </dd>
          </Fragment>
        );
      })}
    </dl>
  );
}

/**
 * One block per materialization: what was declared, beside what ran.
 *
 * The two sides come from different systems and can disagree in both directions. A run
 * can succeed and write nothing (Maestro green, coverage flat -- which is how a
 * FATALLY_FAILED ingest step hid under a SUCCEEDED parent), a run can fail after
 * writing, and data can arrive with no run at all. Neither side may stand in for the
 * other, so they are separate regions rather than one merged list.
 *
 * Both sides are label-to-value. On the right the label is the workflow itself, which
 * is why neither side needs a heading over it: `Schedule` and `main` are each already
 * saying what their value is.
 *
 * No coverage here, and no internal name. Coverage belongs to the cube revision and is
 * stated once in the header; the materialization's name identifies it to the API, not
 * to a reader.
 */
function MaterializationBlock({ mat }) {
  const { intent } = mat;
  // Neutral by design: the status glyph is the block's only coloured element.
  const strategy = strategyBadge(intent);
  return (
    <div className="mat-item">
      <div className="mat-item__head">
        <span className="mat-item__name">{mat.engine || mat.label}</span>
        {strategy ? <span className="mat-badge">{strategy}</span> : null}
        {mat.active ? null : (
          <span className="badge partition_value">inactive</span>
        )}
      </div>
      <div className="mat-item__cols">
        <dl className="mat-facts">
          <dt>Schedule</dt>
          <dd>
            {intent.scheduleHuman || intent.schedule}
            {/* The cron reads as the machine-readable form of the sentence beside
                it, so it is set apart rather than trailing it as more prose. */}
            {intent.scheduleHuman ? (
              <span className="mat-cron">{intent.schedule}</span>
            ) : null}
          </dd>

          {/* Cubes with no temporal partition column exist; they simply have none to
              declare, and inventing one would misreport the cube. */}
          <dt>Partition</dt>
          <dd>
            {intent.partition ? (
              <>
                <code>{intent.partition.column}</code> (
                {intent.partition.granularity})
              </>
            ) : (
              <span className="mat-dim">none</span>
            )}
          </dd>

          {/* Omitted rather than rendered as "none": an absent window is not a window
              of zero, and a full rebuild has none to speak of. */}
          {intent.lookbackWindow ? (
            <>
              <dt>Lookback</dt>
              <dd>{intent.lookbackWindow.toLowerCase()}</dd>
            </>
          ) : null}
        </dl>
        <WorkflowActivity workflows={mat.workflows} />
      </div>
    </div>
  );
}

/**
 * Coverage as a short run of fixed-size squares, bucketed by `coverageSquares` so the
 * strip stays scannable at a year or at hourly grain. Deliberately not stretched: a
 * 2000px bar across four days reads as precision this data does not have.
 */
const SQUARE_CLASS = {
  covered: 'covered',
  behind: 'behind',
  notDue: 'not-due',
};

/**
 * The squares themselves. A list rather than one labelled image, so a reader can reach
 * an individual behind bucket instead of only a "Coverage" summary.
 */
function SquaresRun({ squares }) {
  return (
    <span className="mat-squares" role="list" aria-label="Coverage">
      {squares.map(square => (
        <span
          key={square.from}
          role="listitem"
          className={`mat-squares__cell mat-squares__cell--${
            SQUARE_CLASS[square.state]
          }`}
          title={square.label}
          aria-label={square.label}
        />
      ))}
    </span>
  );
}

export default function MaterializationStatePanel({ state, versionSelect }) {
  const mats = state.materializations || [];
  // One serving row for the whole panel: all the materializations report the same one,
  // because availability keys off the revision they share.
  const serving = mats.find(mat => mat.outcome.servingTable)?.outcome ?? null;

  return (
    <div className="mat-panel">
      {/* The version scopes everything below it -- the serving table and the
          materializations alike -- so it belongs at the top of the panel rather than
          floating above it as neither page chrome nor panel content. */}
      <div className="mat-controls">{versionSelect}</div>
      <CubeHeader state={state} serving={serving} coverage={state.coverage} />
      <h4 className="mat-section">
        Materializations{' '}
        <span className="mat-section__count">{mats.length}</span>
      </h4>
      <div className="mat-items">
        {/* Keyed by index as well: the same materialization name recurs across cube
            revisions, so the name alone is not unique within a node. */}
        {mats.map((mat, index) => (
          <MaterializationBlock key={`${mat.name}-${index}`} mat={mat} />
        ))}
      </div>
    </div>
  );
}
