/**
 * Proposed materialization panel, in three candidate layouts behind an A/B/C switcher.
 *
 * Renders a `MaterializationState` (see src/mocks/materializationState.js) rather
 * than stitching `materializations()` and `availabilityStates()` together in the
 * component, which is why the current tab can show an empty "Output Tables" while
 * an output dataset sits at the bottom of the same page.
 *
 * Rules all three layouts enforce:
 *
 *  - Coverage is always shown against a target. A bare "20260806 to 20260806" is
 *    uninterpretable; "covered through 20260806, target 20260808" is not.
 *  - Serving is a property of the cube revision, not of a materialization, so it is
 *    rendered once above them rather than repeated (and misattributed) per card.
 *  - Anything the engine reports is labelled as such. Absent run data qualifies the
 *    verdict; it does not replace it, because DJ still knows the coverage.
 */
import { useState } from 'react';
import { Link } from 'react-router-dom';
import {
  coverageSquares,
  dayPartitionsBetween,
  mergeCoverage,
  strategyBadge,
} from './materializationState';

// Glyphs and semantic classes follow the planner's `getStatusInfo`
// (QueryPlannerPage/PreAggDetailsPanel.jsx), so the two panels read as one product.
const VERDICT = {
  healthy: { glyph: '●', className: 'mat-verdict--healthy' },
  stale: { glyph: '◐', className: 'mat-verdict--stale' },
  failing: { glyph: '■', className: 'mat-verdict--failing' },
  unknown: { glyph: '○', className: 'mat-verdict--unknown' },
};

const LAYOUTS = [
  { id: 'A', title: 'Table' },
  { id: 'B', title: 'Master / detail' },
  { id: 'C', title: 'Stacked rows' },
];

/** Run data is absent for every materialization today; say so without burying coverage. */
const RUN_UNKNOWN = 'Run status unknown.';

function plural(count, noun) {
  return `${count} ${noun}${count === 1 ? '' : 's'}`;
}

/**
 * One honest sentence about a materialization, plus the two-or-three word form the
 * table layout puts in its status column.
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
    return {
      verdict: 'unknown',
      headline: 'Coverage unknown',
      className: VERDICT.unknown.className,
    };
  }
  if (coverage.missing?.length) {
    return {
      verdict: 'stale',
      headline: `${plural(coverage.missing.length, 'day')} behind`,
      className: VERDICT.stale.className,
    };
  }
  return {
    verdict: 'healthy',
    headline: 'On target',
    className: VERDICT.healthy.className,
  };
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
 * Middle-elided table name. Serving tables run to ~85 characters, of which the head is
 * a constant `dj__` prefix and the tail is the node suffix, version and hash that
 * actually distinguish one from another; the namespace in between is already on the
 * page. The tail is sized to clear `_cube_v<version>_<16 hex>` whole.
 */
const NAME_HEAD = 4;
const NAME_TAIL = 26;

function elideTableName(name) {
  const text = String(name || '');
  return text.length <= NAME_HEAD + NAME_TAIL + 1
    ? text
    : `${text.slice(0, NAME_HEAD)}…${text.slice(-NAME_TAIL)}`;
}

/** "20260809" -> "0809", for the cramped end-label on the table layout's bar. */
function shortDay(partition) {
  return String(partition || '').slice(4);
}

/**
 * Coverage as a proportional range bar.
 *
 * Was one cell per partition, to make an interior backfill hole read as a hole.
 * Real availability rows carry only `min_temporal_partition` and
 * `max_temporal_partition` -- `partitions` is empty -- so DJ cannot see interior
 * holes at all and coverage is always a contiguous prefix. Per-partition cells
 * therefore claimed a fidelity the data does not have.
 */
function CoverageBar({ outcome, labels = 'ends' }) {
  if (!outcome.coverageKnown) {
    return (
      <div className="coverage coverage--unknown" aria-label="Coverage unknown">
        coverage unknown
      </div>
    );
  }

  const span = dayPartitionsBetween(
    outcome.target.from,
    outcome.target.through,
  );
  const missing = new Set(outcome.missing);
  const notDue = new Set(outcome.notDueYet);
  const counts = span.reduce(
    (acc, key) => {
      const state = missing.has(key)
        ? 'missing'
        : notDue.has(key)
        ? 'notDue'
        : 'covered';
      acc[state] += 1;
      return acc;
    },
    { covered: 0, missing: 0, notDue: 0 },
  );

  const segments = [
    ['covered', counts.covered],
    ['missing', counts.missing],
    ['not-due', counts.notDue],
  ].filter(([, count]) => count > 0);

  const track = (
    <div className="coverage__track">
      {segments.map(([state, count]) => (
        <span
          key={state}
          className={`coverage__seg coverage__seg--${state}`}
          style={{ flexGrow: count }}
          aria-label={`${count} ${state}`}
          title={`${plural(count, 'day')} ${state}`}
        />
      ))}
    </div>
  );

  if (labels === 'trailing') {
    return (
      <div className="coverage coverage--inline" aria-label="Coverage">
        {track}
        <span className="coverage__tick">
          → {shortDay(outcome.target.through)}
        </span>
      </div>
    );
  }
  return (
    <div className="coverage" aria-label="Coverage">
      <div className="coverage__scale">
        <span>from {outcome.target.from}</span>
        <span>through {outcome.target.through}</span>
      </div>
      {track}
    </div>
  );
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
      <span className="mat-chip__name mat-mono">{elideTableName(table)}</span>
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
 * Serving on one line: where the data is, how fresh it is, and where to go look at it.
 *
 * The four-line block this replaced spent three of them on a table name and a GMT
 * timestamp, neither of which anyone reads at that length. Both survive in `title`.
 */
function ServingLine({ node, serving }) {
  const preview = previewUrl(node);
  const freshness = serving.validThrough
    ? `updated ${relativeTime(serving.validThrough)}`
    : 'update time unknown';

  return (
    <div className="mat-serving">
      <TableChip
        catalog={serving.servingCatalog}
        table={serving.servingTable}
      />
      <span className="mat-serving__sep">·</span>
      <span
        className="mat-dim"
        title={
          serving.validThrough
            ? `valid through ${formatUtc(serving.validThrough)}`
            : undefined
        }
      >
        {freshness}
      </span>
      <span className="mat-serving__links">
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
      </span>
    </div>
  );
}

/**
 * Coverage on its own labelled row, beneath serving.
 *
 * It sits at cube scope because that is the scope of the fact -- availability is keyed
 * to the node revision, so every materialization on it reports the same watermarks --
 * but it is a different question from "where is the data", and trailing it onto the
 * serving line squeezed four separate things onto one row.
 */
function CoverageLine({ coverage }) {
  const verdict = coverageVerdict(coverage);
  return (
    <div className="mat-serving mat-serving--coverage">
      <span className={verdict.className}>
        <span className="mat-glyph">{VERDICT[verdict.verdict].glyph}</span>{' '}
        {verdict.headline}
      </span>
      {coverage?.coverageKnown ? <LabelledSquares coverage={coverage} /> : null}
    </div>
  );
}

/**
 * The strip with its range written under the ends it labels.
 *
 * The dates are what make the squares mean anything -- a run of colour says "behind"
 * without saying since when -- so they sit under the squares they describe rather than
 * trailing off to the right as a sentence, which put the reader's eye past the strip
 * to find out what it spanned.
 *
 * Only the ends are labelled. Every bucket carries its own range in `title`, and 25
 * dates written out would be a table, not an axis.
 */
function LabelledSquares({ coverage }) {
  const squares = coverageSquares(coverage);
  if (!squares.length) {
    return null;
  }
  const first = squares[0].from;
  const last = squares[squares.length - 1].through;
  return (
    <span className="mat-squares-axis">
      <SquaresRun squares={squares} />
      <span className="mat-squares-axis__ends">
        <span>{first}</span>
        {first === last ? null : <span>{last}</span>}
      </span>
    </span>
  );
}

/**
 * The serving block as layout B renders it, kept verbatim so B stays the control the
 * one-line treatment in A and C is judged against. Delete with B.
 */
function ServingBlockLegacy({ serving }) {
  return (
    <div>
      <div className="mat-header__table">
        <span className="mat-header__catalog">{serving.servingCatalog}</span>
        <span className="mat-header__mono">{serving.servingTable}</span>
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
      {serving.validThrough ? (
        <div className="mat-header__note">
          valid through {formatUtc(serving.validThrough)}
        </div>
      ) : null}
    </div>
  );
}

/**
 * Serving, hoisted out of the materializations.
 *
 * `nodeavailabilitystate.node_id` points at a node revision; `Materialization` carries
 * no availability link at all. Rendering the same table and valid-through inside each
 * card claimed an attribution the schema cannot make.
 */
function CubeHeader({ state, mats, serving, coverage, layout }) {
  return (
    <div className="mat-header">
      <div className="mat-header__title">
        <h4>Materializations</h4>
        <span className="mat-header__count">
          {plural(mats.length, 'materialization')} configured
          {state.node.version ? ` · ${state.node.version}` : ''}
        </span>
      </div>
      <div className="mat-header__serving">
        <span className="mat-header__key">Serving</span>
        {!serving ? (
          <div className="mat-header__note">
            nothing built for this revision yet
          </div>
        ) : layout === 'B' ? (
          <ServingBlockLegacy serving={serving} />
        ) : (
          <ServingLine node={state.node} serving={serving} />
        )}
      </div>
      {/* Layout B keeps its per-card coverage, so it stays the control the cube-scoped
          treatment in A and C is judged against. */}
      {serving && layout !== 'B' ? (
        <div className="mat-header__serving">
          <span className="mat-header__key">Coverage</span>
          <CoverageLine coverage={coverage} />
        </div>
      ) : null}
    </div>
  );
}

function ScheduleCell({ intent }) {
  return (
    <>
      <div>{intent.scheduleHuman || intent.schedule}</div>
      {intent.scheduleHuman ? (
        <div className="mat-dim mat-mono">{intent.schedule}</div>
      ) : null}
    </>
  );
}

function InactiveBadge({ mat }) {
  return mat.active ? null : (
    <span className="badge partition_value">inactive</span>
  );
}

/**
 * The workflow links, as a first-class column rather than a footnote.
 *
 * This is the one thing on the row a reader can act on: everything else describes the
 * build, and these are how you go look at it. They were buried at the bottom of the
 * old tree view, three levels into a disclosure nobody opened.
 */
function WorkflowLinks({ workflows }) {
  if (!workflows?.length) {
    return <span className="mat-dim">none</span>;
  }
  return (
    <span className="mat-workflows">
      {workflows.map(wf => (
        <a
          key={wf.url}
          href={wf.url}
          target="_blank"
          rel="noreferrer"
          className="mat-workflow-link"
          title={wf.url}
        >
          {wf.label} ↗
        </a>
      ))}
    </span>
  );
}

/**
 * A -- one row per materialization.
 *
 * No coverage column: coverage is a property of the cube, not of a materialization,
 * and it now sits once on the serving line. What is left here is what genuinely
 * differs between the rows -- strategy, schedule, workflows, run -- which is also
 * what frees the width to give the workflow links a column of their own.
 */
function LayoutTable({ mats }) {
  return (
    <div className="mat-table" role="table" aria-label="Materialization state">
      <div className="mat-table__row mat-table__row--head" role="row">
        <span role="columnheader">Materialization</span>
        <span role="columnheader">Schedule</span>
        <span role="columnheader">Workflows</span>
        <span role="columnheader">Last run</span>
      </div>
      {mats.map((mat, index) => (
        <div className="mat-table__row" role="row" key={`${mat.name}-${index}`}>
          <span role="cell">
            {/* Engine plus badge, as layout C renders it: `label` folded the
                strategy into the name and then repeated it. */}
            <div className="mat-table__label">
              {mat.engine || mat.label}
              <StrategyBadge intent={mat.intent} />
              <InactiveBadge mat={mat} />
            </div>
          </span>
          <span role="cell">
            <ScheduleCell intent={mat.intent} />
          </span>
          <span role="cell">
            <WorkflowLinks workflows={mat.workflows} />
          </span>
          <span role="cell" className="mat-dim">
            {mat.execution ? 'reported' : 'unknown'}
          </span>
        </div>
      ))}
    </div>
  );
}

/** Declared intent. DJ owns this outright, so it is never `unknown`. */
function DeclaredBlock({ intent }) {
  return (
    <div className="mat-block">
      <h5>Declared</h5>
      <div>{intent.scheduleHuman}</div>
      <div className="mat-dim mat-mono">{intent.schedule}</div>
      <div>{intent.strategy}</div>
      {intent.lookbackWindow ? (
        <div>lookback {intent.lookbackWindow.toLowerCase()}</div>
      ) : null}
      {/* Cubes with no temporal partition column exist; they simply have no
          partition to declare, and inventing one would misreport the cube. */}
      <div className="mat-dim">
        {intent.partition
          ? `partition ${intent.partition.column} (${intent.partition.granularity})`
          : 'no temporal partition'}
      </div>
    </div>
  );
}

/** Engine-reported execution. Absent means absent — never backfilled from intent. */
function LastRunBlock({ execution }) {
  if (!execution) {
    return (
      <div className="mat-block">
        <h5>Last run</h5>
        <div className="mat-dim">no run information</div>
      </div>
    );
  }
  const run = execution.lastRun;
  return (
    <div className="mat-block">
      <h5>Last run</h5>
      {run ? (
        <>
          <div>
            {run.status} {formatUtc(run.endedAt)}
          </div>
          <div className="mat-dim">
            attempt {run.attempt} of {run.maxAttempts}
          </div>
          <div className="mat-dim">processing {run.processingPartition}</div>
        </>
      ) : (
        <div className="mat-dim">no runs recorded</div>
      )}
      {execution.nextScheduledAt ? (
        <div className="mat-dim">
          next {formatUtc(execution.nextScheduledAt)}
        </div>
      ) : null}
    </div>
  );
}

/**
 * B -- planner-style master/detail. This is the one layout that keeps the planner's
 * 11px scale, because it is the planner's selection-panel idiom: a narrow rail of
 * choices beside a detail pane, both bounded rather than page-wide.
 */
function LayoutMasterDetail({ mats }) {
  const [selected, setSelected] = useState(0);
  const mat = mats[Math.min(selected, mats.length - 1)];
  if (!mat) {
    return null;
  }
  const { verdict, detail } = summarize(mat);

  return (
    <div className="mat-split">
      <div
        className="mat-split__rail"
        role="listbox"
        aria-label="Materializations"
      >
        {mats.map((candidate, index) => {
          const tone = VERDICT[summarize(candidate).verdict];
          return (
            <button
              type="button"
              role="option"
              aria-selected={index === selected}
              key={`${candidate.name}-${index}`}
              className={`mat-split__item ${
                index === selected ? 'mat-split__item--active' : ''
              }`}
              onClick={() => setSelected(index)}
            >
              <span className={`mat-glyph ${tone.className}`}>
                {tone.glyph}
              </span>
              <span className="mat-split__item-text">
                <span className="mat-split__item-label">{candidate.label}</span>
                <span className="mat-dim">{summarize(candidate).headline}</span>
              </span>
            </button>
          );
        })}
      </div>
      <div className="mat-split__detail">
        <div className="mat-split__head">
          <span className={`mat-glyph ${VERDICT[verdict].className}`}>
            {VERDICT[verdict].glyph}
          </span>
          <span className="mat-split__head-label">{mat.label}</span>
          <InactiveBadge mat={mat} />
        </div>
        <div className="mat-split__verdict">{detail}</div>
        <CoverageBar outcome={mat.outcome} />
        <div className="mat-split__blocks">
          <DeclaredBlock intent={mat.intent} />
          <LastRunBlock execution={mat.execution} />
          <div className="mat-block">
            <h5>Workflows</h5>
            <WorkflowLinks workflows={mat.workflows} />
          </div>
        </div>
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

function CoverageSquares({ outcome, partition }) {
  const squares = coverageSquares(outcome);
  const note = partition
    ? `${partition.column} (${partition.granularity})`
    : 'no temporal partition';

  if (!squares.length) {
    return (
      <div className="mat-stack__coverage">
        <span className="coverage--unknown">coverage unknown</span>
        <span className="mat-dim mat-stack__partition">{note}</span>
      </div>
    );
  }
  return (
    <div className="mat-stack__coverage">
      {/* A list rather than one labelled image: each bucket's meaning lives in its own
          label, so a reader can reach the behind ones instead of a "Coverage" summary. */}
      <SquaresRun squares={squares} />
      <span className="mat-dim">
        {outcome.target.from} → {outcome.target.through}
      </span>
      <span className="mat-dim mat-stack__partition">{note}</span>
    </div>
  );
}

/** Strategy, neutral by design: the status glyph is the row's only coloured element. */
function StrategyBadge({ intent }) {
  const text = strategyBadge(intent);
  return text ? <span className="mat-badge">{text}</span> : null;
}

/** C -- full-width rows, no columns; the header carries the scannable facts. */
function LayoutStacked({ mats }) {
  return (
    <div className="mat-stack">
      {mats.map((mat, index) => {
        const { verdict, detail } = summarize(mat);
        const tone = VERDICT[verdict];
        return (
          <div className="mat-stack__row" key={`${mat.name}-${index}`}>
            <div className="mat-stack__head">
              <span className={`mat-glyph ${tone.className}`}>
                {tone.glyph}
              </span>
              <span className="mat-stack__label">
                {mat.engine || mat.label}
              </span>
              <StrategyBadge intent={mat.intent} />
              <InactiveBadge mat={mat} />
              <span className="mat-stack__schedule">
                {mat.intent.scheduleHuman || mat.intent.schedule}
                <span className="mat-dim mat-mono"> {mat.intent.schedule}</span>
              </span>
              <WorkflowLinks workflows={mat.workflows} />
            </div>
            <div className="mat-stack__verdict">{detail}</div>
            <CoverageSquares
              outcome={mat.outcome}
              partition={mat.intent.partition}
            />
          </div>
        );
      })}
    </div>
  );
}

const LAYOUT_COMPONENTS = {
  A: LayoutTable,
  B: LayoutMasterDetail,
  C: LayoutStacked,
};

export default function MaterializationStatePanel({ state }) {
  const [layout, setLayout] = useState('A');
  const mats = state.materializations || [];
  // Every card previously repeated this; all of them are the same row, because
  // availability keys off the revision the materializations share.
  const serving = mats.find(mat => mat.outcome.servingTable)?.outcome ?? null;
  // The adapter supplies this; fixtures that predate it are merged here instead.
  const coverage = state.coverage ?? mergeCoverage(mats);
  const Layout = LAYOUT_COMPONENTS[layout];

  return (
    <div className={`mat-panel mat-panel--${layout.toLowerCase()}`}>
      <div className="mat-switcher" role="group" aria-label="Panel layout">
        {LAYOUTS.map(option => (
          <button
            type="button"
            key={option.id}
            title={option.title}
            aria-pressed={layout === option.id}
            className={`mat-switcher__btn ${
              layout === option.id ? 'mat-switcher__btn--active' : ''
            }`}
            onClick={() => setLayout(option.id)}
          >
            {option.id}
          </button>
        ))}
      </div>
      <CubeHeader
        state={state}
        mats={mats}
        serving={serving}
        coverage={coverage}
        layout={layout}
      />
      {/* Keyed by index as well: the same materialization name recurs across cube
          revisions, so the name alone is not unique within a node. */}
      <Layout mats={mats} />
    </div>
  );
}
