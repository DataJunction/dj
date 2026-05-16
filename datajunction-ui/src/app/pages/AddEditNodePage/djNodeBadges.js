/**
 * CodeMirror 6 extension that styles DJ node references in the SQL editor
 * as inline chips with a status indicator.
 *
 * Catalog-qualified tables (`catalog.schema.table`, where `catalog` is in
 * the known-catalogs list) get wrapped in a chip whose color + trailing icon
 * reflect the auto-register flow:
 *
 *   ⟳  registering   ✓  valid (registered or already known)   ✗  invalid
 *
 * The extension is purely a display layer — it reads status from a
 * caller-provided `getStatus(key)` and never writes data itself. The host
 * component (NodeQueryField) drives the registration flow and dispatches
 * `refreshBadges` whenever status changes so decorations rebuild even when
 * the doc itself hasn't changed.
 */
import { ViewPlugin, Decoration, hoverTooltip } from '@codemirror/view';
import { StateEffect } from '@codemirror/state';

// Match dotted identifiers of any length (2+ segments). Greedy `+` keeps
// `a.b.c.d` as a single match instead of splitting into `a.b` + `.c.d`.
// Each segment is `[a-zA-Z_]\w*`, optionally backtick-quoted.
const DOTTED_REF_RE =
  /`?[a-zA-Z_][a-zA-Z0-9_]*`?(?:\.`?[a-zA-Z_][a-zA-Z0-9_]*`?)+/g;

function unbacktick(s) {
  return s.replace(/`/g, '');
}

export const refreshBadges = StateEffect.define();

function classForStatus(statusKind) {
  switch (statusKind) {
    case 'registering':
      return 'dj-node-chip--registering';
    case 'valid':
      return 'dj-node-chip--valid';
    case 'warning':
      return 'dj-node-chip--warning';
    case 'invalid':
      return 'dj-node-chip--invalid';
    default:
      return 'dj-node-chip--unknown';
  }
}

function buildDecorations(view, getStatus, getKnownCatalogs) {
  const known = new Set((getKnownCatalogs() || []).map(c => c.toLowerCase()));
  const doc = view.state.doc;
  const ranges = [];

  for (const { from, to } of view.visibleRanges) {
    const slice = doc.sliceString(from, to);
    DOTTED_REF_RE.lastIndex = 0;
    let m;
    while ((m = DOTTED_REF_RE.exec(slice)) !== null) {
      const whole = m[0];
      const key = unbacktick(whole);
      const status = getStatus(key);
      const segments = key.split('.');

      let refType;
      if (status?.refType) {
        // validateNode told us what this is; honor that.
        refType = status.refType;
      } else if (
        segments.length === 3 &&
        known.has(segments[0].toLowerCase())
      ) {
        // 3-part with a known catalog: candidate source registration.
        refType = 'source';
      } else {
        // No status entry and not a catalog-qualified table — skip.
        // This avoids false positives on `alias.column` patterns.
        continue;
      }

      const cls = `dj-node-chip dj-node-chip--${refType} ${classForStatus(
        status?.kind,
      )}`;
      const start = from + m.index;
      const end = start + whole.length;
      ranges.push({
        from: start,
        to: end,
        deco: Decoration.mark({
          class: cls,
          attributes: status?.message ? { title: status.message } : undefined,
        }),
      });
    }
  }

  ranges.sort((a, b) => a.from - b.from || a.to - b.to);
  return Decoration.set(ranges.map(r => r.deco.range(r.from, r.to)));
}

/**
 * Factory. Returns a CodeMirror extension that decorates DJ node refs.
 *
 *   getStatus(key)       — returns { kind, message? } | undefined for `catalog.schema.table`
 *   getKnownCatalogs()   — returns string[] (lowercase catalog names)
 *
 * Both are read on every rebuild, so pass ref-backed closures and update
 * the ref synchronously before dispatching `refreshBadges` — otherwise the
 * extension reads pre-update state.
 */
export function djNodeBadges({ getStatus, getKnownCatalogs }) {
  return ViewPlugin.fromClass(
    class {
      constructor(view) {
        this.decorations = buildDecorations(view, getStatus, getKnownCatalogs);
      }
      update(update) {
        const refreshed = update.transactions.some(tr =>
          tr.effects.some(e => e.is(refreshBadges)),
        );
        if (update.docChanged || update.viewportChanged || refreshed) {
          this.decorations = buildDecorations(
            update.view,
            getStatus,
            getKnownCatalogs,
          );
        }
      }
    },
    { decorations: v => v.decorations },
  );
}

/**
 * Find the dotted ref under a given document position. Returns
 * `{ key, from, to }` if the position falls inside a 2+-segment dotted
 * identifier, else null. Used by the hover tooltip to identify which chip
 * the cursor is on.
 */
export function refAtPos(view, pos) {
  const line = view.state.doc.lineAt(pos);
  DOTTED_REF_RE.lastIndex = 0;
  let m;
  while ((m = DOTTED_REF_RE.exec(line.text)) !== null) {
    const start = line.from + m.index;
    const end = start + m[0].length;
    if (pos >= start && pos <= end) {
      return { key: unbacktick(m[0]), from: start, to: end };
    }
  }
  return null;
}

export function renderTooltipDom(status, refKey) {
  const wrap = document.createElement('div');
  wrap.className = 'dj-node-tooltip';

  const node = status.node || {};

  // Header — type pill + qualified name.
  const header = document.createElement('div');
  header.className = 'dj-node-tooltip__header';

  const pill = document.createElement('span');
  pill.className = `dj-node-chip dj-node-chip--${status.refType || 'node'}`;
  pill.textContent = status.refType || 'node';
  header.appendChild(pill);

  const name = document.createElement('span');
  name.className = 'dj-node-tooltip__name';
  name.textContent = node.name || refKey;
  header.appendChild(name);

  wrap.appendChild(header);

  // Special-case transient + error states — no metadata grid, just one line.
  if (status.kind === 'invalid') {
    const note = document.createElement('div');
    note.className = 'dj-node-tooltip__note';
    note.textContent = 'Not found in DJ.';
    wrap.appendChild(note);
    return wrap;
  }
  if (status.kind === 'registering') {
    const note = document.createElement('div');
    note.className = 'dj-node-tooltip__note';
    note.textContent = 'Registering…';
    wrap.appendChild(note);
    return wrap;
  }

  // Metadata grid (2 columns of label/value pairs).
  const grid = document.createElement('div');
  grid.className = 'dj-node-tooltip__grid';
  const addCell = (label, value) => {
    if (value == null || value === '') return;
    const cell = document.createElement('div');
    cell.className = 'dj-node-tooltip__cell';
    const l = document.createElement('span');
    l.className = 'dj-node-tooltip__label';
    l.textContent = label;
    const v = document.createElement('span');
    v.className = 'dj-node-tooltip__value';
    v.textContent = value;
    cell.appendChild(l);
    cell.appendChild(v);
    grid.appendChild(cell);
  };
  if (node.status) addCell('Status', node.status);
  if (node.version) addCell('Version', node.version);
  if (node.mode) addCell('Mode', node.mode);
  if (Array.isArray(node.columns)) {
    addCell('Columns', String(node.columns.length));
  }
  if (grid.childElementCount > 0) wrap.appendChild(grid);

  // Description (truncated if long).
  if (node.description) {
    const d = document.createElement('div');
    d.className = 'dj-node-tooltip__description';
    const text = node.description;
    d.textContent = text.length > 280 ? text.slice(0, 277) + '…' : text;
    wrap.appendChild(d);
  }

  // Column list — scrollable. The point of the popover for someone writing
  // SQL is to see what's available to SELECT without leaving the editor.
  if (Array.isArray(node.columns) && node.columns.length > 0) {
    const list = document.createElement('div');
    list.className = 'dj-node-tooltip__columns';
    for (const col of node.columns) {
      const row = document.createElement('div');
      row.className = 'dj-node-tooltip__col-row';

      const n = document.createElement('span');
      n.className = 'dj-node-tooltip__col-name';
      n.textContent = col.name;
      row.appendChild(n);

      // Mark columns that link to a dimension — useful signal for cube/SQL work.
      if (col.dimension || col.dimension_column) {
        const tag = document.createElement('span');
        tag.className = 'dj-node-tooltip__col-tag';
        tag.textContent = 'dim';
        tag.title = col.dimension?.name
          ? `Links to ${col.dimension.name}`
          : 'Linked dimension';
        row.appendChild(tag);
      }

      const t = document.createElement('span');
      t.className = 'dj-node-tooltip__col-type';
      // Struct/array types can be enormous (multi-line) — keep it on one line
      // and let CSS clamp; full info is on the node page.
      t.textContent = (col.type || '').split('\n')[0];
      t.title = col.type || '';
      row.appendChild(t);

      list.appendChild(row);
    }
    wrap.appendChild(list);
  }

  // Footer link.
  if (node.name) {
    const foot = document.createElement('div');
    foot.className = 'dj-node-tooltip__footer';
    const link = document.createElement('a');
    link.href = `/nodes/${encodeURIComponent(node.name)}`;
    link.target = '_blank';
    link.rel = 'noreferrer noopener';
    link.textContent = 'Open node →';
    foot.appendChild(link);
    wrap.appendChild(foot);
  }

  return wrap;
}

/**
 * Hover tooltip extension. Pairs with djNodeBadges so the same
 * `getStatus` source of truth drives the popover content.
 */
export function djNodeHoverTooltip({ getStatus, getKnownCatalogs }) {
  return hoverTooltip(
    (view, pos) => {
      const hit = refAtPos(view, pos);
      if (!hit) return null;
      const status = getStatus(hit.key);
      if (!status) {
        // No status entry; only show tooltip for catalog-qualified refs that
        // would render as a chip — otherwise we'd pop up on every `t.column`.
        const known = new Set(
          (getKnownCatalogs() || []).map(c => c.toLowerCase()),
        );
        const segments = hit.key.split('.');
        if (segments.length !== 3 || !known.has(segments[0].toLowerCase())) {
          return null;
        }
      }
      return {
        pos: hit.from,
        end: hit.to,
        above: true,
        create: () => ({
          dom: renderTooltipDom(status || { refType: 'source' }, hit.key),
        }),
      };
    },
    { hoverTime: 150 },
  );
}
