/**
 * Unit tests for djNodeBadges. We test the pure DOM renderer (no CodeMirror)
 * directly. The full ViewPlugin + hoverTooltip integration is exercised
 * implicitly via NodeQueryField.test.jsx — here we just lock in the shape of
 * the popover content for each status / node combo.
 */
import { renderTooltipDom, refAtPos } from '../djNodeBadges';

describe('renderTooltipDom', () => {
  const sourceNode = {
    name: 'prodhive.dse.playback_f',
    type: 'source',
    status: 'valid',
    version: 'v1.0',
    mode: 'published',
    description: 'Playback fact table.',
    catalog: { name: 'prodhive' },
    columns: [
      { name: 'account_id', type: 'bigint' },
      {
        name: 'session_meta',
        type: 'struct<a int,b int>\nstruct<c int>',
      },
      {
        name: 'profile_id',
        type: 'bigint',
        dimension: { name: 'common.dimensions.profile' },
      },
    ],
  };

  const validStatus = {
    kind: 'valid',
    refType: 'source',
    node: sourceNode,
  };

  it('renders the header with type pill and qualified name', () => {
    const dom = renderTooltipDom(validStatus, sourceNode.name);
    const pill = dom.querySelector('.dj-node-tooltip__header .dj-node-chip');
    expect(pill.textContent).toBe('source');
    expect(pill.className).toContain('dj-node-chip--source');
    expect(dom.querySelector('.dj-node-tooltip__name').textContent).toBe(
      'prodhive.dse.playback_f',
    );
  });

  it('renders the metadata grid (status / version / mode / column count)', () => {
    const dom = renderTooltipDom(validStatus, sourceNode.name);
    const cells = dom.querySelectorAll('.dj-node-tooltip__cell');
    const map = {};
    cells.forEach(cell => {
      const label = cell.querySelector('.dj-node-tooltip__label').textContent;
      const value = cell.querySelector('.dj-node-tooltip__value').textContent;
      map[label] = value;
    });
    expect(map).toEqual({
      Status: 'valid',
      Version: 'v1.0',
      Mode: 'published',
      Columns: '3',
    });
  });

  it('renders the description', () => {
    const dom = renderTooltipDom(validStatus, sourceNode.name);
    expect(dom.querySelector('.dj-node-tooltip__description').textContent).toBe(
      'Playback fact table.',
    );
  });

  it('truncates descriptions over 280 chars', () => {
    const long = 'x'.repeat(500);
    const status = {
      ...validStatus,
      node: { ...sourceNode, description: long },
    };
    const dom = renderTooltipDom(status, sourceNode.name);
    const text = dom.querySelector('.dj-node-tooltip__description').textContent;
    expect(text.length).toBe(278); // 277 + ellipsis
    expect(text.endsWith('…')).toBe(true);
  });

  it('renders the scrollable column list with name + type', () => {
    const dom = renderTooltipDom(validStatus, sourceNode.name);
    const rows = dom.querySelectorAll('.dj-node-tooltip__col-row');
    expect(rows).toHaveLength(3);
    const first = rows[0];
    expect(first.querySelector('.dj-node-tooltip__col-name').textContent).toBe(
      'account_id',
    );
    expect(first.querySelector('.dj-node-tooltip__col-type').textContent).toBe(
      'bigint',
    );
  });

  it('clamps multi-line struct types to one line, with full text on title', () => {
    const dom = renderTooltipDom(validStatus, sourceNode.name);
    const rows = dom.querySelectorAll('.dj-node-tooltip__col-row');
    const structType = rows[1].querySelector('.dj-node-tooltip__col-type');
    expect(structType.textContent).toBe('struct<a int,b int>');
    expect(structType.getAttribute('title')).toBe(
      'struct<a int,b int>\nstruct<c int>',
    );
  });

  it('tags columns that link to a dimension', () => {
    const dom = renderTooltipDom(validStatus, sourceNode.name);
    const rows = dom.querySelectorAll('.dj-node-tooltip__col-row');
    expect(rows[0].querySelector('.dj-node-tooltip__col-tag')).toBeNull();
    expect(rows[1].querySelector('.dj-node-tooltip__col-tag')).toBeNull();
    const tag = rows[2].querySelector('.dj-node-tooltip__col-tag');
    expect(tag).not.toBeNull();
    expect(tag.textContent).toBe('dim');
    expect(tag.getAttribute('title')).toBe(
      'Links to common.dimensions.profile',
    );
  });

  it('renders the Open node footer link', () => {
    const dom = renderTooltipDom(validStatus, sourceNode.name);
    const link = dom.querySelector('.dj-node-tooltip__footer a');
    expect(link.textContent).toBe('Open node →');
    expect(link.getAttribute('href')).toBe('/nodes/prodhive.dse.playback_f');
    expect(link.getAttribute('target')).toBe('_blank');
  });

  it('collapses to a "Not found" note for invalid status', () => {
    const dom = renderTooltipDom(
      { kind: 'invalid', refType: 'node', message: 'gone' },
      'foo.bar',
    );
    expect(dom.querySelector('.dj-node-tooltip__note').textContent).toBe(
      'Not found in DJ.',
    );
    expect(dom.querySelector('.dj-node-tooltip__grid')).toBeNull();
    expect(dom.querySelector('.dj-node-tooltip__columns')).toBeNull();
    expect(dom.querySelector('.dj-node-tooltip__footer')).toBeNull();
  });

  it('collapses to a "Registering…" note for registering status', () => {
    const dom = renderTooltipDom(
      { kind: 'registering', refType: 'source' },
      'prodhive.x.y',
    );
    expect(dom.querySelector('.dj-node-tooltip__note').textContent).toBe(
      'Registering…',
    );
    expect(dom.querySelector('.dj-node-tooltip__grid')).toBeNull();
  });

  it('falls back to the refKey when the node object has no name', () => {
    const dom = renderTooltipDom(
      { kind: 'valid', refType: 'source', node: {} },
      'unregistered.table.ref',
    );
    expect(dom.querySelector('.dj-node-tooltip__name').textContent).toBe(
      'unregistered.table.ref',
    );
    // No node name → no footer link.
    expect(dom.querySelector('.dj-node-tooltip__footer')).toBeNull();
  });

  it('skips the description block when the node has none', () => {
    const node = { ...sourceNode, description: undefined };
    const dom = renderTooltipDom({ ...validStatus, node }, sourceNode.name);
    expect(dom.querySelector('.dj-node-tooltip__description')).toBeNull();
  });

  it('skips the column list when there are no columns', () => {
    const node = { ...sourceNode, columns: [] };
    const dom = renderTooltipDom({ ...validStatus, node }, sourceNode.name);
    expect(dom.querySelector('.dj-node-tooltip__columns')).toBeNull();
  });
});

describe('refAtPos', () => {
  // refAtPos walks the doc line containing `pos`. We fake an EditorView that
  // exposes just the bits the helper actually touches.
  const fakeView = text => ({
    state: {
      doc: {
        lineAt: pos => {
          // Single-line doc, line.from = 0, line.text = whole text.
          if (pos < 0 || pos > text.length) {
            throw new Error('out of range');
          }
          return { from: 0, text };
        },
      },
    },
  });

  it('returns the dotted ref under the cursor position', () => {
    const view = fakeView('SELECT * FROM prodhive.dse.playback_f WHERE 1=1');
    const hit = refAtPos(view, 20); // somewhere inside `prodhive.dse.playback_f`
    expect(hit).toEqual({
      key: 'prodhive.dse.playback_f',
      from: 14,
      to: 37,
    });
  });

  it('returns null when the position is not inside any dotted ref', () => {
    const view = fakeView('SELECT * FROM prodhive.dse.playback_f WHERE 1=1');
    expect(refAtPos(view, 0)).toBeNull(); // on SELECT
    expect(refAtPos(view, 9)).toBeNull(); // on FROM
  });

  it('strips backticks from the returned key', () => {
    const view = fakeView('FROM `prodhive`.`dse`.`playback_f`');
    const hit = refAtPos(view, 10);
    expect(hit.key).toBe('prodhive.dse.playback_f');
  });

  it('matches 2-part refs (namespace.name)', () => {
    const view = fakeView('JOIN common.dimensions.account ON x = y');
    const hit = refAtPos(view, 8);
    expect(hit.key).toBe('common.dimensions.account');
  });
});
