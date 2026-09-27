/**
 * SQL display that links each upstream node the query references.
 *
 * The highlighter hands the renderer one hast row per line. Matches are found
 * on the row's full text, because a dotted name can span several tokens, and
 * the matched spans are spliced into the row as anchor nodes. The row is then
 * handed to the highlighter's own `createElement`, so linked and unlinked
 * rows are styled by exactly the same code path.
 */
import * as React from 'react';
import {
  Light as SyntaxHighlighter,
  createElement,
} from 'react-syntax-highlighter';
import sql from 'react-syntax-highlighter/dist/esm/languages/hljs/sql';
import foundation from 'react-syntax-highlighter/dist/esm/styles/hljs/foundation';

import './linkedSQL.css';

SyntaxHighlighter.registerLanguage('sql', sql);

const IDENTIFIER_CHAR = /[A-Za-z0-9_.]/;

// The shared highlighter style carries 2rem all round; the vertical half of
// that reads as dead space above and below the query.
const CODE_STYLE = {
  margin: 0,
  paddingTop: '0.75rem',
  paddingBottom: '0.75rem',
};

const escapeForPattern = name => name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/**
 * One pattern matching any upstream name, so a row is scanned once however
 * many upstreams there are. Longest name first, so a name that prefixes
 * another does not claim the match, and a trailing identifier character
 * rejects the match outright.
 */
export const buildUpstreamPattern = upstreams => {
  const names = [...new Set(upstreams)]
    .filter(Boolean)
    .sort((a, b) => b.length - a.length);
  if (!names.length) {
    return null;
  }
  return new RegExp(
    `(?:${names.map(escapeForPattern).join('|')})(?![A-Za-z0-9_.])`,
    'g',
  );
};

/** Where upstream names appear in `text`, never inside a longer identifier. */
export const findUpstreamMatches = (text, pattern) => {
  const matches = [];
  if (!pattern) {
    return matches;
  }
  pattern.lastIndex = 0;
  let found = pattern.exec(text);
  while (found) {
    const from = found.index;
    const before = from > 0 ? text[from - 1] : '';
    if (IDENTIFIER_CHAR.test(before)) {
      // Part of a longer identifier. Resume just past this start, since a
      // later occurrence of the same name may still stand alone.
      pattern.lastIndex = from + 1;
    } else {
      matches.push({ from, to: from + found[0].length, name: found[0] });
    }
    found = pattern.exec(text);
  }
  return matches;
};

const rowText = node =>
  node.type === 'text'
    ? node.value
    : (node.children || []).map(rowText).join('');

/** One text node split into text and anchor nodes at the match boundaries. */
const splitTextNode = (node, cursor, matches) => {
  const end = cursor + node.value.length;
  const pieces = [];
  let at = cursor;
  for (const match of matches) {
    if (match.from >= end || match.to <= cursor) {
      continue;
    }
    const from = Math.max(match.from, cursor);
    const to = Math.min(match.to, end);
    if (from > at) {
      pieces.push({
        type: 'text',
        value: node.value.slice(at - cursor, from - cursor),
      });
    }
    pieces.push({
      type: 'element',
      tagName: 'a',
      properties: {
        href: `/nodes/${match.name}`,
        className: ['sql-upstream-link'],
      },
      children: [
        { type: 'text', value: node.value.slice(from - cursor, to - cursor) },
      ],
    });
    at = to;
  }
  if (!pieces.length) {
    return { nodes: [node], length: node.value.length };
  }
  if (at < end) {
    pieces.push({ type: 'text', value: node.value.slice(at - cursor) });
  }
  return { nodes: pieces, length: node.value.length };
};

/**
 * `node` with anchors spliced in, plus the length of its text, so the caller
 * can advance the row offset without walking the subtree a second time.
 */
const linkNode = (node, cursor, matches) => {
  if (node.type === 'text') {
    return splitTextNode(node, cursor, matches);
  }
  const children = [];
  let length = 0;
  for (const child of node.children || []) {
    const linked = linkNode(child, cursor + length, matches);
    children.push(...linked.nodes);
    length += linked.length;
  }
  return { nodes: [{ ...node, children }], length };
};

export function LinkedSQL({ sql: query, upstreams = [] }) {
  const pattern = React.useMemo(
    () => buildUpstreamPattern(upstreams),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [upstreams.join('\n')],
  );

  const renderer = React.useCallback(
    ({ rows, stylesheet, useInlineStyles }) =>
      rows.map((row, rowIndex) => {
        const matches = findUpstreamMatches(rowText(row), pattern);
        const node = matches.length ? linkNode(row, 0, matches).nodes[0] : row;
        return createElement({
          node,
          stylesheet,
          useInlineStyles,
          key: `row-${rowIndex}`,
        });
      }),
    [pattern],
  );

  return (
    <SyntaxHighlighter
      language="sql"
      style={foundation}
      wrapLongLines={true}
      customStyle={CODE_STYLE}
      renderer={pattern ? renderer : undefined}
    >
      {query}
    </SyntaxHighlighter>
  );
}

/**
 * Highlighting a long query costs hundreds of milliseconds, and callers tend
 * to rebuild the upstream array on every render, so compare it by content.
 */
const sameProps = (before, after) =>
  before.sql === after.sql &&
  (before.upstreams || []).length === (after.upstreams || []).length &&
  (before.upstreams || []).every((name, i) => name === after.upstreams[i]);

export default React.memo(LinkedSQL, sameProps);
