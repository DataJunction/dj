/**
 * SQL display that links each upstream node the query references.
 *
 * The highlighter splits a dotted name like `a.b.c` across several tokens, so
 * matches are found on each row's full text and the tokens are then split at
 * those boundaries. Tokens outside a match keep the styling they arrived with.
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

/**
 * Where each upstream name appears in `text`, longest name first so that a
 * name prefixing another does not claim the match, and never inside a longer
 * identifier.
 */
export const findUpstreamMatches = (text, upstreams) => {
  const matches = [];
  const byLongest = [...upstreams].sort((a, b) => b.length - a.length);
  for (const name of byLongest) {
    let from = text.indexOf(name);
    while (from !== -1) {
      const to = from + name.length;
      const before = from > 0 ? text[from - 1] : '';
      const after = to < text.length ? text[to] : '';
      const standsAlone =
        !IDENTIFIER_CHAR.test(before) && !IDENTIFIER_CHAR.test(after);
      const overlaps = matches.some(m => from < m.to && to > m.from);
      if (standsAlone && !overlaps) {
        matches.push({ from, to, name });
      }
      from = text.indexOf(name, from + 1);
    }
  }
  return matches.sort((a, b) => a.from - b.from);
};

const rowText = node => {
  if (node.type === 'text') {
    return node.value;
  }
  return (node.children || []).map(rowText).join('');
};

/**
 * Re-emit one token, split at any match boundary it crosses. `cursor` is the
 * token's start offset within the row.
 */
const linkToken = (node, cursor, matches, key) => {
  if (node.type !== 'text') {
    const children = [];
    let at = cursor;
    (node.children || []).forEach((child, index) => {
      children.push(linkToken(child, at, matches, `${key}-${index}`));
      at += rowText(child).length;
    });
    return React.createElement(
      node.tagName || 'span',
      {
        key,
        ...(node.properties?.className
          ? { className: node.properties.className.join(' ') }
          : {}),
      },
      children,
    );
  }

  const value = node.value;
  const end = cursor + value.length;
  const pieces = [];
  let at = cursor;
  matches
    .filter(m => m.from < end && m.to > cursor)
    .forEach((match, index) => {
      const from = Math.max(match.from, cursor);
      const to = Math.min(match.to, end);
      if (from > at) {
        pieces.push(value.slice(at - cursor, from - cursor));
      }
      pieces.push(
        <a
          key={`${key}-a${index}`}
          href={`/nodes/${match.name}`}
          className="sql-upstream-link"
        >
          {value.slice(from - cursor, to - cursor)}
        </a>,
      );
      at = to;
    });
  if (at < end) {
    pieces.push(value.slice(at - cursor));
  }
  return pieces.length ? (
    <React.Fragment key={key}>{pieces}</React.Fragment>
  ) : null;
};

export default function LinkedSQL({ sql: query, upstreams = [] }) {
  const renderer = ({ rows, stylesheet, useInlineStyles }) =>
    rows.map((row, rowIndex) => {
      const text = rowText(row);
      const matches = findUpstreamMatches(text, upstreams);
      if (!matches.length) {
        return createElement({
          node: row,
          stylesheet,
          useInlineStyles,
          key: `row-${rowIndex}`,
        });
      }
      let at = 0;
      const children = (row.children || []).map((child, index) => {
        const element = linkToken(child, at, matches, `${rowIndex}-${index}`);
        at += rowText(child).length;
        return element;
      });
      return <span key={rowIndex}>{children}</span>;
    });

  return (
    <SyntaxHighlighter
      language="sql"
      style={foundation}
      wrapLongLines={true}
      customStyle={CODE_STYLE}
      renderer={upstreams.length ? renderer : undefined}
    >
      {query}
    </SyntaxHighlighter>
  );
}
