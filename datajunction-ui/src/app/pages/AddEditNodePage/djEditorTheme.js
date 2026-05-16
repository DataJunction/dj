/**
 * CodeMirror 6 theme tuned to share a palette with the rest of the DJ UI
 * (especially the `.node_type__*` pills and the `.dj-node-chip--*` chips
 * rendered inline by djNodeBadges). The goal: syntax colors and chip
 * colors come from the same family, so chips read as theme-native rather
 * than as foreign decoration.
 */
import { EditorView } from '@codemirror/view';
import { HighlightStyle, syntaxHighlighting } from '@codemirror/language';
import { tags as t } from '@lezer/highlight';

// Palette — kept in one place so it stays in sync with node-creation.scss.
const palette = {
  bg: '#fbfbfd',
  surface: '#f6f6f9',
  gutter: '#9aa2b1',
  selection: '#dbe2ec',
  caret: '#3b3f46',
  text: '#27303d',
  muted: '#8b94a5',
  keyword: '#6b3aa0', // matches cube/dimension violet family
  string: '#0e7c5a', // forest green
  number: '#a96621', // warm amber (same as dimension chip)
  operator: '#475569',
  functionName: '#0063b4', // transform blue
  punctuation: '#5c6573',
  comment: '#9ca3af',
};

export const djEditorTheme = EditorView.theme(
  {
    '&': {
      color: palette.text,
      backgroundColor: palette.bg,
      fontSize: '13px',
    },
    '.cm-content': {
      caretColor: palette.caret,
      fontFamily:
        '"JetBrains Mono", "SF Mono", Menlo, Consolas, "Liberation Mono", monospace',
    },
    '.cm-cursor, .cm-dropCursor': { borderLeftColor: palette.caret },
    '&.cm-focused .cm-selectionBackground, ::selection, .cm-selectionBackground':
      {
        backgroundColor: palette.selection,
      },
    '.cm-activeLine': {
      backgroundColor: 'rgba(99, 102, 241, 0.05)',
    },
    '.cm-gutters': {
      backgroundColor: palette.surface,
      color: palette.gutter,
      border: 'none',
    },
    '.cm-activeLineGutter': {
      backgroundColor: 'rgba(99, 102, 241, 0.08)',
      color: palette.text,
    },
    '.cm-lineNumbers .cm-gutterElement': {
      padding: '0 8px 0 6px',
    },
    '.cm-foldGutter .cm-gutterElement': {
      color: palette.muted,
    },
    '.cm-tooltip': {
      background: '#ffffff',
      border: '1px solid #e2e8f0',
      borderRadius: '6px',
      boxShadow: '0 4px 12px rgba(15, 23, 42, 0.06)',
    },
    '.cm-tooltip-autocomplete > ul > li[aria-selected]': {
      background: '#eef2ff',
      color: palette.text,
    },
    '.cm-matchingBracket, .cm-nonmatchingBracket': {
      backgroundColor: '#fef3c7',
      outline: 'none',
    },
    '.cm-panels': {
      backgroundColor: palette.surface,
      color: palette.text,
    },
  },
  { dark: false },
);

const highlight = HighlightStyle.define([
  {
    tag: [t.keyword, t.modifier, t.controlKeyword, t.operatorKeyword],
    color: palette.keyword,
    fontWeight: '500',
  },
  { tag: [t.string, t.special(t.string)], color: palette.string },
  { tag: [t.number, t.bool, t.null], color: palette.number },
  {
    tag: [t.function(t.variableName), t.function(t.propertyName)],
    color: palette.functionName,
  },
  {
    tag: [t.operator, t.compareOperator, t.arithmeticOperator, t.logicOperator],
    color: palette.operator,
  },
  {
    tag: [t.punctuation, t.paren, t.brace, t.bracket, t.separator],
    color: palette.punctuation,
  },
  {
    tag: [t.variableName, t.propertyName, t.attributeName],
    color: palette.text,
  },
  { tag: [t.typeName, t.className], color: palette.functionName },
  {
    tag: [t.comment, t.lineComment, t.blockComment],
    color: palette.comment,
    fontStyle: 'italic',
  },
  { tag: t.invalid, color: '#cc2222' },
]);

export const djEditorExtensions = [
  djEditorTheme,
  syntaxHighlighting(highlight),
];
