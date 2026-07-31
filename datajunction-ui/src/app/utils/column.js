export const getColumnIdentifier = (node, column) => {
  if (node?.type === 'cube') {
    return `${column.name}${column.dimension_column || ''}`;
  }
  return column.name;
};

export const decodeColumnIdentifier = identifier =>
  identifier
    .replaceAll('_DOT_', '.')
    .replaceAll('_DOT', '.')
    .replaceAll('_LBRACK_', '[')
    .replaceAll('_LBRACK', '[')
    .replaceAll('_RBRACK_', ']')
    .replaceAll('_RBRACK', ']');
