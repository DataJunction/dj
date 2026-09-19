import { decodeColumnIdentifier, getColumnIdentifier } from '../column';

describe('getColumnIdentifier', () => {
  it('includes the role for cube columns', () => {
    expect(
      getColumnIdentifier(
        { type: 'cube' },
        { name: 'v3.date.date_id', dimension_column: '[ship]' },
      ),
    ).toBe('v3.date.date_id[ship]');
  });

  it('does not treat a non-cube reference link as a role', () => {
    expect(
      getColumnIdentifier(
        { type: 'transform' },
        { name: 'ship_date_id', dimension_column: 'date_id' },
      ),
    ).toBe('ship_date_id');
  });

  it('decodes a role-qualified backfill column', () => {
    expect(
      decodeColumnIdentifier('v3_DOT_date_DOT_date_id_LBRACK_ship_RBRACK'),
    ).toBe('v3.date.date_id[ship]');
  });
});
