import { extractCatalogTables } from '../catalogTables';

describe('extractCatalogTables', () => {
  it('extracts a 3-part identifier when the catalog matches', () => {
    const result = extractCatalogTables(
      'select * from prodhive.dse_dev.identity_ab_alloc_f f',
      ['prodhive'],
    );
    expect(result).toEqual([['prodhive', 'dse_dev', 'identity_ab_alloc_f']]);
  });

  it('extracts multiple distinct tables', () => {
    const result = extractCatalogTables(
      'select a.* from prodhive.s1.t1 a join prodhive.s2.t2 b on a.id = b.id',
      ['prodhive'],
    );
    expect(result).toEqual([
      ['prodhive', 's1', 't1'],
      ['prodhive', 's2', 't2'],
    ]);
  });

  it('deduplicates the same table referenced twice in the same query', () => {
    const result = extractCatalogTables(
      'select * from prodhive.s.t a join prodhive.s.t b on a.id = b.id',
      ['prodhive'],
    );
    expect(result).toEqual([['prodhive', 's', 't']]);
  });

  it('skips 3-part identifiers whose catalog is not in the known list', () => {
    const result = extractCatalogTables(
      'select * from foo.bar.baz, prodhive.s.t',
      ['prodhive'],
    );
    expect(result).toEqual([['prodhive', 's', 't']]);
  });

  it('matches catalog name case-insensitively', () => {
    const result = extractCatalogTables('select * from ProdHive.s.t', [
      'prodhive',
    ]);
    expect(result).toEqual([['ProdHive', 's', 't']]);
  });

  it('handles backtick-quoted identifiers', () => {
    const result = extractCatalogTables(
      'select * from `prodhive`.`my_schema`.`my_table`',
      ['prodhive'],
    );
    expect(result).toEqual([['prodhive', 'my_schema', 'my_table']]);
  });

  it('returns nothing when no catalog is known', () => {
    const result = extractCatalogTables('select * from prodhive.s.t', []);
    expect(result).toEqual([]);
  });

  it('returns nothing when the SQL contains no 3-part identifiers', () => {
    const result = extractCatalogTables('select * from my_node where x > 0', [
      'prodhive',
    ]);
    expect(result).toEqual([]);
  });
});
