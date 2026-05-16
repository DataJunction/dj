// Matches catalog.schema.table (3-part dot-separated identifiers, backtick-quoted or plain)
const CATALOG_TABLE_PATTERN =
  /`?([a-zA-Z_][a-zA-Z0-9_]*)`?\.`?([a-zA-Z_][a-zA-Z0-9_]*)`?\.`?([a-zA-Z_][a-zA-Z0-9_]*)`?/;

/**
 * Extracts all `catalog.schema.table` references in `sql` whose catalog is in
 * `knownCatalogs` (case-insensitive). Returns deduplicated [catalog, schema, table] tuples.
 */
export function extractCatalogTables(sql, knownCatalogs) {
  const re = new RegExp(CATALOG_TABLE_PATTERN.source, 'g');
  const known = new Set(knownCatalogs.map(c => c.toLowerCase()));
  const seen = new Set();
  const out = [];
  let m;
  while ((m = re.exec(sql)) !== null) {
    const [, catalog, schema, table] = m;
    if (!known.has(catalog.toLowerCase())) continue;
    const key = `${catalog}.${schema}.${table}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push([catalog, schema, table]);
  }
  return out;
}
