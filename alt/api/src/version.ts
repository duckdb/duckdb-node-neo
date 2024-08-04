import duckdb from '@jraymakers/duckdb-node-bindings';

export function version(): string {
  return duckdb.library_version();
}
