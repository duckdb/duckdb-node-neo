import duckdb from '@duckdb/node-bindings';

export function version(): string {
  return duckdb.library_version();
}
