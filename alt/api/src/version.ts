import duckdb from '@duckdb-node-neo/node-bindings';

export function version(): string {
  return duckdb.library_version();
}
