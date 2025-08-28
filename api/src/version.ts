import duckdb from '@databrainhq/node-bindings';

export function version(): string {
  return duckdb.library_version();
}
