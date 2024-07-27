import duckdb from 'duckdb';

export function version(): string {
  return duckdb.library_version();
}
