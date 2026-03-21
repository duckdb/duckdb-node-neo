import duckdb from '@duckdb/node-bindings';

export async function withDatabase(
  { path, config }: { path?: string; config?: duckdb.Config },
  fn: (db: duckdb.Database) => Promise<void>,
): Promise<void> {
  const db = await duckdb.open(path, config);
  try {
    await fn(db);
  } finally {
    duckdb.close_sync(db);
  }
}
