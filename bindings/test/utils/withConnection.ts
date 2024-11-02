import duckdb from '@duckdb/node-bindings';

export async function withConnection(fn: (connection: duckdb.Connection) => Promise<void>): Promise<void> {
  const db = await duckdb.open();
  const connection = await duckdb.connect(db);
  await fn(connection);
}
