import duckdb from '@duckdb/node-bindings';

export async function withConnection(fn: (connection: duckdb.Connection) => Promise<void>): Promise<void> {
  const db = await duckdb.open();
  try {
    const connection = await duckdb.connect(db);
    try {
      await fn(connection);
    } finally {
      await duckdb.disconnect(connection);
    }
  } finally {
    await duckdb.close(db);
  }
}
