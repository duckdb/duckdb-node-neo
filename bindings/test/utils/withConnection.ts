import duckdb from '@duckdb/node-bindings';

export async function withConnection(fn: (connection: duckdb.Connection) => Promise<void>): Promise<void> {
  const db = await duckdb.open();
  try {
    const con = await duckdb.connect(db);
    try {
      await fn(con);
    } finally {
      await duckdb.disconnect(con);
    }
  } finally {
    await duckdb.close(db);
  }
}
