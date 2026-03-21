import duckdb from '@duckdb/node-bindings';
import { withDatabase } from './withDatabase';

export async function withConnection(
  fn: (connection: duckdb.Connection, db: duckdb.Database) => Promise<void>,
): Promise<void> {
  await withDatabase({}, async (db) => {
    const connection = await duckdb.connect(db);
    try {
      await fn(connection, db);
    } finally {
      duckdb.disconnect_sync(connection);
    }
  });
}
