import duckdb from '@duckdb/node-bindings';
import { test } from 'vitest';

test('issue154', async () => {
  // const db = await duckdb.open();
  // const conn = await duckdb.connect(db);
  // await duckdb.query(conn, `CREATE SECRET secret1 (TYPE AZURE,CONNECTION_STRING 'ABC');`);
  // await duckdb.prepare(conn, `SELECT * FROM delta_scan('az://testing/data')`);
  // console.log('done');
  duckdb.test_issue154();
});
