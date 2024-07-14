import duckdb from 'duckdb';
import { expect, suite, test } from 'vitest';

suite('errors', () => {
  test('wrong external type', async () => {
    const db = await duckdb.open(':memory:');
    try {
      expect(() => duckdb.query(db as unknown as duckdb.Connection, 'select 1')).toThrowError(/^Invalid connection argument$/);
    } finally {
      await duckdb.close(db);
    }
  });
});
