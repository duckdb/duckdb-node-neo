import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { withConnection } from './utils/withConnection';

suite('fetch errors', () => {
  test.each([0, 5000])('preserves normal streaming EOF for %i rows', async (count) => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, `SELECT i FROM range(${count}) t(i)`);
      const pending = duckdb.pending_prepared_streaming(prepared);
      const result = await duckdb.execute_pending(pending);
      let rows = 0;
      while (true) {
        const chunk = await duckdb.fetch_chunk(result);
        if (!chunk || duckdb.data_chunk_get_size(chunk) === 0) break;
        rows += duckdb.data_chunk_get_size(chunk);
      }
      expect(rows).toBe(count);
    });
  });

  test('rejects a late streaming error after delivering rows', async () => {
    await withConnection(async (connection) => {
      await duckdb.query(connection, 'SET threads = 1');
      // Keep the error beyond the initial streaming execution buffer.
      const prepared = await duckdb.prepare(connection, `
        SELECT CASE WHEN i = 999999 THEN error('late-fetch-error') ELSE i END
        FROM range(1000000) t(i)
      `);
      const pending = duckdb.pending_prepared_streaming(prepared);
      const result = await duckdb.execute_pending(pending);
      expect(duckdb.result_is_streaming(result)).toBe(true);
      let rows = 0;
      const consume = async () => {
        while (true) {
          const chunk = await duckdb.fetch_chunk(result);
          if (!chunk || duckdb.data_chunk_get_size(chunk) === 0) break;
          rows += duckdb.data_chunk_get_size(chunk);
        }
      };
      await expect(consume()).rejects.toThrow('late-fetch-error');
      expect(rows).toBeGreaterThan(0);
      expect(rows).toBeLessThan(1000000);
      // A failed result must not prevent subsequent queries on the connection.
      const recovery = await duckdb.query(connection, 'SELECT 42');
      expect(duckdb.row_count(recovery)).toBe(1);
    });
  });
});
