import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { expectResult } from './utils/expectResult';
import { BIGINT, INTEGER } from './utils/expectedLogicalTypes';
import { data } from './utils/expectedVectors';
import { sleep } from './utils/sleep';
import { withConnection } from './utils/withConnection';

suite('pending', () => {
  test('execute', async () => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, 'select 11 as a');
      const pending = duckdb.pending_prepared(prepared);
      const result = await duckdb.execute_pending(pending);
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 1,
        columns: [
          { name: 'a', logicalType: INTEGER },
        ],
        chunks: [
          { rowCount: 1, vectors: [data(4, [true], [11])]},
        ],
      });
    });
  });
  test('streaming', async () => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, 'select n::integer as int from range(5000) t(n)');
      const pending = duckdb.pending_prepared_streaming(prepared);
      const result = await duckdb.execute_pending(pending);
      await expectResult(result, {
        isStreaming: true,
        columns: [
          { name: 'int', logicalType: INTEGER },
        ],
        chunks: [
          { rowCount: 2048, vectors: [data(4, null, Array.from({ length: 2048 }).map((_, i) => i))]},
          { rowCount: 2048, vectors: [data(4, null, Array.from({ length: 2048 }).map((_, i) => 2048 + i))]},
          { rowCount: 904, vectors: [data(4, null, Array.from({ length: 904 }).map((_, i) => 4096 + i))]},
        ],
      });
    });
  });
  test('tasks', async () => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, 'select count(*) as count from range(10_000)');
      const pending = duckdb.pending_prepared(prepared);
      let pending_state = duckdb.pending_execute_check_state(pending);
      while (!duckdb.pending_execution_is_finished(pending_state)) {
        pending_state = duckdb.pending_execute_task(pending);
        await sleep(0); // yield to allow progress
      }
      const result = await duckdb.execute_pending(pending);
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 1,
        columns: [
          { name: 'count', logicalType: BIGINT },
        ],
        chunks: [
          { rowCount: 1, vectors: [data(8, [true], [10_000n])]},
        ],
      });
    });
  });
  test.skip('interrupt', async () => { // interrupt does not appear to be entirely deterministic
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, 'select count(*) as count from range(10_000)');
      const pending = duckdb.pending_prepared(prepared);
      duckdb.interrupt(connection);
      await sleep(0); // yield to allow progress

      let pending_state = duckdb.pending_execute_check_state(pending);
      while (!duckdb.pending_execution_is_finished(pending_state)) {
        pending_state = duckdb.pending_execute_task(pending);
        await sleep(0); // yield to allow progress
      }

      expect(pending_state).toBe(duckdb.PendingState.ERROR);
      expect(duckdb.pending_error(pending)).toBe('INTERRUPT Error: Interrupted!');
    });
  });
  // TODO: query progress?
});
