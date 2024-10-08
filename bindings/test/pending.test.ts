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
      try {
        const pending = duckdb.pending_prepared(prepared);
        try {
          const result = await duckdb.execute_pending(pending);
          try {
            await expectResult(result, {
              columns: [
                { name: 'a', logicalType: INTEGER },
              ],
              chunks: [
                { rowCount: 1, vectors: [data(4, [true], [11])]},
              ],
            });
          } finally {
            duckdb.destroy_result(result);
          }
        } finally {
          duckdb.destroy_pending(pending);
        }
      } finally {
        duckdb.destroy_prepare(prepared);
      }
    });
  });
  test('tasks', async () => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, 'select count(*) as count from range(10_000)');
      try {
        const pending = duckdb.pending_prepared(prepared);
        try {
          let pending_state = duckdb.pending_execute_check_state(pending);
          while (!duckdb.pending_execution_is_finished(pending_state)) {
            pending_state = duckdb.pending_execute_task(pending);
            await sleep(0); // yield to allow progress
          }
          const result = await duckdb.execute_pending(pending);
          try {
            await expectResult(result, {
              columns: [
                { name: 'count', logicalType: BIGINT },
              ],
              chunks: [
                { rowCount: 1, vectors: [data(8, [true], [10_000n])]},
              ],
            });
          } finally {
            duckdb.destroy_result(result);
          }
        } finally {
          duckdb.destroy_pending(pending);
        }
      } finally {
        duckdb.destroy_prepare(prepared);
      }
    });
  });
  test.skip('interrupt', async () => { // interrupt does not appear to be entirely deterministic
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, 'select count(*) as count from range(10_000)');
      try {
        const pending = duckdb.pending_prepared(prepared);
        try {
          duckdb.interrupt(connection);
          await sleep(0); // yield to allow progress

          let pending_state = duckdb.pending_execute_check_state(pending);
          while (!duckdb.pending_execution_is_finished(pending_state)) {
            pending_state = duckdb.pending_execute_task(pending);
            await sleep(0); // yield to allow progress
          }

          expect(pending_state).toBe(duckdb.PendingState.ERROR);
          expect(duckdb.pending_error(pending)).toBe('INTERRUPT Error: Interrupted!');
        } finally {
          duckdb.destroy_pending(pending);
        }
      } finally {
        duckdb.destroy_prepare(prepared);
      }
    });
  });
  // TODO: query progress?
});
