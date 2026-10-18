import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { withConnection } from './utils/withConnection';

// On 2.0, an unfinished streaming or pending result blocks its connection.
const blocksConnection = duckdb.library_version().startsWith('v2');

const streamingQuery = 'select n from range(100_000) t(n)';

async function streamingResult(connection: duckdb.Connection) {
  const prepared = await duckdb.prepare(connection, streamingQuery);
  return duckdb.execute_prepared_streaming(prepared);
}

async function expectQueryWorks(connection: duckdb.Connection) {
  const result = await duckdb.query(connection, 'select 1');
  expect(duckdb.row_count(result)).toBe(1);
}

suite('destroy_result_sync', () => {
  test('use after destroy throws', async () => {
    await withConnection(async (connection) => {
      const result = await streamingResult(connection);
      duckdb.destroy_result_sync(result);
      expect(() => duckdb.column_count(result)).toThrowError('Result destroyed');
      expect(() => duckdb.column_name(result, 0)).toThrowError('Result destroyed');
      expect(() => duckdb.result_is_streaming(result)).toThrowError('Result destroyed');
      expect(() => duckdb.fetch_chunk(result)).toThrowError('Result destroyed');
    });
  });
  test('destroy twice is a no-op', async () => {
    await withConnection(async (connection) => {
      const result = await duckdb.query(connection, 'select 1');
      duckdb.destroy_result_sync(result);
      duckdb.destroy_result_sync(result);
      expect(() => duckdb.row_count(result)).toThrowError('Result destroyed');
    });
  });
  test('destroy during an in-flight fetch defers until it finishes', async () => {
    await withConnection(async (connection) => {
      const result = await streamingResult(connection);
      const chunkPromise = duckdb.fetch_chunk(result);
      duckdb.destroy_result_sync(result);
      expect(() => duckdb.fetch_chunk(result)).toThrowError('Result destroyed');
      const chunk = await chunkPromise;
      expect(chunk).not.toBeNull();
      expect(duckdb.data_chunk_get_size(chunk!)).toBe(2048);
      expect(() => duckdb.fetch_chunk(result)).toThrowError('Result destroyed');
      await expectQueryWorks(connection);
    });
  });
  test('destroy during several in-flight fetches', async () => {
    await withConnection(async (connection) => {
      const result = await streamingResult(connection);
      const chunkPromises = [duckdb.fetch_chunk(result), duckdb.fetch_chunk(result)];
      duckdb.destroy_result_sync(result);
      duckdb.destroy_result_sync(result);
      const chunks = await Promise.all(chunkPromises);
      for (const chunk of chunks) {
        expect(chunk).not.toBeNull();
      }
      await expectQueryWorks(connection);
    });
  });
  test('destroy frees the connection of a partly read stream', async () => {
    await withConnection(async (connection) => {
      const result = await streamingResult(connection);
      await duckdb.fetch_chunk(result);
      if (blocksConnection) {
        await expect(duckdb.query(connection, 'select 1')).rejects.toThrowError(/Resource In Use/);
      }
      duckdb.destroy_result_sync(result);
      await expectQueryWorks(connection);
    });
  });
});

suite('destroy_pending_sync', () => {
  test('use after destroy throws', async () => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, 'select 1');
      const pending = duckdb.pending_prepared(prepared);
      duckdb.destroy_pending_sync(pending);
      expect(() => duckdb.pending_execute_task(pending)).toThrowError('Pending result destroyed');
      expect(() => duckdb.pending_execute_check_state(pending)).toThrowError('Pending result destroyed');
      expect(() => duckdb.pending_error(pending)).toThrowError('Pending result destroyed');
      expect(() => duckdb.execute_pending(pending)).toThrowError('Pending result destroyed');
    });
  });
  test('destroy twice is a no-op', async () => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, 'select 1');
      const pending = duckdb.pending_prepared(prepared);
      duckdb.destroy_pending_sync(pending);
      duckdb.destroy_pending_sync(pending);
      expect(() => duckdb.pending_execute_task(pending)).toThrowError('Pending result destroyed');
    });
  });
  test('destroy during an in-flight execute defers until it finishes', async () => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, 'select count(*)::integer as c from range(1_000_000)');
      const pending = duckdb.pending_prepared(prepared);
      const resultPromise = duckdb.execute_pending(pending);
      duckdb.destroy_pending_sync(pending);
      expect(() => duckdb.execute_pending(pending)).toThrowError('Pending result destroyed');
      const result = await resultPromise;
      const chunk = await duckdb.fetch_chunk(result);
      expect(duckdb.data_chunk_get_size(chunk!)).toBe(1);
      await expectQueryWorks(connection);
    });
  });
  test('destroy frees the connection of an unexecuted pending result', async () => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, streamingQuery);
      const pending = duckdb.pending_prepared_streaming(prepared);
      if (blocksConnection) {
        await expect(duckdb.query(connection, 'select 1')).rejects.toThrowError(/Resource In Use/);
      }
      duckdb.destroy_pending_sync(pending);
      await expectQueryWorks(connection);
    });
  });
});
