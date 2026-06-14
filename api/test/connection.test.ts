import { assert, beforeAll, describe, test } from 'vitest';
import {
  DuckDBInstance,
  DuckDBIntegerVector,
  INTEGER,
} from '../src';
import {
  assertColumns,
  assertValues,
  setDefaultTimezone,
  withConnection,
} from './util/testHelpers';

describe('connection', () => {
  beforeAll(setDefaultTimezone);

  test('should support creating, connecting, running a basic query, and reading results', async () => {
    const instance = await DuckDBInstance.create();
    const connection = await instance.connect();
    const result = await connection.run('select 42 as num');
    assertColumns(result, [{ name: 'num', type: INTEGER }]);
    const chunk = await result.fetchChunk();
    assert.isDefined(chunk);
    if (chunk) {
      assert.strictEqual(chunk.columnCount, 1);
      assert.strictEqual(chunk.rowCount, 1);
      assertValues<number, DuckDBIntegerVector>(
        chunk,
        0,
        DuckDBIntegerVector,
        [42],
      );
    }
    instance.closeSync();
  });
  test('disconnecting connections', async () => {
    const instance = await DuckDBInstance.create();
    const connection = await instance.connect();
    const prepared1 = await connection.prepare('select 1');
    assert.isDefined(prepared1);
    prepared1.destroySync();
    connection.disconnectSync();
    try {
      await connection.prepare('select 2');
      assert.fail('should throw');
    } catch (err) {
      assert.deepEqual(
        err,
        new Error('Failed to prepare: connection disconnected'),
      );
    }
    // ensure double-disconnect doesn't break anything
    connection.disconnectSync();
  });
  test('client context', async () => {
    await withConnection(async (connection) => {
      const clientContext = connection.clientContext;
      assert.isDefined(clientContext);
      assert.isDefined(clientContext.connectionId);
    });
  });
});
