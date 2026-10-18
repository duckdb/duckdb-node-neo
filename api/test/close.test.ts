import { assert, describe, test } from 'vitest';
import {
  BIGINT,
  DuckDBConnection,
  DuckDBTypeId,
  ResultReturnType,
  StatementType,
  version,
} from '../src';
import { withConnection } from './util/testHelpers';

// From 2.0, an unfinished streaming or pending result blocks its connection.
// Before, the next statement aborts it instead.
const majorVersion = Number(version().slice(1).split('.')[0]);
const blocksConnection = majorVersion >= 2;

const streamingSql = 'select n from range(100_000) t(n)';

async function assertQueryWorks(connection: DuckDBConnection) {
  const reader = await connection.runAndReadAll('select 1 as x');
  assert.deepEqual(reader.getRows(), [[1]]);
}

async function assertQueryWithOpenResult(connection: DuckDBConnection) {
  if (!blocksConnection) {
    await assertQueryWorks(connection);
    return;
  }
  try {
    await connection.run('select 1');
    assert.fail('should throw');
  } catch (err) {
    assert.match(String(err), /Resource In Use/);
  }
}

async function assertRejects(promise: Promise<unknown>, message: string) {
  try {
    await promise;
    assert.fail('should throw');
  } catch (err) {
    assert.equal((err as Error).message, message);
  }
}

describe('closeSync', () => {
  test('result: metadata stays available, fetching throws', async () => {
    await withConnection(async (connection) => {
      const result = await connection.stream(streamingSql);
      result.closeSync();
      assert.equal(result.returnType, ResultReturnType.QUERY_RESULT);
      assert.equal(result.statementType, StatementType.SELECT);
      assert.equal(result.columnCount, 1);
      assert.deepEqual(result.columnNames(), ['n']);
      assert.equal(result.columnTypeId(0), DuckDBTypeId.BIGINT);
      assert.equal(result.columnType(0), BIGINT);
      assert.equal(result.isStreaming, true);
      assert.equal(result.rowsChanged, 0);
      assert.throws(
        () => result.columnName(1),
        RangeError,
        'column index 1 is out of range (count: 1)'
      );
      assert.equal(result.columnName(0.5), 'n');
      const logicalType = result.columnLogicalType(0);
      assert.notStrictEqual(result.columnLogicalType(0), logicalType);
      logicalType.alias = 'my_alias';
      assert.equal(result.columnType(0), BIGINT);
      await assertRejects(result.fetchChunk(), 'Result destroyed');
    });
  });
  test('result: closing twice is a no-op', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run('select 1 as x');
      result.closeSync();
      result.closeSync();
      assert.deepEqual(result.columnNames(), ['x']);
    });
  });
  test('materialized result: counts stay available, chunks throw', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run('select 1 as x');
      result.closeSync();
      assert.equal(result.rowCount, 1);
      assert.equal(result.chunkCount, 1);
      assert.throws(() => result.getChunk(0), 'Result destroyed');
    });
  });
  test('result: closing during an in-flight fetch', async () => {
    await withConnection(async (connection) => {
      const result = await connection.stream(streamingSql);
      const chunkPromise = result.fetchChunk();
      result.closeSync();
      const chunk = await chunkPromise;
      assert.equal(chunk?.rowCount, 2048);
      await assertRejects(result.fetchChunk(), 'Result destroyed');
      await assertQueryWorks(connection);
    });
  });
  test('result: closing a partly read stream frees the connection', async () => {
    await withConnection(async (connection) => {
      const result = await connection.stream(streamingSql);
      await result.fetchChunk();
      await assertQueryWithOpenResult(connection);
      result.closeSync();
      await assertQueryWorks(connection);
    });
  });
  test('reader: rows read stay available, reading more throws', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.streamAndReadUntil(streamingSql, 10);
      await assertQueryWithOpenResult(connection);
      reader.closeSync();
      reader.closeSync();
      assert.equal(reader.currentRowCount, 2048);
      assert.deepEqual(reader.getRowObjects()[1], { n: 1n });
      assert.deepEqual(reader.columnNames(), ['n']);
      await assertRejects(reader.readAll(), 'Result destroyed');
      await assertQueryWorks(connection);
    });
  });
  test('pending result: running tasks or getting the result throws', async () => {
    await withConnection(async (connection) => {
      const pending = await connection.start('select 1');
      pending.closeSync();
      pending.closeSync();
      assert.throws(() => pending.runTask(), 'Pending result destroyed');
      await assertRejects(pending.getResult(), 'Pending result destroyed');
    });
  });
  test('pending result: closing during an in-flight getResult', async () => {
    await withConnection(async (connection) => {
      const pending = await connection.start(
        'select count(*)::integer as c from range(1_000_000)'
      );
      const resultPromise = pending.getResult();
      pending.closeSync();
      const result = await resultPromise;
      assert.deepEqual((await result.getRows()), [[1_000_000]]);
      await assertQueryWorks(connection);
    });
  });
  test('pending result: closing an unexecuted one frees the connection', async () => {
    await withConnection(async (connection) => {
      const pending = await connection.startStream(streamingSql);
      await assertQueryWithOpenResult(connection);
      pending.closeSync();
      await assertQueryWorks(connection);
    });
  });
});
