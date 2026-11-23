import { assert, describe, test } from 'vitest';
import { DuckDBConnection, DuckDBInstance, INTEGER, VARCHAR } from '../src';
import { DuckDBScalarFunction } from '../src/DuckDBScalarFunction';
import { DuckDBTableFunction } from '../src/DuckDBTableFunction';
import { withConnection } from './util/testHelpers';

// The `using` declaration needs TypeScript 5.2 and a runtime with
// Symbol.dispose (Node 20+); vitest transpiles it for older targets.

const streamingSql = 'select n from range(100_000) t(n)';

async function assertQueryWorks(connection: DuckDBConnection) {
  const reader = await connection.runAndReadAll('select 1 as x');
  assert.deepEqual(reader.getRows(), [[1]]);
}

async function assertRejects(promise: Promise<unknown>, pattern: RegExp) {
  try {
    await promise;
    assert.fail('should throw');
  } catch (err) {
    assert.match(String(err), pattern);
  }
}

describe('Symbol.dispose', () => {
  test('result: closed on leaving the scope', async () => {
    await withConnection(async (connection) => {
      let result;
      {
        using scoped = await connection.stream(streamingSql);
        result = scoped;
        await result.fetchChunk();
      }
      assert.deepEqual(result.columnNames(), ['n']);
      await assertRejects(result.fetchChunk(), /Result destroyed/);
      await assertQueryWorks(connection);
    });
  });
  test('result: closed when the scope throws', async () => {
    await withConnection(async (connection) => {
      let result;
      try {
        using scoped = await connection.stream(streamingSql);
        result = scoped;
        throw new Error('leaving early');
      } catch (err) {
        assert.equal((err as Error).message, 'leaving early');
      }
      await assertRejects(result!.fetchChunk(), /Result destroyed/);
      await assertQueryWorks(connection);
    });
  });
  test('reader and pending result: closed on leaving the scope', async () => {
    await withConnection(async (connection) => {
      {
        using reader = await connection.streamAndReadUntil(streamingSql, 10);
        assert.equal(reader.currentRowCount, 2048);
      }
      await assertQueryWorks(connection);
      let pending;
      {
        using scoped = await connection.startStream(streamingSql);
        pending = scoped;
      }
      assert.throws(() => pending.runTask(), /Pending result destroyed/);
      await assertQueryWorks(connection);
    });
  });
  test('prepared statement: destroyed on leaving the scope', async () => {
    await withConnection(async (connection) => {
      let prepared;
      {
        using scoped = await connection.prepare('select $1::integer as x');
        prepared = scoped;
        scoped.bindInteger(1, 7);
        const reader = await scoped.runAndReadAll();
        assert.deepEqual(reader.getRows(), [[7]]);
      }
      assert.throws(() => prepared.bindInteger(1, 8), /Failed to bind/);
    });
  });
  test('appender: flushed and closed on leaving the scope', async () => {
    await withConnection(async (connection) => {
      await connection.run('create table target(i integer)');
      {
        using appender = await connection.createAppender('target');
        appender.appendInteger(1);
        appender.endRow();
        appender.appendInteger(2);
        appender.endRow();
      }
      const reader = await connection.runAndReadAll('select i from target order by i');
      assert.deepEqual(reader.getRows(), [[1], [2]]);
    });
  });
  test('appender: clear before leaving discards the unflushed rows', async () => {
    await withConnection(async (connection) => {
      await connection.run('create table target(i integer)');
      {
        using appender = await connection.createAppender('target');
        appender.appendInteger(1);
        appender.endRow();
        appender.clear();
      }
      const reader = await connection.runAndReadAll('select count(*)::integer from target');
      assert.deepEqual(reader.getRows(), [[0]]);
    });
  });
  test('closing inside the scope leaves nothing for dispose to do', async () => {
    await withConnection(async (connection) => {
      await connection.run('create table target(i integer)');
      {
        using appender = await connection.createAppender('target');
        appender.appendInteger(5);
        appender.endRow();
        appender.closeSync();
      }
      {
        using result = await connection.stream(streamingSql);
        result.closeSync();
      }
      {
        using prepared = await connection.prepare('select 1');
        prepared.destroySync();
      }
      const reader = await connection.runAndReadAll('select i from target');
      assert.deepEqual(reader.getRows(), [[5]]);
    });
  });
  test('scalar and table functions: destroyed on leaving the scope', async () => {
    await withConnection(async (connection) => {
      let scalar;
      let table;
      {
        using scalarFunction = DuckDBScalarFunction.create({
          name: 'seven',
          mainFunction: (_info, input, output) => {
            for (let row = 0; row < input.rowCount; row++) {
              output.setItem(row, 7);
            }
            output.flush();
          },
          returnType: INTEGER,
        });
        using tableFunction = DuckDBTableFunction.create({
          name: 'one_row',
          bindFunction: (info) => {
            info.addResultColumn('v', VARCHAR);
          },
          initFunction: (info) => {
            info.setInitData({ done: false });
          },
          mainFunction: (info, output) => {
            const initData = info.initData as { done: boolean };
            if (initData.done) {
              output.rowCount = 0;
              return;
            }
            output.rowCount = 1;
            const column = output.getColumnVector(0);
            column.setItem(0, 'row');
            column.flush();
            initData.done = true;
          },
        });
        connection.registerScalarFunction(scalarFunction);
        connection.registerTableFunction(tableFunction);
        scalar = scalarFunction;
        table = tableFunction;
      }
      const reader = await connection.runAndReadAll('select seven() as s, v from one_row()');
      assert.deepEqual(reader.getRows(), [[7, 'row']]);
      assert.throws(() => connection.registerScalarFunction(scalar), /Failed to register scalar function/);
      assert.throws(() => connection.registerTableFunction(table), /Failed to register table function/);
    });
  });
  test('connection and instance: closed on leaving the scope', async () => {
    let connection;
    {
      using instance = await DuckDBInstance.create();
      using scoped = await instance.connect();
      connection = scoped;
      await assertQueryWorks(connection);
    }
    await assertRejects(connection.run('select 1'), /disconnected/);
  });
  test('DisposableStack closes several objects in reverse order', async () => {
    await withConnection(async (connection) => {
      await connection.run('create table target(i integer)');
      const order: string[] = [];
      const stack = new DisposableStack();
      const appender = stack.use(await connection.createAppender('target'));
      stack.defer(() => order.push('appender'));
      const prepared = stack.use(await connection.prepare('select 1'));
      stack.defer(() => order.push('prepared'));
      appender.appendInteger(3);
      appender.endRow();
      stack.dispose();
      assert.deepEqual(order, ['prepared', 'appender']);
      assert.throws(() => prepared.bindInteger(1, 8), /Failed to bind/);
      const reader = await connection.runAndReadAll('select i from target');
      assert.deepEqual(reader.getRows(), [[3]]);
    });
  });
});
