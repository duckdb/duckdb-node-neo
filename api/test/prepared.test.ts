import { assert, beforeAll, describe, test } from 'vitest';
import {
  ANY,
  ARRAY,
  BIGINT,
  BIGNUM,
  BIT,
  BOOLEAN,
  DECIMAL,
  DOUBLE,
  DuckDBArrayVector,
  DuckDBBigIntVector,
  DuckDBBigNumVector,
  DuckDBBitVector,
  DuckDBBooleanVector,
  DuckDBDataChunk,
  DuckDBDoubleVector,
  DuckDBEnum8Vector,
  DuckDBIntegerVector,
  DuckDBListVector,
  DuckDBMapVector,
  DuckDBPendingResultState,
  DuckDBStructVector,
  DuckDBTimeTZVector,
  DuckDBTimestampMillisecondsVector,
  DuckDBTimestampNanosecondsVector,
  DuckDBTimestampSecondsVector,
  DuckDBTimestampTZVector,
  DuckDBTypeId,
  DuckDBUUIDVector,
  DuckDBUnionVector,
  DuckDBVarCharVector,
  ENUM,
  FLOAT,
  INTEGER,
  LIST,
  MAP,
  SMALLINT,
  SQLNULL,
  STRUCT,
  TIMESTAMPTZ,
  TIMESTAMP_MS,
  TIMESTAMP_NS,
  TIMESTAMP_S,
  TIMETZ,
  UNION,
  UUID,
  VARCHAR,
  arrayValue,
  bitValue,
  decimalValue,
  listValue,
  mapValue,
  structValue,
  unionValue,
  uuidValue,
} from '../src';
import { replaceSqlNullWithInteger } from './util/replaceSqlNullWithInteger';
import {
  ColumnNameAndType,
} from './util/testAllTypes';
import {
  assertColumns,
  assertValues,
  bigints,
  setDefaultTimezone,
  sleep,
  withConnection,
} from './util/testHelpers';

describe('prepared statements', () => {
  beforeAll(setDefaultTimezone);

  test('should support running prepared statements', async () => {
    await withConnection(async (connection) => {
      const params: ColumnNameAndType[] = [
        { name: 'boolean', type: BOOLEAN },
        { name: 'integer', type: INTEGER },
        { name: 'varchar', type: VARCHAR },
        { name: 'timestamp_s', type: TIMESTAMP_S },
        { name: 'timestamp_ms', type: TIMESTAMP_MS },
        { name: 'timestamp_ns', type: TIMESTAMP_NS },
        { name: 'enum', type: ENUM(['fly', 'swim', 'walk']) },
        { name: 'list_int', type: LIST(INTEGER) },
        { name: 'list_dec', type: LIST(DECIMAL(4, 1)) },
        { name: 'list_null', type: LIST(SQLNULL) },
        { name: 'struct', type: STRUCT({ 'a': INTEGER, 'b': VARCHAR }) },
        { name: 'array', type: ARRAY(INTEGER, 3) },
        { name: 'map', type: MAP(INTEGER, VARCHAR) },
        { name: 'union', type: UNION({ 'name': VARCHAR, 'age': SMALLINT }) },
        { name: 'uuid', type: UUID },
        { name: 'bit', type: BIT },
        { name: 'timetz', type: TIMETZ },
        { name: 'timestamptz', type: TIMESTAMPTZ },
        { name: 'bignum', type: BIGNUM },
        { name: 'null_value', type: SQLNULL },
      ];

      const sql = `select ${params
        .map((p) => `$${p.name} as ${p.name}`)
        .join(', ')}`;
      const prepared = await connection.prepare(sql);

      assert.strictEqual(prepared.parameterCount, params.length);
      for (let i = 0; i < params.length; i++) {
        assert.strictEqual(
          prepared.parameterName(i + 1),
          params[i].name,
          `param ${i} name mismatch`,
        );
      }

      let i = 1;
      prepared.bindBoolean(i++, true);
      prepared.bindInteger(i++, 10);
      prepared.bindVarchar(i++, 'abc');
      prepared.bindTimestampSeconds(i++, TIMESTAMP_S.max);
      prepared.bindTimestampMilliseconds(i++, TIMESTAMP_MS.max);
      prepared.bindTimestampNanoseconds(i++, TIMESTAMP_NS.max);
      prepared.bindEnum(i++, 'swim', ENUM(['fly', 'swim', 'walk']));
      prepared.bindList(i++, [100, 200, 300]);
      prepared.bindList(
        i++,
        [decimalValue(9876n, 4, 1), decimalValue(5432n, 4, 1)],
        LIST(DECIMAL(4, 1)),
      );
      prepared.bindList(i++, [null]);
      prepared.bindStruct(i++, { 'a': 42, 'b': 'duck' });
      prepared.bindArray(i++, [100, 200, 300]);
      prepared.bindMap(
        i++,
        mapValue([
          { key: 100, value: 'swim' },
          { key: 101, value: 'walk' },
          { key: 102, value: 'fly' },
        ]),
      );
      (prepared.bindUnion(
        i++,
        unionValue('age', 42),
        UNION({ 'name': VARCHAR, 'age': SMALLINT }),
      ),
        prepared.bindUUID(i++, uuidValue(0xf0e1d2c3b4a596870123456789abcdefn)));
      prepared.bindBit(i++, bitValue('0010001001011100010101011010111'));
      prepared.bindTimeTZ(i++, TIMETZ.max);
      prepared.bindTimestampTZ(i++, TIMESTAMPTZ.max);
      prepared.bindBigNum(i++, BIGNUM.max);
      prepared.bindNull(i++);

      for (let i = 0; i < params.length; i++) {
        let type = params[i].type;
        if (type.typeId === DuckDBTypeId.VARCHAR) {
          // VARCHAR type is reported incorrectly; see https://github.com/duckdb/duckdb/issues/16137
          continue;
        }
        assert.equal(
          prepared.parameterTypeId(i + 1),
          type.typeId,
          `param ${i} type id mismatch`,
        );
        assert.deepEqual(
          prepared.parameterType(i + 1),
          type,
          `param ${i} type mismatch`,
        );
      }

      const result = await prepared.run();

      // In the result, SQLNULL params get type INTEGER.
      const expectedColumns = params.map((p) => {
        const replacedType = replaceSqlNullWithInteger(p.type);
        if (replacedType !== p.type) {
          return { ...p, type: replacedType };
        }
        return p;
      });

      assertColumns(result, expectedColumns);

      const chunk = await result.fetchChunk();

      assert.isDefined(chunk);
      if (chunk) {
        assert.strictEqual(chunk.columnCount, expectedColumns.length);
        assert.strictEqual(chunk.rowCount, 1);
        let i = 0;
        assertValues<boolean, DuckDBBooleanVector>(
          chunk,
          i++,
          DuckDBBooleanVector,
          [true],
        );
        assertValues<number, DuckDBIntegerVector>(
          chunk,
          i++,
          DuckDBIntegerVector,
          [10],
        );
        assertValues<string, DuckDBVarCharVector>(
          chunk,
          i++,
          DuckDBVarCharVector,
          ['abc'],
        );
        assertValues(chunk, i++, DuckDBTimestampSecondsVector, [
          TIMESTAMP_S.max,
        ]);
        assertValues(chunk, i++, DuckDBTimestampMillisecondsVector, [
          TIMESTAMP_MS.max,
        ]);
        assertValues(chunk, i++, DuckDBTimestampNanosecondsVector, [
          TIMESTAMP_NS.max,
        ]);
        assertValues<string, DuckDBEnum8Vector>(chunk, i++, DuckDBEnum8Vector, [
          'swim',
        ]);
        assertValues(chunk, i++, DuckDBListVector, [
          listValue([100, 200, 300]),
        ]);
        assertValues(chunk, i++, DuckDBListVector, [
          listValue([decimalValue(9876n, 4, 1), decimalValue(5432n, 4, 1)]),
        ]);
        assertValues(chunk, i++, DuckDBListVector, [listValue([null])]);
        assertValues(chunk, i++, DuckDBStructVector, [
          structValue({ 'a': 42, 'b': 'duck' }),
        ]);
        assertValues(chunk, i++, DuckDBArrayVector, [
          arrayValue([100, 200, 300]),
        ]);
        assertValues(chunk, i++, DuckDBMapVector, [
          mapValue([
            { key: 100, value: 'swim' },
            { key: 101, value: 'walk' },
            { key: 102, value: 'fly' },
          ]),
        ]);
        assertValues(chunk, i++, DuckDBUnionVector, [unionValue('age', 42)]);
        assertValues(chunk, i++, DuckDBUUIDVector, [
          uuidValue(0xf0e1d2c3b4a596870123456789abcdefn),
        ]);
        assertValues(chunk, i++, DuckDBBitVector, [
          bitValue('0010001001011100010101011010111'),
        ]);
        assertValues(chunk, i++, DuckDBTimeTZVector, [TIMETZ.max]);
        assertValues(chunk, i++, DuckDBTimestampTZVector, [TIMESTAMPTZ.max]);
        assertValues(chunk, i++, DuckDBBigNumVector, [BIGNUM.max]);
        assertValues<number, DuckDBIntegerVector>(
          chunk,
          i++,
          DuckDBIntegerVector,
          [null],
        );
      }
    });
  });
  test('should support prepare statement bind with list', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare(
        'select $1 as a, $2 as b, $3 as c',
      );
      prepared.bind([42, 'duck', listValue([10, 11, 12])]);
      const result = await prepared.run();
      assertColumns(result, [
        { name: 'a', type: INTEGER },
        { name: 'b', type: VARCHAR },
        { name: 'c', type: LIST(INTEGER) },
      ]);
      const chunk = await result.fetchChunk();
      assert.isDefined(chunk);
      if (chunk) {
        assert.strictEqual(chunk.columnCount, 3);
        assert.strictEqual(chunk.rowCount, 1);
        assertValues<number, DuckDBIntegerVector>(
          chunk,
          0,
          DuckDBIntegerVector,
          [42],
        );
        assertValues<string, DuckDBVarCharVector>(
          chunk,
          1,
          DuckDBVarCharVector,
          ['duck'],
        );
        assertValues(chunk, 2, DuckDBListVector, [listValue([10, 11, 12])]);
      }
    });
  });
  test('should support prepare statement bind with object', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare(
        'select $a as a, $b as b, $c as c, $d as d, $e as e, $f as f',
      );
      prepared.bind(
        {
          a: 42,
          b: 42.3,
          c: 'duck',
          d: listValue([10, 11, 12]),
          e: arrayValue([10.1, 11.2, 12.3]),
          f: arrayValue([10, 11, 12]),
        },
        {
          f: ARRAY(FLOAT, 2),
        },
      );
      const result = await prepared.run();
      assertColumns(result, [
        { name: 'a', type: INTEGER },
        { name: 'b', type: DOUBLE },
        { name: 'c', type: VARCHAR },
        { name: 'd', type: LIST(INTEGER) },
        { name: 'e', type: ARRAY(DOUBLE, 3) },
        { name: 'f', type: ARRAY(FLOAT, 3) },
      ]);
      const chunk = await result.fetchChunk();
      assert.isDefined(chunk);
      if (chunk) {
        assert.strictEqual(chunk.columnCount, 6);
        assert.strictEqual(chunk.rowCount, 1);
        assertValues<number, DuckDBIntegerVector>(
          chunk,
          0,
          DuckDBIntegerVector,
          [42],
        );
        assertValues<number, DuckDBDoubleVector>(
          chunk,
          1,
          DuckDBDoubleVector,
          [42.3],
        );
        assertValues<string, DuckDBVarCharVector>(
          chunk,
          2,
          DuckDBVarCharVector,
          ['duck'],
        );
        assertValues(chunk, 3, DuckDBListVector, [listValue([10, 11, 12])]);
        assertValues(chunk, 4, DuckDBArrayVector, [
          arrayValue([10.1, 11.2, 12.3]),
        ]);
        assertValues(chunk, 5, DuckDBArrayVector, [arrayValue([10, 11, 12])]);
      }
    });
  });
  test('should fail gracefully when binding structs contain ANY types to prepared statements', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare('select ?');
      try {
        prepared.bindStruct(
          0,
          structValue({ 'a': null }),
          STRUCT({ 'a': ANY }),
        );
        assert.fail('should throw');
      } catch (err) {
        assert.deepEqual(
          err,
          new Error(
            'Cannot create structs with an entry type of ANY. Specify a specific type.',
          ),
        );
      }
    });
  });
  test('should fail gracefully when type cannot be inferred when binding lists to prepared statements', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare('select ?');
      try {
        prepared.bind([listValue([])]);
        assert.fail('should throw');
      } catch (err) {
        assert.deepEqual(
          err,
          new Error(
            'Cannot create lists with item type of ANY. Specify a specific type.',
          ),
        );
      }
    });
  });
  test('should fail gracefully when type cannot be inferred when binding arrays to prepared statements', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare('select ?');
      try {
        prepared.bind([arrayValue([])]);
        assert.fail('should throw');
      } catch (err) {
        assert.deepEqual(
          err,
          new Error(
            'Cannot create arrays with item type of ANY. Specify a specific type.',
          ),
        );
      }
    });
  });
  test('should infer integer and floating-point values when binding to prepared statements', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare('select ? as i1, ? as i2, ? as b1, ? as b2, ? as d');
      prepared.bind([2_147_483_647, -2_147_483_648, 2_147_483_648, -2_147_483_649, 3.14]);
      const result = await prepared.run();
      assertColumns(result, [
        { name: 'i1', type: INTEGER },
        { name: 'i2', type: INTEGER },
        { name: 'b1', type: BIGINT },
        { name: 'b2', type: BIGINT },
        { name: 'd', type: DOUBLE },
      ]);
      const chunk = await result.fetchChunk();
      assert.isDefined(chunk);
      if (chunk) {
        assert.strictEqual(chunk.columnCount, 5);
        assert.strictEqual(chunk.rowCount, 1);
        assertValues<number, DuckDBIntegerVector>(
          chunk,
          0,
          DuckDBIntegerVector,
          [2_147_483_647],
        );
        assertValues<number, DuckDBIntegerVector>(
          chunk,
          1,
          DuckDBIntegerVector,
          [-2_147_483_648],
        );
        assertValues<bigint, DuckDBBigIntVector>(
          chunk,
          2,
          DuckDBBigIntVector,
          [2_147_483_648n],
        );
        assertValues<bigint, DuckDBBigIntVector>(
          chunk,
          3,
          DuckDBBigIntVector,
          [-2_147_483_649n],
        );
        assertValues<number, DuckDBDoubleVector>(
          chunk,
          4,
          DuckDBDoubleVector,
          [3.14],
        );
      }
    });
  });
  test('should support starting prepared statements and running them incrementally', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare(
        'select int from test_all_types()',
      );
      const pending = prepared.start();
      let taskCount = 0;
      while (pending.runTask() !== DuckDBPendingResultState.RESULT_READY) {
        taskCount++;
        // arbitrary upper bound on the number of tasks expected for this simple query
        if (taskCount > 100) {
          assert.fail('Unexpectedly large number of tasks');
        }
        await sleep(1);
      }
      // console.debug('task count: ', taskCount);
      const result = await pending.getResult();
      assertColumns(result, [{ name: 'int', type: INTEGER }]);
      const chunk = await result.fetchChunk();
      assert.isDefined(chunk);
      if (chunk) {
        assert.strictEqual(chunk.columnCount, 1);
        assert.strictEqual(chunk.rowCount, 3);
        assertValues(chunk, 0, DuckDBIntegerVector, [
          INTEGER.min,
          INTEGER.max,
          null,
        ]);
      }
    });
  });
  test('should support streaming results from prepared statements', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare('from range(10000)');
      const pending = prepared.start();
      const result = await pending.getResult();
      assertColumns(result, [{ name: 'range', type: BIGINT }]);
      const chunks: DuckDBDataChunk[] = [];
      let currentChunk: DuckDBDataChunk | null = null;
      currentChunk = await result.fetchChunk();
      while (currentChunk && currentChunk.rowCount > 0) {
        chunks.push(currentChunk);
        currentChunk = await result.fetchChunk();
      }
      currentChunk = null;
      assert.strictEqual(chunks.length, 5); // ceil(10000 / 2048) = 5
      assertValues(chunks[0], 0, DuckDBBigIntVector, bigints(0n, 2048n - 1n));
      assertValues(
        chunks[1],
        0,
        DuckDBBigIntVector,
        bigints(2048n, 2048n * 2n - 1n),
      );
      assertValues(
        chunks[2],
        0,
        DuckDBBigIntVector,
        bigints(2048n * 2n, 2048n * 3n - 1n),
      );
      assertValues(
        chunks[3],
        0,
        DuckDBBigIntVector,
        bigints(2048n * 3n, 2048n * 4n - 1n),
      );
      assertValues(
        chunks[4],
        0,
        DuckDBBigIntVector,
        bigints(2048n * 4n, 9999n),
      );
    });
  });
  test('prepared statement column info (valid)', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare(
        'select $1::INTEGER as a, $2::VARCHAR as b, $3::INTEGER[] as c',
      );
      assert.equal(prepared.columnCount, 3);
      assert.equal(prepared.columnName(0), 'a');
      assert.equal(prepared.columnTypeId(0), DuckDBTypeId.INTEGER);
      assert.deepEqual(prepared.columnType(0), INTEGER);
      assert.equal(prepared.columnName(1), 'b');
      assert.equal(prepared.columnTypeId(1), DuckDBTypeId.VARCHAR);
      assert.deepEqual(prepared.columnType(1), VARCHAR);
      assert.equal(prepared.columnName(2), 'c');
      assert.equal(prepared.columnTypeId(2), DuckDBTypeId.LIST);
      assert.deepEqual(prepared.columnType(2), LIST(INTEGER));
    });
  });
  test('prepared statement column info (invalid)', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare(
        'select $1::INTEGER as a, $2::VARCHAR as b, $3 as c',
      );
      // When any column types are ambiguous, DuckDB returns a column count of 1 with a type of INVALID.
      assert.equal(prepared.columnCount, 1);
      assert.equal(prepared.columnTypeId(0), DuckDBTypeId.INVALID);
    });
  });
  test('runAndReadAll with params', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll('select ? as n', [17]);
      const rows = reader.getRowObjects();
      assert.deepEqual(rows, [{ n: 17 }]);
    });
  });
  test('should support PIVOT (no parameters)', async () => {
    await withConnection(async (connection) => {
      await connection.run(`
CREATE TABLE cities (
    country VARCHAR, name VARCHAR, year INTEGER, population INTEGER
);
INSERT INTO cities VALUES
    ('NL', 'Amsterdam', 2000, 1005),
    ('NL', 'Amsterdam', 2010, 1065),
    ('NL', 'Amsterdam', 2020, 1158),
    ('US', 'Seattle', 2000, 564),
    ('US', 'Seattle', 2010, 608),
    ('US', 'Seattle', 2020, 738),
    ('US', 'New York City', 2000, 8015),
    ('US', 'New York City', 2010, 8175),
    ('US', 'New York City', 2020, 8772);
    `);
      const reader = await connection.runAndReadAll(`
FROM (
  PIVOT cities
  ON year
  USING sum(population)
)
ORDER BY name
      `);
      const columns = reader.getColumnsObject();
      assert.deepEqual(columns, {
        country: ['NL', 'US', 'US'],
        name: ['Amsterdam', 'New York City', 'Seattle'],
        '2000': [1005n, 8015n, 564n],
        '2010': [1065n, 8175n, 608n],
        '2020': [1158n, 8772n, 738n],
      });
    });
  });
  test('should support PIVOT (with parameters)', async () => {
    await withConnection(async (connection) => {
      await connection.run(`
CREATE TABLE cities (
    country VARCHAR, name VARCHAR, year INTEGER, population INTEGER
);
INSERT INTO cities VALUES
    ('NL', 'Amsterdam', 2000, 1005),
    ('NL', 'Amsterdam', 2010, 1065),
    ('NL', 'Amsterdam', 2020, 1158),
    ('US', 'Seattle', 2000, 564),
    ('US', 'Seattle', 2010, 608),
    ('US', 'Seattle', 2020, 738),
    ('US', 'New York City', 2000, 8015),
    ('US', 'New York City', 2010, 8175),
    ('US', 'New York City', 2020, 8772);
    `);
      const reader = await connection.runAndReadAll(
        `
FROM (
  PIVOT cities
  ON year
  USING sum(population)
)
WHERE country = $country
ORDER BY name
      `,
        { country: 'US' },
      );
      const columns = reader.getColumnsObject();
      assert.deepEqual(columns, {
        country: ['US', 'US'],
        name: ['New York City', 'Seattle'],
        '2000': [8015n, 564n],
        '2010': [8175n, 608n],
        '2020': [8772n, 738n],
      });
    });
  });

  test('should not segfault with concurrent prepared statement creation and execution', async () => {
    await withConnection(async (connection) => {
      // Create test table with some data
      await connection.run(`
        CREATE TABLE test_table (
          id INTEGER, 
          name VARCHAR, 
          value INTEGER, 
          created_at VARCHAR
        )
      `);

      await connection.run(`
        INSERT INTO test_table VALUES 
        (1, 'test1', 100, '2023-01-01'),
        (2, 'test2', 200, '2023-01-02'),
        (3, 'test3', 300, '2023-01-03'),
        (4, 'test4', 400, '2023-01-04'),
        (5, 'test5', 500, '2023-01-05')
      `);

      const iterations = 1000;
      const concurrency = 12;

      const runIteration = async (i: number) => {
        const prepared = await connection.prepare(
          'SELECT * FROM test_table WHERE id = $1',
        );
        prepared.bindInteger(1, (i % 5) + 1);

        await prepared.run();
      };

      // Run iterations in batches with controlled concurrency
      let processed = 0;

      while (processed < iterations) {
        const batch = [];
        const batchEnd = Math.min(processed + concurrency, iterations);

        for (let i = processed; i < batchEnd; i++) {
          batch.push(runIteration(i));
        }

        // Wait for all in the batch to complete
        await Promise.allSettled(batch);
        processed = batchEnd;
      }

      // If we reach here without segfaulting, the test passes
      assert.isTrue(true, 'Test completed without error');
    });
  });
});
