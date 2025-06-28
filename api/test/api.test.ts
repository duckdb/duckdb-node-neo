import fs from 'fs';
import { assert, beforeAll, describe, test } from 'vitest';
import {
  ANY,
  ARRAY,
  BIGINT,
  BIT,
  BLOB,
  BOOLEAN,
  DATE,
  DECIMAL,
  DOUBLE,
  DateParts,
  DuckDBArrayVector,
  DuckDBBigIntVector,
  DuckDBBitVector,
  DuckDBBlobVector,
  DuckDBBooleanVector,
  DuckDBConnection,
  DuckDBDataChunk,
  DuckDBDateValue,
  DuckDBDateVector,
  DuckDBDecimal128Vector,
  DuckDBDecimal16Vector,
  DuckDBDecimal32Vector,
  DuckDBDecimal64Vector,
  DuckDBDecimalValue,
  DuckDBDoubleVector,
  DuckDBEnum16Vector,
  DuckDBEnum32Vector,
  DuckDBEnum8Vector,
  DuckDBFloatVector,
  DuckDBHugeIntVector,
  DuckDBInstance,
  DuckDBIntegerVector,
  DuckDBIntervalVector,
  DuckDBListVector,
  DuckDBMapVector,
  DuckDBPendingResultState,
  DuckDBResult,
  DuckDBSmallIntVector,
  DuckDBStructVector,
  DuckDBTimeTZValue,
  DuckDBTimeTZVector,
  DuckDBTimeValue,
  DuckDBTimeVector,
  DuckDBTimestampMillisecondsVector,
  DuckDBTimestampNanosecondsVector,
  DuckDBTimestampSecondsVector,
  DuckDBTimestampTZValue,
  DuckDBTimestampTZVector,
  DuckDBTimestampValue,
  DuckDBTimestampVector,
  DuckDBTinyIntVector,
  DuckDBType,
  DuckDBTypeId,
  DuckDBUBigIntVector,
  DuckDBUHugeIntVector,
  DuckDBUIntegerVector,
  DuckDBUSmallIntVector,
  DuckDBUTinyIntVector,
  DuckDBUUIDVector,
  DuckDBUnionVector,
  DuckDBValue,
  DuckDBVarCharVector,
  DuckDBVarIntVector,
  DuckDBVector,
  ENUM,
  FLOAT,
  HUGEINT,
  INTEGER,
  INTERVAL,
  LIST,
  MAP,
  ResultReturnType,
  SMALLINT,
  SQLNULL,
  STRUCT,
  StatementType,
  TIME,
  TIMESTAMP,
  TIMESTAMPTZ,
  TIMESTAMP_MS,
  TIMESTAMP_NS,
  TIMESTAMP_S,
  TIMETZ,
  TINYINT,
  TimeParts,
  TimeTZParts,
  TimestampParts,
  UBIGINT,
  UHUGEINT,
  UINTEGER,
  UNION,
  USMALLINT,
  UTINYINT,
  UUID,
  VARCHAR,
  VARINT,
  arrayValue,
  bitValue,
  blobValue,
  configurationOptionDescriptions,
  decimalValue,
  intervalValue,
  listValue,
  mapValue,
  structValue,
  timeTZValue,
  timeValue,
  unionValue,
  uuidValue,
  version,
} from '../src';
import { DuckDBInstanceCache } from '../src/DuckDBInstanceCache';
import { replaceSqlNullWithInteger } from './util/replaceSqlNullWithInteger';
import {
  ColumnNameAndType,
  createTestAllTypesColumnNameAndTypeObjects,
  createTestAllTypesColumnNameAndTypeObjectsJson,
  createTestAllTypesColumnNamesAndTypesJson,
  createTestAllTypesColumnTypes,
  createTestAllTypesColumns,
  createTestAllTypesColumnsJson,
  createTestAllTypesColumnsObjectJson,
  createTestAllTypesRowObjectsJson,
  createTestAllTypesRowsJson,
} from './util/testAllTypes';
import {
  createTestJSColumnsJS,
  createTestJSColumnsObjectJS,
  createTestJSQuery,
  createTestJSRowObjectsJS,
  createTestJSRowsJS,
} from './util/testJS';

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function withConnection(
  fn: (connection: DuckDBConnection) => Promise<void>
) {
  const instance = await DuckDBInstance.create();
  const connection = await instance.connect();
  await fn(connection);
}

function assertColumns(
  result: DuckDBResult,
  expectedColumns: readonly ColumnNameAndType[]
) {
  assert.strictEqual(
    result.columnCount,
    expectedColumns.length,
    'column count'
  );
  for (let i = 0; i < expectedColumns.length; i++) {
    const { name, type } = expectedColumns[i];
    assert.strictEqual(result.columnName(i), name, 'column name');
    assert.strictEqual(
      result.columnTypeId(i),
      type.typeId,
      `column type id (column: ${name})`
    );
    assert.deepStrictEqual(
      result.columnType(i),
      type,
      `column type (column: ${name})`
    );
  }
}

function isVectorType<
  TValue extends DuckDBValue,
  TVector extends DuckDBVector<TValue>
>(
  vector: DuckDBVector<any> | null,
  vectorType: new (...args: any[]) => TVector
): vector is TVector {
  return vector instanceof vectorType;
}

function getColumnVector<
  TValue extends DuckDBValue,
  TVector extends DuckDBVector<TValue>
>(
  chunk: DuckDBDataChunk,
  columnIndex: number,
  vectorType: new (...args: any[]) => TVector
): TVector {
  const columnVector = chunk.getColumnVector(columnIndex);
  if (!isVectorType<TValue, TVector>(columnVector, vectorType)) {
    assert.fail(`expected column ${columnIndex} to be a ${vectorType}`);
  }
  return columnVector;
}

function assertVectorValues<TValue extends DuckDBValue>(
  vector: DuckDBVector<TValue> | null | undefined,
  values: readonly TValue[],
  vectorName: string
) {
  if (!vector) {
    assert.fail(`${vectorName} unexpectedly null or undefined`);
  }
  assert.strictEqual(
    vector.itemCount,
    values.length,
    `expected vector ${vectorName} item count to be ${values.length} but found ${vector.itemCount}`
  );
  for (let i = 0; i < values.length; i++) {
    const actual: TValue | null = vector.getItem(i);
    const expected = values[i];
    assert.deepStrictEqual(
      actual,
      expected,
      `expected vector ${vectorName}[${i}] to be ${expected} but found ${actual}`
    );
  }
}

function assertValues<
  TValue extends DuckDBValue,
  TVector extends DuckDBVector<TValue>
>(
  chunk: DuckDBDataChunk,
  columnIndex: number,
  vectorType: new (...args: any[]) => TVector,
  values: readonly (TValue | null)[]
) {
  const vector = getColumnVector(chunk, columnIndex, vectorType);
  assertVectorValues(vector, values, `${columnIndex}`);
}

function bigints(start: bigint, end: bigint) {
  return Array.from({ length: Number(end - start) + 1 }).map(
    (_, i) => start + BigInt(i)
  );
}

describe('api', () => {
  beforeAll(() => {
    // Set this to a constant so tests don't depend on the local timezone.
    DuckDBTimestampTZValue.timezoneOffsetInMinutes = -330;
  });
  test('should expose version', () => {
    const ver = version();
    assert.ok(ver.startsWith('v'), `version starts with 'v'`);
  });
  test('should expose configuration option descriptions', () => {
    const descriptions = configurationOptionDescriptions();
    assert.ok(descriptions['memory_limit'], `descriptions has 'memory_limit'`);
  });
  test('ReturnResultType enum', () => {
    assert.equal(ResultReturnType.INVALID, 0);
    assert.equal(ResultReturnType.CHANGED_ROWS, 1);
    assert.equal(ResultReturnType.NOTHING, 2);
    assert.equal(ResultReturnType.QUERY_RESULT, 3);

    assert.equal(ResultReturnType[ResultReturnType.INVALID], 'INVALID');
    assert.equal(
      ResultReturnType[ResultReturnType.CHANGED_ROWS],
      'CHANGED_ROWS'
    );
    assert.equal(ResultReturnType[ResultReturnType.NOTHING], 'NOTHING');
    assert.equal(
      ResultReturnType[ResultReturnType.QUERY_RESULT],
      'QUERY_RESULT'
    );
  });
  test('StatementType enum', () => {
    assert.equal(StatementType.INVALID, 0);
    assert.equal(StatementType.SELECT, 1);
    assert.equal(StatementType.INSERT, 2);
    assert.equal(StatementType.UPDATE, 3);
    assert.equal(StatementType.EXPLAIN, 4);
    assert.equal(StatementType.DELETE, 5);
    assert.equal(StatementType.PREPARE, 6);
    assert.equal(StatementType.CREATE, 7);
    assert.equal(StatementType.EXECUTE, 8);
    assert.equal(StatementType.ALTER, 9);
    assert.equal(StatementType.TRANSACTION, 10);
    assert.equal(StatementType.COPY, 11);
    assert.equal(StatementType.ANALYZE, 12);
    assert.equal(StatementType.VARIABLE_SET, 13);
    assert.equal(StatementType.CREATE_FUNC, 14);
    assert.equal(StatementType.DROP, 15);
    assert.equal(StatementType.EXPORT, 16);
    assert.equal(StatementType.PRAGMA, 17);
    assert.equal(StatementType.VACUUM, 18);
    assert.equal(StatementType.CALL, 19);
    assert.equal(StatementType.SET, 20);
    assert.equal(StatementType.LOAD, 21);
    assert.equal(StatementType.RELATION, 22);
    assert.equal(StatementType.EXTENSION, 23);
    assert.equal(StatementType.LOGICAL_PLAN, 24);
    assert.equal(StatementType.ATTACH, 25);
    assert.equal(StatementType.DETACH, 26);
    assert.equal(StatementType.MULTI, 27);

    assert.equal(StatementType[StatementType.INVALID], 'INVALID');
    assert.equal(StatementType[StatementType.SELECT], 'SELECT');
    assert.equal(StatementType[StatementType.INSERT], 'INSERT');
    assert.equal(StatementType[StatementType.UPDATE], 'UPDATE');
    assert.equal(StatementType[StatementType.EXPLAIN], 'EXPLAIN');
    assert.equal(StatementType[StatementType.DELETE], 'DELETE');
    assert.equal(StatementType[StatementType.PREPARE], 'PREPARE');
    assert.equal(StatementType[StatementType.CREATE], 'CREATE');
    assert.equal(StatementType[StatementType.EXECUTE], 'EXECUTE');
    assert.equal(StatementType[StatementType.ALTER], 'ALTER');
    assert.equal(StatementType[StatementType.TRANSACTION], 'TRANSACTION');
    assert.equal(StatementType[StatementType.COPY], 'COPY');
    assert.equal(StatementType[StatementType.ANALYZE], 'ANALYZE');
    assert.equal(StatementType[StatementType.VARIABLE_SET], 'VARIABLE_SET');
    assert.equal(StatementType[StatementType.CREATE_FUNC], 'CREATE_FUNC');
    assert.equal(StatementType[StatementType.DROP], 'DROP');
    assert.equal(StatementType[StatementType.EXPORT], 'EXPORT');
    assert.equal(StatementType[StatementType.PRAGMA], 'PRAGMA');
    assert.equal(StatementType[StatementType.VACUUM], 'VACUUM');
    assert.equal(StatementType[StatementType.CALL], 'CALL');
    assert.equal(StatementType[StatementType.SET], 'SET');
    assert.equal(StatementType[StatementType.LOAD], 'LOAD');
    assert.equal(StatementType[StatementType.RELATION], 'RELATION');
    assert.equal(StatementType[StatementType.EXTENSION], 'EXTENSION');
    assert.equal(StatementType[StatementType.LOGICAL_PLAN], 'LOGICAL_PLAN');
    assert.equal(StatementType[StatementType.ATTACH], 'ATTACH');
    assert.equal(StatementType[StatementType.DETACH], 'DETACH');
    assert.equal(StatementType[StatementType.MULTI], 'MULTI');
  });
  test('DuckDBType toString', () => {
    assert.equal(BOOLEAN.toString(), 'BOOLEAN');
    assert.equal(TINYINT.toString(), 'TINYINT');
    assert.equal(SMALLINT.toString(), 'SMALLINT');
    assert.equal(INTEGER.toString(), 'INTEGER');
    assert.equal(BIGINT.toString(), 'BIGINT');
    assert.equal(UTINYINT.toString(), 'UTINYINT');
    assert.equal(USMALLINT.toString(), 'USMALLINT');
    assert.equal(UINTEGER.toString(), 'UINTEGER');
    assert.equal(UBIGINT.toString(), 'UBIGINT');
    assert.equal(FLOAT.toString(), 'FLOAT');
    assert.equal(DOUBLE.toString(), 'DOUBLE');
    assert.equal(TIMESTAMP.toString(), 'TIMESTAMP');
    assert.equal(DATE.toString(), 'DATE');
    assert.equal(TIME.toString(), 'TIME');
    assert.equal(INTERVAL.toString(), 'INTERVAL');
    assert.equal(HUGEINT.toString(), 'HUGEINT');
    assert.equal(UHUGEINT.toString(), 'UHUGEINT');
    assert.equal(VARCHAR.toString(), 'VARCHAR');
    assert.equal(BLOB.toString(), 'BLOB');
    assert.equal(DECIMAL(17, 5).toString(), 'DECIMAL(17,5)');
    assert.equal(TIMESTAMP_S.toString(), 'TIMESTAMP_S');
    assert.equal(TIMESTAMP_MS.toString(), 'TIMESTAMP_MS');
    assert.equal(TIMESTAMP_NS.toString(), 'TIMESTAMP_NS');
    assert.equal(
      ENUM(['fly', 'swim', 'walk']).toString(),
      `ENUM('fly', 'swim', 'walk')`
    );
    assert.equal(LIST(INTEGER).toString(), 'INTEGER[]');
    assert.equal(
      STRUCT({ 'id': VARCHAR, 'ts': TIMESTAMP }).toString(),
      'STRUCT("id" VARCHAR, "ts" TIMESTAMP)'
    );
    assert.equal(MAP(INTEGER, VARCHAR).toString(), 'MAP(INTEGER, VARCHAR)');
    assert.equal(ARRAY(INTEGER, 3).toString(), 'INTEGER[3]');
    assert.equal(UUID.toString(), 'UUID');
    assert.equal(
      UNION({ 'str': VARCHAR, 'num': INTEGER }).toString(),
      'UNION("str" VARCHAR, "num" INTEGER)'
    );
    assert.equal(BIT.toString(), 'BIT');
    assert.equal(TIMETZ.toString(), 'TIME WITH TIME ZONE');
    assert.equal(TIMESTAMPTZ.toString(), 'TIMESTAMP WITH TIME ZONE');
    assert.equal(ANY.toString(), 'ANY');
    assert.equal(VARINT.toString(), 'VARINT');
    assert.equal(SQLNULL.toString(), 'SQLNULL');
  });
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
      assertValues<number, DuckDBIntegerVector>(chunk, 0, DuckDBIntegerVector, [
        42,
      ]);
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
        new Error('Failed to prepare: connection disconnected')
      );
    }
    // ensure double-disconnect doesn't break anything
    connection.disconnectSync();
  });
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
        { name: 'varint', type: VARINT },
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
          `param ${i} name mismatch`
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
        LIST(DECIMAL(4, 1))
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
        ])
      );
      prepared.bindUnion(
        i++,
        unionValue('age', 42),
        UNION({ 'name': VARCHAR, 'age': SMALLINT })
      ),
        prepared.bindUUID(i++, uuidValue(0xf0e1d2c3b4a596870123456789abcdefn));
      prepared.bindBit(i++, bitValue('0010001001011100010101011010111'));
      prepared.bindTimeTZ(i++, TIMETZ.max);
      prepared.bindTimestampTZ(i++, TIMESTAMPTZ.max);
      prepared.bindVarInt(i++, VARINT.max);
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
          `param ${i} type id mismatch`
        );
        assert.deepEqual(
          prepared.parameterType(i + 1),
          type,
          `param ${i} type mismatch`
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
          [true]
        );
        assertValues<number, DuckDBIntegerVector>(
          chunk,
          i++,
          DuckDBIntegerVector,
          [10]
        );
        assertValues<string, DuckDBVarCharVector>(
          chunk,
          i++,
          DuckDBVarCharVector,
          ['abc']
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
        assertValues(chunk, i++, DuckDBVarIntVector, [VARINT.max]);
        assertValues<number, DuckDBIntegerVector>(
          chunk,
          i++,
          DuckDBIntegerVector,
          [null]
        );
      }
    });
  });
  test('should support prepare statement bind with list', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare(
        'select $1 as a, $2 as b, $3 as c'
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
          [42]
        );
        assertValues<string, DuckDBVarCharVector>(
          chunk,
          1,
          DuckDBVarCharVector,
          ['duck']
        );
        assertValues(chunk, 2, DuckDBListVector, [listValue([10, 11, 12])]);
      }
    });
  });
  test('should support prepare statement bind with object', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare(
        'select $a as a, $b as b, $c as c, $d as d, $e as e, $f as f'
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
        }
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
          [42]
        );
        assertValues<number, DuckDBDoubleVector>(chunk, 1, DuckDBDoubleVector, [
          42.3,
        ]);
        assertValues<string, DuckDBVarCharVector>(
          chunk,
          2,
          DuckDBVarCharVector,
          ['duck']
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
          STRUCT({ 'a': ANY })
        );
        assert.fail('should throw');
      } catch (err) {
        assert.deepEqual(
          err,
          new Error(
            'Cannot create structs with an entry type of ANY. Specify a specific type.'
          )
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
            'Cannot create lists with item type of ANY. Specify a specific type.'
          )
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
            'Cannot create arrays with item type of ANY. Specify a specific type.'
          )
        );
      }
    });
  });
  test('should support starting prepared statements and running them incrementally', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare(
        'select int from test_all_types()'
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
        bigints(2048n, 2048n * 2n - 1n)
      );
      assertValues(
        chunks[2],
        0,
        DuckDBBigIntVector,
        bigints(2048n * 2n, 2048n * 3n - 1n)
      );
      assertValues(
        chunks[3],
        0,
        DuckDBBigIntVector,
        bigints(2048n * 3n, 2048n * 4n - 1n)
      );
      assertValues(
        chunks[4],
        0,
        DuckDBBigIntVector,
        bigints(2048n * 4n, 9999n)
      );
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
        { country: 'US' }
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
  test('should support all data types', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(
        'from test_all_types(use_large_enum=true)'
      );
      assertColumns(result, createTestAllTypesColumnNameAndTypeObjects());

      const chunk = await result.fetchChunk();
      assert.isDefined(chunk);
      if (chunk) {
        assert.strictEqual(chunk.columnCount, 54);
        assert.strictEqual(chunk.rowCount, 3);

        const testAllTypesColumns = createTestAllTypesColumns();

        assertValues(chunk, 0, DuckDBBooleanVector, testAllTypesColumns[0]);
        assertValues(chunk, 1, DuckDBTinyIntVector, testAllTypesColumns[1]);
        assertValues(chunk, 2, DuckDBSmallIntVector, testAllTypesColumns[2]);
        assertValues(chunk, 3, DuckDBIntegerVector, testAllTypesColumns[3]);
        assertValues(chunk, 4, DuckDBBigIntVector, testAllTypesColumns[4]);
        assertValues(chunk, 5, DuckDBHugeIntVector, testAllTypesColumns[5]);
        assertValues(chunk, 6, DuckDBUHugeIntVector, testAllTypesColumns[6]);
        assertValues(chunk, 7, DuckDBUTinyIntVector, testAllTypesColumns[7]);
        assertValues(chunk, 8, DuckDBUSmallIntVector, testAllTypesColumns[8]);
        assertValues(chunk, 9, DuckDBUIntegerVector, testAllTypesColumns[9]);
        assertValues(chunk, 10, DuckDBUBigIntVector, testAllTypesColumns[10]);
        assertValues(chunk, 11, DuckDBVarIntVector, testAllTypesColumns[11]);
        assertValues(chunk, 12, DuckDBDateVector, testAllTypesColumns[12]);
        assertValues(chunk, 13, DuckDBTimeVector, testAllTypesColumns[13]);
        assertValues(chunk, 14, DuckDBTimestampVector, testAllTypesColumns[14]);
        assertValues(
          chunk,
          15,
          DuckDBTimestampSecondsVector,
          testAllTypesColumns[15]
        );
        assertValues(
          chunk,
          16,
          DuckDBTimestampMillisecondsVector,
          testAllTypesColumns[16]
        );
        assertValues(
          chunk,
          17,
          DuckDBTimestampNanosecondsVector,
          testAllTypesColumns[17]
        );
        assertValues(chunk, 18, DuckDBTimeTZVector, testAllTypesColumns[18]);
        assertValues(
          chunk,
          19,
          DuckDBTimestampTZVector,
          testAllTypesColumns[19]
        );
        assertValues(chunk, 20, DuckDBFloatVector, testAllTypesColumns[20]);
        assertValues(chunk, 21, DuckDBDoubleVector, testAllTypesColumns[21]);
        assertValues(chunk, 22, DuckDBDecimal16Vector, testAllTypesColumns[22]);
        assertValues(chunk, 23, DuckDBDecimal32Vector, testAllTypesColumns[23]);
        assertValues(chunk, 24, DuckDBDecimal64Vector, testAllTypesColumns[24]);
        assertValues(
          chunk,
          25,
          DuckDBDecimal128Vector,
          testAllTypesColumns[25]
        );
        assertValues(chunk, 26, DuckDBUUIDVector, testAllTypesColumns[26]);
        assertValues(chunk, 27, DuckDBIntervalVector, testAllTypesColumns[27]);
        assertValues(chunk, 28, DuckDBVarCharVector, testAllTypesColumns[28]);
        assertValues(chunk, 29, DuckDBBlobVector, testAllTypesColumns[29]);
        assertValues(chunk, 30, DuckDBBitVector, testAllTypesColumns[30]);
        assertValues(chunk, 31, DuckDBEnum8Vector, testAllTypesColumns[31]);
        assertValues(chunk, 32, DuckDBEnum16Vector, testAllTypesColumns[32]);
        assertValues(chunk, 33, DuckDBEnum32Vector, testAllTypesColumns[33]);
        // int_array
        assertValues(chunk, 34, DuckDBListVector, testAllTypesColumns[34]);
        // double_array
        assertValues(chunk, 35, DuckDBListVector, testAllTypesColumns[35]);
        // date_array
        assertValues(chunk, 36, DuckDBListVector, testAllTypesColumns[36]);
        // timestamp_array
        assertValues(chunk, 37, DuckDBListVector, testAllTypesColumns[37]);
        // timestamptz_array
        assertValues(chunk, 38, DuckDBListVector, testAllTypesColumns[38]);
        // varchar_array
        assertValues(chunk, 39, DuckDBListVector, testAllTypesColumns[39]);
        // nested_int_array
        assertValues(chunk, 40, DuckDBListVector, testAllTypesColumns[40]);
        assertValues(chunk, 41, DuckDBStructVector, testAllTypesColumns[41]);
        // struct_of_arrays
        assertValues(chunk, 42, DuckDBStructVector, testAllTypesColumns[42]);
        // array_of_structs
        assertValues(chunk, 43, DuckDBListVector, testAllTypesColumns[43]);
        assertValues(chunk, 44, DuckDBMapVector, testAllTypesColumns[44]);
        assertValues(chunk, 45, DuckDBUnionVector, testAllTypesColumns[45]);
        // fixed_int_array
        assertValues(chunk, 46, DuckDBArrayVector, testAllTypesColumns[46]);
        // fixed_varchar_array
        assertValues(chunk, 47, DuckDBArrayVector, testAllTypesColumns[47]);
        // fixed_nested_int_array
        assertValues(chunk, 48, DuckDBArrayVector, testAllTypesColumns[48]);
        // fixed_nested_varchar_array
        assertValues(chunk, 49, DuckDBArrayVector, testAllTypesColumns[49]);
        // fixed_struct_array
        assertValues(chunk, 50, DuckDBArrayVector, testAllTypesColumns[50]);
        // struct_of_fixed_array
        assertValues(chunk, 51, DuckDBStructVector, testAllTypesColumns[51]);
        // fixed_array_of_int_list
        assertValues(chunk, 52, DuckDBArrayVector, testAllTypesColumns[52]);
        // list_of_fixed_int_array
        assertValues(chunk, 53, DuckDBListVector, testAllTypesColumns[53]);
      }
    });
  });
  test('values toString', () => {
    // array
    assert.equal(arrayValue([]).toString(), '[]');
    assert.equal(arrayValue([1, 2, 3]).toString(), '[1, 2, 3]');
    assert.equal(arrayValue(['a', 'b', 'c']).toString(), `['a', 'b', 'c']`);

    // bit
    assert.equal(bitValue('').toString(), '');
    assert.equal(bitValue('10101').toString(), '10101');
    assert.equal(
      bitValue('0010001001011100010101011010111').toString(),
      '0010001001011100010101011010111'
    );

    // blob
    assert.equal(blobValue('').toString(), '');
    assert.equal(
      blobValue('thisisalongblob\x00withnullbytes').toString(),
      'thisisalongblob\\x00withnullbytes'
    );
    assert.equal(blobValue('\x00\x00\x00a').toString(), '\\x00\\x00\\x00a');

    // date
    assert.equal(DATE.epoch.toString(), '1970-01-01');
    assert.equal(DATE.max.toString(), '5881580-07-10');
    assert.equal(DATE.min.toString(), '5877642-06-25 (BC)');

    // decimal
    assert.equal(decimalValue(0n, 4, 1).toString(), '0.0');
    assert.equal(decimalValue(9876n, 4, 1).toString(), '987.6');
    assert.equal(decimalValue(-9876n, 4, 1).toString(), '-987.6');

    assert.equal(decimalValue(0n, 9, 4).toString(), '0.0000');
    assert.equal(decimalValue(987654321n, 9, 4).toString(), '98765.4321');
    assert.equal(decimalValue(-987654321n, 9, 4).toString(), '-98765.4321');

    assert.equal(decimalValue(0n, 18, 6).toString(), '0.000000');
    assert.equal(
      decimalValue(987654321098765432n, 18, 6).toString(),
      '987654321098.765432'
    );
    assert.equal(
      decimalValue(-987654321098765432n, 18, 6).toString(),
      '-987654321098.765432'
    );

    assert.equal(decimalValue(0n, 38, 10).toString(), '0.0000000000');
    assert.equal(
      decimalValue(98765432109876543210987654321098765432n, 38, 10).toString(),
      '9876543210987654321098765432.1098765432'
    );
    assert.equal(
      decimalValue(-98765432109876543210987654321098765432n, 38, 10).toString(),
      '-9876543210987654321098765432.1098765432'
    );

    // interval
    assert.equal(intervalValue(0, 0, 0n).toString(), '00:00:00');

    assert.equal(intervalValue(1, 0, 0n).toString(), '1 month');
    assert.equal(intervalValue(-1, 0, 0n).toString(), '-1 month');
    assert.equal(intervalValue(2, 0, 0n).toString(), '2 months');
    assert.equal(intervalValue(-2, 0, 0n).toString(), '-2 months');
    assert.equal(intervalValue(12, 0, 0n).toString(), '1 year');
    assert.equal(intervalValue(-12, 0, 0n).toString(), '-1 year');
    assert.equal(intervalValue(24, 0, 0n).toString(), '2 years');
    assert.equal(intervalValue(-24, 0, 0n).toString(), '-2 years');
    assert.equal(intervalValue(25, 0, 0n).toString(), '2 years 1 month');
    assert.equal(intervalValue(-25, 0, 0n).toString(), '-2 years -1 month');

    assert.equal(intervalValue(0, 1, 0n).toString(), '1 day');
    assert.equal(intervalValue(0, -1, 0n).toString(), '-1 day');
    assert.equal(intervalValue(0, 2, 0n).toString(), '2 days');
    assert.equal(intervalValue(0, -2, 0n).toString(), '-2 days');
    assert.equal(intervalValue(0, 30, 0n).toString(), '30 days');
    assert.equal(intervalValue(0, 365, 0n).toString(), '365 days');

    assert.equal(intervalValue(0, 0, 1n).toString(), '00:00:00.000001');
    assert.equal(intervalValue(0, 0, -1n).toString(), '-00:00:00.000001');
    assert.equal(intervalValue(0, 0, 987654n).toString(), '00:00:00.987654');
    assert.equal(intervalValue(0, 0, -987654n).toString(), '-00:00:00.987654');
    assert.equal(intervalValue(0, 0, 1000000n).toString(), '00:00:01');
    assert.equal(intervalValue(0, 0, -1000000n).toString(), '-00:00:01');
    assert.equal(intervalValue(0, 0, 59n * 1000000n).toString(), '00:00:59');
    assert.equal(intervalValue(0, 0, -59n * 1000000n).toString(), '-00:00:59');
    assert.equal(intervalValue(0, 0, 60n * 1000000n).toString(), '00:01:00');
    assert.equal(intervalValue(0, 0, -60n * 1000000n).toString(), '-00:01:00');
    assert.equal(
      intervalValue(0, 0, 59n * 60n * 1000000n).toString(),
      '00:59:00'
    );
    assert.equal(
      intervalValue(0, 0, -59n * 60n * 1000000n).toString(),
      '-00:59:00'
    );
    assert.equal(
      intervalValue(0, 0, 60n * 60n * 1000000n).toString(),
      '01:00:00'
    );
    assert.equal(
      intervalValue(0, 0, -60n * 60n * 1000000n).toString(),
      '-01:00:00'
    );
    assert.equal(
      intervalValue(0, 0, 24n * 60n * 60n * 1000000n).toString(),
      '24:00:00'
    );
    assert.equal(
      intervalValue(0, 0, -24n * 60n * 60n * 1000000n).toString(),
      '-24:00:00'
    );
    assert.equal(
      intervalValue(0, 0, 2147483647n * 60n * 60n * 1000000n).toString(),
      '2147483647:00:00'
    );
    assert.equal(
      intervalValue(0, 0, -2147483647n * 60n * 60n * 1000000n).toString(),
      '-2147483647:00:00'
    );
    assert.equal(
      intervalValue(0, 0, 2147483647n * 60n * 60n * 1000000n + 1n).toString(),
      '2147483647:00:00.000001'
    );
    assert.equal(
      intervalValue(
        0,
        0,
        -(2147483647n * 60n * 60n * 1000000n + 1n)
      ).toString(),
      '-2147483647:00:00.000001'
    );

    assert.equal(
      intervalValue(
        2 * 12 + 3,
        5,
        (7n * 60n * 60n + 11n * 60n + 13n) * 1000000n + 17n
      ).toString(),
      '2 years 3 months 5 days 07:11:13.000017'
    );
    assert.equal(
      intervalValue(
        -(2 * 12 + 3),
        -5,
        -((7n * 60n * 60n + 11n * 60n + 13n) * 1000000n + 17n)
      ).toString(),
      '-2 years -3 months -5 days -07:11:13.000017'
    );

    // list
    assert.equal(listValue([]).toString(), '[]');
    assert.equal(listValue([1, 2, 3]).toString(), '[1, 2, 3]');
    assert.equal(listValue(['a', 'b', 'c']).toString(), `['a', 'b', 'c']`);

    // map
    assert.equal(mapValue([]).toString(), '{}');
    assert.equal(
      mapValue([
        { key: 1, value: 'a' },
        { key: 2, value: 'b' },
      ]).toString(),
      `{1: 'a', 2: 'b'}`
    );

    // struct
    assert.equal(structValue({}).toString(), '{}');
    assert.equal(structValue({ a: 1, b: 2 }).toString(), `{'a': 1, 'b': 2}`);

    // timestamp milliseconds
    assert.equal(TIMESTAMP_MS.epoch.toString(), '1970-01-01 00:00:00');
    assert.equal(TIMESTAMP_MS.max.toString(), '294247-01-10 04:00:54.775');
    assert.equal(TIMESTAMP_MS.min.toString(), '290309-12-22 (BC) 00:00:00');

    // timestamp nanoseconds
    assert.equal(TIMESTAMP_NS.epoch.toString(), '1970-01-01 00:00:00');
    assert.equal(TIMESTAMP_NS.max.toString(), '2262-04-11 23:47:16.854775806');
    assert.equal(TIMESTAMP_NS.min.toString(), '1677-09-22 00:00:00');

    // timestamp seconds
    assert.equal(TIMESTAMP_S.epoch.toString(), '1970-01-01 00:00:00');
    assert.equal(TIMESTAMP_S.max.toString(), '294247-01-10 04:00:54');
    assert.equal(TIMESTAMP_S.min.toString(), '290309-12-22 (BC) 00:00:00');

    // timestamp tz
    assert.equal(TIMESTAMPTZ.epoch.toString(), '1969-12-31 18:30:00-05:30');
    assert.equal(
      TIMESTAMPTZ.max.toString(),
      '294247-01-09 22:30:54.775806-05:30'
    );
    assert.equal(
      TIMESTAMPTZ.min.toString(),
      '290309-12-21 (BC) 18:30:00-05:30'
    );
    assert.equal(TIMESTAMPTZ.posInf.toString(), 'infinity');
    assert.equal(TIMESTAMPTZ.negInf.toString(), '-infinity');

    // timestamp
    assert.equal(TIMESTAMP.epoch.toString(), '1970-01-01 00:00:00');
    assert.equal(TIMESTAMP.max.toString(), '294247-01-10 04:00:54.775806');
    assert.equal(TIMESTAMP.min.toString(), '290309-12-22 (BC) 00:00:00');
    assert.equal(TIMESTAMP.posInf.toString(), 'infinity');
    assert.equal(TIMESTAMP.negInf.toString(), '-infinity');

    // time tz
    assert.equal(timeTZValue(0n, 0).toString(), '00:00:00+00');
    assert.equal(
      timeTZValue(
        (((12n * 60n + 34n) * 60n + 56n) * 1000n + 789n) * 1000n,
        -((7 * 60 + 9) * 60)
      ).toString(),
      '12:34:56.789-07:09'
    );
    assert.equal(TIMETZ.max.toString(), '24:00:00-15:59:59');
    assert.equal(TIMETZ.min.toString(), '00:00:00+15:59:59');

    // time
    assert.equal(TIME.max.toString(), '24:00:00');
    assert.equal(TIME.min.toString(), '00:00:00');
    assert.equal(
      timeValue(
        (12n * 60n * 60n + 34n * 60n + 56n) * 1000000n + 987654n
      ).toString(),
      '12:34:56.987654'
    );

    // union
    assert.equal(unionValue('a', 42).toString(), '42');
    assert.equal(unionValue('b', 'duck').toString(), 'duck');

    // uuid
    assert.equal(UUID.min.toString(), '00000000-0000-0000-0000-000000000000');
    assert.equal(UUID.max.toString(), 'ffffffff-ffff-ffff-ffff-ffffffffffff');
  });
  test('date isFinite', () => {
    assert.isTrue(DATE.epoch.isFinite);
    assert.isTrue(DATE.max.isFinite);
    assert.isTrue(DATE.min.isFinite);
    assert.isFalse(DATE.posInf.isFinite);
    assert.isFalse(DATE.negInf.isFinite);
  });
  test('timestamp isFinite', () => {
    assert.isTrue(TIMESTAMP.epoch.isFinite);
    assert.isTrue(TIMESTAMP.max.isFinite);
    assert.isTrue(TIMESTAMP.min.isFinite);
    assert.isFalse(TIMESTAMP.posInf.isFinite);
    assert.isFalse(TIMESTAMP.negInf.isFinite);
  });
  test('timestamp_s isFinite', () => {
    assert.isTrue(TIMESTAMP_S.epoch.isFinite);
    assert.isTrue(TIMESTAMP_S.max.isFinite);
    assert.isTrue(TIMESTAMP_S.min.isFinite);
    assert.isFalse(TIMESTAMP_S.posInf.isFinite);
    assert.isFalse(TIMESTAMP_S.negInf.isFinite);
  });
  test('timestamp_ms isFinite', () => {
    assert.isTrue(TIMESTAMP_MS.epoch.isFinite);
    assert.isTrue(TIMESTAMP_MS.max.isFinite);
    assert.isTrue(TIMESTAMP_MS.min.isFinite);
    assert.isFalse(TIMESTAMP_MS.posInf.isFinite);
    assert.isFalse(TIMESTAMP_MS.negInf.isFinite);
  });
  test('timestamp_ns isFinite', () => {
    assert.isTrue(TIMESTAMP_NS.epoch.isFinite);
    assert.isTrue(TIMESTAMP_NS.max.isFinite);
    assert.isTrue(TIMESTAMP_NS.min.isFinite);
    assert.isFalse(TIMESTAMP_NS.posInf.isFinite);
    assert.isFalse(TIMESTAMP_NS.negInf.isFinite);
  });
  test('value conversion', () => {
    const dateParts: DateParts = { year: 2024, month: 6, day: 3 };
    const timeParts: TimeParts = { hour: 12, min: 34, sec: 56, micros: 789123 };
    const timeTZParts: TimeTZParts = {
      time: timeParts,
      offset: DuckDBTimeTZValue.MinOffset,
    };
    const timestampParts: TimestampParts = { date: dateParts, time: timeParts };

    assert.deepEqual(DuckDBDateValue.fromParts(dateParts).toParts(), dateParts);
    assert.deepEqual(DuckDBTimeValue.fromParts(timeParts).toParts(), timeParts);
    assert.deepEqual(
      DuckDBTimeTZValue.fromParts(timeTZParts).toParts(),
      timeTZParts
    );
    assert.deepEqual(
      DuckDBTimestampValue.fromParts(timestampParts).toParts(),
      timestampParts
    );
    assert.deepEqual(
      DuckDBTimestampTZValue.fromParts(timestampParts).toParts(),
      timestampParts
    );

    assert.deepEqual(
      DuckDBDecimalValue.fromDouble(3.14159, 6, 5),
      decimalValue(314159n, 6, 5)
    );
    assert.deepEqual(decimalValue(314159n, 6, 5).toDouble(), 3.14159);
  });
  test('result inspection conveniences', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(
        'select i::int as a, i::int + 10 as b from range(3) t(i)'
      );
      assert.deepEqual(result.columnNames(), ['a', 'b']);
      assert.deepEqual(result.columnTypes(), [INTEGER, INTEGER]);
      const chunks = await result.fetchAllChunks();
      const chunkColumns = chunks.map((chunk) => chunk.getColumns());
      assert.deepEqual(chunkColumns, [
        [
          [0, 1, 2],
          [10, 11, 12],
        ],
      ]);
      const chunkRows = chunks.map((chunk) => chunk.getRows());
      assert.deepEqual(chunkRows, [
        [
          [0, 10],
          [1, 11],
          [2, 12],
        ],
      ]);
    });
  });
  test('row and column objects', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        'select i::int as a, i::int + 10 as b, (i + 100)::varchar as a from range(3) t(i)'
      );
      assert.deepEqual(reader.columnNames(), ['a', 'b', 'a']);
      assert.deepEqual(reader.deduplicatedColumnNames(), ['a', 'b', 'a:1']);
      assert.deepEqual(reader.columnTypes(), [INTEGER, INTEGER, VARCHAR]);
      assert.deepEqual(reader.getRowObjects(), [
        { 'a': 0, 'b': 10, 'a:1': '100' },
        { 'a': 1, 'b': 11, 'a:1': '101' },
        { 'a': 2, 'b': 12, 'a:1': '102' },
      ]);
      assert.deepEqual(reader.getColumnsObject(), {
        'a': [0, 1, 2],
        'b': [10, 11, 12],
        'a:1': ['100', '101', '102'],
      });
    });
  });
  test('columns js', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(createTestJSQuery());
      const columnsJS = reader.getColumnsJS();
      assert.deepEqual(columnsJS, createTestJSColumnsJS());
    });
  });
  test('columns object js', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(createTestJSQuery());
      const columnsJS = reader.getColumnsObjectJS();
      assert.deepEqual(columnsJS, createTestJSColumnsObjectJS());
    });
  });
  test('rows js', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(createTestJSQuery());
      const columnsJS = reader.getRowsJS();
      assert.deepEqual(columnsJS, createTestJSRowsJS());
    });
  });
  test('row objects js', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(createTestJSQuery());
      const columnsJS = reader.getRowObjectsJS();
      assert.deepEqual(columnsJS, createTestJSRowObjectsJS());
    });
  });
  test('columns json', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(`from test_all_types()`);
      const columnsJson = reader.getColumnsJson();
      assert.deepEqual(columnsJson, createTestAllTypesColumnsJson());
    });
  });
  test('columns object json', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(`from test_all_types()`);
      const columnsJson = reader.getColumnsObjectJson();
      assert.deepEqual(columnsJson, createTestAllTypesColumnsObjectJson());
    });
  });
  test('rows json', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(`from test_all_types()`);
      const rowsJson = reader.getRowsJson();
      assert.deepEqual(rowsJson, createTestAllTypesRowsJson());
    });
  });
  test('row objects json', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(`from test_all_types()`);
      const rowObjectsJson = reader.getRowObjectsJson();
      assert.deepEqual(rowObjectsJson, createTestAllTypesRowObjectsJson());
    });
  });
  test('column names and types json', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        `from test_all_types(use_large_enum=true)`
      );
      const columnNamesAndTypesJson = reader.columnNamesAndTypesJson();
      assert.deepEqual(
        columnNamesAndTypesJson,
        createTestAllTypesColumnNamesAndTypesJson()
      );
    });
  });
  test('column name and type objects json', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        `from test_all_types(use_large_enum=true)`
      );
      const columnNameAndTypeObjectsJson =
        reader.columnNameAndTypeObjectsJson();
      assert.deepEqual(
        columnNameAndTypeObjectsJson,
        createTestAllTypesColumnNameAndTypeObjectsJson()
      );
    });
  });
  test('result reader', async () => {
    await withConnection(async (connection) => {
      const reader = await connection.runAndReadAll(
        'select i::int as a, i::int + 10000 as b from range(5000) t(i)'
      );
      assert.deepEqual(reader.columnNames(), ['a', 'b']);
      assert.deepEqual(reader.columnTypes(), [INTEGER, INTEGER]);
      const columns = reader.getColumns();
      assert.equal(columns.length, 2);
      assert.equal(columns[0][0], 0);
      assert.equal(columns[0][4999], 4999);
      assert.equal(columns[1][0], 10000);
      assert.equal(columns[1][4999], 14999);
      const rows = reader.getRows();
      assert.equal(rows.length, 5000);
      assert.deepEqual(rows[0], [0, 10000]);
      assert.deepEqual(rows[4999], [4999, 14999]);
    });
  });
  test('default duckdb_api without explicit options', async () => {
    const instance = await DuckDBInstance.create();
    const connection = await instance.connect();
    const result = await connection.run(
      `select current_setting('duckdb_api') as duckdb_api`
    );
    assertColumns(result, [{ name: 'duckdb_api', type: VARCHAR }]);
    const chunk = await result.fetchChunk();
    assert.isDefined(chunk);
    if (chunk) {
      assert.strictEqual(chunk.columnCount, 1);
      assert.strictEqual(chunk.rowCount, 1);
      assertValues<string, DuckDBVarCharVector>(chunk, 0, DuckDBVarCharVector, [
        'node-neo-api',
      ]);
    }
  });
  test('default duckdb_api with explicit options', async () => {
    const instance = await DuckDBInstance.create(undefined, {});
    const connection = await instance.connect();
    const result = await connection.run(
      `select current_setting('duckdb_api') as duckdb_api`
    );
    assertColumns(result, [{ name: 'duckdb_api', type: VARCHAR }]);
    const chunk = await result.fetchChunk();
    assert.isDefined(chunk);
    if (chunk) {
      assert.strictEqual(chunk.columnCount, 1);
      assert.strictEqual(chunk.rowCount, 1);
      assertValues<string, DuckDBVarCharVector>(chunk, 0, DuckDBVarCharVector, [
        'node-neo-api',
      ]);
    }
  });
  test('overriding duckdb_api', async () => {
    const instance = await DuckDBInstance.create(undefined, {
      'duckdb_api': 'custom-duckdb-api',
    });
    const connection = await instance.connect();
    const result = await connection.run(
      `select current_setting('duckdb_api') as duckdb_api`
    );
    assertColumns(result, [{ name: 'duckdb_api', type: VARCHAR }]);
    const chunk = await result.fetchChunk();
    assert.isDefined(chunk);
    if (chunk) {
      assert.strictEqual(chunk.columnCount, 1);
      assert.strictEqual(chunk.rowCount, 1);
      assertValues<string, DuckDBVarCharVector>(chunk, 0, DuckDBVarCharVector, [
        'custom-duckdb-api',
      ]);
    }
  });
  test('instance cache - same instance', async () => {
    const cache = new DuckDBInstanceCache();
    const instance1 = await cache.getOrCreateInstance();
    const connection1 = await instance1.connect();
    await connection1.run(`attach ':memory:' as mem1`);

    const instance2 = await cache.getOrCreateInstance();
    const connection2 = await instance2.connect();
    await connection2.run(`create table mem1.main.t1 as select 1`);
  });
  // Need to support explicitly destroying instance cache?
  test.skip('instance cache - different instances', async () => {
    let instance1: DuckDBInstance | undefined;
    let instance2: DuckDBInstance | undefined;
    try {
      const cache = new DuckDBInstanceCache();
      const instance1 = await cache.getOrCreateInstance(
        'instance_cache_test_a.db'
      );
      const connection1 = await instance1.connect();
      await connection1.run(`attach ':memory:' as mem1`);

      const instance2 = await cache.getOrCreateInstance(
        'instance_cache_test_b.db'
      );
      const connection2 = await instance2.connect();
      try {
        await connection2.run(`create table mem1.main.t1 as select 1`);
        assert.fail('should throw');
      } catch (err) {
        assert.deepEqual(
          err,
          new Error(`Catalog Error: Catalog with name mem1 does not exist!`)
        );
      }
    } finally {
      if (instance1) {
        instance1.closeSync();
        fs.rmSync('instance_cache_test_a.db');
      }
      if (instance2) {
        instance2.closeSync();
        fs.rmSync('instance_cache_test_b.db');
      }
    }
  });
  test('instance cache - different config', async () => {
    const cache = new DuckDBInstanceCache();
    const instance1 = await cache.getOrCreateInstance();
    const connection1 = await instance1.connect();
    await connection1.run(`attach ':memory:' as mem1`);
    try {
      await cache.getOrCreateInstance(undefined, { accces_mode: 'READ_ONLY' });
      assert.fail('should throw');
    } catch (err) {
      assert.deepEqual(
        err,
        new Error(
          `Connection Error: Can't open a connection to same database file with a different configuration than existing connections`
        )
      );
    }
  });
  test('instance cache - singleton', async () => {
    const instance = await DuckDBInstance.fromCache();
    const connection = await instance.connect();
    await connection.run('select 1');
  });
  test('create connection using instance cache', async () => {
    const connection = await DuckDBConnection.create();
    await connection.run('select 1');
  });
  test('data chunk max rows', () => {
    try {
      DuckDBDataChunk.create([INTEGER], 2049);
      assert.fail('should throw');
    } catch (err) {
      assert.deepEqual(
        err,
        new Error('A data chunk cannot have more than 2048 rows')
      );
    }
  });
  test('write integer vector', () => {
    const chunk = DuckDBDataChunk.create([INTEGER], 3);
    const vector = chunk.getColumnVector(0) as DuckDBIntegerVector;
    assert.equal(vector.itemCount, 3);
    vector.setItem(0, 42);
    vector.setItem(1, 12345);
    vector.setItem(2, 67890);
    assert.equal(vector.getItem(0), 42);
    assert.equal(vector.getItem(1), 12345);
    assert.equal(vector.getItem(2), 67890);
  });
  test('write integer vector with nulls', () => {
    const chunk = DuckDBDataChunk.create([INTEGER], 3);
    const vector = chunk.getColumnVector(0) as DuckDBIntegerVector;
    assert.equal(vector.itemCount, 3);
    vector.setItem(0, 42);
    vector.setItem(1, 12345);
    vector.setItem(2, null);
    assert.equal(vector.getItem(0), 42);
    assert.equal(vector.getItem(1), 12345);
    assert.equal(vector.getItem(2), null);
  });
  test('write list vector', () => {
    const chunk = DuckDBDataChunk.create([LIST(INTEGER)], 3);
    const vector = chunk.getColumnVector(0) as DuckDBListVector;
    assert.equal(vector.itemCount, 3);
    vector.setItem(0, listValue([10, 11, 12]));
    vector.setItem(1, listValue([20, 21, 22]));
    vector.setItem(2, null);
    assert.deepEqual(vector.getItem(0), listValue([10, 11, 12]));
    assert.deepEqual(vector.getItem(1), listValue([20, 21, 22]));
    assert.equal(vector.getItem(2), null);
  });
  test('create and append data chunk', async () => {
    await withConnection(async (connection) => {
      const values = [42, 12345, null];

      const chunk = DuckDBDataChunk.create([INTEGER], values.length);
      chunk.setColumnValues(0, values);

      await connection.run('create table target(col0 int)');
      const appender = await connection.createAppender('target');
      appender.appendDataChunk(chunk);
      appender.flushSync();

      const result = await connection.run('from target');
      const resultChunk = await result.fetchChunk();
      assert.isDefined(resultChunk);
      if (resultChunk) {
        assert.equal(resultChunk.columnCount, 1);
        assert.equal(resultChunk.rowCount, values.length);
        assertValues(resultChunk, 0, DuckDBIntegerVector, values);
      }
    });
  });
  test('create and append data chunk, delayed flush', async () => {
    await withConnection(async (connection) => {
      const baselineValues = [10, 11, 12];
      const targetValues = [42, 12345, null];

      const chunk = DuckDBDataChunk.create([INTEGER], targetValues.length);
      const vector = chunk.getColumnVector(0) as DuckDBIntegerVector;

      // First, set the values to something known as a baseline.
      for (let i = 0; i < baselineValues.length; i++) {
        vector.setItem(i, baselineValues[i]);
      }
      vector.flush();

      // Then, set the values to our target, but don't flush (yet).
      for (let i = 0; i < targetValues.length; i++) {
        vector.setItem(i, targetValues[i]);
      }

      await connection.run('create table target1(col0 int)');
      const appender1 = await connection.createAppender('target1');
      appender1.appendDataChunk(chunk);
      appender1.flushSync();

      const result1 = await connection.run('from target1');
      const result1Chunk = await result1.fetchChunk();
      assert.isDefined(result1Chunk);
      if (result1Chunk) {
        assert.equal(result1Chunk.columnCount, 1);
        assert.equal(result1Chunk.rowCount, targetValues.length);
        assertValues(result1Chunk, 0, DuckDBIntegerVector, baselineValues);
      }

      // now flush
      vector.flush();

      await connection.run('create table target2(col0 int)');
      const appender2 = await connection.createAppender('target2');
      appender2.appendDataChunk(chunk);
      appender2.flushSync();

      const result2 = await connection.run('from target2');
      const result2Chunk = await result2.fetchChunk();
      assert.isDefined(result2Chunk);
      if (result2Chunk) {
        assert.equal(result2Chunk.columnCount, 1);
        assert.equal(result2Chunk.rowCount, 3);
        assertValues(result2Chunk, 0, DuckDBIntegerVector, targetValues);
      }
    });
  });
  test('create and append data chunk with varchars', async () => {
    await withConnection(async (connection) => {
      const values = ['xyz', 'abcdefghijkl', 'ABCDEFGHIJKLM', null];

      const chunk = DuckDBDataChunk.create([VARCHAR], values.length);
      chunk.setColumnValues(0, values);

      await connection.run('create table target(col0 varchar)');
      const appender = await connection.createAppender('target');
      appender.appendDataChunk(chunk);
      appender.flushSync();

      const result = await connection.run('from target');
      const resultChunk = await result.fetchChunk();
      assert.isDefined(resultChunk);
      if (resultChunk) {
        assert.equal(resultChunk.columnCount, 1);
        assert.equal(resultChunk.rowCount, values.length);
        assertValues(resultChunk, 0, DuckDBVarCharVector, values);
      }
    });
  });
  test('create and append data chunk with blobs', async () => {
    await withConnection(async (connection) => {
      const values = [
        blobValue(new Uint8Array([0xab, 0xcd, 0xef])),
        blobValue(
          new Uint8Array([
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
            0x0c,
          ])
        ),
        blobValue(
          new Uint8Array([
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d,
          ])
        ),
        null,
      ];
      const chunk = DuckDBDataChunk.create([BLOB], values.length);
      chunk.setColumnValues(0, values);

      await connection.run('create table target(col0 blob)');
      const appender = await connection.createAppender('target');
      appender.appendDataChunk(chunk);
      appender.flushSync();

      const result = await connection.run('from target');
      const resultChunk = await result.fetchChunk();
      assert.isDefined(resultChunk);
      if (resultChunk) {
        assert.equal(resultChunk.columnCount, 1);
        assert.equal(resultChunk.rowCount, values.length);
        assertValues(resultChunk, 0, DuckDBBlobVector, values);
      }
    });
  });
  test('create and append data chunk with lists', async () => {
    await withConnection(async (connection) => {
      const values = [
        listValue([10, 11, 12]),
        listValue([]),
        listValue([100, 200]),
        null,
      ];

      const chunk = DuckDBDataChunk.create([LIST(INTEGER)], values.length);
      chunk.setColumnValues(0, values);

      await connection.run('create table target(col0 integer[])');
      const appender = await connection.createAppender('target');
      appender.appendDataChunk(chunk);
      appender.flushSync();

      const result = await connection.run('from target');
      const resultChunk = await result.fetchChunk();
      assert.isDefined(resultChunk);
      if (resultChunk) {
        assert.equal(resultChunk.columnCount, 1);
        assert.equal(resultChunk.rowCount, values.length);
        assertValues(resultChunk, 0, DuckDBListVector, values);
      }
    });
  });
  test('create and append data chunk, modify nested list vector', async () => {
    await withConnection(async (connection) => {
      const originalValues = [
        listValue([listValue([110, 111]), listValue([]), listValue([130])]),
        listValue([]),
        listValue([
          listValue([310, 311, 312]),
          listValue([320, 321]),
          listValue([330, 331, 332, 333]),
        ]),
      ];

      const chunk = DuckDBDataChunk.create(
        [LIST(LIST(INTEGER))],
        originalValues.length
      );
      chunk.setColumnValues(0, originalValues);

      const outerListVector = chunk.getColumnVector(0) as DuckDBListVector;
      const innerListVector = outerListVector.getItemVector(
        2
      ) as DuckDBListVector;
      innerListVector.setItem(1, listValue([350, 351, 352, 353, 354]));
      innerListVector.flush();

      const modifiedValues = [...originalValues];
      modifiedValues[2] = listValue([
        listValue([310, 311, 312]),
        listValue([350, 351, 352, 353, 354]),
        listValue([330, 331, 332, 333]),
      ]);

      await connection.run('create table target(col0 integer[][])');
      const appender = await connection.createAppender('target');
      appender.appendDataChunk(chunk);
      appender.flushSync();

      const result = await connection.run('from target');
      const resultChunk = await result.fetchChunk();
      assert.isDefined(resultChunk);
      if (resultChunk) {
        assert.equal(resultChunk.columnCount, 1);
        assert.equal(resultChunk.rowCount, modifiedValues.length);
        assertValues(resultChunk, 0, DuckDBListVector, modifiedValues);
      }
    });
  });
  test('create and append data chunk with arrays of integers', async () => {
    await withConnection(async (connection) => {
      const values = [
        arrayValue([10, 11, 12]),
        arrayValue([20, 21, 22]),
        arrayValue([30, 31, 32]),
        null,
      ];

      const chunk = DuckDBDataChunk.create([ARRAY(INTEGER, 3)], values.length);
      chunk.setColumnValues(0, values);

      await connection.run('create table target(col0 integer[3])');
      const appender = await connection.createAppender('target');
      appender.appendDataChunk(chunk);
      appender.flushSync();

      const result = await connection.run('from target');
      const resultChunk = await result.fetchChunk();
      assert.isDefined(resultChunk);
      if (resultChunk) {
        assert.equal(resultChunk.columnCount, 1);
        assert.equal(resultChunk.rowCount, values.length);
        assertValues(resultChunk, 0, DuckDBArrayVector, values);
      }
    });
  });
  test('create and append data chunk with arrays of varchars', async () => {
    await withConnection(async (connection) => {
      const values = [
        arrayValue(['a', 'b', 'c']),
        arrayValue(['d', 'e', 'f']),
        arrayValue(['g', 'h', 'i']),
        null,
      ];

      const chunk = DuckDBDataChunk.create([ARRAY(VARCHAR, 3)], values.length);
      chunk.setColumnValues(0, values);

      await connection.run('create table target(col0 varchar[3])');
      const appender = await connection.createAppender('target');
      appender.appendDataChunk(chunk);
      appender.flushSync();

      const result = await connection.run('from target');
      const resultChunk = await result.fetchChunk();
      assert.isDefined(resultChunk);
      if (resultChunk) {
        assert.equal(resultChunk.columnCount, 1);
        assert.equal(resultChunk.rowCount, values.length);
        assertValues(resultChunk, 0, DuckDBArrayVector, values);
      }
    });
  });
  test('create and append data chunk with structs', async () => {
    await withConnection(async (connection) => {
      const values = [
        structValue({ 'num': 10, 'str': 'xyz' }),
        structValue({ 'num': 11, 'str': 'abcdefghijkl' }),
        structValue({ 'num': 12, 'str': 'ABCDEFGHIJKLM' }),
        null,
      ];

      const chunk = DuckDBDataChunk.create(
        [STRUCT({ 'num': INTEGER, 'str': VARCHAR })],
        values.length
      );
      chunk.setColumnValues(0, values);

      await connection.run(
        'create table target(col0 struct(num integer, str varchar))'
      );
      const appender = await connection.createAppender('target');
      appender.appendDataChunk(chunk);
      appender.flushSync();

      const result = await connection.run('from target');
      const resultChunk = await result.fetchChunk();
      assert.isDefined(resultChunk);
      if (resultChunk) {
        assert.equal(resultChunk.columnCount, 1);
        assert.equal(resultChunk.rowCount, values.length);
        assertValues(resultChunk, 0, DuckDBStructVector, values);
      }
    });
  });
  test('create and append data chunk with rows', async () => {
    await withConnection(async (connection) => {
      const types: DuckDBType[] = [BOOLEAN, TINYINT, SMALLINT, INTEGER];
      const columnData: DuckDBValue[][] = [
        [false, true, null],
        [TINYINT.min, TINYINT.max, null],
        [SMALLINT.min, SMALLINT.max, null],
        [INTEGER.min, INTEGER.max, null],
      ];
      const rows: DuckDBValue[][] = [
        [false, TINYINT.min, SMALLINT.min, INTEGER.min],
        [true, TINYINT.max, SMALLINT.max, INTEGER.max],
        [null, null, null, null],
      ];

      const chunk = DuckDBDataChunk.create(types);
      chunk.setRows(rows);

      await connection.run(
        'create table target(\
          bool boolean,\
          tinyint tinyint,\
          smallint smallint,\
          int integer\
        )'
      );
      const appender = await connection.createAppender('target');
      appender.appendDataChunk(chunk);
      appender.flushSync();

      const result = await connection.run('from target');
      const resultChunk = await result.fetchChunk();
      assert.isDefined(resultChunk);
      if (resultChunk) {
        assert.strictEqual(resultChunk.columnCount, types.length);
        assert.strictEqual(resultChunk.rowCount, 3);
        assertValues(resultChunk, 0, DuckDBBooleanVector, columnData[0]);
        assertValues(resultChunk, 1, DuckDBTinyIntVector, columnData[1]);
        assertValues(resultChunk, 2, DuckDBSmallIntVector, columnData[2]);
        assertValues(resultChunk, 3, DuckDBIntegerVector, columnData[3]);
      }
    });
  });
  test('create and append data chunk with all types', async () => {
    await withConnection(async (connection) => {
      const types = createTestAllTypesColumnTypes();
      const columns = createTestAllTypesColumns();
      const columnNameAndTypeObjects =
        createTestAllTypesColumnNameAndTypeObjects();

      const chunk = DuckDBDataChunk.create(types);
      chunk.setColumns(columns);

      await connection.run(
        `create table target(${columnNameAndTypeObjects
          .map(({ name, type }) => `"${name.replace(`"`, `""`)}" ${type}`)
          .join(', ')})`
      );
      const appender = await connection.createAppender('target');
      appender.appendDataChunk(chunk);
      appender.flushSync();

      const result = await connection.run('from target');
      const resultChunk = await result.fetchChunk();
      assert.isDefined(resultChunk);
      if (resultChunk) {
        assert.strictEqual(resultChunk.columnCount, columns.length);
        assert.strictEqual(resultChunk.rowCount, 3);
        assertValues(resultChunk, 0, DuckDBBooleanVector, columns[0]);
        assertValues(resultChunk, 1, DuckDBTinyIntVector, columns[1]);
        assertValues(resultChunk, 2, DuckDBSmallIntVector, columns[2]);
        assertValues(resultChunk, 3, DuckDBIntegerVector, columns[3]);
        assertValues(resultChunk, 4, DuckDBBigIntVector, columns[4]);
        assertValues(resultChunk, 5, DuckDBHugeIntVector, columns[5]);
        assertValues(resultChunk, 6, DuckDBUHugeIntVector, columns[6]);
        assertValues(resultChunk, 7, DuckDBUTinyIntVector, columns[7]);
        assertValues(resultChunk, 8, DuckDBUSmallIntVector, columns[8]);
        assertValues(resultChunk, 9, DuckDBUIntegerVector, columns[9]);
        assertValues(resultChunk, 10, DuckDBUBigIntVector, columns[10]);
        assertValues(resultChunk, 11, DuckDBVarIntVector, columns[11]);
        assertValues(resultChunk, 12, DuckDBDateVector, columns[12]);
        assertValues(resultChunk, 13, DuckDBTimeVector, columns[13]);
        assertValues(resultChunk, 14, DuckDBTimestampVector, columns[14]);
        assertValues(
          resultChunk,
          15,
          DuckDBTimestampSecondsVector,
          columns[15]
        );
        assertValues(
          resultChunk,
          16,
          DuckDBTimestampMillisecondsVector,
          columns[16]
        );
        assertValues(
          resultChunk,
          17,
          DuckDBTimestampNanosecondsVector,
          columns[17]
        );
        assertValues(resultChunk, 18, DuckDBTimeTZVector, columns[18]);
        assertValues(resultChunk, 19, DuckDBTimestampTZVector, columns[19]);
        assertValues(resultChunk, 20, DuckDBFloatVector, columns[20]);
        assertValues(resultChunk, 21, DuckDBDoubleVector, columns[21]);
        assertValues(resultChunk, 22, DuckDBDecimal16Vector, columns[22]);
        assertValues(resultChunk, 23, DuckDBDecimal32Vector, columns[23]);
        assertValues(resultChunk, 24, DuckDBDecimal64Vector, columns[24]);
        assertValues(resultChunk, 25, DuckDBDecimal128Vector, columns[25]);
        assertValues(resultChunk, 26, DuckDBUUIDVector, columns[26]);
        assertValues(resultChunk, 27, DuckDBIntervalVector, columns[27]);
        assertValues(resultChunk, 28, DuckDBVarCharVector, columns[28]);
        assertValues(resultChunk, 29, DuckDBBlobVector, columns[29]);
        assertValues(resultChunk, 30, DuckDBBitVector, columns[30]);
        assertValues(resultChunk, 31, DuckDBEnum8Vector, columns[31]);
        assertValues(resultChunk, 32, DuckDBEnum16Vector, columns[32]);
        assertValues(resultChunk, 33, DuckDBEnum32Vector, columns[33]);
        assertValues(resultChunk, 34, DuckDBListVector, columns[34]); // int_array
        assertValues(resultChunk, 35, DuckDBListVector, columns[35]); // double_array
        assertValues(resultChunk, 36, DuckDBListVector, columns[36]); // date_array
        assertValues(resultChunk, 37, DuckDBListVector, columns[37]); // timestamp_array
        assertValues(resultChunk, 38, DuckDBListVector, columns[38]); // timestamptz_array
        assertValues(resultChunk, 39, DuckDBListVector, columns[39]); // varchar_array
        assertValues(resultChunk, 40, DuckDBListVector, columns[40]); // nested_int_array
        assertValues(resultChunk, 41, DuckDBStructVector, columns[41]);
        assertValues(resultChunk, 42, DuckDBStructVector, columns[42]); // struct_of_arrays
        assertValues(resultChunk, 43, DuckDBListVector, columns[43]); // array_of_structs
        assertValues(resultChunk, 44, DuckDBMapVector, columns[44]);
        assertValues(resultChunk, 45, DuckDBUnionVector, columns[45]);
        assertValues(resultChunk, 46, DuckDBArrayVector, columns[46]); // fixed_int_array
        assertValues(resultChunk, 47, DuckDBArrayVector, columns[47]); // fixed_varchar_array
        assertValues(resultChunk, 48, DuckDBArrayVector, columns[48]); // fixed_nested_int_array
        assertValues(resultChunk, 49, DuckDBArrayVector, columns[49]); // fixed_nested_varchar_array
        assertValues(resultChunk, 50, DuckDBArrayVector, columns[50]); // fixed_struct_array
        assertValues(resultChunk, 51, DuckDBStructVector, columns[51]); // struct_of_fixed_array
        assertValues(resultChunk, 52, DuckDBArrayVector, columns[52]); // fixed_array_of_int_list
        assertValues(resultChunk, 53, DuckDBListVector, columns[53]); // list_of_fixed_int_array
      }
    });
  });
  test('append all types row-by-row', async () => {
    await withConnection(async (connection) => {
      const types = createTestAllTypesColumnTypes();
      const columns = createTestAllTypesColumns();
      const columnNameAndTypeObjects =
        createTestAllTypesColumnNameAndTypeObjects();

      await connection.run(
        `create table target(${columnNameAndTypeObjects
          .map(({ name, type }) => `"${name.replace(`"`, `""`)}" ${type}`)
          .join(', ')})`
      );

      const appender = await connection.createAppender('target');
      for (let rowIndex = 0; rowIndex < columns[0].length; rowIndex++) {
        for (let columnIndex = 0; columnIndex < columns.length; columnIndex++) {
          const type = types[columnIndex];
          const value = columns[columnIndex][rowIndex];
          if (value === null) {
            appender.appendNull();
          } else {
            appender.appendValue(value, type);
          }
        }
        appender.endRow();
      }
      appender.flushSync();

      const result = await connection.run('from target');
      const resultChunk = await result.fetchChunk();
      assert.isDefined(resultChunk);
      if (resultChunk) {
        assert.strictEqual(resultChunk.columnCount, columns.length);
        assert.strictEqual(resultChunk.rowCount, 3);
        assertValues(resultChunk, 0, DuckDBBooleanVector, columns[0]);
        assertValues(resultChunk, 1, DuckDBTinyIntVector, columns[1]);
        assertValues(resultChunk, 2, DuckDBSmallIntVector, columns[2]);
        assertValues(resultChunk, 3, DuckDBIntegerVector, columns[3]);
        assertValues(resultChunk, 4, DuckDBBigIntVector, columns[4]);
        assertValues(resultChunk, 5, DuckDBHugeIntVector, columns[5]);
        assertValues(resultChunk, 6, DuckDBUHugeIntVector, columns[6]);
        assertValues(resultChunk, 7, DuckDBUTinyIntVector, columns[7]);
        assertValues(resultChunk, 8, DuckDBUSmallIntVector, columns[8]);
        assertValues(resultChunk, 9, DuckDBUIntegerVector, columns[9]);
        assertValues(resultChunk, 10, DuckDBUBigIntVector, columns[10]);
        assertValues(resultChunk, 11, DuckDBVarIntVector, columns[11]);
        assertValues(resultChunk, 12, DuckDBDateVector, columns[12]);
        assertValues(resultChunk, 13, DuckDBTimeVector, columns[13]);
        assertValues(resultChunk, 14, DuckDBTimestampVector, columns[14]);
        assertValues(
          resultChunk,
          15,
          DuckDBTimestampSecondsVector,
          columns[15]
        );
        assertValues(
          resultChunk,
          16,
          DuckDBTimestampMillisecondsVector,
          columns[16]
        );
        assertValues(
          resultChunk,
          17,
          DuckDBTimestampNanosecondsVector,
          columns[17]
        );
        assertValues(resultChunk, 18, DuckDBTimeTZVector, columns[18]);
        assertValues(resultChunk, 19, DuckDBTimestampTZVector, columns[19]);
        assertValues(resultChunk, 20, DuckDBFloatVector, columns[20]);
        assertValues(resultChunk, 21, DuckDBDoubleVector, columns[21]);
        assertValues(resultChunk, 22, DuckDBDecimal16Vector, columns[22]);
        assertValues(resultChunk, 23, DuckDBDecimal32Vector, columns[23]);
        assertValues(resultChunk, 24, DuckDBDecimal64Vector, columns[24]);
        assertValues(resultChunk, 25, DuckDBDecimal128Vector, columns[25]);
        assertValues(resultChunk, 26, DuckDBUUIDVector, columns[26]);
        assertValues(resultChunk, 27, DuckDBIntervalVector, columns[27]);
        assertValues(resultChunk, 28, DuckDBVarCharVector, columns[28]);
        assertValues(resultChunk, 29, DuckDBBlobVector, columns[29]);
        assertValues(resultChunk, 30, DuckDBBitVector, columns[30]);
        assertValues(resultChunk, 31, DuckDBEnum8Vector, columns[31]);
        assertValues(resultChunk, 32, DuckDBEnum16Vector, columns[32]);
        assertValues(resultChunk, 33, DuckDBEnum32Vector, columns[33]);
        assertValues(resultChunk, 34, DuckDBListVector, columns[34]); // int_array
        assertValues(resultChunk, 35, DuckDBListVector, columns[35]); // double_array
        assertValues(resultChunk, 36, DuckDBListVector, columns[36]); // date_array
        assertValues(resultChunk, 37, DuckDBListVector, columns[37]); // timestamp_array
        assertValues(resultChunk, 38, DuckDBListVector, columns[38]); // timestamptz_array
        assertValues(resultChunk, 39, DuckDBListVector, columns[39]); // varchar_array
        assertValues(resultChunk, 40, DuckDBListVector, columns[40]); // nested_int_array
        assertValues(resultChunk, 41, DuckDBStructVector, columns[41]);
        assertValues(resultChunk, 42, DuckDBStructVector, columns[42]); // struct_of_arrays
        assertValues(resultChunk, 43, DuckDBListVector, columns[43]); // array_of_structs
        assertValues(resultChunk, 44, DuckDBMapVector, columns[44]);
        assertValues(resultChunk, 45, DuckDBUnionVector, columns[45]);
        assertValues(resultChunk, 46, DuckDBArrayVector, columns[46]); // fixed_int_array
        assertValues(resultChunk, 47, DuckDBArrayVector, columns[47]); // fixed_varchar_array
        assertValues(resultChunk, 48, DuckDBArrayVector, columns[48]); // fixed_nested_int_array
        assertValues(resultChunk, 49, DuckDBArrayVector, columns[49]); // fixed_nested_varchar_array
        assertValues(resultChunk, 50, DuckDBArrayVector, columns[50]); // fixed_struct_array
        assertValues(resultChunk, 51, DuckDBStructVector, columns[51]); // struct_of_fixed_array
        assertValues(resultChunk, 52, DuckDBArrayVector, columns[52]); // fixed_array_of_int_list
        assertValues(resultChunk, 53, DuckDBListVector, columns[53]); // list_of_fixed_int_array
      }
    });
  });
});
