import { assert, describe, test } from 'vitest';
import {
  DuckDBArrayType,
  DuckDBArrayVector,
  DuckDBBigIntType,
  DuckDBBigIntVector,
  DuckDBBitType,
  DuckDBBitValue,
  DuckDBBitVector,
  DuckDBBlobType,
  DuckDBBlobVector,
  DuckDBBooleanType,
  DuckDBBooleanVector,
  DuckDBConnection,
  DuckDBDataChunk,
  DuckDBDateType,
  DuckDBDateVector,
  DuckDBDecimal16Vector,
  DuckDBDecimal2Vector,
  DuckDBDecimal4Vector,
  DuckDBDecimal8Vector,
  DuckDBDecimalType,
  DuckDBDoubleType,
  DuckDBDoubleVector,
  DuckDBEnum1Vector,
  DuckDBEnum2Vector,
  DuckDBEnum4Vector,
  DuckDBEnumType,
  DuckDBFloatType,
  DuckDBFloatVector,
  DuckDBHugeIntType,
  DuckDBHugeIntVector,
  DuckDBInstance,
  DuckDBIntegerType,
  DuckDBIntegerVector,
  DuckDBInterval,
  DuckDBIntervalType,
  DuckDBIntervalVector,
  DuckDBLargeDecimal,
  DuckDBListType,
  DuckDBListVector,
  DuckDBMapEntry,
  DuckDBMapType,
  DuckDBMapVector,
  DuckDBPendingResultState,
  DuckDBResult,
  DuckDBSmallDecimal,
  DuckDBSmallIntType,
  DuckDBSmallIntVector,
  DuckDBStructEntry,
  DuckDBStructType,
  DuckDBStructVector,
  DuckDBTimeTZType,
  DuckDBTimeTZValue,
  DuckDBTimeTZVector,
  DuckDBTimeType,
  DuckDBTimeVector,
  DuckDBTimestampMillisecondsType,
  DuckDBTimestampMillisecondsVector,
  DuckDBTimestampNanosecondsType,
  DuckDBTimestampNanosecondsVector,
  DuckDBTimestampSecondsType,
  DuckDBTimestampSecondsVector,
  DuckDBTimestampTZType,
  DuckDBTimestampType,
  DuckDBTimestampVector,
  DuckDBTinyIntType,
  DuckDBTinyIntVector,
  DuckDBType,
  DuckDBTypeId,
  DuckDBUBigIntType,
  DuckDBUBigIntVector,
  DuckDBUHugeIntType,
  DuckDBUHugeIntVector,
  DuckDBUIntegerType,
  DuckDBUIntegerVector,
  DuckDBUSmallIntType,
  DuckDBUSmallIntVector,
  DuckDBUTinyIntType,
  DuckDBUTinyIntVector,
  DuckDBUUIDType,
  DuckDBUUIDVector,
  DuckDBUnionType,
  DuckDBUnionVector,
  DuckDBVarCharType,
  DuckDBVarCharVector,
  DuckDBVector,
  configurationOptionDescriptions,
  version
} from '../src';

const N_2_7 = 2 ** 7;
const N_2_8 = 2 ** 8;
const N_2_15 = 2 ** 15;
const N_2_16 = 2 ** 16;
const N_2_31 = 2 ** 31;
const N_2_32 = 2 ** 32;

const BI_0 = BigInt(0);
const BI_1 = BigInt(1);
// const BI_2 = BigInt(2);
const BI_24 = BigInt(24);
const BI_60 = BigInt(60);
const BI_1000 = BigInt(1000);
const BI_2_63 = BI_1 << BigInt(63);
const BI_2_64 = BI_1 << BigInt(64);
const BI_2_127 = BI_1 << BigInt(127);
const BI_2_128 = BI_1 << BigInt(128);
const BI_10_8 = BigInt(100000000);
const BI_10_10 = BigInt(10000000000);
const BI_18_9s = BI_10_8 * BI_10_10 - BI_1;
const BI_38_9s = BI_10_8 * BI_10_10 * BI_10_10 * BI_10_10 - BI_1;

const MinInt8 = -N_2_7;
const MaxInt8 = N_2_7 - 1;
const MinUInt8 = 0;
const MaxUInt8 = N_2_8 - 1;
const MinInt16 = -N_2_15;
const MaxInt16 = N_2_15 - 1;
const MinUInt16 = 0;
const MaxUInt16 = N_2_16 - 1;
const MinInt32 = -N_2_31;
const MaxInt32 = N_2_31 - 1;
const MinUInt32 = 0;
const MaxUInt32 = N_2_32 - 1;
const MinInt64 = -BI_2_63;
const MaxInt64 = BI_2_63 - BI_1;
const MinUInt64 = BI_0;
const MaxUInt64 = BI_2_64 - BI_1;
const MinInt128 = -BI_2_127;
const MaxInt128 = BI_2_127 - BI_1;
const MaxUInt128 = BI_2_128 - BI_1;
const MinHugeInt = MinInt128;
const MinUHugeInt = BI_0;
const MinDate = MinInt32 + 2;
const MaxDate = MaxInt32 - 1;
const DateInf = MaxInt32;
const MinTime = BI_0;
const MaxTime = BI_24 * BI_60 * BI_60 * BI_1000 * BI_1000; // 86400000000
const MinTimeTZMicroseconds = 0;
const MaxTimeTZMicroseconds = 24 * 60 * 60 * 1000 * 1000; // 86400000000
const MaxTimeTZOffset = 16 * 60 * 60 - 1; // from dtime_tz_t (MAX_OFFSET)
const MinTimeTZOffset = -MaxTimeTZOffset;
const MinTimeTZ = new DuckDBTimeTZValue(MinTimeTZMicroseconds, MaxTimeTZOffset);
const MaxTimeTZ = new DuckDBTimeTZValue(MaxTimeTZMicroseconds, MinTimeTZOffset);
const MinTS_S = BigInt(-9223372022400); // from test_all_types() select epoch(timestamp_s)::bigint;
const MaxTS_S = BigInt(9223372036854);
const MinTS_MS = MinTS_S * BI_1000;
const MaxTS_MS = (MaxInt64 - BI_1) / BI_1000;
const MinTS_US = MinTS_MS * BI_1000;
const MaxTS_US = MaxInt64 - BI_1;
const TS_US_Inf = MaxInt64;
const MinTS_NS = MinInt64;
const MaxTS_NS = MaxInt64 - BI_1;
const MinFloat32 = Math.fround(-3.4028235e+38);
const MaxFloat32 = Math.fround(3.4028235e+38);
const MinFloat64 = -Number.MAX_VALUE;
const MaxFloat64 = Number.MAX_VALUE;
const MinUUID = MinInt128;
const MaxUUID = MaxInt128;

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function withConnection(fn: (connection: DuckDBConnection) => Promise<void>) {
  const instance = await DuckDBInstance.create();
  try {
    const connection = await instance.connect();
    try {
      await fn(connection);
    } finally {
      connection.dispose();
    }
  } finally {
    instance.dispose();
  }
}

interface ExpectedColumn {
  readonly name: string;
  readonly type: DuckDBType;
}

function assertColumns(result: DuckDBResult, expectedColumns: readonly ExpectedColumn[]) {
  assert.strictEqual(result.columnCount, expectedColumns.length, 'column count');
  for (let i = 0; i < expectedColumns.length; i++) {
    const { name, type } = expectedColumns[i];
    assert.strictEqual(result.columnName(i), name, 'column name');
    assert.strictEqual(result.columnTypeId(i), type.typeId, `column type id (column: ${name})`);
    assert.deepStrictEqual(result.columnType(i), type, `column type (column: ${name})`);
  }
}

function isVectorType<TValue, TVector extends DuckDBVector<TValue>>(
  vector: DuckDBVector<any> | null,
  vectorType: new (...args: any[]) => TVector,
): vector is TVector {
  return vector instanceof vectorType;
}

function getColumnVector<TValue, TVector extends DuckDBVector<TValue>>(
  chunk: DuckDBDataChunk,
  columnIndex: number,
  vectorType: new (...args: any[]) => TVector
): TVector {
  const column = chunk.getColumn(columnIndex);
  if (!isVectorType<TValue, TVector>(column, vectorType)) {
    assert.fail(`expected column ${columnIndex} to be a ${vectorType}`);
  }
  return column;
}

function assertVectorValues<T>(vector: DuckDBVector<T> | null, values: readonly T[], vectorName: string) {
  if (!vector) {
    assert.fail(`${vectorName} unexpectedly null`);
  }
  assert.strictEqual(vector.itemCount, values.length,
      `expected vector ${vectorName} item count to be ${values.length} but found ${vector.itemCount}`);
  for (let i = 0; i < values.length; i++) {
    const actual: T | null = vector.getItem(i);
    const expected = values[i];
    assert.deepStrictEqual(actual, expected,
      `expected vector ${vectorName}[${i}] to be ${expected} but found ${actual}`);
  }
}

function assertNestedVectorValues<T>(
  vector: DuckDBVector<T> | null,
  checkVectorValueFns: ((value: T | null, valueName: string) => void)[],
  vectorName: string,
) {
  if (!vector) {
    assert.fail(`${vectorName} unexpectedly null`);
  }
  assert.strictEqual(vector.itemCount, checkVectorValueFns.length,
      `expected vector ${vectorName} item count to be ${checkVectorValueFns.length} but found ${vector.itemCount}`);
  for (let i = 0; i < vector.itemCount; i++) {
    checkVectorValueFns[i](vector.getItem(i), `${vectorName}[${i}]`)
  }
}

function assertValues<TValue, TVector extends DuckDBVector<TValue>>(
  chunk: DuckDBDataChunk,
  columnIndex: number,
  vectorType: new (...args: any[]) => TVector,
  values: readonly (TValue | null)[],
) {
  const vector = getColumnVector(chunk, columnIndex, vectorType);
  assertVectorValues(vector, values, `${columnIndex}`);
}

function assertNestedValues<TValue, TVector extends DuckDBVector<TValue>>(
  chunk: DuckDBDataChunk,
  columnIndex: number,
  vectorType: new (...args: any[]) => TVector,
  checkVectorValueFns: ((value: TValue | null, valueName: string) => void)[],
) {
  const vector = getColumnVector(chunk, columnIndex, vectorType);
  assertNestedVectorValues(vector, checkVectorValueFns, `col${columnIndex}`);
}

const textEncoder = new TextEncoder();
function blobFromString(str: string): Uint8Array {
  return Buffer.from(textEncoder.encode(str));
}

function bigints(start: bigint, end: bigint) {
  return Array.from({ length: Number(end - start) + 1 }).map((_, i) => start + BigInt(i));
}

describe('api', () => {
  test('should expose version', () => {
    const ver = version();
    assert.ok(ver.startsWith('v'), `version starts with 'v'`);
  });
  test('should expose configuration option descriptions', () => {
    const descriptions = configurationOptionDescriptions();
    assert.ok(descriptions['memory_limit'], `descriptions has 'memory_limit'`);
  });
  test('should support creating, connecting, running a basic query, and reading results', async () => {
    const instance = await DuckDBInstance.create();
    const connection = await instance.connect();
    const result = await connection.run('select 42 as num');
    try {
      assertColumns(result, [{ name: 'num', type: DuckDBIntegerType.instance }]);
      const chunk = await result.fetchChunk();
      try {
        assert.strictEqual(chunk.columnCount, 1);
        assert.strictEqual(chunk.rowCount, 1);
        assertValues(chunk, 0, DuckDBIntegerVector, [42]);
      } finally {
        chunk.dispose();
      }
    } finally {
      result.dispose();
      connection.dispose();
      instance.dispose();
    }
  });
  test('should support running prepared statements', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare('select $num as a, $str as b, $bool as c, $null as d');
      try {
        assert.strictEqual(prepared.parameterCount, 4);
        assert.strictEqual(prepared.parameterName(1), 'num');
        assert.strictEqual(prepared.parameterName(2), 'str');
        assert.strictEqual(prepared.parameterName(3), 'bool');
        assert.strictEqual(prepared.parameterName(4), 'null');
        prepared.bindInteger(1, 10);
        prepared.bindVarchar(2, 'abc');
        prepared.bindBoolean(3, true);
        prepared.bindNull(4);
        const result = await prepared.run();
        try {
          assertColumns(result, [
            { name: 'a', type: DuckDBIntegerType.instance },
            { name: 'b', type: DuckDBVarCharType.instance },
            { name: 'c', type: DuckDBBooleanType.instance },
            { name: 'd', type: DuckDBIntegerType.instance },
          ]);
          const chunk = await result.fetchChunk();
          try {
            assert.strictEqual(chunk.columnCount, 4);
            assert.strictEqual(chunk.rowCount, 1);
            assertValues(chunk, 0, DuckDBIntegerVector, [10]);
            assertValues(chunk, 1, DuckDBVarCharVector, ['abc']);
            assertValues(chunk, 2, DuckDBBooleanVector, [true]);
            assertValues<number, DuckDBIntegerVector>(chunk, 3, DuckDBIntegerVector, [null]);
          } finally {
            chunk.dispose();
          }
        } finally {
          result.dispose();
        }
      } finally {
        prepared.dispose();
      }
    });
  });
  test('should support starting prepared statements and running them incrementally', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare('select int from test_all_types()');
      try {
        const pending = prepared.start();
        try {
          let taskCount = 0;
          while (pending.runTask() !== DuckDBPendingResultState.RESULT_READY) {
            taskCount++;
            if (taskCount > 100) { // arbitrary upper bound on the number of tasks expected for this simple query
              assert.fail('Unexpectedly large number of tasks');
            }
            await sleep(1);
          }
          // console.debug('task count: ', taskCount);
          const result = await pending.getResult();
          try {
            assertColumns(result, [
              { name: 'int', type: DuckDBIntegerType.instance },
            ]);
            const chunk = await result.fetchChunk();
            try {
              assert.strictEqual(chunk.columnCount, 1);
              assert.strictEqual(chunk.rowCount, 3);
              assertValues(chunk, 0, DuckDBIntegerVector, [MinInt32, MaxInt32, null]);
            } finally {
              chunk.dispose();
            }
          } finally {
            result.dispose();
          }
        } finally {
          pending.dispose();
        }
      } finally {
        prepared.dispose();
      }
    });
  });
  test.skip('should support streaming results from prepared statements', async () => {
    await withConnection(async (connection) => {
      const prepared = await connection.prepare('from range(10000)');
      try {
        const pending = prepared.start();
        try {
          const result = await pending.getResult();
          try {
            assertColumns(result, [
              { name: 'range', type: DuckDBBigIntType.instance },
            ]);
            const chunks: DuckDBDataChunk[] = [];
            let currentChunk: DuckDBDataChunk | null = null;
            try {
              currentChunk = await result.fetchChunk();
              while (currentChunk.rowCount > 0) {
                chunks.push(currentChunk);
                currentChunk = await result.fetchChunk();
              }
              currentChunk.dispose(); // this is the empty chunk that signifies the end of the stream
              currentChunk = null;
              assert.strictEqual(chunks.length, 5); // ceil(10000 / 2048) = 5
              assertValues(chunks[0], 0, DuckDBBigIntVector, bigints(BigInt(0), BigInt(2048-1)));
              assertValues(chunks[1], 0, DuckDBBigIntVector, bigints(BigInt(2048), BigInt(2048*2-1)));
              assertValues(chunks[2], 0, DuckDBBigIntVector, bigints(BigInt(2048*2), BigInt(2048*3-1)));
              assertValues(chunks[3], 0, DuckDBBigIntVector, bigints(BigInt(2048*3), BigInt(2048*4-1)));
              assertValues(chunks[4], 0, DuckDBBigIntVector, bigints(BigInt(2048*4), BigInt(9999)));
            } finally {
              if (currentChunk) {
                currentChunk.dispose();
              }
              for (const chunk of chunks) {
                chunk.dispose();
              }
            }
          } finally {
            result.dispose();
          }
        } finally {
          pending.dispose();
        }
      } finally {
        prepared.dispose();
      }
    });
  });
  test('should support all data types', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run('from test_all_types(use_large_enum=true)');
      try {
        const smallEnumValues = ['DUCK_DUCK_ENUM', 'GOOSE'];
        const mediumEnumValues = Array.from({ length: 300 }).map((_, i) => `enum_${i}`);
        const largeEnumValues = Array.from({ length: 70000 }).map((_, i) => `enum_${i}`);
        assertColumns(result, [
          { name: 'bool', type: DuckDBBooleanType.instance },
          { name: 'tinyint', type: DuckDBTinyIntType.instance },
          { name: 'smallint', type: DuckDBSmallIntType.instance },
          { name: 'int', type: DuckDBIntegerType.instance },
          { name: 'bigint', type: DuckDBBigIntType.instance },
          { name: 'hugeint', type: DuckDBHugeIntType.instance },
          { name: 'uhugeint', type: DuckDBUHugeIntType.instance },
          { name: 'utinyint', type: DuckDBUTinyIntType.instance },
          { name: 'usmallint', type: DuckDBUSmallIntType.instance },
          { name: 'uint', type: DuckDBUIntegerType.instance },
          { name: 'ubigint', type: DuckDBUBigIntType.instance },
          { name: 'date', type: DuckDBDateType.instance },
          { name: 'time', type: DuckDBTimeType.instance },
          { name: 'timestamp', type: DuckDBTimestampType.instance },
          { name: 'timestamp_s', type: DuckDBTimestampSecondsType.instance },
          { name: 'timestamp_ms', type: DuckDBTimestampMillisecondsType.instance },
          { name: 'timestamp_ns', type: DuckDBTimestampNanosecondsType.instance },
          { name: 'time_tz', type: DuckDBTimeTZType.instance },
          { name: 'timestamp_tz', type: DuckDBTimestampTZType.instance },
          { name: 'float', type: DuckDBFloatType.instance },
          { name: 'double', type: DuckDBDoubleType.instance },
          { name: 'dec_4_1', type: new DuckDBDecimalType(4, 1) },
          { name: 'dec_9_4', type: new DuckDBDecimalType(9, 4) },
          { name: 'dec_18_6', type: new DuckDBDecimalType(18, 6) },
          { name: 'dec38_10', type: new DuckDBDecimalType(38, 10) },
          { name: 'uuid', type: DuckDBUUIDType.instance },
          { name: 'interval', type: DuckDBIntervalType.instance },
          { name: 'varchar', type: DuckDBVarCharType.instance },
          { name: 'blob', type: DuckDBBlobType.instance },
          { name: 'bit', type: DuckDBBitType.instance },
          { name: 'small_enum', type: new DuckDBEnumType(smallEnumValues, DuckDBTypeId.UTINYINT) },
          { name: 'medium_enum', type: new DuckDBEnumType(mediumEnumValues, DuckDBTypeId.USMALLINT) },
          { name: 'large_enum', type: new DuckDBEnumType(largeEnumValues, DuckDBTypeId.UINTEGER) },
          { name: 'int_array', type: new DuckDBListType(DuckDBIntegerType.instance) },
          { name: 'double_array', type: new DuckDBListType(DuckDBDoubleType.instance) },
          { name: 'date_array', type: new DuckDBListType(DuckDBDateType.instance) },
          { name: 'timestamp_array', type: new DuckDBListType(DuckDBTimestampType.instance) },
          { name: 'timestamptz_array', type: new DuckDBListType(DuckDBTimestampTZType.instance) },
          { name: 'varchar_array', type: new DuckDBListType(DuckDBVarCharType.instance) },
          { name: 'nested_int_array', type: new DuckDBListType(new DuckDBListType(DuckDBIntegerType.instance)) },
          { name: 'struct', type: new DuckDBStructType([
            { name: 'a', valueType: DuckDBIntegerType.instance },
            { name: 'b', valueType: DuckDBVarCharType.instance },
          ])},
          { name: 'struct_of_arrays', type: new DuckDBStructType([
            { name: 'a', valueType: new DuckDBListType(DuckDBIntegerType.instance) },
            { name: 'b', valueType: new DuckDBListType(DuckDBVarCharType.instance) },
          ])},
          { name: 'array_of_structs', type: new DuckDBListType(new DuckDBStructType([
            { name: 'a', valueType: DuckDBIntegerType.instance },
            { name: 'b', valueType: DuckDBVarCharType.instance },
          ]))},
          { name: 'map', type: new DuckDBMapType(DuckDBVarCharType.instance, DuckDBVarCharType.instance) },
          { name: 'union', type: new DuckDBUnionType([
            { tag: 'name', valueType: DuckDBVarCharType.instance },
            { tag: 'age', valueType: DuckDBSmallIntType.instance },
          ])},
          { name: 'fixed_int_array', type: new DuckDBArrayType(DuckDBIntegerType.instance, 3) },
          { name: 'fixed_varchar_array', type: new DuckDBArrayType(DuckDBVarCharType.instance, 3) },
          { name: 'fixed_nested_int_array', type: new DuckDBArrayType(new DuckDBArrayType(DuckDBIntegerType.instance, 3), 3) },
          { name: 'fixed_nested_varchar_array', type: new DuckDBArrayType(new DuckDBArrayType(DuckDBVarCharType.instance, 3), 3) },
          { name: 'fixed_struct_array', type: new DuckDBArrayType(new DuckDBStructType([
            { name: 'a', valueType: DuckDBIntegerType.instance },
            { name: 'b', valueType: DuckDBVarCharType.instance },
          ]), 3) },
          { name: 'struct_of_fixed_array', type: new DuckDBStructType([
            { name: 'a', valueType: new DuckDBArrayType(DuckDBIntegerType.instance, 3) },
            { name: 'b', valueType: new DuckDBArrayType(DuckDBVarCharType.instance, 3) },
          ]) },
          { name: 'fixed_array_of_int_list', type: new DuckDBArrayType(new DuckDBListType(DuckDBIntegerType.instance), 3) },
          { name: 'list_of_fixed_int_array', type: new DuckDBListType(new DuckDBArrayType(DuckDBIntegerType.instance, 3)) },
        ]);

        const chunk = await result.fetchChunk();
        try {
          assert.strictEqual(chunk.columnCount, 53);
          assert.strictEqual(chunk.rowCount, 3);

          assertValues(chunk, 0, DuckDBBooleanVector, [false, true, null]);
          assertValues(chunk, 1, DuckDBTinyIntVector, [MinInt8, MaxInt8, null]);
          assertValues(chunk, 2, DuckDBSmallIntVector, [MinInt16, MaxInt16, null]);
          assertValues(chunk, 3, DuckDBIntegerVector, [MinInt32, MaxInt32, null]);
          assertValues(chunk, 4, DuckDBBigIntVector, [MinInt64, MaxInt64, null]);
          assertValues(chunk, 5, DuckDBHugeIntVector, [MinHugeInt, MaxInt128, null]);
          assertValues(chunk, 6, DuckDBUHugeIntVector, [MinUHugeInt, MaxUInt128, null]);
          assertValues(chunk, 7, DuckDBUTinyIntVector, [MinUInt8, MaxUInt8, null]);
          assertValues(chunk, 8, DuckDBUSmallIntVector, [MinUInt16, MaxUInt16, null]);
          assertValues(chunk, 9, DuckDBUIntegerVector, [MinUInt32, MaxUInt32, null]);
          assertValues(chunk, 10, DuckDBUBigIntVector, [MinUInt64, MaxUInt64, null]);
          assertValues(chunk, 11, DuckDBDateVector, [MinDate, MaxDate, null]);
          assertValues(chunk, 12, DuckDBTimeVector, [MinTime, MaxTime, null]);
          assertValues(chunk, 13, DuckDBTimestampVector, [MinTS_US, MaxTS_US, null]);
          assertValues(chunk, 14, DuckDBTimestampSecondsVector, [MinTS_S, MaxTS_S, null]);
          assertValues(chunk, 15, DuckDBTimestampMillisecondsVector, [MinTS_MS, MaxTS_MS, null]);
          assertValues(chunk, 16, DuckDBTimestampNanosecondsVector, [MinTS_NS, MaxTS_NS, null]);
          assertValues(chunk, 17, DuckDBTimeTZVector, [MinTimeTZ, MaxTimeTZ, null]);
          assertValues(chunk, 18, DuckDBTimestampVector, [MinTS_US, MaxTS_US, null]);
          assertValues(chunk, 19, DuckDBFloatVector, [MinFloat32, MaxFloat32, null]);
          assertValues(chunk, 20, DuckDBDoubleVector, [MinFloat64, MaxFloat64, null]);
          assertValues(chunk, 21, DuckDBDecimal2Vector, [
            new DuckDBSmallDecimal(-9999, new DuckDBDecimalType(4, 1)),
            new DuckDBSmallDecimal(9999, new DuckDBDecimalType(4, 1)),
            null,
          ]);
          assertValues(chunk, 22, DuckDBDecimal4Vector, [
            new DuckDBSmallDecimal(-999999999, new DuckDBDecimalType(9, 4)),
            new DuckDBSmallDecimal(999999999, new DuckDBDecimalType(9, 4)),
            null,
          ]);
          assertValues(chunk, 23, DuckDBDecimal8Vector, [
            new DuckDBLargeDecimal(-BI_18_9s, new DuckDBDecimalType(18, 6)),
            new DuckDBLargeDecimal(BI_18_9s, new DuckDBDecimalType(18, 6)),
            null,
          ]);
          assertValues(chunk, 24, DuckDBDecimal16Vector, [
            new DuckDBLargeDecimal(-BI_38_9s, new DuckDBDecimalType(38, 10)),
            new DuckDBLargeDecimal(BI_38_9s, new DuckDBDecimalType(38, 10)),
            null,
          ]);
          assertValues(chunk, 25, DuckDBUUIDVector, [MinUUID, MaxUUID, null]);
          assertValues(chunk, 26, DuckDBIntervalVector, [
            new DuckDBInterval(0, 0, BigInt(0)),
            new DuckDBInterval(999, 999, BigInt(999999999)),
            null,
          ]);
          assertValues(chunk, 27, DuckDBVarCharVector, ['🦆🦆🦆🦆🦆🦆', 'goo\0se', null]);
          assertValues(chunk, 28, DuckDBBlobVector, [
            blobFromString('thisisalongblob\x00withnullbytes'),
            blobFromString('\x00\x00\x00a'),
            null,
          ]);
          assertValues(chunk, 29, DuckDBBitVector, [
            DuckDBBitValue.fromString('0010001001011100010101011010111'),
            DuckDBBitValue.fromString('10101'),
            null,
          ]);
          assertValues(chunk, 30, DuckDBEnum1Vector, [
            smallEnumValues[0],
            smallEnumValues[smallEnumValues.length - 1],
            null,
          ]);
          assertValues(chunk, 31, DuckDBEnum2Vector, [
            mediumEnumValues[0],
            mediumEnumValues[mediumEnumValues.length - 1],
            null,
          ]);
          assertValues(chunk, 32, DuckDBEnum4Vector, [
            largeEnumValues[0],
            largeEnumValues[largeEnumValues.length - 1],
            null,
          ]);
          // int_array
          assertNestedValues<DuckDBVector<number>, DuckDBListVector<number>>(chunk, 33, DuckDBListVector, [
            (v, n) => assertVectorValues(v, [], n),
            (v, n) => assertVectorValues(v, [42, 999, null, null, -42], n),
            (v, n) => assert.strictEqual(v, null, n),
          ]);
          // double_array
          assertNestedValues<DuckDBVector<number>, DuckDBListVector<number>>(chunk, 34, DuckDBListVector, [
            (v, n) => assertVectorValues(v, [], n),
            (v, n) => assertVectorValues(v, [42.0, NaN, Infinity, -Infinity, null, -42.0], n),
            (v, n) => assert.strictEqual(v, null, n),
          ]);
          // date_array
          assertNestedValues<DuckDBVector<number>, DuckDBListVector<number>>(chunk, 35, DuckDBListVector, [
            (v, n) => assertVectorValues(v, [], n),
            (v, n) => assertVectorValues(v, [0, DateInf, -DateInf, null, 19124], n),
            (v, n) => assert.strictEqual(v, null, n),
          ]);
          // timestamp_array
          assertNestedValues<DuckDBVector<bigint>, DuckDBListVector<bigint>>(chunk, 36, DuckDBListVector, [
            (v, n) => assertVectorValues(v, [], n),
            // 1652372625 is 2022-05-12 16:23:45
            (v, n) => assertVectorValues(v, [BI_0, TS_US_Inf, -TS_US_Inf, null, BigInt(1652372625)*BI_1000*BI_1000], n),
            (v, n) => assert.strictEqual(v, null, n),
          ]);
          // timestamptz_array
          assertNestedValues<DuckDBVector<bigint>, DuckDBListVector<bigint>>(chunk, 37, DuckDBListVector, [
            (v, n) => assertVectorValues(v, [], n),
            // 1652397825 = 1652372625 + 25200, 25200 = 7 * 60 * 60 = 7 hours in seconds
            // This 7 hour difference is hard coded into test_all_types (value is 2022-05-12 16:23:45-07)
            (v, n) => assertVectorValues(v, [BI_0, TS_US_Inf, -TS_US_Inf, null, BigInt(1652397825)*BI_1000*BI_1000], n),
            (v, n) => assert.strictEqual(v, null, n),
          ]);
          // varchar_array
          assertNestedValues<DuckDBVector<string>, DuckDBListVector<string>>(chunk, 38, DuckDBListVector, [
            (v, n) => assertVectorValues(v, [], n),
            // Note that the string 'goose' in varchar_array does NOT have an embedded null character.
            (v, n) => assertVectorValues(v, ['🦆🦆🦆🦆🦆🦆', 'goose', null, ''], n),
            (v, n) => assert.strictEqual(v, null, n),
          ]);
          // nested_int_array
          assertNestedValues<DuckDBVector<DuckDBVector<number>>, DuckDBListVector<DuckDBVector<number>>>(chunk, 39, DuckDBListVector, [
            (v, n) => {
              assert.ok(v, `${n} unexpectedly null`);
              if (!v) return;
              assert.strictEqual(v.itemCount, 0, `${n} not empty`);
            },
            (v, n) => assertNestedVectorValues(v, [
              (vv, nn) => assertVectorValues(vv, [], nn),
              (vv, nn) => assertVectorValues(vv, [42, 999, null, null, -42], nn),
              (vv, nn) => assert.strictEqual(vv, null, nn),
              (vv, nn) => assertVectorValues(vv, [], nn),
              (vv, nn) => assertVectorValues(vv, [42, 999, null, null, -42], nn),
            ], n),
            (v, n) => assert.strictEqual(v, null, n),
          ]);
          assertValues(chunk, 40, DuckDBStructVector, [
            [{ name: 'a', value: null }, { name: 'b', value: null }],
            [{ name: 'a', value: 42 }, { name: 'b', value: '🦆🦆🦆🦆🦆🦆' }],
            null,
          ] as (readonly DuckDBStructEntry[])[]);
          // struct_of_arrays
          assertNestedValues<readonly DuckDBStructEntry[], DuckDBStructVector>(chunk, 41, DuckDBStructVector, [
            (entries, n) => assert.deepStrictEqual(entries, [{ name: 'a', value: null }, { name: 'b', value: null }], n),
            (entries, n) => {
              assert.ok(entries, `${n} unexpectedly null`);
              if (!entries) return;
              assert.strictEqual(entries.length, 2, n);
              assert.strictEqual(entries[0].name, 'a', n);
              assert.ok(isVectorType(entries[0].value, DuckDBIntegerVector));
              assertVectorValues(entries[0].value, [42, 999, null, null, -42], n);
              assert.strictEqual(entries[1].name, 'b', n);
              assert.ok(isVectorType(entries[1].value, DuckDBVarCharVector));
              assertVectorValues(entries[1].value, ['🦆🦆🦆🦆🦆🦆', 'goose', null, ''], n);
            },
            (entries, n) => assert.strictEqual(entries, null, n),
          ]);
          // array_of_structs
          assertNestedValues<DuckDBVector<readonly DuckDBStructEntry[]>, DuckDBListVector<readonly DuckDBStructEntry[]>>(chunk, 42, DuckDBListVector, [
            (v, n) => assertVectorValues(v, [], n),
            (v, n) => assertVectorValues(v, [
              [{ name: 'a', value: null }, { name: 'b', value: null }],
              [{ name: 'a', value: 42 }, { name: 'b', value: '🦆🦆🦆🦆🦆🦆' }],
              null,
            ], n),
            (v, n) => assert.strictEqual(v, null, n),
          ]);
          assertValues(chunk, 43, DuckDBMapVector, [
            [],
            [{ key: 'key1', value: '🦆🦆🦆🦆🦆🦆' }, { key: 'key2', value: 'goose' }],
            null,
          ] as (readonly DuckDBMapEntry[])[]);
          assertValues(chunk, 44, DuckDBUnionVector, [
            { tag: 'name', value: 'Frank' },
            { tag: 'age', value: 5 },
            null,
          ]);
          // fixed_int_array
          assertNestedValues<DuckDBVector<number>, DuckDBArrayVector>(chunk, 45, DuckDBArrayVector, [
            (v, n) => assertVectorValues(v, [null, 2, 3], n),
            (v, n) => assertVectorValues(v, [4, 5, 6], n),
            (v, n) => assert.strictEqual(v, null, n),
          ]);
          // fixed_varchar_array
          assertNestedValues<DuckDBVector<string>, DuckDBArrayVector>(chunk, 46, DuckDBArrayVector, [
            (v, n) => assertVectorValues(v, ['a', null, 'c'], n),
            (v, n) => assertVectorValues(v, ['d', 'e', 'f'], n),
            (v, n) => assert.strictEqual(v, null, n),
          ]);
          // fixed_nested_int_array
          assertNestedValues<DuckDBVector<DuckDBVector<number>>, DuckDBArrayVector>(chunk, 47, DuckDBArrayVector, [
            (v, n) => assertNestedVectorValues(v, [
              (vv, nn) => assertVectorValues(vv, [null, 2, 3], nn),
              (vv, nn) => assert.strictEqual(vv, null, nn),
              (vv, nn) => assertVectorValues(vv, [null, 2, 3], nn),
            ], n),
            (v, n) => assertNestedVectorValues(v, [
              (vv, nn) => assertVectorValues(vv, [4, 5, 6], nn),
              (vv, nn) => assertVectorValues(vv, [null, 2, 3], nn),
              (vv, nn) => assertVectorValues(vv, [4, 5, 6], nn),
            ], n),
            (v, n) => assert.strictEqual(v, null, n),
          ]);
          // fixed_nested_varchar_array
          assertNestedValues<DuckDBVector<DuckDBVector<string>>, DuckDBArrayVector>(chunk, 48, DuckDBArrayVector, [
            (v, n) => assertNestedVectorValues(v, [
              (vv, nn) => assertVectorValues(vv, ['a', null, 'c'], nn),
              (vv, nn) => assert.strictEqual(vv, null, nn),
              (vv, nn) => assertVectorValues(vv, ['a', null, 'c'], nn),
            ], n),
            (v, n) => assertNestedVectorValues(v, [
              (vv, nn) => assertVectorValues(vv, ['d', 'e', 'f'], nn),
              (vv, nn) => assertVectorValues(vv, ['a', null, 'c'], nn),
              (vv, nn) => assertVectorValues(vv, ['d', 'e', 'f'], nn),
            ], n),
            (v, n) => assert.strictEqual(v, null, n),
          ]);
          // fixed_struct_array
          assertNestedValues<DuckDBVector<readonly DuckDBStructEntry[]>, DuckDBArrayVector>(chunk, 49, DuckDBArrayVector, [
            (v, n) => assertVectorValues(v, [
              [{ name: 'a', value: null }, { name: 'b', value: null }],
              [{ name: 'a', value: 42 }, { name: 'b', value: '🦆🦆🦆🦆🦆🦆' }],
              [{ name: 'a', value: null }, { name: 'b', value: null }],
            ], n),
            (v, n) => assertVectorValues(v, [
              [{ name: 'a', value: 42 }, { name: 'b', value: '🦆🦆🦆🦆🦆🦆' }],
              [{ name: 'a', value: null }, { name: 'b', value: null }],
              [{ name: 'a', value: 42 }, { name: 'b', value: '🦆🦆🦆🦆🦆🦆' }],
            ], n),
            (v, n) => assert.strictEqual(v, null, n),
          ]);
          // struct_of_fixed_array
          assertNestedValues<readonly DuckDBStructEntry[], DuckDBStructVector>(chunk, 50, DuckDBStructVector, [
            (entries, n) => {
              assert.ok(entries, `${n} unexpectedly null`);
              if (!entries) return;
              assert.strictEqual(entries.length, 2, n);
              assert.strictEqual(entries[0].name, 'a', n);
              assert.ok(isVectorType(entries[0].value, DuckDBIntegerVector));
              assertVectorValues(entries[0].value, [null, 2, 3], n);
              assert.strictEqual(entries[1].name, 'b', n);
              assert.ok(isVectorType(entries[1].value, DuckDBVarCharVector));
              assertVectorValues(entries[1].value, ['a', null, 'c'], n);
            },
            (entries, n) => {
              assert.ok(entries, `${n} unexpectedly null`);
              if (!entries) return;
              assert.strictEqual(entries.length, 2, n);
              assert.strictEqual(entries[0].name, 'a', n);
              assert.ok(isVectorType(entries[0].value, DuckDBIntegerVector));
              assertVectorValues(entries[0].value, [4, 5, 6], n);
              assert.strictEqual(entries[1].name, 'b', n);
              assert.ok(isVectorType(entries[1].value, DuckDBVarCharVector));
              assertVectorValues(entries[1].value, ['d', 'e', 'f'], n);
            },
            (entries, n) => assert.strictEqual(entries, null, n),
          ]);
          // fixed_array_of_int_list
          assertNestedValues<DuckDBVector<DuckDBVector<number>>, DuckDBArrayVector>(chunk, 51, DuckDBArrayVector, [
            (v, n) => assertNestedVectorValues(v, [
              (vv, nn) => assertVectorValues(vv, [], nn),
              (vv, nn) => assertVectorValues(vv, [42, 999, null, null, -42], nn),
              (vv, nn) => assertVectorValues(vv, [], nn),
            ], n),
            (v, n) => assertNestedVectorValues(v, [
              (vv, nn) => assertVectorValues(vv, [42, 999, null, null, -42], nn),
              (vv, nn) => assertVectorValues(vv, [], nn),
              (vv, nn) => assertVectorValues(vv, [42, 999, null, null, -42], nn),
            ], n),
            (v, n) => assert.strictEqual(v, null, n),
          ]);
          // list_of_fixed_int_array
          assertNestedValues<DuckDBVector<DuckDBVector<number>>, DuckDBListVector>(chunk, 52, DuckDBListVector, [
            (v, n) => assertNestedVectorValues(v, [
              (vv, nn) => assertVectorValues(vv, [null, 2, 3], nn),
              (vv, nn) => assertVectorValues(vv, [4, 5, 6], nn),
              (vv, nn) => assertVectorValues(vv, [null, 2, 3], nn),
            ], n),
            (v, n) => assertNestedVectorValues(v, [
              (vv, nn) => assertVectorValues(vv, [4, 5, 6], nn),
              (vv, nn) => assertVectorValues(vv, [null, 2, 3], nn),
              (vv, nn) => assertVectorValues(vv, [4, 5, 6], nn),
            ], n),
            (v, n) => assert.strictEqual(v, null, n),
          ]);
        } finally {
          chunk.dispose();
        }
      } finally {
        result.dispose();
      }
    });
  });
});
