import { assert, beforeAll, describe, test } from 'vitest';
import {
  ANY,
  ARRAY,
  BIGINT,
  BIGNUM,
  BIT,
  BLOB,
  BOOLEAN,
  DATE,
  DECIMAL,
  DOUBLE,
  DateParts,
  DuckDBArrayVector,
  DuckDBBigIntVector,
  DuckDBBigNumVector,
  DuckDBBitVector,
  DuckDBBlobVector,
  DuckDBBooleanVector,
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
  DuckDBGeometryType,
  DuckDBHugeIntVector,
  DuckDBIntegerVector,
  DuckDBIntervalVector,
  DuckDBListVector,
  DuckDBMapVector,
  DuckDBSmallIntVector,
  DuckDBStructVector,
  DuckDBTimeNSVector,
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
  DuckDBTypeId,
  DuckDBUBigIntVector,
  DuckDBUHugeIntVector,
  DuckDBUIntegerVector,
  DuckDBUSmallIntVector,
  DuckDBUTinyIntVector,
  DuckDBUUIDVector,
  DuckDBUnionVector,
  DuckDBVarCharVector,
  ENUM,
  FLOAT,
  HUGEINT,
  INTEGER,
  INTERVAL,
  LIST,
  MAP,
  SMALLINT,
  SQLNULL,
  STRUCT,
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
  arrayValue,
  bitValue,
  blobValue,
  decimalValue,
  intervalValue,
  listValue,
  mapValue,
  structValue,
  timeTZValue,
  timeValue,
  unionValue,
} from '../src';
import {
  createTestAllTypesColumnNameAndTypeObjects,
  createTestAllTypesColumns,
} from './util/testAllTypes';
import {
  assertColumns,
  assertValues,
  setDefaultTimezone,
  withConnection,
} from './util/testHelpers';

describe('types', () => {
  beforeAll(setDefaultTimezone);

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
      `ENUM('fly', 'swim', 'walk')`,
    );
    assert.equal(LIST(INTEGER).toString(), 'INTEGER[]');
    assert.equal(
      STRUCT({ 'id': VARCHAR, 'ts': TIMESTAMP }).toString(),
      'STRUCT("id" VARCHAR, "ts" TIMESTAMP)',
    );
    assert.equal(MAP(INTEGER, VARCHAR).toString(), 'MAP(INTEGER, VARCHAR)');
    assert.equal(ARRAY(INTEGER, 3).toString(), 'INTEGER[3]');
    assert.equal(UUID.toString(), 'UUID');
    assert.equal(
      UNION({ 'str': VARCHAR, 'num': INTEGER }).toString(),
      'UNION("str" VARCHAR, "num" INTEGER)',
    );
    assert.equal(BIT.toString(), 'BIT');
    assert.equal(TIMETZ.toString(), 'TIME WITH TIME ZONE');
    assert.equal(TIMESTAMPTZ.toString(), 'TIMESTAMP WITH TIME ZONE');
    assert.equal(ANY.toString(), 'ANY');
    assert.equal(BIGNUM.toString(), 'BIGNUM');
    assert.equal(SQLNULL.toString(), 'SQLNULL');
  });
  test('should support all data types', async () => {
    await withConnection(async (connection) => {
      const result = await connection.run(
        'from test_all_types(use_large_enum=true)',
      );
      assertColumns(result, createTestAllTypesColumnNameAndTypeObjects());

      const chunk = await result.fetchChunk();
      assert.isDefined(chunk);
      if (chunk) {
        assert.strictEqual(chunk.columnCount, 56);
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
        assertValues(chunk, 11, DuckDBBigNumVector, testAllTypesColumns[11]);
        assertValues(chunk, 12, DuckDBDateVector, testAllTypesColumns[12]);
        assertValues(chunk, 13, DuckDBTimeVector, testAllTypesColumns[13]);
        assertValues(chunk, 14, DuckDBTimestampVector, testAllTypesColumns[14]);
        assertValues(
          chunk,
          15,
          DuckDBTimestampSecondsVector,
          testAllTypesColumns[15],
        );
        assertValues(
          chunk,
          16,
          DuckDBTimestampMillisecondsVector,
          testAllTypesColumns[16],
        );
        assertValues(
          chunk,
          17,
          DuckDBTimestampNanosecondsVector,
          testAllTypesColumns[17],
        );
        assertValues(chunk, 18, DuckDBTimeTZVector, testAllTypesColumns[18]);
        assertValues(
          chunk,
          19,
          DuckDBTimestampTZVector,
          testAllTypesColumns[19],
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
          testAllTypesColumns[25],
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
        // time_ns
        assertValues(chunk, 54, DuckDBTimeNSVector, testAllTypesColumns[54]);
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
      '0010001001011100010101011010111',
    );

    // blob
    assert.equal(blobValue('').toString(), '');
    assert.equal(
      blobValue('thisisalongblob\x00withnullbytes').toString(),
      'thisisalongblob\\x00withnullbytes',
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
      '987654321098.765432',
    );
    assert.equal(
      decimalValue(-987654321098765432n, 18, 6).toString(),
      '-987654321098.765432',
    );

    assert.equal(decimalValue(0n, 38, 10).toString(), '0.0000000000');
    assert.equal(
      decimalValue(98765432109876543210987654321098765432n, 38, 10).toString(),
      '9876543210987654321098765432.1098765432',
    );
    assert.equal(
      decimalValue(-98765432109876543210987654321098765432n, 38, 10).toString(),
      '-9876543210987654321098765432.1098765432',
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
      '00:59:00',
    );
    assert.equal(
      intervalValue(0, 0, -59n * 60n * 1000000n).toString(),
      '-00:59:00',
    );
    assert.equal(
      intervalValue(0, 0, 60n * 60n * 1000000n).toString(),
      '01:00:00',
    );
    assert.equal(
      intervalValue(0, 0, -60n * 60n * 1000000n).toString(),
      '-01:00:00',
    );
    assert.equal(
      intervalValue(0, 0, 24n * 60n * 60n * 1000000n).toString(),
      '24:00:00',
    );
    assert.equal(
      intervalValue(0, 0, -24n * 60n * 60n * 1000000n).toString(),
      '-24:00:00',
    );
    assert.equal(
      intervalValue(0, 0, 2147483647n * 60n * 60n * 1000000n).toString(),
      '2147483647:00:00',
    );
    assert.equal(
      intervalValue(0, 0, -2147483647n * 60n * 60n * 1000000n).toString(),
      '-2147483647:00:00',
    );
    assert.equal(
      intervalValue(0, 0, 2147483647n * 60n * 60n * 1000000n + 1n).toString(),
      '2147483647:00:00.000001',
    );
    assert.equal(
      intervalValue(
        0,
        0,
        -(2147483647n * 60n * 60n * 1000000n + 1n),
      ).toString(),
      '-2147483647:00:00.000001',
    );

    assert.equal(
      intervalValue(
        2 * 12 + 3,
        5,
        (7n * 60n * 60n + 11n * 60n + 13n) * 1000000n + 17n,
      ).toString(),
      '2 years 3 months 5 days 07:11:13.000017',
    );
    assert.equal(
      intervalValue(
        -(2 * 12 + 3),
        -5,
        -((7n * 60n * 60n + 11n * 60n + 13n) * 1000000n + 17n),
      ).toString(),
      '-2 years -3 months -5 days -07:11:13.000017',
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
      `{1: 'a', 2: 'b'}`,
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
      '294247-01-09 22:30:54.775806-05:30',
    );
    assert.equal(
      TIMESTAMPTZ.min.toString(),
      '290309-12-21 (BC) 18:30:00-05:30',
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
        -((7 * 60 + 9) * 60),
      ).toString(),
      '12:34:56.789-07:09',
    );
    assert.equal(TIMETZ.max.toString(), '24:00:00-15:59:59');
    assert.equal(TIMETZ.min.toString(), '00:00:00+15:59:59');

    // time
    assert.equal(TIME.max.toString(), '24:00:00');
    assert.equal(TIME.min.toString(), '00:00:00');
    assert.equal(
      timeValue(
        (12n * 60n * 60n + 34n * 60n + 56n) * 1000000n + 987654n,
      ).toString(),
      '12:34:56.987654',
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
      timeTZParts,
    );
    assert.deepEqual(
      DuckDBTimestampValue.fromParts(timestampParts).toParts(),
      timestampParts,
    );
    assert.deepEqual(
      DuckDBTimestampTZValue.fromParts(timestampParts).toParts(),
      timestampParts,
    );

    assert.deepEqual(
      DuckDBDecimalValue.fromDouble(3.14159, 6, 5),
      decimalValue(314159n, 6, 5),
    );
    assert.deepEqual(decimalValue(314159n, 6, 5).toDouble(), 3.14159);
  });

  test('DuckDBGeometryType exposes the CRS from the logical type', async () => {
    // Direct construction: crs is preserved, and the cached `instance` is
    // only used when both alias and crs are absent.
    const explicit = DuckDBGeometryType.create(undefined, 'GEOGCRS["x"]');
    assert.strictEqual(explicit.crs, 'GEOGCRS["x"]');
    assert.notStrictEqual(explicit, DuckDBGeometryType.instance);
    assert.strictEqual(DuckDBGeometryType.create(), DuckDBGeometryType.instance);
    assert.strictEqual(DuckDBGeometryType.instance.crs, undefined);

    // toString / toJson include the CRS when present.
    assert.strictEqual(DuckDBGeometryType.instance.toString(), 'GEOMETRY');
    assert.strictEqual(explicit.toString(), `GEOMETRY('GEOGCRS["x"]')`);
    assert.deepStrictEqual(DuckDBGeometryType.instance.toJson(), {
      typeId: DuckDBTypeId.GEOMETRY,
    });
    assert.deepStrictEqual(explicit.toJson(), {
      typeId: DuckDBTypeId.GEOMETRY,
      crs: 'GEOGCRS["x"]',
    });
    // CRS strings containing single quotes are escaped in toString.
    assert.strictEqual(
      new DuckDBGeometryType(undefined, `it's`).toString(),
      `GEOMETRY('it''s')`,
    );

    // Round-trip through SQL: `column_logical_type` -> `asType()` should
    // produce a DuckDBGeometryType carrying the CRS.
    await withConnection(async (connection) => {
      const withCrs = await connection.run(
        `SELECT 'POINT(1 2)'::GEOMETRY('GEOGCRS["x"]') AS g`,
      );
      const withCrsType = withCrs.columnTypes()[0];
      assert.ok(withCrsType instanceof DuckDBGeometryType);
      assert.strictEqual(withCrsType.crs, 'GEOGCRS["x"]');

      const plain = await connection.run(`SELECT 'POINT(1 2)'::GEOMETRY AS g`);
      const plainType = plain.columnTypes()[0];
      assert.ok(plainType instanceof DuckDBGeometryType);
      assert.strictEqual(plainType.crs, undefined);
    });
  });
});
