import { assert, beforeAll, describe, test } from 'vitest';
import {
  ARRAY,
  BLOB,
  BOOLEAN,
  DuckDBArrayVector,
  DuckDBBigIntVector,
  DuckDBBigNumVector,
  DuckDBBitVector,
  DuckDBBlobVector,
  DuckDBBooleanVector,
  DuckDBDataChunk,
  DuckDBDateVector,
  DuckDBDecimal128Vector,
  DuckDBDecimal16Vector,
  DuckDBDecimal32Vector,
  DuckDBDecimal64Vector,
  DuckDBDoubleVector,
  DuckDBEnum16Vector,
  DuckDBEnum32Vector,
  DuckDBEnum8Vector,
  DuckDBFloatVector,
  DuckDBHugeIntVector,
  DuckDBIntegerVector,
  DuckDBIntervalVector,
  DuckDBListVector,
  DuckDBMapVector,
  DuckDBSmallIntVector,
  DuckDBStructVector,
  DuckDBTimeNSVector,
  DuckDBTimeTZVector,
  DuckDBTimeVector,
  DuckDBTimestampMillisecondsVector,
  DuckDBTimestampNanosecondsVector,
  DuckDBTimestampSecondsVector,
  DuckDBTimestampTZVector,
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
  INTEGER,
  LIST,
  SMALLINT,
  STRUCT,
  TINYINT,
  VARCHAR,
  arrayValue,
  blobValue,
  listValue,
  structValue,
} from '../src';
import {
  createTestAllTypesColumnNameAndTypeObjects,
  createTestAllTypesColumnTypes,
  createTestAllTypesColumns,
} from './util/testAllTypes';
import {
  assertValues,
  setDefaultTimezone,
  withConnection,
} from './util/testHelpers';

describe('appender', () => {
  beforeAll(setDefaultTimezone);

  test('data chunk max rows', () => {
    try {
      DuckDBDataChunk.create([INTEGER], 2049);
      assert.fail('should throw');
    } catch (err) {
      assert.deepEqual(
        err,
        new Error('A data chunk cannot have more than 2048 rows'),
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
          ]),
        ),
        blobValue(
          new Uint8Array([
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d,
          ]),
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
        originalValues.length,
      );
      chunk.setColumnValues(0, originalValues);

      const outerListVector = chunk.getColumnVector(0) as DuckDBListVector;
      const innerListVector = outerListVector.getItemVector(
        2,
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
        values.length,
      );
      chunk.setColumnValues(0, values);

      await connection.run(
        'create table target(col0 struct(num integer, str varchar))',
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
        )',
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
          .join(', ')})`,
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
        assertValues(resultChunk, 11, DuckDBBigNumVector, columns[11]);
        assertValues(resultChunk, 12, DuckDBDateVector, columns[12]);
        assertValues(resultChunk, 13, DuckDBTimeVector, columns[13]);
        assertValues(resultChunk, 14, DuckDBTimestampVector, columns[14]);
        assertValues(
          resultChunk,
          15,
          DuckDBTimestampSecondsVector,
          columns[15],
        );
        assertValues(
          resultChunk,
          16,
          DuckDBTimestampMillisecondsVector,
          columns[16],
        );
        assertValues(
          resultChunk,
          17,
          DuckDBTimestampNanosecondsVector,
          columns[17],
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
          .join(', ')})`,
      );

      const appender = await connection.createAppender('target');
      for (let rowIndex = 0; rowIndex < columns[0].length; rowIndex++) {
        for (let columnIndex = 0; columnIndex < columns.length; columnIndex++) {
          const type = types[columnIndex];
          const value = columns[columnIndex][rowIndex];
          if (value === null) {
            appender.appendNull();
          } else {
            if (type.typeId === DuckDBTypeId.GEOMETRY) {
              // TODO: Appending GEOMETRY is not yet supported
              appender.appendNull();
            } else {
              appender.appendValue(value, type);
            }
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
        assertValues(resultChunk, 11, DuckDBBigNumVector, columns[11]);
        assertValues(resultChunk, 12, DuckDBDateVector, columns[12]);
        assertValues(resultChunk, 13, DuckDBTimeVector, columns[13]);
        assertValues(resultChunk, 14, DuckDBTimestampVector, columns[14]);
        assertValues(
          resultChunk,
          15,
          DuckDBTimestampSecondsVector,
          columns[15],
        );
        assertValues(
          resultChunk,
          16,
          DuckDBTimestampMillisecondsVector,
          columns[16],
        );
        assertValues(
          resultChunk,
          17,
          DuckDBTimestampNanosecondsVector,
          columns[17],
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
        assertValues(resultChunk, 54, DuckDBTimeNSVector, columns[54]); // time_ns
      }
    });
  });
});
