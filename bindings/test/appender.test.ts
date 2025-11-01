import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { expectLogicalType } from './utils/expectLogicalType';
import { expectResult } from './utils/expectResult';
import {
  BIGINT,
  BLOB,
  BOOLEAN,
  DATE,
  DECIMAL,
  DOUBLE,
  FLOAT,
  HUGEINT,
  INTEGER,
  INTERVAL,
  SMALLINT,
  TIME,
  TIMESTAMP,
  TINYINT,
  UBIGINT,
  UHUGEINT,
  UINTEGER,
  USMALLINT,
  UTINYINT,
  VARCHAR,
} from './utils/expectedLogicalTypes';
import { data } from './utils/expectedVectors';
import { withConnection } from './utils/withConnection';

suite('appender', () => {
  test('error: no table', async () => {
    await withConnection(async (connection) => {
      expect(() =>
        duckdb.appender_create_ext(connection, 'memory', 'main', 'bogus_table')
      ).toThrowError(`Table "memory.main.bogus_table" could not be found`);
    });
  });
  test('one column', async () => {
    await withConnection(async (connection) => {
      const createResult = await duckdb.query(
        connection,
        'create table appender_target(i integer)'
      );
      await expectResult(createResult, {
        statementType: duckdb.StatementType.CREATE,
        resultType: duckdb.ResultType.NOTHING,
        chunkCount: 0,
        rowCount: 0,
        columns: [{ name: 'Count', logicalType: BIGINT }],
        chunks: [],
      });

      const appender = duckdb.appender_create_ext(
        connection,
        'memory',
        'main',
        'appender_target'
      );
      expect(duckdb.appender_column_count(appender)).toBe(1);
      const column_type = duckdb.appender_column_type(appender, 0);
      expectLogicalType(column_type, INTEGER);

      duckdb.append_int32(appender, 11);
      duckdb.appender_end_row(appender);
      duckdb.append_int32(appender, 22);
      duckdb.appender_end_row(appender);
      duckdb.append_int32(appender, 33);
      duckdb.appender_end_row(appender);
      duckdb.appender_flush_sync(appender);

      const result = await duckdb.query(connection, 'from appender_target');
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 3,
        columns: [{ name: 'i', logicalType: INTEGER }],
        chunks: [
          { rowCount: 3, vectors: [data(4, [true, true, true], [11, 22, 33])] },
        ],
      });
    });
  });
  test('multiple columns', async () => {
    await withConnection(async (connection) => {
      const createResult = await duckdb.query(
        connection,
        'create table appender_target(\
          bool boolean, \
          int8 tinyint, \
          int16 smallint, \
          int32 integer, \
          int64 bigint, \
          hugeint hugeint, \
          uint8 utinyint, \
          uint16 usmallint, \
          uint32 uinteger, \
          uint64 ubigint, \
          uhugeint uhugeint, \
          float float, \
          double double, \
          date date, \
          time time, \
          timestamp timestamp, \
          interval interval, \
          varchar varchar, \
          blob blob, \
          dec4_1 decimal(4,1), \
          dec9_4 decimal(9,4), \
          dec18_6 decimal(18,6), \
          dec38_10 decimal(38,10), \
          null_column integer, \
          integer_with_default integer default 42\
        )'
      );
      await expectResult(createResult, {
        statementType: duckdb.StatementType.CREATE,
        resultType: duckdb.ResultType.NOTHING,
        chunkCount: 0,
        rowCount: 0,
        columns: [{ name: 'Count', logicalType: BIGINT }],
        chunks: [],
      });

      const appender = duckdb.appender_create_ext(
        connection,
        'memory',
        'main',
        'appender_target'
      );
      expect(duckdb.appender_column_count(appender)).toBe(25);

      const expectedLogicalTypes = [
        BOOLEAN,
        TINYINT,
        SMALLINT,
        INTEGER,
        BIGINT,
        HUGEINT,
        UTINYINT,
        USMALLINT,
        UINTEGER,
        UBIGINT,
        UHUGEINT,
        FLOAT,
        DOUBLE,
        DATE,
        TIME,
        TIMESTAMP,
        INTERVAL,
        VARCHAR,
        BLOB,
        DECIMAL(4, 1, duckdb.Type.SMALLINT),
        DECIMAL(9, 4, duckdb.Type.INTEGER),
        DECIMAL(18, 6, duckdb.Type.BIGINT),
        DECIMAL(38, 10, duckdb.Type.HUGEINT),
        INTEGER,
        INTEGER,
      ];
      for (let i = 0; i < expectLogicalType.length; i++) {
        const column_type = duckdb.appender_column_type(appender, i);
        expectLogicalType(column_type, expectedLogicalTypes[i]);
      }

      duckdb.append_bool(appender, true);
      duckdb.append_int8(appender, 127);
      duckdb.append_int16(appender, 32767);
      duckdb.append_int32(appender, 2147483647);
      duckdb.append_int64(appender, 9223372036854775807n);
      duckdb.append_hugeint(appender, 170141183460469231731687303715884105727n);
      duckdb.append_uint8(appender, 255);
      duckdb.append_uint16(appender, 65535);
      duckdb.append_uint32(appender, 4294967295);
      duckdb.append_uint64(appender, 18446744073709551615n);
      duckdb.append_uhugeint(
        appender,
        340282366920938463463374607431768211455n
      );
      duckdb.append_float(appender, 3.4028234663852886e38);
      duckdb.append_double(appender, 1.7976931348623157e308);
      duckdb.append_date(appender, { days: 2147483646 });
      duckdb.append_time(appender, { micros: 86400000000n });
      duckdb.append_timestamp(appender, { micros: 9223372036854775806n });
      duckdb.append_interval(appender, {
        months: 999,
        days: 999,
        micros: 999999999n,
      });
      duckdb.append_varchar(appender, '🦆🦆🦆🦆🦆🦆');
      duckdb.append_blob(
        appender,
        Buffer.from('thisisalongblob\x00withnullbytes')
      );
      duckdb.append_value(
        appender,
        duckdb.create_decimal({ width: 4, scale: 1, value: 9999n })
      );
      duckdb.append_value(
        appender,
        duckdb.create_decimal({ width: 9, scale: 4, value: 999999999n })
      );
      duckdb.append_value(
        appender,
        duckdb.create_decimal({ width: 18, scale: 6, value: 999999999999999999n })
      );
      duckdb.append_value(
        appender,
        duckdb.create_decimal({ width: 38, scale: 10, value: -99999999999999999999999999999999999999n })
      );
      duckdb.append_null(appender);
      duckdb.append_default(appender);

      duckdb.appender_end_row(appender);
      // explicitly calling both flush and close is unnecessary because close does a flush, but this exercises them.
      duckdb.appender_flush_sync(appender);
      duckdb.appender_close_sync(appender);

      const result = await duckdb.query(connection, 'from appender_target');
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 1,
        columns: [
          { name: 'bool', logicalType: BOOLEAN },
          { name: 'int8', logicalType: TINYINT },
          { name: 'int16', logicalType: SMALLINT },
          { name: 'int32', logicalType: INTEGER },
          { name: 'int64', logicalType: BIGINT },
          { name: 'hugeint', logicalType: HUGEINT },
          { name: 'uint8', logicalType: UTINYINT },
          { name: 'uint16', logicalType: USMALLINT },
          { name: 'uint32', logicalType: UINTEGER },
          { name: 'uint64', logicalType: UBIGINT },
          { name: 'uhugeint', logicalType: UHUGEINT },
          { name: 'float', logicalType: FLOAT },
          { name: 'double', logicalType: DOUBLE },
          { name: 'date', logicalType: DATE },
          { name: 'time', logicalType: TIME },
          { name: 'timestamp', logicalType: TIMESTAMP },
          { name: 'interval', logicalType: INTERVAL },
          { name: 'varchar', logicalType: VARCHAR },
          { name: 'blob', logicalType: BLOB },
          { name: 'dec4_1', logicalType: DECIMAL(4, 1, duckdb.Type.SMALLINT) },
          { name: 'dec9_4', logicalType: DECIMAL(9, 4, duckdb.Type.INTEGER) },
          { name: 'dec18_6', logicalType: DECIMAL(18, 6, duckdb.Type.BIGINT) },
          { name: 'dec38_10', logicalType: DECIMAL(38, 10, duckdb.Type.HUGEINT) },
          { name: 'null_column', logicalType: INTEGER },
          { name: 'integer_with_default', logicalType: INTEGER },
        ],
        chunks: [
          {
            rowCount: 1,
            vectors: [
              data(1, [true], [true]),
              data(1, [true], [127]),
              data(2, [true], [32767]),
              data(4, [true], [2147483647]),
              data(8, [true], [9223372036854775807n]),
              data(16, [true], [170141183460469231731687303715884105727n]),
              data(1, [true], [255]),
              data(2, [true], [65535]),
              data(4, [true], [4294967295]),
              data(8, [true], [18446744073709551615n]),
              data(16, [true], [340282366920938463463374607431768211455n]),
              data(4, [true], [3.4028234663852886e38]),
              data(8, [true], [1.7976931348623157e308]),
              data(4, [true], [2147483646]),
              data(8, [true], [86400000000n]),
              data(8, [true], [9223372036854775806n]),
              data(
                16,
                [true],
                [{ months: 999, days: 999, micros: 999999999n }]
              ),
              data(16, [true], ['🦆🦆🦆🦆🦆🦆']),
              data(
                16,
                [true],
                [Buffer.from('thisisalongblob\x00withnullbytes')]
              ),
              data(2, [true], [9999]),
              data(4, [true], [999999999]),
              data(8, [true], [999999999999999999n]),
              data(16, [true], [-99999999999999999999999999999999999999n]),
              data(4, [false], [null]),
              data(4, [true], [42]),
            ],
          },
        ],
      });
    });
  });
  test('data chunk', async () => {
    await withConnection(async (connection) => {
      const createResult = await duckdb.query(
        connection,
        'create table appender_target(i integer, v varchar)'
      );
      await expectResult(createResult, {
        statementType: duckdb.StatementType.CREATE,
        resultType: duckdb.ResultType.NOTHING,
        chunkCount: 0,
        rowCount: 0,
        columns: [{ name: 'Count', logicalType: BIGINT }],
        chunks: [],
      });

      const appender = duckdb.appender_create_ext(
        connection,
        'memory',
        'main',
        'appender_target'
      );
      expect(duckdb.appender_column_count(appender)).toBe(2);

      const source_result = await duckdb.query(
        connection,
        'select int, varchar from test_all_types()'
      );
      const source_chunk = await duckdb.fetch_chunk(source_result);
      expect(source_chunk).toBeDefined();
      if (source_chunk) {
        duckdb.append_data_chunk(appender, source_chunk);
      }
      duckdb.appender_flush_sync(appender);

      const result = await duckdb.query(connection, 'from appender_target');
      await expectResult(result, {
        chunkCount: 1,
        rowCount: 3,
        columns: [
          { name: 'i', logicalType: INTEGER },
          { name: 'v', logicalType: VARCHAR },
        ],
        chunks: [
          {
            rowCount: 3,
            vectors: [
              data(4, [true, true, false], [-2147483648, 2147483647, null]),
              data(16, [true, true, false], ['🦆🦆🦆🦆🦆🦆', 'goo\0se', null]),
            ],
          },
        ],
      });
    });
  });

  test('error_data: basic error access', async () => {
    await withConnection(async (connection) => {
      await duckdb.query(
        connection,
        'create table test_error_data(i integer)'
      );

      const appender = duckdb.appender_create_ext(
        connection,
        'memory',
        'main',
        'test_error_data'
      );

      // Get error data even when there's no error
      const error_data = duckdb.appender_error_data(appender);
      expect(duckdb.error_data_has_error(error_data)).toBe(false);
      expect(duckdb.error_data_message(error_data)).toBe('');

      // Successful append operations should not have errors
      duckdb.append_int32(appender, 42);
      duckdb.appender_end_row(appender);
      duckdb.appender_flush_sync(appender);

      // Error data should still indicate no error after successful operations
      const error_data_after = duckdb.appender_error_data(appender);
      expect(duckdb.error_data_has_error(error_data_after)).toBe(false);
    });
  });

  test('error_data: type conversion', async () => {
    await withConnection(async (connection) => {
      await duckdb.query(
        connection,
        'create table test_error_type(i integer)'
      );

      const appender = duckdb.appender_create_ext(
        connection,
        'memory',
        'main',
        'test_error_type'
      );

      // Get the error type enumeration (should be valid even with no error)
      const error_data = duckdb.appender_error_data(appender);
      const error_type = duckdb.error_data_error_type(error_data);

      // Error type should be a valid number in the ErrorType enum range
      expect(typeof error_type).toBe('number');
      expect(error_type).toBeGreaterThanOrEqual(0);
      expect(error_type).toBeLessThanOrEqual(41); // Max known error type (SEQUENCE)
    });
  });
});
