import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { withConnection } from './utils/withConnection';
import { expectResult } from './utils/expectResult';
import { expectLogicalType } from './utils/expectLogicalType';
import { BIGINT, BLOB, BOOLEAN, DATE, DOUBLE, FLOAT, HUGEINT, INTEGER, INTERVAL, SMALLINT, TIME, TIMESTAMP, TINYINT, UBIGINT, UHUGEINT, UINTEGER, USMALLINT, UTINYINT, VARCHAR } from './utils/expectedLogicalTypes';
import { data } from './utils/expectedVectors';

suite('appender', () => {
  test('error: no table', async () => {
    await withConnection(async (connection) => {
      expect(() => duckdb.appender_create(connection, 'main', 'bogus_table'))
        .toThrowError(`Table "main.bogus_table" could not be found`);
    });
  });
  test('one column', async () => {
    await withConnection(async (connection) => {
      const createResult = await duckdb.query(connection, 'create table appender_target(i integer)');
      try {
        await expectResult(createResult, {
          statementType: duckdb.StatementType.CREATE,
          resultType: duckdb.ResultType.NOTHING,
          columns: [
            { name: 'Count', logicalType: BIGINT },
          ],
          chunks: [
            { columnCount: 0, rowCount: 0, vectors: [] },
          ],
        });
      } finally {
        duckdb.destroy_result(createResult);
      }

      const appender = duckdb.appender_create(connection, 'main', 'appender_target');
      try {
        expect(duckdb.appender_column_count(appender)).toBe(1);
        const column_type = duckdb.appender_column_type(appender, 0);
        try {
          expectLogicalType(column_type, INTEGER);
        } finally {
          duckdb.destroy_logical_type(column_type);
        }
        duckdb.append_int32(appender, 11);
        duckdb.appender_end_row(appender);
        duckdb.append_int32(appender, 22);
        duckdb.appender_end_row(appender);
        duckdb.append_int32(appender, 33);
        duckdb.appender_end_row(appender);
      } finally {
        duckdb.appender_destroy(appender);
      }

      const result = await duckdb.query(connection, 'from appender_target');
      try {
        await expectResult(result, {
          columns: [
            { name: 'i', logicalType: INTEGER },
          ],
          chunks: [
            { rowCount: 3, vectors: [data(4, [true, true, true], [11, 22, 33])] },
          ],
        });
      } finally {
        duckdb.destroy_result(result);
      }
    });
  });
  test('multiple columns', async () => {
    await withConnection(async (connection) => {
      const createResult = await duckdb.query(connection,
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
          null_column integer, \
          integer_with_default integer default 42\
        )');
      try {
        await expectResult(createResult, {
          statementType: duckdb.StatementType.CREATE,
          resultType: duckdb.ResultType.NOTHING,
          columns: [
            { name: 'Count', logicalType: BIGINT },
          ],
          chunks: [
            { columnCount: 0, rowCount: 0, vectors: [] },
          ],
        });
      } finally {
        duckdb.destroy_result(createResult);
      }

      const appender = duckdb.appender_create(connection, 'main', 'appender_target');
      try {
        expect(duckdb.appender_column_count(appender)).toBe(21);
        
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
          INTEGER,
          INTEGER,
        ];
        for (let i = 0; i < expectLogicalType.length; i++) {
          const column_type = duckdb.appender_column_type(appender, i);
          try {
            expectLogicalType(column_type, expectedLogicalTypes[i]);
          } finally {
            duckdb.destroy_logical_type(column_type);
          }
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
        duckdb.append_uhugeint(appender, 340282366920938463463374607431768211455n);
        duckdb.append_float(appender, 3.4028234663852886e+38);
        duckdb.append_double(appender, 1.7976931348623157e+308);
        duckdb.append_date(appender, { days: 2147483646 });
        duckdb.append_time(appender, { micros: 86400000000 });
        duckdb.append_timestamp(appender, { micros: 9223372036854775806n });
        duckdb.append_interval(appender, { months: 999, days: 999, micros: 999999999n });
        duckdb.append_varchar(appender, '🦆🦆🦆🦆🦆🦆');
        duckdb.append_blob(appender, Buffer.from('thisisalongblob\x00withnullbytes'));
        duckdb.append_null(appender);
        duckdb.append_default(appender);
        
        duckdb.appender_end_row(appender);
        // explicitly calling flush and close is unnecessary because destroy does both, but this exercises them.
        duckdb.appender_flush(appender);
        duckdb.appender_close(appender);
      } finally {
        duckdb.appender_destroy(appender);
      }

      const result = await duckdb.query(connection, 'from appender_target');
      try {
        await expectResult(result, {
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
                data(4, [true], [3.4028234663852886e+38]),
                data(8, [true], [1.7976931348623157e+308]),
                data(4, [true], [2147483646]),
                data(8, [true], [86400000000n]),
                data(8, [true], [9223372036854775806n]),
                data(16, [true], [{ months: 999, days: 999, micros: 999999999n }]),
                data(16, [true], ['🦆🦆🦆🦆🦆🦆']),
                data(16, [true], [Buffer.from('thisisalongblob\x00withnullbytes')]),
                data(4, [false], [null]),
                data(4, [true], [42]),
              ],
            },
          ],
        });
      } finally {
        duckdb.destroy_result(result);
      }
    });
  });
  test('data chunk', async () => {
    await withConnection(async (connection) => {
      const createResult = await duckdb.query(connection, 'create table appender_target(i integer, v varchar)');
      try {
        await expectResult(createResult, {
          statementType: duckdb.StatementType.CREATE,
          resultType: duckdb.ResultType.NOTHING,
          columns: [
            { name: 'Count', logicalType: BIGINT },
          ],
          chunks: [
            { columnCount: 0, rowCount: 0, vectors: [] },
          ],
        });
      } finally {
        duckdb.destroy_result(createResult);
      }

      const appender = duckdb.appender_create(connection, 'main', 'appender_target');
      try {
        expect(duckdb.appender_column_count(appender)).toBe(2);

        const source_result = await duckdb.query(connection, 'select int, varchar from test_all_types()');
        try {
          const source_chunk = await duckdb.fetch_chunk(source_result);
          try {
            duckdb.append_data_chunk(appender, source_chunk);
          } finally {
            duckdb.destroy_data_chunk(source_chunk);
          }
        } finally {
          duckdb.destroy_result(source_result);
        }
        
      } finally {
        duckdb.appender_destroy(appender);
      }

      const result = await duckdb.query(connection, 'from appender_target');
      try {
        await expectResult(result, {
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
      } finally {
        duckdb.destroy_result(result);
      }
    });
  });
});
