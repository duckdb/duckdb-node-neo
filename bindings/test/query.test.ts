import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import {
  ALT,
  ARRAY,
  BIGINT,
  BIT,
  BLOB,
  BOOLEAN,
  DATE,
  DECIMAL,
  DOUBLE,
  ENTRY,
  ENUM,
  FLOAT,
  HUGEINT,
  INTEGER,
  INTERVAL,
  LIST,
  MAP,
  SMALLINT,
  STRUCT,
  TIME,
  TIME_TZ,
  TIMESTAMP,
  TIMESTAMP_MS,
  TIMESTAMP_NS,
  TIMESTAMP_S,
  TIMESTAMP_TZ,
  TINYINT,
  UBIGINT,
  UHUGEINT,
  UINTEGER,
  UNION,
  USMALLINT,
  UTINYINT,
  UUID,
  VARCHAR,
} from './utils/expectedLogicalTypes';
import { array, data, list, map, struct, union } from './utils/expectedVectors';
import { expectResult } from './utils/expectResult';
import { withConnection } from './utils/withConnection';

const useLargeEnum = false;

suite('query', () => {
  test('basic select', async () => {
    await withConnection(async (connection) => {
      const result = await duckdb.query(connection, 'select 17 as seventeen');
      try {
        await expectResult(result, {
          columns: [
            { name: 'seventeen', logicalType: { typeId: duckdb.Type.INTEGER } },
          ],
          chunks: [
            { rowCount: 1, vectors: [data(4, [true], [17])]},
          ],
        });
      } finally {
        duckdb.destroy_result(result);
      }
    });
  });
  test('basic error', async () => {
    await withConnection(async (connection) => {
      await expect(duckdb.query(connection, 'selct 1')).rejects.toThrow('Parser Error');
    });
  });
  test('test_all_types()', async () => {
    await withConnection(async (connection) => {
      const result = await duckdb.query(connection, `from test_all_types(use_large_enum=${useLargeEnum})`);
      try {
        const validity = [true, true, false];
        await expectResult(result, {
          columns: [
            { name: 'bool', logicalType: BOOLEAN },
            { name: 'tinyint', logicalType: TINYINT },
            { name: 'smallint', logicalType: SMALLINT },
            { name: 'int', logicalType: INTEGER },
            { name: 'bigint', logicalType: BIGINT },
            { name: 'hugeint', logicalType: HUGEINT },
            { name: 'uhugeint', logicalType: UHUGEINT },
            { name: 'utinyint', logicalType: UTINYINT },
            { name: 'usmallint', logicalType: USMALLINT },
            { name: 'uint', logicalType: UINTEGER },
            { name: 'ubigint', logicalType: UBIGINT },
            { name: 'date', logicalType: DATE },
            { name: 'time', logicalType: TIME },
            { name: 'timestamp', logicalType: TIMESTAMP },
            { name: 'timestamp_s', logicalType: TIMESTAMP_S },
            { name: 'timestamp_ms', logicalType: TIMESTAMP_MS },
            { name: 'timestamp_ns', logicalType: TIMESTAMP_NS },
            { name: 'time_tz', logicalType: TIME_TZ },
            { name: 'timestamp_tz', logicalType: TIMESTAMP_TZ },
            { name: 'float', logicalType: FLOAT },
            { name: 'double', logicalType: DOUBLE },
            { name: 'dec_4_1', logicalType: DECIMAL(4, 1, duckdb.Type.SMALLINT) },
            { name: 'dec_9_4', logicalType: DECIMAL(9, 4, duckdb.Type.INTEGER) },
            { name: 'dec_18_6', logicalType: DECIMAL(18, 6, duckdb.Type.BIGINT) },
            { name: 'dec38_10', logicalType: DECIMAL(38, 10, duckdb.Type.HUGEINT) },
            { name: 'uuid', logicalType: UUID },
            { name: 'interval', logicalType: INTERVAL },
            { name: 'varchar', logicalType: VARCHAR },
            { name: 'blob', logicalType: BLOB },
            { name: 'bit', logicalType: BIT },
            { name: 'small_enum', logicalType: ENUM(['DUCK_DUCK_ENUM', 'GOOSE'], duckdb.Type.UTINYINT) },
            { name: 'medium_enum', logicalType: ENUM(Array.from({ length: 300}).map((_, i) => `enum_${i}`), duckdb.Type.USMALLINT) },
            { name: 'large_enum', logicalType: useLargeEnum
              ? ENUM(Array.from({ length: 70000}).map((_, i) => `enum_${i}`), duckdb.Type.UINTEGER)
              : ENUM(['enum_0', 'enum_69999'], duckdb.Type.UTINYINT)
            },
            { name: 'int_array', logicalType: LIST(INTEGER) },
            { name: 'double_array', logicalType: LIST(DOUBLE) },
            { name: 'date_array', logicalType: LIST(DATE) },
            { name: 'timestamp_array', logicalType: LIST(TIMESTAMP) },
            { name: 'timestamptz_array', logicalType: LIST(TIMESTAMP_TZ) },
            { name: 'varchar_array', logicalType: LIST(VARCHAR) },
            { name: 'nested_int_array', logicalType: LIST(LIST(INTEGER)) },
            { name: 'struct', logicalType: STRUCT(ENTRY('a', INTEGER), ENTRY('b', VARCHAR)) },
            { name: 'struct_of_arrays', logicalType: STRUCT(ENTRY('a', LIST(INTEGER)), ENTRY('b', LIST(VARCHAR))) },
            { name: 'array_of_structs', logicalType: LIST(STRUCT(ENTRY('a', INTEGER), ENTRY('b', VARCHAR))) },
            { name: 'map', logicalType: MAP(VARCHAR, VARCHAR) },
            { name: 'union', logicalType: UNION(ALT('name', VARCHAR), ALT('age', SMALLINT)) },
            { name: 'fixed_int_array', logicalType: ARRAY(INTEGER, 3) },
            { name: 'fixed_varchar_array', logicalType: ARRAY(VARCHAR, 3) },
            { name: 'fixed_nested_int_array', logicalType: ARRAY(ARRAY(INTEGER, 3), 3) },
            { name: 'fixed_nested_varchar_array', logicalType: ARRAY(ARRAY(VARCHAR, 3), 3) },
            { name: 'fixed_struct_array', logicalType: ARRAY(STRUCT(ENTRY('a', INTEGER), ENTRY('b', VARCHAR)), 3) },
            { name: 'struct_of_fixed_array', logicalType: STRUCT(ENTRY('a', ARRAY(INTEGER, 3)), ENTRY('b', ARRAY(VARCHAR, 3))) },
            { name: 'fixed_array_of_int_list', logicalType: ARRAY(LIST(INTEGER), 3) },
            { name: 'list_of_fixed_int_array', logicalType: LIST(ARRAY(INTEGER, 3)) },
          ],
          chunks: [
            {
              rowCount: 3,
              vectors: [
                data(1, validity, [false, true, null]), // 0: bool
                data(1, validity, [-128, 127, null]), // 1: tinyint
                data(2, validity, [-32768, 32767, null]), // 2: smallint
                data(4, validity, [-2147483648, 2147483647, null]), // 3: int
                data(8, validity, [-9223372036854775808n, 9223372036854775807n, null]), // 4: bigint
                data(16, validity, [-170141183460469231731687303715884105728n, 170141183460469231731687303715884105727n, null]), // 5: hugeint
                data(16, validity, [0n, 340282366920938463463374607431768211455n, null]), // 6: uhugeint
                data(1, validity, [0, 255, null]), // 7: utinyint
                data(2, validity, [0, 65535, null]), // 8: usmallint
                data(4, validity, [0, 4294967295, null]), // 9: uint
                data(8, validity, [0n, 18446744073709551615n, null]), // 10: ubigint
                data(4, validity, [-2147483646, 2147483646, null]), // 11: date
                data(8, validity, [0n, 86400000000n, null]), // 12: time
                data(8, validity, [-9223372022400000000n, 9223372036854775806n, null]), // 13: timestamp
                data(8, validity, [-9223372022400n, 9223372036854n, null]), // 14: timestamp_s
                data(8, validity, [-9223372022400000n, 9223372036854775n, null]), // 15: timestamp_ms
                data(8, validity, [-9223372036854775808n, 9223372036854775806n, null]), // 16: timestamp_ns
                data(8, validity, [0n, 1449551462400115198n, null]), // 17: time_tz
                data(8, validity, [-9223372022400000000n, 9223372036854775806n, null]), // 18: timestamp_tz
                data(4, validity, [-3.4028234663852886e+38, 3.4028234663852886e+38, null]), // 19: float
                data(8, validity, [-1.7976931348623157e+308, 1.7976931348623157e+308, null]), // 20: double
                data(2, validity, [-9999, 9999, null]), // 21: dec_4_1
                data(4, validity, [-999999999, 999999999, null]), // 22: dec_9_4
                data(8, validity, [-999999999999999999n, 999999999999999999n, null]), // 23: dec_18_6
                data(16, validity, [-99999999999999999999999999999999999999n, 99999999999999999999999999999999999999n, null]), // 24: dec38_10
                data(16, validity, [-170141183460469231731687303715884105728n, 170141183460469231731687303715884105727n, null]), // 25: uuid
                data(16, validity, [{ months: 0, days: 0, micros: 0n }, { months: 999, days: 999, micros: 999999999n }, null]), // 26: interval
                data(16, validity, ['🦆🦆🦆🦆🦆🦆', 'goo\0se', null]), // 27: varchar
                data(16, validity, [Buffer.from('thisisalongblob\x00withnullbytes'), Buffer.from('\x00\x00\x00a'), null]), // 28: blob
                data(16, validity, [Buffer.from([1, 0b10010001, 0b00101110, 0b00101010, 0b11010111]), Buffer.from([3, 0b11110101]), null]), // 29: bit (x0010001 00101110 00101010 11010111, xxx10101)
                data(1, validity,  [0, 1, null]), // 30: small_enum
                data(2, validity, [0, 299, null]), // 31: medium_enum
                data(4, validity, [0, useLargeEnum ? 69999 : 1, null]), // 32: large_enum
                list(validity, [[0n, 0n], [0n, 5n], null], 5, data(4, [true, true, false, false, true], [42, 999, null, null, -42])), // 33: int_array
                list(validity, [[0n, 0n], [0n, 6n], null], 6, data(8, [true, true, true, true, false, true], [42.0, NaN, Infinity, -Infinity, null, -42.0])), // 34: double_array
                list(validity, [[0n, 0n], [0n, 5n], null], 5, data(4, [true, true, true, false, true], [0, 2147483647, -2147483647, null, 19124])), // 35: date_array
                list(validity, [[0n, 0n], [0n, 5n], null], 5, data(8, [true, true, true, false, true], [0n, 9223372036854775807n, -9223372036854775807n, null, 1652372625000000n])), // 36: timestamp_array
                list(validity, [[0n, 0n], [0n, 5n], null], 5, data(8, [true, true, true, false, true], [0n, 9223372036854775807n, -9223372036854775807n, null, 1652397825000000n])), // 37: timestamptz_array
                list(validity, [[0n, 0n], [0n, 4n], null], 4, data(16, [true, true, false, true], ['🦆🦆🦆🦆🦆🦆', 'goose', null, ''])), // 38: varchar_array
                list(validity, [[0n, 0n], [0n, 5n], null], 5,
                  list([true, true, false, true, true], [[0n, 0n], [0n, 5n], null, [5n, 0n], [5n, 5n]], 10,
                    data(10, [true, true, false, false, true, true, true, false, false, true], [42, 999, null, null, -42, 42, 999, null, null, -42]))), // 39: nested_int_array
                struct(3, validity, [data(4, [false, true, false], [null, 42, null]), data(16, [false, true, false], [null, '🦆🦆🦆🦆🦆🦆', null])]), // 40: struct
                struct(3, validity, [
                  list([false, true, false], [null, [0n, 5n], null], 5, data(4, [true, true, false, false, true], [42, 999, null, null, -42])),
                  list([false, true, false], [null, [0n, 4n], null], 4, data(16, [true, true, false, true], ['🦆🦆🦆🦆🦆🦆', 'goose', null, ''])),
                ]), // 41: struct_of_arrays
                list(validity, [[0n, 0n], [0n, 3n], null], 3,
                  struct(3, [true, true, false], [
                    data(4, [false, true], [null, 42]),
                    data(16, [false, true], [null, '🦆🦆🦆🦆🦆🦆']),
                  ])
                ), // 42: array_of_structs
                map(validity, [[0n, 0n], [0n, 2n], null],
                  data(16, [true, true], ['key1', 'key2']),
                  data(16, [true, true], ['🦆🦆🦆🦆🦆🦆', 'goose']),
                ), // 43: map
                union([
                  data(1, validity, [0, 1, null]), // tags
                  data(16, [true, false, false], ['Frank', null, null]), 
                  data(2, [false, true, false], [null, 5, null]),
                ]), // 44: union
                array(3, validity, data(4, [false, true, true, true, true, true], [null, 2, 3, 4, 5, 6])), // 45: fixed_int_array
                array(3, validity, data(16, [true, false, true, true, true, true], ['a', null, 'c', 'd', 'e', 'f'])), // 46: fixed_varchar_array
                array(3, validity,
                  array(3, [true, false, true, true, true, true, false, false, false],
                    data(4, [false, true, true, false, false, false, false, true, true, true, true, true, false, true, true, true, true, true],
                      [null, 2, 3, null, null, null, null, 2, 3, 4, 5, 6, null, 2, 3, 4, 5, 6])
                  )
                ), // 47: fixed_nested_int_array
                array(3, validity,
                  array(3, [true, false, true, true, true, true, false, false, false],
                    data(16, [true, false, true, false, false, false, true, false, true, true, true, true, true, false, true, true, true, true],
                      ['a', null, 'c', null, null, null, 'a', null, 'c', 'd', 'e', 'f', 'a', null, 'c', 'd', 'e', 'f'])
                  )
                ), // 48: fixed_nested_varchar_array
                array(3, validity,
                  struct(9, [true, true, true, true, true, true, false, false, false], [
                    data(4, [false, true, false, true, false, true, false, false, false], [null, 42, null, 42, null, 42, null, null, null]),
                    data(16, [false, true, false, true, false, true, false, false, false], [null, '🦆🦆🦆🦆🦆🦆', null, '🦆🦆🦆🦆🦆🦆', null, '🦆🦆🦆🦆🦆🦆', null, null, null]),
                  ])
                ), // 49: fixed_struct_array
                struct(3, validity, [
                  array(2, [true, true], data(4, [false, true, true, true, true, true], [null, 2, 3, 4, 5, 6])),
                  array(2, [true, true], data(16, [true, false, true, true, true, true], ['a', null, 'c', 'd', 'e', 'f'])),
                ]), // 50: struct_of_fixed_array
                array(3, validity,
                  list([true, true, true, true, true, true, false, false, false], [[0n, 0n], [0n, 5n], [5n, 0n], [5n, 5n], [10n, 0n], [10n, 5n], null, null, null], 15,
                    data(4, [true, true, false, false, true, true, true, false, false, true, true, true, false, false, true],
                      [42, 999, null, null, -42, 42, 999, null, null, -42, 42, 999, null, null, -42])
                  )
                ), // 51: fixed_array_of_int_list
                list(validity, [[0n, 3n], [3n, 3n]], 6,
                  array(6, [true, true, true, true, true, true],
                    data(4, [false, true, true, true, true, true, false, true, true, true, true, true, false, true, true, true, true, true],
                      [null, 2, 3, 4, 5, 6, null, 2, 3, 4, 5, 6, null, 2, 3, 4, 5, 6])
                  )
                ), // 52: list_of_fixed_int_array
              ],
            },
          ],
        });
      } finally {
        duckdb.destroy_result(result);
      }
    });
  });
  test('create and insert', async () => {
    await withConnection(async (connection) => {
      const createResult = await duckdb.query(connection, 'create table test_create_and_insert(i integer)');
      try {
        await expectResult(createResult, {
          statementType: duckdb.StatementType.CREATE,
          resultType: duckdb.ResultType.NOTHING,
          columns: [
            { name: 'Count', logicalType: { typeId: duckdb.Type.BIGINT } },
          ],
          chunks: [
            { columnCount: 0, rowCount: 0, vectors: [] },
          ],
        });
      } finally {
        duckdb.destroy_result(createResult);
      }
      const insertResult = await duckdb.query(connection, 'insert into test_create_and_insert from range(17)');
      try {
        await expectResult(insertResult, {
          statementType: duckdb.StatementType.INSERT,
          resultType: duckdb.ResultType.CHANGED_ROWS,
          rowsChanged: 17,
          columns: [
            { name: 'Count', logicalType: { typeId: duckdb.Type.BIGINT } },
          ],
          chunks: [
            { rowCount: 1, vectors: [data(8, [true], [17n])] },
          ],
        });
      } finally {
        duckdb.destroy_result(insertResult);
      }
    });
  });
});
