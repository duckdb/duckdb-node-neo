import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import {
  ARRAY,
  BIGINT,
  BLOB,
  BOOLEAN,
  DATE,
  DECIMAL,
  DOUBLE,
  ENTRY,
  FLOAT,
  HUGEINT,
  INTEGER,
  INTERVAL,
  LIST,
  SMALLINT,
  STRUCT,
  TIME,
  TIMESTAMP,
  TIMESTAMP_TZ,
  TINYINT,
  UBIGINT,
  UHUGEINT,
  UINTEGER,
  USMALLINT,
  UTINYINT,
  VARCHAR,
} from './utils/expectedLogicalTypes';
import { array, data, list, struct } from './utils/expectedVectors';
import { expectResult } from './utils/expectResult';
import { withConnection } from './utils/withConnection';

suite('prepared statements', () => {
  test('no parameters', async () => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, 'select 17 as seventeen');
      try {
        expect(duckdb.nparams(prepared)).toBe(0);
        expect(duckdb.prepared_statement_type(prepared)).toBe(duckdb.StatementType.SELECT);
        const result = await duckdb.execute_prepared(prepared);
        try {
          await expectResult(result, {
            columns: [
              { name: 'seventeen', logicalType: INTEGER },
            ],
            chunks: [
              { rowCount: 1, vectors: [data(4, [true], [17])]},
            ],
          });
        } finally {
          duckdb.destroy_result(result);
        }
      } finally {
        duckdb.destroy_prepare(prepared);
      }
    });
  });
  test('auto-increment parameters', async () => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, 'select ? as a, ? as b');
      try {
        expect(duckdb.prepared_statement_type(prepared)).toBe(duckdb.StatementType.SELECT);
        expect(duckdb.nparams(prepared)).toBe(2);

        expect(duckdb.parameter_name(prepared, 1)).toBe('1');
        expect(duckdb.bind_parameter_index(prepared, '1')).toBe(1);
        duckdb.bind_int32(prepared, 1, 11);
        expect(duckdb.param_type(prepared, 1)).toBe(duckdb.Type.INTEGER);

        expect(duckdb.parameter_name(prepared, 2)).toBe('2');
        expect(duckdb.bind_parameter_index(prepared, '2')).toBe(2);
        duckdb.bind_int32(prepared, 2, 22);
        expect(duckdb.param_type(prepared, 2)).toBe(duckdb.Type.INTEGER);

        const result = await duckdb.execute_prepared(prepared);
        try {
          await expectResult(result, {
            columns: [
              { name: 'a', logicalType: INTEGER },
              { name: 'b', logicalType: INTEGER },
            ],
            chunks: [
              { rowCount: 1, vectors: [data(4, [true], [11]), data(4, [true], [22])] },
            ],
          });
        } finally {
          duckdb.destroy_result(result);
        }
      } finally {
        duckdb.destroy_prepare(prepared);
      }
    });
  });
  test('positional parameters', async () => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, 'select $2 as two, $1 as one');
      try {
        expect(duckdb.prepared_statement_type(prepared)).toBe(duckdb.StatementType.SELECT);
        expect(duckdb.nparams(prepared)).toBe(2);

        expect(duckdb.parameter_name(prepared, 1)).toBe('1');
        expect(duckdb.bind_parameter_index(prepared, '1')).toBe(1);
        duckdb.bind_int32(prepared, 1, 11);
        expect(duckdb.param_type(prepared, 1)).toBe(duckdb.Type.INTEGER);

        expect(duckdb.parameter_name(prepared, 2)).toBe('2');
        expect(duckdb.bind_parameter_index(prepared, '2')).toBe(2);
        duckdb.bind_int32(prepared, 2, 22);
        expect(duckdb.param_type(prepared, 2)).toBe(duckdb.Type.INTEGER);

        const result = await duckdb.execute_prepared(prepared);
        try {
          await expectResult(result, {
            columns: [
              { name: 'two', logicalType: INTEGER },
              { name: 'one', logicalType: INTEGER },
            ],
            chunks: [
              { rowCount: 1, vectors: [data(4, [true], [22]), data(4, [true], [11])] },
            ],
          });
        } finally {
          duckdb.destroy_result(result);
        }
      } finally {
        duckdb.destroy_prepare(prepared);
      }
    });
  });
  test('named parameters', async () => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, 'select $x as a, $y as b');
      try {
        expect(duckdb.prepared_statement_type(prepared)).toBe(duckdb.StatementType.SELECT);
        expect(duckdb.nparams(prepared)).toBe(2);

        expect(duckdb.parameter_name(prepared, 1)).toBe('x');
        expect(duckdb.bind_parameter_index(prepared, 'x')).toBe(1);
        duckdb.bind_int32(prepared, 1, 11);
        // param_type doesn't appear to work for named parameters
        // expect(duckdb.param_type(prepared, 1)).toBe(duckdb.Type.INTEGER);

        expect(duckdb.parameter_name(prepared, 2)).toBe('y');
        expect(duckdb.bind_parameter_index(prepared, 'y')).toBe(2);
        duckdb.bind_int32(prepared, 2, 22);
        // param_type doesn't appear to work for named parameters
        // expect(duckdb.param_type(prepared, 2)).toBe(duckdb.Type.INTEGER);

        const result = await duckdb.execute_prepared(prepared);
        try {
          await expectResult(result, {
            columns: [
              { name: 'a', logicalType: INTEGER },
              { name: 'b', logicalType: INTEGER },
            ],
            chunks: [
              { rowCount: 1, vectors: [data(4, [true], [11]), data(4, [true], [22])] },
            ],
          });
        } finally {
          duckdb.destroy_result(result);
        }
      } finally {
        duckdb.destroy_prepare(prepared);
      }
    });
  });
  test('clear bindings', async () => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection, 'select ? as a, ? as b');
      try {
        duckdb.bind_int32(prepared, 1, 11);
        expect(duckdb.param_type(prepared, 1)).toBe(duckdb.Type.INTEGER);
        duckdb.bind_int32(prepared, 2, 22);
        expect(duckdb.param_type(prepared, 2)).toBe(duckdb.Type.INTEGER);

        duckdb.clear_bindings(prepared);
        expect(duckdb.param_type(prepared, 1)).toBe(duckdb.Type.INVALID);
        expect(duckdb.param_type(prepared, 2)).toBe(duckdb.Type.INVALID);

        duckdb.bind_int32(prepared, 1, 111);
        expect(duckdb.param_type(prepared, 1)).toBe(duckdb.Type.INTEGER);
        duckdb.bind_int32(prepared, 2, 222);
        expect(duckdb.param_type(prepared, 2)).toBe(duckdb.Type.INTEGER);

        const result = await duckdb.execute_prepared(prepared);
        try {
          await expectResult(result, {
            columns: [
              { name: 'a', logicalType: INTEGER },
              { name: 'b', logicalType: INTEGER },
            ],
            chunks: [
              { rowCount: 1, vectors: [data(4, [true], [111]), data(4, [true], [222])] },
            ],
          });
        } finally {
          duckdb.destroy_result(result);
        }
      } finally {
        duckdb.destroy_prepare(prepared);
      }
    });
  });
  test('bind primitive types', async () => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection,
        'select \
        ? as boolean, \
        ? as int8, \
        ? as int16, \
        ? as int32, \
        ? as int64, \
        ? as hugeint, \
        ? as uhugeint, \
        ? as decimal, \
        ? as uint8, \
        ? as uint16, \
        ? as uint32, \
        ? as uint64, \
        ? as float, \
        ? as double, \
        ? as date, \
        ? as time, \
        ? as timestamp, \
        ? as timestamp_tz, \
        ? as interval, \
        ? as varchar, \
        ? as blob, \
        ? as null'
      );
      try {
        duckdb.bind_boolean(prepared, 1, true);
        expect(duckdb.param_type(prepared, 1)).toBe(duckdb.Type.BOOLEAN);

        duckdb.bind_int8(prepared, 2, 127);
        expect(duckdb.param_type(prepared, 2)).toBe(duckdb.Type.TINYINT);

        duckdb.bind_int16(prepared, 3, 32767);
        expect(duckdb.param_type(prepared, 3)).toBe(duckdb.Type.SMALLINT);

        duckdb.bind_int32(prepared, 4, 2147483647);
        expect(duckdb.param_type(prepared, 4)).toBe(duckdb.Type.INTEGER);

        duckdb.bind_int64(prepared, 5, 9223372036854775807n);
        expect(duckdb.param_type(prepared, 5)).toBe(duckdb.Type.BIGINT);

        duckdb.bind_hugeint(prepared, 6, 170141183460469231731687303715884105727n);
        expect(duckdb.param_type(prepared, 6)).toBe(duckdb.Type.HUGEINT);

        duckdb.bind_uhugeint(prepared, 7, 340282366920938463463374607431768211455n);
        expect(duckdb.param_type(prepared, 7)).toBe(duckdb.Type.UHUGEINT);

        duckdb.bind_decimal(prepared, 8, { width: 4, scale: 1, value: 9999n });
        expect(duckdb.param_type(prepared, 8)).toBe(duckdb.Type.DECIMAL);

        duckdb.bind_uint8(prepared, 9, 255);
        expect(duckdb.param_type(prepared, 9)).toBe(duckdb.Type.UTINYINT);

        duckdb.bind_uint16(prepared, 10, 65535);
        expect(duckdb.param_type(prepared, 10)).toBe(duckdb.Type.USMALLINT);

        duckdb.bind_uint32(prepared, 11, 4294967295);
        expect(duckdb.param_type(prepared, 11)).toBe(duckdb.Type.UINTEGER);

        duckdb.bind_uint64(prepared, 12, 18446744073709551615n);
        expect(duckdb.param_type(prepared, 12)).toBe(duckdb.Type.UBIGINT);

        duckdb.bind_float(prepared, 13, 3.4028234663852886e+38);
        expect(duckdb.param_type(prepared, 13)).toBe(duckdb.Type.FLOAT);

        duckdb.bind_double(prepared, 14, 1.7976931348623157e+308);
        expect(duckdb.param_type(prepared, 14)).toBe(duckdb.Type.DOUBLE);

        duckdb.bind_date(prepared, 15, { days: 2147483646 });
        expect(duckdb.param_type(prepared, 15)).toBe(duckdb.Type.DATE);

        duckdb.bind_time(prepared, 16, { micros: 86400000000n });
        expect(duckdb.param_type(prepared, 16)).toBe(duckdb.Type.TIME);

        duckdb.bind_timestamp(prepared, 17, { micros: 9223372036854775806n });
        expect(duckdb.param_type(prepared, 17)).toBe(duckdb.Type.TIMESTAMP);

        duckdb.bind_timestamp_tz(prepared, 18, { micros: 9223372036854775806n });
        expect(duckdb.param_type(prepared, 18)).toBe(duckdb.Type.TIMESTAMP_TZ);

        duckdb.bind_interval(prepared, 19, { months: 999, days: 999, micros: 999999999n });
        expect(duckdb.param_type(prepared, 19)).toBe(duckdb.Type.INTERVAL);

        duckdb.bind_varchar(prepared, 20, '🦆🦆🦆🦆🦆🦆');
        expect(duckdb.param_type(prepared, 20)).toBe(duckdb.Type.VARCHAR);

        duckdb.bind_blob(prepared, 21, Buffer.from('thisisalongblob\x00withnullbytes'));
        expect(duckdb.param_type(prepared, 21)).toBe(duckdb.Type.BLOB);

        duckdb.bind_null(prepared, 22);
        expect(duckdb.param_type(prepared, 22)).toBe(duckdb.Type.SQLNULL);

        const result = await duckdb.execute_prepared(prepared);
        try {
          await expectResult(result, {
            columns: [
              { name: 'boolean', logicalType: BOOLEAN },
              { name: 'int8', logicalType: TINYINT },
              { name: 'int16', logicalType: SMALLINT },
              { name: 'int32', logicalType: INTEGER },
              { name: 'int64', logicalType: BIGINT },
              { name: 'hugeint', logicalType: HUGEINT },
              { name: 'uhugeint', logicalType: UHUGEINT },
              { name: 'decimal', logicalType: DECIMAL(4, 1, duckdb.Type.SMALLINT) },
              { name: 'uint8', logicalType: UTINYINT },
              { name: 'uint16', logicalType: USMALLINT },
              { name: 'uint32', logicalType: UINTEGER },
              { name: 'uint64', logicalType: UBIGINT },
              { name: 'float', logicalType: FLOAT },
              { name: 'double', logicalType: DOUBLE },
              { name: 'date', logicalType: DATE },
              { name: 'time', logicalType: TIME },
              { name: 'timestamp', logicalType: TIMESTAMP },
              { name: 'timestamp_tz', logicalType: TIMESTAMP_TZ },
              { name: 'interval', logicalType: INTERVAL },
              { name: 'varchar', logicalType: VARCHAR },
              { name: 'blob', logicalType: BLOB },
              { name: 'null', logicalType: INTEGER },
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
                  data(16, [true], [340282366920938463463374607431768211455n]),
                  data(2, [true], [9999]),
                  data(1, [true], [255]),
                  data(2, [true], [65535]),
                  data(4, [true], [4294967295]),
                  data(8, [true], [18446744073709551615n]),
                  data(4, [true], [3.4028234663852886e+38]),
                  data(8, [true], [1.7976931348623157e+308]),
                  data(4, [true], [2147483646]),
                  data(8, [true], [86400000000n]),
                  data(8, [true], [9223372036854775806n]),
                  data(8, [true], [9223372036854775806n]),
                  data(16, [true], [{ months: 999, days: 999, micros: 999999999n }]),
                  data(16, [true], ['🦆🦆🦆🦆🦆🦆']),
                  data(16, [true], [Buffer.from('thisisalongblob\x00withnullbytes')]),
                  data(4, [false], [null]),
                ]
              },
            ],
          });
        } finally {
          duckdb.destroy_result(result);
        }
      } finally {
        duckdb.destroy_prepare(prepared);
      }
    });
  });
  test('bind nested types', async () => {
    await withConnection(async (connection) => {
      const prepared = await duckdb.prepare(connection,
        'select \
        ? as struct, \
        ? as list, \
        ? as array'
      );
      try {
        const int_type = duckdb.create_logical_type(duckdb.Type.INTEGER);
        const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
        const struct_type = duckdb.create_struct_type([int_type, varchar_type], ['a', 'b']);

        const int_value = duckdb.create_int64(42n);
        const varchar_value = duckdb.create_varchar('🦆🦆🦆🦆🦆🦆');
        
        const struct_value = duckdb.create_struct_value(struct_type, [int_value, varchar_value]);
        duckdb.bind_value(prepared, 1, struct_value);
        expect(duckdb.param_type(prepared, 1)).toBe(duckdb.Type.STRUCT);

        const list_value = duckdb.create_list_value(int_type, [int_value]);
        duckdb.bind_value(prepared, 2, list_value);
        expect(duckdb.param_type(prepared, 2)).toBe(duckdb.Type.LIST);

        const array_value = duckdb.create_array_value(int_type, [int_value]);
        duckdb.bind_value(prepared, 3, array_value);
        expect(duckdb.param_type(prepared, 3)).toBe(duckdb.Type.ARRAY);

        // TODO: map value?

        const result = await duckdb.execute_prepared(prepared);
        try {
          await expectResult(result, {
            columns: [
              { name: 'struct', logicalType: STRUCT(ENTRY('a', INTEGER), ENTRY('b', VARCHAR)) },
              { name: 'list', logicalType: LIST(INTEGER) },
              { name: 'array', logicalType: ARRAY(INTEGER, 1) },
            ],
            chunks: [
              {
                rowCount: 1,
                vectors: [
                  struct(1, [true], [data(4, [true], [42]), data(16, [true], ['🦆🦆🦆🦆🦆🦆'])]),
                  list([true], [[0n, 1n]], 1, data(4, [true], [42])),
                  array(1, [true], data(4, [true], [42])),
                ]
              },
            ],
          });
        } finally {
          duckdb.destroy_result(result);

          duckdb.destroy_value(int_value);
          duckdb.destroy_value(varchar_value);
          duckdb.destroy_value(struct_value);
          duckdb.destroy_value(list_value);
          duckdb.destroy_value(array_value);

          duckdb.destroy_logical_type(int_type);
          duckdb.destroy_logical_type(varchar_type);
          duckdb.destroy_logical_type(struct_type);
        }
      } finally {
        duckdb.destroy_prepare(prepared);
      }
    });
  });
});
