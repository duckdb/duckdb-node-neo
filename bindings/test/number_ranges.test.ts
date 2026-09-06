import duckdb, {
  Appender,
  PreparedStatement,
  Value,
} from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { withConnection } from './utils/withConnection';

/**
 * The number-typed integer entry points used to convert their input with N-API's
 * Int32Value or Uint32Value, which wrap out-of-range values modulo 2^32 and
 * truncate fractional ones. That silently stored a value other than the one
 * supplied; now it throws instead. See duckdb/duckdb-node-neo#469.
 */

interface IntegerType {
  name: string;
  sqlType: string;
  min: number;
  max: number;
  create: (input: number) => Value;
  get: (value: Value) => number;
  bind: (prepared: PreparedStatement, index: number, value: number) => void;
  append: (appender: Appender, value: number) => void;
}

const integerTypes: IntegerType[] = [
  {
    name: 'int8',
    sqlType: 'tinyint',
    min: -128,
    max: 127,
    create: (input) => duckdb.create_int8(input),
    get: (value) => duckdb.get_int8(value),
    bind: (prepared, index, value) => duckdb.bind_int8(prepared, index, value),
    append: (appender, value) => duckdb.append_int8(appender, value),
  },
  {
    name: 'uint8',
    sqlType: 'utinyint',
    min: 0,
    max: 255,
    create: (input) => duckdb.create_uint8(input),
    get: (value) => duckdb.get_uint8(value),
    bind: (prepared, index, value) => duckdb.bind_uint8(prepared, index, value),
    append: (appender, value) => duckdb.append_uint8(appender, value),
  },
  {
    name: 'int16',
    sqlType: 'smallint',
    min: -32768,
    max: 32767,
    create: (input) => duckdb.create_int16(input),
    get: (value) => duckdb.get_int16(value),
    bind: (prepared, index, value) => duckdb.bind_int16(prepared, index, value),
    append: (appender, value) => duckdb.append_int16(appender, value),
  },
  {
    name: 'uint16',
    sqlType: 'usmallint',
    min: 0,
    max: 65535,
    create: (input) => duckdb.create_uint16(input),
    get: (value) => duckdb.get_uint16(value),
    bind: (prepared, index, value) => duckdb.bind_uint16(prepared, index, value),
    append: (appender, value) => duckdb.append_uint16(appender, value),
  },
  {
    name: 'int32',
    sqlType: 'integer',
    min: -2147483648,
    max: 2147483647,
    create: (input) => duckdb.create_int32(input),
    get: (value) => duckdb.get_int32(value),
    bind: (prepared, index, value) => duckdb.bind_int32(prepared, index, value),
    append: (appender, value) => duckdb.append_int32(appender, value),
  },
  {
    name: 'uint32',
    sqlType: 'uinteger',
    min: 0,
    max: 4294967295,
    create: (input) => duckdb.create_uint32(input),
    get: (value) => duckdb.get_uint32(value),
    bind: (prepared, index, value) => duckdb.bind_uint32(prepared, index, value),
    append: (appender, value) => duckdb.append_uint32(appender, value),
  },
];

/** Values rejected for every integer type, whatever its range. */
const nonIntegers = [
  { name: 'fractional', input: 1.5 },
  { name: 'NaN', input: NaN },
  { name: 'Infinity', input: Infinity },
  { name: '-Infinity', input: -Infinity },
];

for (const integerType of integerTypes) {
  const { name, sqlType, min, max } = integerType;
  const outOfRange = [
    { name: 'above max', input: max + 1 },
    { name: 'below min', input: min - 1 },
  ];

  suite(name, () => {
    suite(`create_${name}`, () => {
      test('min', () => {
        expect(integerType.get(integerType.create(min))).toBe(min);
      });
      test('max', () => {
        expect(integerType.get(integerType.create(max))).toBe(max);
      });
      for (const { name: caseName, input } of outOfRange) {
        test(caseName, () => {
          expect(() => integerType.create(input)).toThrowError(
            `number out of ${name} range`
          );
        });
      }
      for (const { name: caseName, input } of nonIntegers) {
        test(caseName, () => {
          expect(() => integerType.create(input)).toThrowError(
            'number is not an integer'
          );
        });
      }
    });

    suite(`bind_${name}`, () => {
      async function withPrepared(
        fn: (prepared: PreparedStatement) => void
      ): Promise<void> {
        await withConnection(async (connection) => {
          const prepared = await duckdb.prepare(
            connection,
            `select ?::${sqlType} as v`
          );
          fn(prepared);
        });
      }
      test('min and max', async () => {
        await withPrepared((prepared) => {
          expect(() => integerType.bind(prepared, 1, min)).not.toThrow();
          expect(() => integerType.bind(prepared, 1, max)).not.toThrow();
        });
      });
      for (const { name: caseName, input } of outOfRange) {
        test(caseName, async () => {
          await withPrepared((prepared) => {
            expect(() => integerType.bind(prepared, 1, input)).toThrowError(
              `number out of ${name} range`
            );
          });
        });
      }
      for (const { name: caseName, input } of nonIntegers) {
        test(caseName, async () => {
          await withPrepared((prepared) => {
            expect(() => integerType.bind(prepared, 1, input)).toThrowError(
              'number is not an integer'
            );
          });
        });
      }
    });

    suite(`append_${name}`, () => {
      async function withAppender(
        fn: (appender: Appender) => void
      ): Promise<void> {
        await withConnection(async (connection) => {
          await duckdb.query(connection, `create table t(v ${sqlType})`);
          const appender = duckdb.appender_create_ext(
            connection,
            'memory',
            'main',
            't'
          );
          fn(appender);
        });
      }
      test('min and max', async () => {
        await withAppender((appender) => {
          expect(() => integerType.append(appender, min)).not.toThrow();
          duckdb.appender_end_row(appender);
          expect(() => integerType.append(appender, max)).not.toThrow();
          duckdb.appender_end_row(appender);
        });
      });
      for (const { name: caseName, input } of outOfRange) {
        test(caseName, async () => {
          await withAppender((appender) => {
            expect(() => integerType.append(appender, input)).toThrowError(
              `number out of ${name} range`
            );
          });
        });
      }
      for (const { name: caseName, input } of nonIntegers) {
        test(caseName, async () => {
          await withAppender((appender) => {
            expect(() => integerType.append(appender, input)).toThrowError(
              'number is not an integer'
            );
          });
        });
      }
    });
  });
}

/**
 * The struct-from-object converters had the same defect: they read their numeric
 * fields with Int32Value or Uint32Value, so an out-of-range field wrapped rather
 * than reporting itself. These report the offending field by name, matching the
 * existing "micros out of int64 range" style.
 */
suite('struct fields', () => {
  test('date days', async () => {
    await withConnection(async (connection) => {
      await duckdb.query(connection, 'create table t(v date)');
      const appender = duckdb.appender_create_ext(
        connection,
        'memory',
        'main',
        't'
      );
      expect(() => duckdb.append_date(appender, { days: 2 ** 31 })).toThrowError(
        'days out of int32 range'
      );
      expect(() => duckdb.append_date(appender, { days: 1.5 })).toThrowError(
        'days is not an integer'
      );
      // 2**31 - 1 is the date infinity sentinel and must still be accepted.
      expect(() =>
        duckdb.append_date(appender, { days: 2 ** 31 - 1 })
      ).not.toThrow();
    });
  });
  test('interval months and days', () => {
    expect(() =>
      duckdb.create_interval({ months: 2 ** 31, days: 0, micros: 0n })
    ).toThrowError('months out of int32 range');
    expect(() =>
      duckdb.create_interval({ months: 0, days: -(2 ** 31) - 1, micros: 0n })
    ).toThrowError('days out of int32 range');
    expect(() =>
      duckdb.create_interval({ months: 2 ** 31 - 1, days: 0, micros: 0n })
    ).not.toThrow();
  });
  test('date parts', () => {
    expect(() =>
      duckdb.to_date({ year: 2024, month: 300, day: 1 })
    ).toThrowError('month out of int8 range');
    expect(() =>
      duckdb.to_date({ year: 2024, month: 1, day: 300 })
    ).toThrowError('day out of int8 range');
    expect(() => duckdb.to_date({ year: 2024, month: 1, day: 1 })).not.toThrow();
  });
  test('time parts', () => {
    expect(() =>
      duckdb.to_time({ hour: 200, min: 0, sec: 0, micros: 0 })
    ).toThrowError('hour out of int8 range');
    expect(() =>
      duckdb.to_time({ hour: 0, min: 0, sec: 0, micros: 2 ** 31 })
    ).toThrowError('micros out of int32 range');
    expect(() =>
      duckdb.to_time({ hour: 12, min: 34, sec: 56, micros: 789123 })
    ).not.toThrow();
  });
  test('decimal width and scale', () => {
    expect(() =>
      duckdb.create_decimal({ width: 256, scale: 0, value: 0n })
    ).toThrowError('width out of uint8 range');
    expect(() =>
      duckdb.create_decimal({ width: 4, scale: -1, value: 0n })
    ).toThrowError('scale out of uint8 range');
    expect(() =>
      duckdb.create_decimal({ width: 4, scale: 1, value: 1234n })
    ).not.toThrow();
  });
});
