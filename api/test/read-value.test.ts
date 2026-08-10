import { assert, beforeAll, describe, expect, test } from 'vitest';
import {
  ARRAY,
  BIGINT,
  BIGNUM,
  BIT,
  BLOB,
  BOOLEAN,
  DATE,
  DECIMAL,
  DOUBLE,
  DuckDBArrayValue,
  DuckDBBitValue,
  DuckDBBlobValue,
  DuckDBDateValue,
  DuckDBDecimalValue,
  DuckDBGeometryValue,
  DuckDBIntervalValue,
  DuckDBListValue,
  DuckDBMapValue,
  DuckDBStructValue,
  DuckDBTableFunction,
  DuckDBTimeValue,
  DuckDBTimestampValue,
  DuckDBType,
  DuckDBUnionValue,
  DuckDBUUIDValue,
  DuckDBValue,
  FLOAT,
  GEOMETRY,
  HUGEINT,
  INTEGER,
  INTERVAL,
  LIST,
  MAP,
  SMALLINT,
  STRUCT,
  TIME,
  TIMESTAMP,
  TINYINT,
  UBIGINT,
  UHUGEINT,
  UINTEGER,
  USMALLINT,
  UTINYINT,
  UNION,
  UUID,
  VARCHAR,
} from '../src';
import { setDefaultTimezone, withConnection } from './util/testHelpers';

// readValue only runs where the C API hands back a duckdb_value rather than a
// vector, and a table function parameter is the way to reach it from the api.
// Each case declares a parameter of the given type, passes the given SQL literal,
// and reports what the bind function received.
async function readParameter(
  type: DuckDBType,
  literal: string,
): Promise<DuckDBValue> {
  let received: DuckDBValue | undefined;
  let threw: Error | undefined;
  await withConnection(async (connection) => {
    connection.registerTableFunction(
      DuckDBTableFunction.create({
        name: 'my_func',
        parameterTypes: [type],
        bindFunction: (info) => {
          try {
            received = info.getParameter(0);
          } catch (e) {
            threw = e as Error;
          }
          info.addResultColumn('my_column', INTEGER);
          info.setBindData({});
        },
        initFunction: (info) => {
          info.setInitData({});
        },
        mainFunction: (_info, output) => {
          output.rowCount = 0;
        },
      }),
    );
    await connection.runAndReadAll(`select * from my_func(${literal})`);
  });
  if (threw) {
    throw threw;
  }
  return received as DuckDBValue;
}

describe('readValue', () => {
  beforeAll(setDefaultTimezone);

  test('booleans and integers', async () => {
    expect(await readParameter(BOOLEAN, 'true')).toBe(true);
    expect(await readParameter(TINYINT, '(-128)::TINYINT')).toBe(-128);
    expect(await readParameter(SMALLINT, '(-32768)::SMALLINT')).toBe(-32768);
    expect(await readParameter(INTEGER, '(-2147483648)::INTEGER')).toBe(-2147483648);
    expect(await readParameter(BIGINT, '(-9223372036854775808)::BIGINT')).toBe(
      -9223372036854775808n,
    );
    expect(await readParameter(UTINYINT, '255::UTINYINT')).toBe(255);
    expect(await readParameter(USMALLINT, '65535::USMALLINT')).toBe(65535);
    expect(await readParameter(UINTEGER, '4294967295::UINTEGER')).toBe(4294967295);
    expect(await readParameter(UBIGINT, '18446744073709551615::UBIGINT')).toBe(
      18446744073709551615n,
    );
    expect(
      await readParameter(HUGEINT, '170141183460469231731687303715884105727::HUGEINT'),
    ).toBe(170141183460469231731687303715884105727n);
    expect(
      await readParameter(UHUGEINT, '340282366920938463463374607431768211455::UHUGEINT'),
    ).toBe(340282366920938463463374607431768211455n);
  });

  test('floats, decimals and bignums', async () => {
    expect(await readParameter(FLOAT, '0.5::FLOAT')).toBe(0.5);
    expect(await readParameter(DOUBLE, '0.25::DOUBLE')).toBe(0.25);
    const decimal = await readParameter(DECIMAL(5, 2), "'123.45'::DECIMAL(5,2)");
    assert.instanceOf(decimal, DuckDBDecimalValue);
    expect((decimal as DuckDBDecimalValue).value).toBe(12345n);
    expect((decimal as DuckDBDecimalValue).width).toBe(5);
    expect((decimal as DuckDBDecimalValue).scale).toBe(2);
    expect(await readParameter(BIGNUM, "'12345678901234567890'::VARINT")).toBe(
      12345678901234567890n,
    );
  });

  test('strings, blobs and bits', async () => {
    expect(await readParameter(VARCHAR, "'hello'")).toBe('hello');
    const blob = await readParameter(BLOB, "'\\x41\\x42'::BLOB");
    assert.instanceOf(blob, DuckDBBlobValue);
    expect(Array.from((blob as DuckDBBlobValue).bytes)).toEqual([0x41, 0x42]);
    const bit = await readParameter(BIT, "'101'::BIT");
    assert.instanceOf(bit, DuckDBBitValue);
    expect(bit.toString()).toBe('101');
  });

  test('temporal types', async () => {
    const date = await readParameter(DATE, "'2024-06-01'::DATE");
    assert.instanceOf(date, DuckDBDateValue);
    expect(date.toString()).toBe('2024-06-01');
    const time = await readParameter(TIME, "'12:34:56'::TIME");
    assert.instanceOf(time, DuckDBTimeValue);
    expect(time.toString()).toBe('12:34:56');
    const timestamp = await readParameter(
      TIMESTAMP,
      "'2024-06-01 12:34:56'::TIMESTAMP",
    );
    assert.instanceOf(timestamp, DuckDBTimestampValue);
    expect(timestamp.toString()).toBe('2024-06-01 12:34:56');
    const interval = await readParameter(INTERVAL, "INTERVAL 3 DAY");
    assert.instanceOf(interval, DuckDBIntervalValue);
    expect((interval as DuckDBIntervalValue).days).toBe(3);
  });

  test('uuid', async () => {
    const uuid = await readParameter(
      UUID,
      "'10203040-5060-7080-90a0-b0c0d0e0f000'::UUID",
    );
    // toBeInstanceOf rather than assert.instanceOf: the constructor is private.
    expect(uuid).toBeInstanceOf(DuckDBUUIDValue);
    expect(uuid?.toString()).toBe('10203040-5060-7080-90a0-b0c0d0e0f000');
  });

  test('geometry', async () => {
    // Well known binary for POINT(1 2): byte order, type 1, then two doubles.
    const geometry = await readParameter(GEOMETRY, "'POINT(1 2)'::GEOMETRY");
    assert.instanceOf(geometry, DuckDBGeometryValue);
    const bytes = (geometry as DuckDBGeometryValue).bytes;
    expect(bytes.length).toBe(21);
    expect(bytes[0]).toBe(1); // little endian
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    expect(view.getUint32(1, true)).toBe(1); // point
    expect(view.getFloat64(5, true)).toBe(1);
    expect(view.getFloat64(13, true)).toBe(2);
  });

  // VARIANT has no coverage here: DuckDB fails with an internal error while
  // binding a call to a table function that declares a VARIANT parameter, before
  // the bind callback runs, so a value of that type cannot be reached this way.

  test('arrays and unions, which the value accessors cannot read', async () => {
    // duckdb_get_list_child and duckdb_get_struct_child guard on the exact type
    // id, so neither an array nor a union can be read through them. Referencing
    // the value into a vector is what makes these work.
    const union = await readParameter(
      UNION({ i: INTEGER, s: VARCHAR }),
      "union_value(s := 'hello')",
    );
    assert.instanceOf(union, DuckDBUnionValue);
    expect((union as DuckDBUnionValue).tag).toBe('s');
    expect((union as DuckDBUnionValue).value).toBe('hello');
  });

  test('nested types', async () => {
    const list = await readParameter(LIST(INTEGER), '[1, 2, 3]');
    assert.instanceOf(list, DuckDBListValue);
    expect((list as DuckDBListValue).items).toEqual([1, 2, 3]);

    const array = await readParameter(
      ARRAY(INTEGER, 3),
      '[1, 2, 3]::INTEGER[3]',
    );
    assert.instanceOf(array, DuckDBArrayValue);
    expect((array as DuckDBArrayValue).items).toEqual([1, 2, 3]);

    const struct = await readParameter(
      STRUCT({ a: INTEGER, b: VARCHAR }),
      "{'a': 1, 'b': 'two'}",
    );
    assert.instanceOf(struct, DuckDBStructValue);
    expect((struct as DuckDBStructValue).entries).toEqual({ a: 1, b: 'two' });

    const map = await readParameter(
      MAP(VARCHAR, INTEGER),
      "MAP{'a': 1, 'b': 2}",
    );
    assert.instanceOf(map, DuckDBMapValue);
    expect((map as DuckDBMapValue).entries).toEqual([
      { key: 'a', value: 1 },
      { key: 'b', value: 2 },
    ]);

    const nested = await readParameter(
      LIST(STRUCT({ a: INTEGER })),
      "[{'a': 1}, {'a': 2}]",
    );
    assert.instanceOf(nested, DuckDBListValue);
    expect(
      ((nested as DuckDBListValue).items as DuckDBStructValue[]).map(
        (s) => s.entries,
      ),
    ).toEqual([{ a: 1 }, { a: 2 }]);
  });
});
