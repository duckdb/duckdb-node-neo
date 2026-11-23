import { expect, test } from 'vitest';
import {
  BIGNUM,
  BIT,
  BLOB,
  DuckDBBlobVector,
  DuckDBDataChunk,
  DuckDBConnection,
  DuckDBGeometryValue,
  DuckDBType,
  DuckDBValue,
  ARRAY,
  GEOMETRY,
  INTEGER,
  STRUCT,
  VARCHAR,
  arrayValue,
  bitValue,
  blobValue,
  structValue,
} from '../src';
import { getBigNumFromBytes } from '../src/vectors/dataAccessors';
import { withConnection } from './util/testHelpers';

async function geometry(
  connection: DuckDBConnection,
  wkt: string
): Promise<DuckDBGeometryValue> {
  const reader = await connection.runAndReadAll(
    `select '${wkt}'::GEOMETRY as g`
  );
  return reader.getRows()[0][0] as DuckDBGeometryValue;
}

// Each type gets a short value (stored inline, twelve bytes or fewer) and a
// long one (stored behind a pointer).
async function cases(
  connection: DuckDBConnection
): Promise<[DuckDBType, DuckDBValue, DuckDBValue][]> {
  return [
    [VARCHAR, 'short', 'a string longer than twelve bytes'],
    [BLOB, blobValue('short'), blobValue('a blob longer than twelve bytes')],
    [BIT, bitValue('0101'), bitValue('01'.repeat(100))],
    [BIGNUM, 12345n, -(10n ** 100n)],
    [
      GEOMETRY,
      await geometry(connection, 'POINT(1 2)'),
      await geometry(connection, 'LINESTRING(0 0, 1 1, 2 2)'),
    ],
  ];
}

test('a vector reads back its own writes, before and after flush', async () => {
  await withConnection(async (connection) => {
    for (const [type, short, long] of await cases(connection)) {
      const chunk = DuckDBDataChunk.create([type], 3);
      const vector = chunk.getColumnVector(0);
      vector.setItem(0, short);
      vector.setItem(1, long);
      vector.setItem(2, short);
      vector.setItem(2, null);
      for (const when of ['before flush', 'after flush']) {
        expect(
          [vector.getItem(0), vector.getItem(1), vector.getItem(2)],
          `${type} ${when}`
        ).toEqual([short, long, null]);
        vector.flush();
      }
    }
  });
});

test('the written values are what DuckDB stores', async () => {
  await withConnection(async (connection) => {
    for (const [type, short, long] of await cases(connection)) {
      await connection.run(`create or replace table t (v ${type})`);
      const chunk = DuckDBDataChunk.create([type]);
      chunk.setRows([[short], [long], [null]]);
      const appender = await connection.createAppender('t');
      appender.appendDataChunk(chunk);
      appender.closeSync();
      const reader = await connection.runAndReadAll(`select v from t`);
      // Compared as strings: DuckDB returns a Buffer where the input may hold
      // a plain Uint8Array.
      expect(
        reader.getRows().map(([v]) => (v === null ? null : String(v))),
        String(type)
      ).toEqual([String(short), String(long), null]);
    }
  });
});

test('a BLOB vector returns written bytes from getItemBytes', () => {
  const chunk = DuckDBDataChunk.create([BLOB], 2);
  const vector = chunk.getColumnVector(0) as DuckDBBlobVector;
  vector.setItem(0, blobValue('abc'));
  vector.setItem(1, null);
  expect(Array.from(vector.getItemBytes(0)!)).toEqual([97, 98, 99]);
  expect(vector.getItemBytes(1)).toBeNull();
});

test('too few bytes for a BIGNUM is a clear error', () => {
  expect(() => getBigNumFromBytes(new Uint8Array(0))).toThrowError(
    'Malformed BIGNUM: 0 bytes, expected at least 4'
  );
});

// Writing through a slice. One SQL expression per fixed-width vector class,
// giving a distinct value for each `i` in 1..5.
const fixedWidthTypes: [string, string][] = [
  ['BOOLEAN', 'i % 2 = 0'],
  ['TINYINT', 'i::TINYINT'],
  ['SMALLINT', 'i::SMALLINT'],
  ['INTEGER', 'i::INTEGER'],
  ['BIGINT', 'i::BIGINT'],
  ['HUGEINT', 'i::HUGEINT'],
  ['UTINYINT', 'i::UTINYINT'],
  ['USMALLINT', 'i::USMALLINT'],
  ['UINTEGER', 'i::UINTEGER'],
  ['UBIGINT', 'i::UBIGINT'],
  ['UHUGEINT', 'i::UHUGEINT'],
  ['FLOAT', '(i / 2)::FLOAT'],
  ['DOUBLE', '(i / 2)::DOUBLE'],
  ['DECIMAL(4,1)', '(i / 2)::DECIMAL(4,1)'],
  ['DECIMAL(9,2)', '(i / 2)::DECIMAL(9,2)'],
  ['DECIMAL(18,3)', '(i / 2)::DECIMAL(18,3)'],
  ['DECIMAL(38,4)', '(i / 2)::DECIMAL(38,4)'],
  ['DATE', "DATE '2024-01-01' + i::INTEGER"],
  ['TIME', "TIME '00:00:00' + to_seconds(i)"],
  ['TIME_NS', "(TIME '00:00:00' + to_seconds(i))::TIME_NS"],
  ['TIMETZ', "('12:00:0' || i || '+01')::TIMETZ"],
  ['TIMESTAMP', "TIMESTAMP '2024-01-01' + to_days(i::INTEGER)"],
  ['TIMESTAMP_S', "(TIMESTAMP '2024-01-01' + to_days(i::INTEGER))::TIMESTAMP_S"],
  ['TIMESTAMP_MS', "(TIMESTAMP '2024-01-01' + to_days(i::INTEGER))::TIMESTAMP_MS"],
  ['TIMESTAMP_NS', "(TIMESTAMP '2024-01-01' + to_days(i::INTEGER))::TIMESTAMP_NS"],
  ['TIMESTAMPTZ', "('2024-01-0' || i || ' 00:00:00+00')::TIMESTAMPTZ"],
  ['TIMESTAMPTZ_NS', "('2024-01-0' || i || ' 00:00:00+00')::TIMESTAMPTZ_NS"],
  ['INTERVAL', 'to_days(i::INTEGER)'],
  ['UUID', "('00000000-0000-0000-0000-00000000000' || i)::UUID"],
  ['ENUM', "('e' || i)::ENUM('e1', 'e2', 'e3', 'e4', 'e5')"],
  ['ENUM16', "('e' || i)::ENUM(" + Array.from({ length: 300 }, (_, i) => `'e${i}'`).join(', ') + ')'],
  ['ENUM32', "('e' || i)::ENUM(" + Array.from({ length: 70000 }, (_, i) => `'e${i}'`).join(', ') + ')'],
];

test('a write through a slice lands in the slice\'s rows', async () => {
  await withConnection(async (connection) => {
    for (const [name, expression] of fixedWidthTypes) {
      const source = await connection.runAndReadAll(
        `select ${expression} as v from range(1, 6) t(i)`
      );
      const type = source.columnType(0);
      const [a, b, c, d, e] = source.getRows().map(([v]) => v);
      const chunk = DuckDBDataChunk.create([type]);
      chunk.setRows([[a], [b], [c], [d]]);
      const parent = chunk.getColumnVector(0);
      const slice = parent.slice(2, 2);
      slice.setItem(0, e); // row 2
      parent.setItem(3, a); // row 3, which the slice also covers
      expect(parent.getItem(2), name).toEqual(e);
      expect(slice.getItem(1), name).toEqual(a);
      slice.flush();
      // A later flush of the parent must not undo the slice's write.
      parent.flush();

      await connection.run(`create or replace table t (v ${type})`);
      const appender = await connection.createAppender('t');
      appender.appendDataChunk(chunk);
      appender.closeSync();
      const stored = await connection.runAndReadAll(`select v from t`);
      expect(
        stored.getRows().map(([v]) => String(v)),
        name
      ).toEqual([a, b, e, a].map(String));
    }
  });
});

test('a NULL written through a slice is stored in the slice\'s row', async () => {
  await withConnection(async (connection) => {
    // No NULLs yet, so the vector has no validity mask until the slice
    // writes one, and row 100 is past the first 64 rows.
    const chunk = DuckDBDataChunk.create([INTEGER]);
    chunk.setRows(Array.from({ length: 130 }, (_, i) => [i]));
    const parent = chunk.getColumnVector(0);
    const slice = parent.slice(100, 10);
    slice.setItem(0, null);
    expect(parent.getItem(100)).toBeNull();
    expect(parent.getItem(101)).toBe(101);
    slice.flush();

    await connection.run(`create table t (v INTEGER)`);
    const appender = await connection.createAppender('t');
    appender.appendDataChunk(chunk);
    appender.closeSync();
    const reader = await connection.runAndReadAll(
      `select rowid from t where v is null`
    );
    expect(reader.getRowsJS()).toEqual([[100n]]);
  });
});

test('writes through slices of STRUCT and ARRAY columns land in place', async () => {
  await withConnection(async (connection) => {
    const types = [STRUCT({ a: INTEGER, b: VARCHAR }), ARRAY(INTEGER, 2)];
    const chunk = DuckDBDataChunk.create(types);
    chunk.setRows(
      [1, 2, 3].map((i) => [
        structValue({ a: i, b: `s${i}` }),
        arrayValue([i, i * 10]),
      ])
    );
    const struct = chunk.getColumnVector(0).slice(1, 2);
    struct.setItem(1, structValue({ a: 99, b: 'new' }));
    struct.flush();
    const array = chunk.getColumnVector(1).slice(1, 2);
    array.setItem(1, arrayValue([7, 8]));
    array.flush();

    await connection.run(
      `create table t (s STRUCT(a INTEGER, b VARCHAR), a INTEGER[2])`
    );
    const appender = await connection.createAppender('t');
    appender.appendDataChunk(chunk);
    appender.closeSync();
    const reader = await connection.runAndReadAll(
      `select s.a, s.b, a from t`
    );
    expect(reader.getRowsJS()).toEqual([
      [1, 's1', [1, 10]],
      [2, 's2', [2, 20]],
      [99, 'new', [7, 8]],
    ]);
  });
});
