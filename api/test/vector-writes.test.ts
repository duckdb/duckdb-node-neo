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
  GEOMETRY,
  VARCHAR,
  bitValue,
  blobValue,
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
