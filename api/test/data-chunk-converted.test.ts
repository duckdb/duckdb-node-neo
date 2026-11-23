import { expect, test } from 'vitest';
import {
  ARRAY,
  BIGINT,
  BLOB,
  BOOLEAN,
  DATE,
  DECIMAL,
  DOUBLE,
  DuckDBConnection,
  DuckDBDataChunk,
  DuckDBInstance,
  DuckDBType,
  INTEGER,
  JS,
  JSToDuckDBValueConverter,
  LIST,
  STRUCT,
  TIMESTAMP,
  TIMESTAMP_MS,
  UUID,
  VARCHAR,
  VARIANT,
  listValue,
} from '../src';

async function connect(): Promise<DuckDBConnection> {
  return (await DuckDBInstance.create(':memory:')).connect();
}

async function roundTrip(
  types: readonly DuckDBType[],
  rows: readonly (readonly (JS | null)[])[]
): Promise<Record<string, JS>[]> {
  const connection = await connect();
  const columns = types.map((t, i) => `c${i} ${t}`).join(', ');
  await connection.run(`create table t (${columns})`);
  const appender = await connection.createAppender('t');
  const capacity = 2048;
  for (let i = 0; i < rows.length; i += capacity) {
    const chunk = DuckDBDataChunk.create(types);
    chunk.setRowsConverted(rows.slice(i, i + capacity), JSToDuckDBValueConverter);
    appender.appendDataChunk(chunk);
  }
  appender.closeSync();
  const reader = await connection.runAndReadAll(`select * from t`);
  return reader.getRowObjectsJS();
}

test('scalars round-trip through a data chunk', async () => {
  const ts = new Date('2024-01-15T12:34:56.000Z');
  const result = await roundTrip(
    [INTEGER, BIGINT, VARCHAR, BOOLEAN, DOUBLE, BLOB, DATE, TIMESTAMP],
    [
      [
        42,
        9007199254740993n,
        'hello',
        true,
        1.5,
        new Uint8Array([1, 2, 3]),
        new Date('2024-01-15T00:00:00.000Z'),
        ts,
      ],
      [null, null, null, null, null, null, null, null],
    ]
  );
  expect(result[0]['c0']).toBe(42);
  expect(result[0]['c1']).toBe(9007199254740993n);
  expect(result[0]['c2']).toBe('hello');
  expect(result[0]['c3']).toBe(true);
  expect(result[0]['c4']).toBe(1.5);
  expect(Array.from(result[0]['c5'] as Uint8Array)).toEqual([1, 2, 3]);
  expect(result[0]['c6']).toEqual(new Date('2024-01-15T00:00:00.000Z'));
  expect(result[0]['c7']).toEqual(ts);
  expect(Object.values(result[1]).every((v) => v === null)).toBe(true);
});

test('BIGINT accepts number as well as bigint', async () => {
  const result = await roundTrip([BIGINT], [[42], [42n]]);
  expect(result[0]['c0']).toBe(42n);
  expect(result[1]['c0']).toBe(42n);
});

test('parameterized and nested types round-trip', async () => {
  const result = await roundTrip(
    [
      DECIMAL(9, 2),
      UUID,
      TIMESTAMP_MS,
      LIST(INTEGER),
      ARRAY(INTEGER, 2),
      STRUCT({ a: INTEGER, b: VARCHAR }),
    ],
    [
      [
        12.34,
        '10203040-5060-7080-90a0-b0c0d0e0f000',
        new Date('2024-01-15T12:34:56.789Z'),
        [1, 2, 3],
        [7, 8],
        { a: 1, b: 'x' },
      ],
    ]
  );
  expect(result[0]['c0']).toBe(12.34);
  expect(result[0]['c1']).toBe('10203040-5060-7080-90a0-b0c0d0e0f000');
  expect(result[0]['c2']).toEqual(new Date('2024-01-15T12:34:56.789Z'));
  expect(result[0]['c3']).toEqual([1, 2, 3]);
  expect(result[0]['c4']).toEqual([7, 8]);
  expect(result[0]['c5']).toEqual({ a: 1, b: 'x' });
});

test('writes span multiple data chunks', async () => {
  const rows = Array.from({ length: 5000 }, (_, i) => [i]);
  const result = await roundTrip([INTEGER], rows);
  expect(result.length).toBe(5000);
  expect(result[4999]['c0']).toBe(4999);
});

test('setRowsConverted fills a chunk from JS rows', async () => {
  const connection = await connect();
  await connection.run(`create table t (a integer, b varchar, c timestamp)`);
  const appender = await connection.createAppender('t');

  const chunk = DuckDBDataChunk.create([INTEGER, VARCHAR, TIMESTAMP]);
  chunk.setRowsConverted(
    [
      [1, 'x', new Date('2024-01-15T00:00:00.000Z')],
      [2, null, null],
    ],
    JSToDuckDBValueConverter
  );
  appender.appendDataChunk(chunk);
  appender.closeSync();

  const reader = await connection.runAndReadAll(`select * from t order by a`);
  expect(reader.getRowObjectsJS()).toEqual([
    { a: 1, b: 'x', c: new Date('2024-01-15T00:00:00.000Z') },
    { a: 2, b: null, c: null },
  ]);
});

test('setColumnsConverted fills a chunk from JS columns', async () => {
  const connection = await connect();
  await connection.run(`create table t (a integer, b varchar)`);
  const appender = await connection.createAppender('t');

  const chunk = DuckDBDataChunk.create([INTEGER, VARCHAR]);
  chunk.setColumnsConverted([[1, 2], ['x', 'y']], JSToDuckDBValueConverter);
  appender.appendDataChunk(chunk);
  appender.closeSync();

  const reader = await connection.runAndReadAll(`select * from t order by a`);
  expect(reader.getRowsJS()).toEqual([
    [1, 'x'],
    [2, 'y'],
  ]);
});

test('conversion errors name the column', () => {
  const chunk = DuckDBDataChunk.create([INTEGER, UUID]);
  expect(() =>
    chunk.setRowsConverted([[1, 'not-a-uuid']], JSToDuckDBValueConverter)
  ).toThrowError('Failed to set column 1 (UUID)');
});

test('a VARIANT column takes converted values', async () => {
  const connection = await connect();
  await connection.run(`create table t (i integer, v variant)`);
  const chunk = DuckDBDataChunk.create([INTEGER, VARIANT]);
  chunk.setRowsConverted(
    [
      [1, 42],
      [2, 'x'],
    ],
    JSToDuckDBValueConverter
  );
  const appender = await connection.createAppender('t');
  appender.appendDataChunk(chunk);
  appender.closeSync();

  const reader = await connection.runAndReadAll(
    `select i, variant_typeof(v), v::varchar from t order by i`
  );
  expect(reader.getRowsJS()).toEqual([
    [1, 'INT32', '42'],
    [2, 'VARCHAR', 'x'],
  ]);
});

test('a bad VARIANT value reports the column', () => {
  const chunk = DuckDBDataChunk.create([INTEGER, VARIANT]);
  expect(() =>
    chunk.setRowsConverted([[1, 2n ** 127n]], JSToDuckDBValueConverter)
  ).toThrowError('Failed to set column 1 (VARIANT)');
});

test('setting some rows of a LIST from a result keeps the others', async () => {
  // Flushing a list rewrites every row's offset; the rows not set must be
  // read at their old offsets first.
  const connection = await connect();
  const result = await connection.run(
    `select [i, i + 1] as l from range(3) t(i)`
  );
  const chunk = await result.fetchChunk();
  const vector = chunk!.getColumnVector(0);
  vector.setItem(0, listValue([10n, 20n, 30n]));
  vector.flush();
  await connection.run(`create table t (l bigint[])`);
  const appender = await connection.createAppender('t');
  appender.appendDataChunk(chunk!);
  appender.closeSync();

  const reader = await connection.runAndReadAll(`select l from t`);
  expect(reader.getRowsJS()).toEqual([
    [[10n, 20n, 30n]],
    [[1n, 2n]],
    [[2n, 3n]],
  ]);
});

test('a short columns array is refused rather than left unwritten', () => {
  const chunk = DuckDBDataChunk.create([INTEGER, VARCHAR]);
  // Without this the second vector is never written or flushed while the chunk
  // reports two rows, and using it afterwards crashes the process.
  expect(() =>
    chunk.setColumnsConverted([[1, 2]], JSToDuckDBValueConverter)
  ).toThrowError(
    'Provided number of columns (1) does not match chunk column count (2)'
  );
  expect(() =>
    chunk.setColumnsConverted([[1, 2], ['x', 'y']], JSToDuckDBValueConverter)
  ).not.toThrow();
});
