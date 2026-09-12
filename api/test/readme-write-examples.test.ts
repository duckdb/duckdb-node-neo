// Runs the examples from the write-side sections of the package README, so
// they cannot drift from the API they document. Kept in step with:
//   "Specifying Values", "Infer JS Types from DuckDB Types",
//   "Append Data Chunk of JS Values", "Buffer Rows Into Data Chunks"
import { expect, test } from 'vitest';
import {
  DATE, DuckDBBigIntType, DuckDBDataChunk, DuckDBDataChunkWriter, DuckDBInstance,
  DuckDBIntegerType, DuckDBTimestampTZType, DuckDBTypeId, INTEGER, JSToDuckDBValueConverter,
  JSTypeFor, JSTypeForTypeId, JsonTypeFor, TIMESTAMP, TIMESTAMP_MS, VARCHAR,
  dateValue, jsToDuckDBValue, timestampMillisValue,
} from '../src';

// --- "Specifying Values" additions ---
test('README: jsToDuckDBValue examples', () => {
  expect(jsToDuckDBValue(new Date('2024-01-15T12:34:56.789Z'), TIMESTAMP_MS))
    .toEqual(timestampMillisValue(1705322096789n));
  expect(jsToDuckDBValue(new Date('2024-01-15T00:00:00.000Z'), DATE))
    .toEqual(dateValue(19737));
  expect(jsToDuckDBValue(null, DATE)).toBeNull();
  void (() => {
    jsToDuckDBValue(new Date(), TIMESTAMP);
    // @ts-expect-error README claims this is rejected at compile time
    jsToDuckDBValue(new Date(), INTEGER);
  });
});

// --- "Infer JS Types from DuckDB Types" ---
type Eq<A, B> = (<G>() => G extends A ? 1 : 2) extends <G>() => G extends B ? 1 : 2 ? true : false;
type Assert<T extends true> = T;
export type _README = [
  Assert<Eq<JSTypeFor<DuckDBIntegerType>, number>>,
  Assert<Eq<JSTypeFor<DuckDBBigIntType>, bigint>>,
  Assert<Eq<JSTypeFor<DuckDBTimestampTZType>, Date>>,
  Assert<Eq<JsonTypeFor<DuckDBBigIntType>, string>>,
  Assert<Eq<JsonTypeFor<DuckDBTimestampTZType>, string>>,
  Assert<Eq<JSTypeForTypeId[DuckDBTypeId.INTEGER], number>>,
];

// --- "Append Data Chunk of JS Values" ---
test('README: setRowsConverted example', async () => {
  const connection = await (await DuckDBInstance.create(':memory:')).connect();
  await connection.run(
    `create or replace table target_table(
      i integer, v varchar, t timestamp
    )`
  );
  const appender = await connection.createAppender('target_table');
  const chunk = DuckDBDataChunk.create([INTEGER, VARCHAR, TIMESTAMP]);
  chunk.setRowsConverted(
    [
      [42, 'duck', new Date('2024-01-15T12:34:56.000Z')],
      [123, 'mallard', null],
    ],
    JSToDuckDBValueConverter
  );
  appender.appendDataChunk(chunk);
  appender.flushSync();

  const reader = await connection.runAndReadAll(`select * from target_table order by i`);
  expect(reader.getRowsJS()).toEqual([
    [42, 'duck', new Date('2024-01-15T12:34:56.000Z')],
    [123, 'mallard', null],
  ]);
});

// --- "Buffer Rows Into Data Chunks" ---
test('README: DuckDBDataChunkWriter example', async () => {
  const connection = await (await DuckDBInstance.create(':memory:')).connect();
  await connection.run(`create or replace table target_table(i integer, v varchar)`);
  const appender = await connection.createAppender('target_table');
  const writer = DuckDBDataChunkWriter.forAppender(appender, {
    converter: JSToDuckDBValueConverter,
  });
  writer.appendRow([42, 'duck']);
  writer.appendRow([123, 'mallard']);
  writer.appendRow([17, 'goose']);
  writer.flush();
  appender.closeSync();

  const reader = await connection.runAndReadAll(`select * from target_table order by i`);
  expect(reader.getRowsJS()).toEqual([
    [17, 'goose'],
    [42, 'duck'],
    [123, 'mallard'],
  ]);
});
