import { expect, test } from 'vitest';
import duckdb from '@duckdb/node-bindings';
import {
  DuckDBConnection,
  DuckDBDataChunk,
  DuckDBDataChunkWriter,
  DuckDBInstance,
  JSToDuckDBValueConverter,
  DATE,
  INTEGER,
  JS,
  TIMESTAMP_MS,
  UUID,
  VARCHAR,
  dateValue,
} from '../src';

async function connect(): Promise<DuckDBConnection> {
  return (await DuckDBInstance.create(':memory:')).connect();
}

test('rows are emitted as one data chunk per rowsPerDataChunk', () => {
  const emitted: number[] = [];
  const writer = new DuckDBDataChunkWriter(
    [INTEGER],
    (chunk) => emitted.push(chunk.rowCount),
    { rowsPerDataChunk: 3 }
  );
  for (let i = 0; i < 7; i++) {
    writer.appendRow([i]);
  }
  expect(emitted).toEqual([3, 3]);
  expect(writer.bufferedRowCount).toBe(1);
  writer.flush();
  expect(emitted).toEqual([3, 3, 1]);
  expect(writer.bufferedRowCount).toBe(0);
});

test('flush is a no-op when nothing is buffered', () => {
  let emitted = 0;
  const writer = new DuckDBDataChunkWriter([INTEGER], () => emitted++);
  writer.flush();
  expect(emitted).toBe(0);
  writer.appendRow([1]);
  writer.flush();
  writer.flush();
  expect(emitted).toBe(1);
});

test('rowsPerDataChunk defaults to the DuckDB vector size', () => {
  const emitted: number[] = [];
  const writer = new DuckDBDataChunkWriter([INTEGER], (chunk) =>
    emitted.push(chunk.rowCount)
  );
  const size = duckdb.vector_size();
  for (let i = 0; i < size; i++) {
    writer.appendRow([i]);
  }
  expect(emitted).toEqual([size]);
});

test('a row whose length does not match the types is refused', () => {
  const writer = new DuckDBDataChunkWriter([INTEGER, VARCHAR], () => {});
  expect(() => writer.appendRow([1])).toThrowError(
    'Provided number of values (1) does not match number of types (2)'
  );
  expect(() => writer.appendRow([1, 'x', 2])).toThrowError(
    'Provided number of values (3) does not match number of types (2)'
  );
});

test('rows written through an appender round-trip', async () => {
  const connection = await connect();
  await connection.run(
    `create table t (x integer, s varchar, ts timestamp_ms)`
  );
  const appender = await connection.createAppender('t');
  const writer = new DuckDBDataChunkWriter(
    [INTEGER, VARCHAR, TIMESTAMP_MS],
    (chunk: DuckDBDataChunk) => appender.appendDataChunk(chunk),
    { rowsPerDataChunk: 100, converter: JSToDuckDBValueConverter }
  );
  const when = new Date('2024-01-15T12:34:56.789Z');
  for (let i = 0; i < 250; i++) {
    writer.appendRow([i, `row-${i}`, when]);
  }
  writer.flush();
  appender.closeSync();

  const reader = await connection.runAndReadAll(
    `select count(*) as n, sum(x) as total, max(ts) as latest from t`
  );
  expect(reader.getRowsJS()).toEqual([[250n, 31125n, when]]);
});

test('values are converted, so nulls and JS types pass through', async () => {
  const connection = await connect();
  await connection.run(`create table t (x integer, s varchar)`);
  const appender = await connection.createAppender('t');
  const writer = new DuckDBDataChunkWriter([INTEGER, VARCHAR], (chunk) =>
    appender.appendDataChunk(chunk), { converter: JSToDuckDBValueConverter }
  );
  writer.appendRow([1, 'x']);
  writer.appendRow([null, null]);
  writer.flush();
  appender.closeSync();

  const reader = await connection.runAndReadAll(`select * from t order by x`);
  expect(reader.getRowsJS()).toEqual([
    [1, 'x'],
    [null, null],
  ]);
});

test('without a converter, rows are DuckDBValues', async () => {
  const connection = await connect();
  await connection.run(`create table t (x integer, s varchar, d date)`);
  const appender = await connection.createAppender('t');
  // No converter: DuckDBValue in, DuckDBDataChunk.setRows used directly.
  const writer = new DuckDBDataChunkWriter([INTEGER, VARCHAR, DATE], (chunk) =>
    appender.appendDataChunk(chunk)
  );
  writer.appendRow([1, 'x', dateValue(19737)]);
  writer.appendRow([2, null, null]);
  writer.flush();
  appender.closeSync();

  const reader = await connection.runAndReadAll(`select * from t order by x`);
  expect(reader.getRows()).toEqual([
    [1, 'x', dateValue(19737)],
    [2, null, null],
  ]);
});

test('a reused row array is copied, not aliased', async () => {
  const connection = await connect();
  await connection.run(`create table t (x integer, s varchar)`);
  const appender = await connection.createAppender('t');
  const writer = new DuckDBDataChunkWriter(
    [INTEGER, VARCHAR],
    (chunk) => appender.appendDataChunk(chunk),
    { converter: JSToDuckDBValueConverter }
  );
  // One scratch array, mutated between calls. Without the copy in appendRow
  // every buffered row would alias it and the table would hold row 4 five
  // times over.
  const scratch: [number, string] = [0, ''];
  for (let i = 0; i < 5; i++) {
    scratch[0] = i;
    scratch[1] = `row-${i}`;
    writer.appendRow(scratch);
  }
  writer.flush();
  appender.closeSync();

  const reader = await connection.runAndReadAll(`select * from t order by x`);
  expect(reader.getRowsJS()).toEqual([
    [0, 'row-0'],
    [1, 'row-1'],
    [2, 'row-2'],
    [3, 'row-3'],
    [4, 'row-4'],
  ]);
});

test('the row type is checked against the converter', () => {
  const sink = () => {};
  void (() => {
    const raw = new DuckDBDataChunkWriter([INTEGER, DATE], sink);
    // @ts-expect-error a JS Date is not a DuckDBValue; supply a converter
    raw.appendRow([1, new Date()]);
    raw.appendRow([1, dateValue(0)]);

    const js = new DuckDBDataChunkWriter([INTEGER, DATE], sink, {
      converter: JSToDuckDBValueConverter,
    });
    js.appendRow([1, new Date()]);

    // @ts-expect-error a converter is required once the rows are not DuckDBValues
    new DuckDBDataChunkWriter<JS>([INTEGER], sink, {});
  });
  expect(true).toBe(true);
});

test('rowsPerDataChunk is validated when the writer is made', () => {
  const max = duckdb.vector_size();
  // A data chunk cannot hold more than the vector size. Left unchecked this
  // surfaces at the first flush, a whole buffer's worth of rows later.
  expect(
    () =>
      new DuckDBDataChunkWriter([INTEGER], () => {}, {
        rowsPerDataChunk: max + 1,
      })
  ).toThrowError(`rowsPerDataChunk must be between 1 and ${max}, got ${max + 1}`);
  expect(
    () =>
      new DuckDBDataChunkWriter([INTEGER], () => {}, { rowsPerDataChunk: 0 })
  ).toThrowError(`rowsPerDataChunk must be between 1 and ${max}, got 0`);
  expect(
    () =>
      new DuckDBDataChunkWriter([INTEGER], () => {}, { rowsPerDataChunk: max })
  ).not.toThrow();
});

test('forAppender takes its column types from the appender', async () => {
  const connection = await connect();
  await connection.run(
    `create table t (x integer, s varchar, ts timestamp_ms)`
  );
  const appender = await connection.createAppender('t');
  // No types argument, and no sink: both come from the appender.
  const writer = DuckDBDataChunkWriter.forAppender(appender, {
    converter: JSToDuckDBValueConverter,
  });
  const when = new Date('2024-01-15T12:34:56.789Z');
  for (let i = 0; i < 250; i++) {
    writer.appendRow([i, `row-${i}`, when]);
  }
  writer.flush();
  appender.closeSync();

  const reader = await connection.runAndReadAll(
    `select count(*) as n, sum(x) as total, max(ts) as latest from t`
  );
  expect(reader.getRowsJS()).toEqual([[250n, 31125n, when]]);
});

test('forAppender without a converter takes DuckDBValues', async () => {
  const connection = await connect();
  await connection.run(`create table t (x integer, d date)`);
  const appender = await connection.createAppender('t');
  const writer = DuckDBDataChunkWriter.forAppender(appender);
  writer.appendRow([1, dateValue(19737)]);
  writer.flush();
  appender.closeSync();

  const reader = await connection.runAndReadAll(`select * from t`);
  expect(reader.getRows()).toEqual([[1, dateValue(19737)]]);
});

test('forAppender keeps the row type checked', async () => {
  const connection = await connect();
  await connection.run(`create table t (x integer, d date)`);
  const appender = await connection.createAppender('t');
  void (() => {
    const raw = DuckDBDataChunkWriter.forAppender(appender);
    // @ts-expect-error a JS Date is not a DuckDBValue; supply a converter
    raw.appendRow([1, new Date()]);

    const js = DuckDBDataChunkWriter.forAppender(appender, {
      converter: JSToDuckDBValueConverter,
    });
    js.appendRow([1, new Date()]);

    // @ts-expect-error a converter is required once the rows are not DuckDBValues
    DuckDBDataChunkWriter.forAppender<JS>(appender, {});
  });
  expect(appender.columnCount).toBe(2);
  appender.closeSync();
});

test('a failed flush empties the buffer rather than retaining it', () => {
  // Retained rows would grow past rowsPerDataChunk on later appends, and
  // eventually past what a data chunk can hold, leaving the writer unable to
  // flush at all.
  const writer = new DuckDBDataChunkWriter([INTEGER, UUID], () => {}, {
    rowsPerDataChunk: 4,
    converter: JSToDuckDBValueConverter,
  });
  const uuid = '10203040-5060-7080-90a0-b0c0d0e0f000';
  for (let i = 0; i < 3; i++) {
    writer.appendRow([i, uuid]);
  }
  expect(() => writer.appendRow([3, 'not-a-uuid'])).toThrowError(
    'Failed to set column 1 (UUID)'
  );
  expect(writer.bufferedRowCount).toBe(0);

  // The writer still works afterwards: one bad row does not poison it. A bad
  // value is only detected when a flush is triggered, so it sits in the buffer
  // until then — here by an explicit flush rather than by reaching capacity.
  const emitted: number[] = [];
  const recovered = new DuckDBDataChunkWriter(
    [INTEGER, UUID],
    (chunk) => emitted.push(chunk.rowCount),
    { rowsPerDataChunk: 100, converter: JSToDuckDBValueConverter }
  );
  recovered.appendRow([0, uuid]);
  expect(() => recovered.appendRow([1, 'not-a-uuid'])).not.toThrow();
  expect(recovered.bufferedRowCount).toBe(2);
  expect(() => recovered.flush()).toThrowError('Failed to set column 1 (UUID)');
  expect(recovered.bufferedRowCount).toBe(0);
  expect(emitted).toEqual([]);

  recovered.appendRow([2, uuid]);
  recovered.flush();
  expect(emitted).toEqual([1]);
});

test('a rejecting sink cannot grow the buffer past the maximum', () => {
  const max = duckdb.vector_size();
  let failing = true;
  const emitted: number[] = [];
  const writer = new DuckDBDataChunkWriter(
    [INTEGER],
    (chunk) => {
      if (failing) {
        throw new Error('sink down');
      }
      emitted.push(chunk.rowCount);
    },
    { rowsPerDataChunk: max, converter: JSToDuckDBValueConverter }
  );
  let thrown = 0;
  for (let i = 0; i < max + 5; i++) {
    try {
      writer.appendRow([i]);
    } catch {
      thrown++;
    }
  }
  expect(thrown).toBe(1);
  expect(writer.bufferedRowCount).toBeLessThan(max);
  expect(writer.bufferedRowCount).toBe(5);

  // And once the sink recovers, the writer flushes normally.
  failing = false;
  writer.flush();
  expect(emitted).toEqual([5]);
});
