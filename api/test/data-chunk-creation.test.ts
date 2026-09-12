import { expect, test } from 'vitest';
import {
  DuckDBDataChunk,
  INTEGER,
  LIST,
  STRUCT,
  VARCHAR,
  VARIANT,
} from '../src';

// `duckdb_create_data_chunk` returns null for a column type it will not
// accept, rather than reporting the refusal. Wrapped and handed to JS, that
// null looks like an empty chunk — `duckdb_data_chunk_get_column_count` reads
// it as zero columns — so every set* is a silent no-op and appending it
// crashes the process. VARIANT is such a type today.
test('a data chunk cannot be created with a type DuckDB will not accept', () => {
  expect(() => DuckDBDataChunk.create([INTEGER, VARIANT])).toThrowError(
    'Cannot create a data chunk with these column types: INTEGER, VARIANT'
  );
  expect(() => DuckDBDataChunk.create([VARIANT])).toThrowError(
    'Cannot create a data chunk with these column types: VARIANT'
  );
  expect(() => DuckDBDataChunk.create([LIST(VARIANT)])).toThrowError(
    'Cannot create a data chunk with these column types: VARIANT[]'
  );
  expect(() =>
    DuckDBDataChunk.create([STRUCT({ a: INTEGER, v: VARIANT })])
  ).toThrowError('Cannot create a data chunk with these column types');
});

test('the refusal from the binding is preserved as the cause', () => {
  try {
    DuckDBDataChunk.create([VARIANT]);
    expect.unreachable();
  } catch (error) {
    expect((error as Error).cause).toBeInstanceOf(Error);
    expect(((error as Error).cause as Error).message).toContain(
      'One or more column types are not supported'
    );
  }
});

test('accepted types, including none, still create a chunk', () => {
  expect(DuckDBDataChunk.create([INTEGER, VARCHAR]).columnCount).toBe(2);
  expect(DuckDBDataChunk.create([LIST(INTEGER)]).columnCount).toBe(1);
  // Zero columns is a legitimate chunk, not a refusal.
  expect(DuckDBDataChunk.create([]).columnCount).toBe(0);
});
