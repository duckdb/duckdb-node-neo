import duckdb from '@duckdb/node-bindings';
import { expect } from 'vitest';
import { ExpectedChunk, ExpectedColumn } from './ExpectedResult';
import { expectLogicalType } from './expectLogicalType';
import { expectVector } from './expectVector';
import { withLogicalType } from './withLogicalType';

export function expectChunk(chunk: duckdb.DataChunk, expectedChunk: ExpectedChunk, expectedColumns: ExpectedColumn[]) {
  const chunkColumnCount = expectedChunk.columnCount ?? expectedColumns.length;
  expect(duckdb.data_chunk_get_column_count(chunk)).toBe(chunkColumnCount);
  expect(duckdb.data_chunk_get_size(chunk)).toBe(expectedChunk.rowCount);
  for (let col = 0; col < expectedChunk.vectors.length; col++) {
    const expectedVector = expectedChunk.vectors[col];
    const vector = duckdb.data_chunk_get_vector(chunk, col);

    const expectedLogicalType = expectedColumns[col].logicalType;
    withLogicalType(duckdb.vector_get_column_type(vector),
      (logical_type) => expectLogicalType(logical_type, expectedLogicalType, `col ${col}`)
    );

    expectVector(vector, expectedVector, expectedLogicalType, `col ${col}`);
  }
}
