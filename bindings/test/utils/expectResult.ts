import duckdb from '@duckdb/node-bindings';
import { expect } from 'vitest';
import { expectValidity } from './validityTestUtils';
import { getValue } from './valueTestUtils';

export interface ExpectedResult {
  statementType?: duckdb.StatementType;
  resultType?: duckdb.ResultType;
  rowsChanged?: number;
  columns: ExpectedColumn[];
  chunks: ExpectedChunk[];
}

export interface ExpectedColumn {
  name: string;
  type: duckdb.Type;
}

export interface ExpectedChunk {
  rowCount: number;
  vectors: ExpectedVector[];
}

export interface ExpectedVector {
  byteCount: number;
  validity: boolean[];
  values: any[];
}

export async function expectResult(res: duckdb.Result, expected: ExpectedResult) {
  expect(duckdb.result_statement_type(res)).toBe(expected.statementType ?? duckdb.StatementType.SELECT);
  expect(duckdb.result_return_type(res)).toBe(expected.resultType ?? duckdb.ResultType.QUERY_RESULT);
  expect(duckdb.rows_changed(res)).toBe(expected.rowsChanged ?? 0);
  expect(duckdb.column_count(res)).toBe(expected.columns.length);
  for (let col = 0; col < expected.columns.length; col++) {
    const expectedColumn = expected.columns[col];
    expect(duckdb.column_name(res, col)).toBe(expectedColumn.name);
    expect(duckdb.column_type(res, col)).toBe(expectedColumn.type);
    const logical_type = duckdb.column_logical_type(res, col);
    try {
      expect(duckdb.get_type_id(logical_type)).toBe(expectedColumn.type);
    } finally {
      duckdb.destroy_logical_type(logical_type);
    }
  }
  for (const expectedChunk of expected.chunks) {
    const chunk = await duckdb.fetch_chunk(res);
    try {
      expect(duckdb.data_chunk_get_column_count(chunk)).toBe(expected.columns.length);
      expect(duckdb.data_chunk_get_size(chunk)).toBe(expectedChunk.rowCount);
      for (let col = 0; col < expected.columns.length; col++) {
        const expectedColumn = expected.columns[col];
        const expectedVector = expectedChunk.vectors[col];
        const vector = duckdb.data_chunk_get_vector(chunk, col);
        const logical_type = duckdb.vector_get_column_type(vector);
        try {
          expect(duckdb.get_type_id(logical_type)).toBe(expectedColumn.type);
        } finally {
          duckdb.destroy_logical_type(logical_type);
        }
        const uint64Count = Math.ceil(expectedChunk.rowCount / 64);
        const byteCount = uint64Count * 8;
        const validity_bytes = duckdb.vector_get_validity(vector, byteCount);
        const validity = new BigUint64Array(validity_bytes.buffer, 0, uint64Count);
        const data = duckdb.vector_get_data(vector, expectedVector.byteCount);
        const dv = new DataView(data.buffer);
        for (let row = 0; row < expectedChunk.rowCount; row++) {
          expectValidity(validity_bytes, validity, row, expectedVector.validity[row]);
          expect(getValue(expectedColumn.type, dv, row)).toBe(expectedVector.values[row]);
        }
      }
    } finally {
      duckdb.destroy_data_chunk(chunk);
    }
  }
}
