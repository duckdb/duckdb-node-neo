import duckdb from '@duckdb/node-bindings';
import { expect } from 'vitest';
import { expectChunk } from './expectChunk';
import { ExpectedResult } from './ExpectedResult';
import { expectLogicalType } from './expectLogicalType';

export async function expectResult(result: duckdb.Result, expectedResult: ExpectedResult) {
  expect(duckdb.result_statement_type(result)).toBe(expectedResult.statementType ?? duckdb.StatementType.SELECT);
  expect(duckdb.result_return_type(result)).toBe(expectedResult.resultType ?? duckdb.ResultType.QUERY_RESULT);
  expect(duckdb.result_is_streaming(result)).toBe(!!expectedResult.isStreaming);
  if (expectedResult.chunkCount != undefined) {
    expect(duckdb.result_chunk_count(result)).toBe(expectedResult.chunkCount);
  }
  if (expectedResult.rowCount != undefined) {
    expect(duckdb.row_count(result)).toBe(expectedResult.rowCount);
  }
  expect(duckdb.rows_changed(result)).toBe(expectedResult.rowsChanged ?? 0);
  expect(duckdb.column_count(result)).toBe(expectedResult.columns.length);
  for (let col = 0; col < expectedResult.columns.length; col++) {
    const expectedColumn = expectedResult.columns[col];
    expect(duckdb.column_name(result, col), `${col}`).toBe(expectedColumn.name);
    expect(duckdb.column_type(result, col), `${col}`).toBe(expectedColumn.logicalType.typeId);
    expectLogicalType(duckdb.column_logical_type(result, col), expectedColumn.logicalType, `col ${col}`);
  }
  if (expectedResult.chunkCount != undefined && expectedResult.chunkCount > 0) {
    for (let chunkIndex = 0; chunkIndex < expectedResult.chunkCount; chunkIndex++) {
      const chunk = duckdb.result_get_chunk(result, chunkIndex);
      expectChunk(chunk, expectedResult.chunks[chunkIndex], expectedResult.columns);
    }
  }
  for (const expectedChunk of expectedResult.chunks) {
    const chunk = await duckdb.fetch_chunk(result);
    expect(chunk).toBeDefined();
    if (chunk) {
      expectChunk(chunk, expectedChunk, expectedResult.columns);
    }
  }
}
