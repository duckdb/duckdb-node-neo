import duckdb from '@duckdb/node-bindings';
import { expect } from 'vitest';
import { expectChunk } from './expectChunk';
import { ExpectedResult } from './ExpectedResult';
import { expectLogicalType } from './expectLogicalType';
import { withLogicalType } from './withLogicalType';

export async function expectResult(result: duckdb.Result, expectedResult: ExpectedResult) {
  expect(duckdb.result_statement_type(result)).toBe(expectedResult.statementType ?? duckdb.StatementType.SELECT);
  expect(duckdb.result_return_type(result)).toBe(expectedResult.resultType ?? duckdb.ResultType.QUERY_RESULT);
  expect(duckdb.rows_changed(result)).toBe(expectedResult.rowsChanged ?? 0);
  expect(duckdb.column_count(result)).toBe(expectedResult.columns.length);
  for (let col = 0; col < expectedResult.columns.length; col++) {
    const expectedColumn = expectedResult.columns[col];
    expect(duckdb.column_name(result, col), `${col}`).toBe(expectedColumn.name);
    expect(duckdb.column_type(result, col), `${col}`).toBe(expectedColumn.logicalType.typeId);
    withLogicalType(duckdb.column_logical_type(result, col),
      (logical_type) => expectLogicalType(logical_type, expectedColumn.logicalType, `col ${col}`)
    );
  }
  for (const expectedChunk of expectedResult.chunks) {
    const chunk = await duckdb.fetch_chunk(result);
    expectChunk(chunk, expectedChunk, expectedResult.columns);
  }
}
