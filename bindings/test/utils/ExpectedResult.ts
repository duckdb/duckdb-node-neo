import duckdb from '@duckdb/node-bindings';
import { ExpectedLogicalType } from './ExpectedLogicalType';
import { ExpectedVector } from './ExpectedVector';

export interface ExpectedColumn {
  name: string;
  logicalType: ExpectedLogicalType;
}

export interface ExpectedChunk {
  columnCount?: number;
  rowCount: number;
  vectors: ExpectedVector[];
}

export interface ExpectedResult {
  statementType?: duckdb.StatementType;
  resultType?: duckdb.ResultType;
  rowsChanged?: number;
  columns: ExpectedColumn[];
  chunks: ExpectedChunk[];
}
