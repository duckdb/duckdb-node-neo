import duckdb from '@duckdb/node-bindings';
import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBLogicalType } from './DuckDBLogicalType';
import { DuckDBType } from './DuckDBType';
import { DuckDBTypeId } from './DuckDBTypeId';
import { ResultReturnType, StatementType } from './enums';
import { getColumnsFromChunks } from './getColumnsFromChunks';
import { getColumnsObjectFromChunks } from './getColumnsObjectFromChunks';
import { getRowObjectsFromChunks } from './getRowObjectsFromChunks';
import { getRowsFromChunks } from './getRowsFromChunks';
import { DuckDBValue } from './values';

export class DuckDBResult {
  protected readonly result: duckdb.Result;
  constructor(result: duckdb.Result) {
    this.result = result;
  }
  public get returnType(): ResultReturnType {
    return duckdb.result_return_type(this.result);
  }
  public get statementType(): StatementType {
    return duckdb.result_statement_type(this.result);
  }
  public get columnCount(): number {
    return duckdb.column_count(this.result);
  }
  public columnName(columnIndex: number): string {
    return duckdb.column_name(this.result, columnIndex);
  }
  public columnNames(): string[] {
    const columnNames: string[] = [];
    const columnCount = this.columnCount;
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      columnNames.push(this.columnName(columnIndex));
    }
    return columnNames;
  }
  public deduplicatedColumnNames(): string[] {
    const outputColumnNames: string[] = [];
    const columnCount = this.columnCount;
    const columnNameCount: { [columnName: string]: number } = {};
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      const inputColumnName = this.columnName(columnIndex);
      const nameCount = (columnNameCount[inputColumnName] || 0) + 1;
      columnNameCount[inputColumnName] = nameCount;
      if (nameCount > 1) {
        outputColumnNames.push(`${inputColumnName}:${nameCount - 1}`);
      } else {
        outputColumnNames.push(inputColumnName);
      }
    }
    return outputColumnNames;
  }
  public columnTypeId(columnIndex: number): DuckDBTypeId {
    return duckdb.column_type(
      this.result,
      columnIndex
    ) as number as DuckDBTypeId;
  }
  public columnLogicalType(columnIndex: number): DuckDBLogicalType {
    return DuckDBLogicalType.create(
      duckdb.column_logical_type(this.result, columnIndex)
    );
  }
  public columnType(columnIndex: number): DuckDBType {
    return DuckDBLogicalType.create(
      duckdb.column_logical_type(this.result, columnIndex)
    ).asType();
  }
  public columnTypes(): DuckDBType[] {
    const columnTypes: DuckDBType[] = [];
    const columnCount = this.columnCount;
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      columnTypes.push(this.columnType(columnIndex));
    }
    return columnTypes;
  }
  public get isStreaming(): boolean {
    return duckdb.result_is_streaming(this.result);
  }
  public get rowsChanged(): number {
    return duckdb.rows_changed(this.result);
  }
  public async fetchChunk(): Promise<DuckDBDataChunk | null> {
    const chunk = await duckdb.fetch_chunk(this.result);
    return chunk ? new DuckDBDataChunk(chunk) : null;
  }
  public async fetchAllChunks(): Promise<DuckDBDataChunk[]> {
    const chunks: DuckDBDataChunk[] = [];
    while (true) {
      const chunk = await this.fetchChunk();
      if (!chunk || chunk.rowCount === 0) {
        return chunks;
      }
      chunks.push(chunk);
    }
  }
  public async getColumns(): Promise<DuckDBValue[][]> {
    const chunks = await this.fetchAllChunks();
    return getColumnsFromChunks(chunks);
  }
  public async getColumnsObject(): Promise<Record<string, DuckDBValue[]>> {
    const chunks = await this.fetchAllChunks();
    return getColumnsObjectFromChunks(chunks, this.deduplicatedColumnNames());
  }
  public async getRows(): Promise<DuckDBValue[][]> {
    const chunks = await this.fetchAllChunks();
    return getRowsFromChunks(chunks);
  }
  public async getRowObjects(): Promise<Record<string, DuckDBValue>[]> {
    const chunks = await this.fetchAllChunks();
    return getRowObjectsFromChunks(chunks, this.deduplicatedColumnNames());
  }
}
