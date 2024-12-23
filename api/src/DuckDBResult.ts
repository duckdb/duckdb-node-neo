import duckdb from '@duckdb/node-bindings';
import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBLogicalType } from './DuckDBLogicalType';
import { DuckDBType } from './DuckDBType';
import { DuckDBTypeId } from './DuckDBTypeId';
import { DuckDBVector } from './DuckDBVector';
import { ResultReturnType, StatementType } from './enums';
import { DuckDBValue } from './values';

export class DuckDBResult {
  private readonly result: duckdb.Result;
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
    if (chunks.length === 0) {
      return [];
    }
    const firstChunk = chunks[0];
    const columns: DuckDBValue[][] = [];
    const columnCount = this.columnCount;
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      columns.push(firstChunk.getColumnValues(columnIndex));
    } 
    for (let chunkIndex = 1; chunkIndex < chunks.length; chunkIndex++) {
      for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        const vector = chunks[chunkIndex].getColumnVector(columnIndex);
        for (let itemIndex = 0; itemIndex < vector.itemCount; itemIndex++) {
          columns[columnIndex].push(vector.getItem(itemIndex));
        }
      }
    }
    return columns;
  }
  public async getRows(): Promise<DuckDBValue[][]> {
    const chunks = await this.fetchAllChunks();
    const rows: DuckDBValue[][] = [];
    for (const chunk of chunks) {
      const chunkVectors: DuckDBVector[] = [];
      const columnCount = chunk.columnCount;
      for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        chunkVectors.push(chunk.getColumnVector(columnIndex));
      }
      const rowCount = chunk.rowCount;
      for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const row: DuckDBValue[] = [];
        for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
          row.push(chunkVectors[columnIndex].getItem(rowIndex));
        }
        rows.push(row);
      }
    }
    return rows;
  }
}
