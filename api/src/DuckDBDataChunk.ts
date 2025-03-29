import duckdb from '@duckdb/node-bindings';
import { DuckDBType } from './DuckDBType';
import { DuckDBValueConverter } from './DuckDBValueConverter';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValue } from './values';

export class DuckDBDataChunk {
  public readonly chunk: duckdb.DataChunk;
  private readonly vectors: DuckDBVector[] = [];
  constructor(chunk: duckdb.DataChunk) {
    this.chunk = chunk;
  }
  public static create(
    types: readonly DuckDBType[],
    rowCount?: number
  ): DuckDBDataChunk {
    const chunk = new DuckDBDataChunk(
      duckdb.create_data_chunk(types.map((t) => t.toLogicalType().logical_type))
    );
    if (rowCount != undefined) {
      chunk.rowCount = rowCount;
    }
    return chunk;
  }
  public reset() {
    duckdb.data_chunk_reset(this.chunk);
  }
  public get columnCount(): number {
    return duckdb.data_chunk_get_column_count(this.chunk);
  }
  public get rowCount(): number {
    return duckdb.data_chunk_get_size(this.chunk);
  }
  public set rowCount(count: number) {
    duckdb.data_chunk_set_size(this.chunk, count);
  }
  public getColumnVector(columnIndex: number): DuckDBVector {
    if (this.vectors[columnIndex]) {
      return this.vectors[columnIndex];
    }
    const vector = DuckDBVector.create(
      duckdb.data_chunk_get_vector(this.chunk, columnIndex),
      this.rowCount
    );
    this.vectors[columnIndex] = vector;
    return vector;
  }
  public visitColumnValues(
    columnIndex: number,
    visitValue: (
      value: DuckDBValue,
      rowIndex: number,
      columnIndex: number,
      type: DuckDBType
    ) => void
  ) {
    const vector = this.getColumnVector(columnIndex);
    const type = vector.type;
    for (let rowIndex = 0; rowIndex < vector.itemCount; rowIndex++) {
      visitValue(vector.getItem(rowIndex), rowIndex, columnIndex, type);
    }
  }
  public getColumnValues(columnIndex: number): DuckDBValue[] {
    const values: DuckDBValue[] = [];
    this.visitColumnValues(columnIndex, (value) => values.push(value));
    return values;
  }
  public convertColumnValues<T>(
    columnIndex: number,
    converter: DuckDBValueConverter<T>
  ): (T | null)[] {
    const convertedValues: (T | null)[] = [];
    const type = this.getColumnVector(columnIndex).type;
    this.visitColumnValues(columnIndex, (value) =>
      convertedValues.push(converter(value, type, converter))
    );
    return convertedValues;
  }
  public setColumnValues(columnIndex: number, values: readonly DuckDBValue[]) {
    const vector = this.getColumnVector(columnIndex);
    if (vector.itemCount !== values.length) {
      throw new Error(`number of values must equal chunk row count`);
    }
    for (let i = 0; i < values.length; i++) {
      vector.setItem(i, values[i]);
    }
    vector.flush();
  }
  public visitColumns(
    visitColumn: (
      column: DuckDBValue[],
      columnIndex: number,
      type: DuckDBType
    ) => void
  ) {
    const columnCount = this.columnCount;
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      visitColumn(
        this.getColumnValues(columnIndex),
        columnIndex,
        this.getColumnVector(columnIndex).type
      );
    }
  }
  public getColumns(): DuckDBValue[][] {
    const columns: DuckDBValue[][] = [];
    this.visitColumns((column) => columns.push(column));
    return columns;
  }
  public convertColumns<T>(converter: DuckDBValueConverter<T>): (T | null)[][] {
    const convertedColumns: (T | null)[][] = [];
    const columnCount = this.columnCount;
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      convertedColumns.push(this.convertColumnValues(columnIndex, converter));
    }
    return convertedColumns;
  }
  public setColumns(columns: readonly (readonly DuckDBValue[])[]) {
    if (columns.length > 0) {
      this.rowCount = columns[0].length;
    }
    for (let columnIndex = 0; columnIndex < columns.length; columnIndex++) {
      this.setColumnValues(columnIndex, columns[columnIndex]);
    }
  }
  public visitColumnMajor(
    visitValue: (
      value: DuckDBValue,
      rowIndex: number,
      columnIndex: number,
      type: DuckDBType
    ) => void
  ) {
    const columnCount = this.columnCount;
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      this.visitColumnValues(columnIndex, visitValue);
    }
  }
  public visitRowValues(
    rowIndex: number,
    visitValue: (
      value: DuckDBValue,
      rowIndex: number,
      columnIndex: number,
      type: DuckDBType
    ) => void
  ) {
    const columnCount = this.columnCount;
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      const vector = this.getColumnVector(columnIndex);
      visitValue(vector.getItem(rowIndex), rowIndex, columnIndex, vector.type);
    }
  }
  public getRowValues(rowIndex: number): DuckDBValue[] {
    const values: DuckDBValue[] = [];
    this.visitRowValues(rowIndex, (value) => values.push(value));
    return values;
  }
  public convertRowValues<T>(
    rowIndex: number,
    converter: DuckDBValueConverter<T>
  ): (T | null)[] {
    const convertedValues: (T | null)[] = [];
    this.visitRowValues(rowIndex, (value, _, columnIndex) =>
      convertedValues.push(
        converter(value, this.getColumnVector(columnIndex).type, converter)
      )
    );
    return convertedValues;
  }
  public visitRows(visitRow: (row: DuckDBValue[], rowIndex: number) => void) {
    const rowCount = this.rowCount;
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      visitRow(this.getRowValues(rowIndex), rowIndex);
    }
  }
  public getRows(): DuckDBValue[][] {
    const rows: DuckDBValue[][] = [];
    this.visitRows((row) => rows.push(row));
    return rows;
  }
  public convertRows<T>(converter: DuckDBValueConverter<T>): (T | null)[][] {
    const convertedRows: (T | null)[][] = [];
    const rowCount = this.rowCount;
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      convertedRows.push(this.convertRowValues(rowIndex, converter));
    }
    return convertedRows;
  }
  public setRows(rows: readonly (readonly DuckDBValue[])[]) {
    this.rowCount = rows.length;
    const columnCount = this.columnCount;
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      const vector = this.getColumnVector(columnIndex);
      for (let rowIndex = 0; rowIndex < rows.length; rowIndex++) {
        vector.setItem(rowIndex, rows[rowIndex][columnIndex]);
      }
      vector.flush();
    }
  }
  public visitRowMajor(
    visitValue: (
      value: DuckDBValue,
      rowIndex: number,
      columnIndex: number,
      type: DuckDBType
    ) => void
  ) {
    const rowCount = this.rowCount;
    const columnCount = this.columnCount;
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        const vector = this.getColumnVector(columnIndex);
        visitValue(
          vector.getItem(rowIndex),
          rowIndex,
          columnIndex,
          vector.type
        );
      }
    }
  }
}
