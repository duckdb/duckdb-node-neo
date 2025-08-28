import duckdb from '@databrainhq/node-bindings';
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
    rowCount?: number,
  ): DuckDBDataChunk {
    const chunk = new DuckDBDataChunk(
      duckdb.create_data_chunk(
        types.map((t) => t.toLogicalType().logical_type),
      ),
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
    const maxRowCount = duckdb.vector_size();
    if (count > maxRowCount) {
      throw new Error(`A data chunk cannot have more than ${maxRowCount} rows`);
    }
    duckdb.data_chunk_set_size(this.chunk, count);
  }
  public getColumnVector(columnIndex: number): DuckDBVector {
    if (this.vectors[columnIndex]) {
      return this.vectors[columnIndex];
    }
    const vector = DuckDBVector.create(
      duckdb.data_chunk_get_vector(this.chunk, columnIndex),
      this.rowCount,
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
      type: DuckDBType,
    ) => void,
  ) {
    const vector = this.getColumnVector(columnIndex);
    const type = vector.type;
    for (let rowIndex = 0; rowIndex < vector.itemCount; rowIndex++) {
      visitValue(vector.getItem(rowIndex), rowIndex, columnIndex, type);
    }
  }
  public appendColumnValues(columnIndex: number, values: DuckDBValue[]) {
    this.visitColumnValues(columnIndex, (value) => values.push(value));
  }
  public getColumnValues(columnIndex: number): DuckDBValue[] {
    const values: DuckDBValue[] = [];
    this.appendColumnValues(columnIndex, values);
    return values;
  }
  public convertColumnValues<T>(
    columnIndex: number,
    converter: DuckDBValueConverter<T>,
  ): (T | null)[] {
    const convertedValues: (T | null)[] = [];
    this.visitColumnValues(columnIndex, (value, _r, _c, type) =>
      convertedValues.push(converter(value, type, converter)),
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
      type: DuckDBType,
    ) => void,
  ) {
    const columnCount = this.columnCount;
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      visitColumn(
        this.getColumnValues(columnIndex),
        columnIndex,
        this.getColumnVector(columnIndex).type,
      );
    }
  }
  public appendToColumns(columns: (DuckDBValue[] | undefined)[]) {
    const columnCount = this.columnCount;
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      let column = columns[columnIndex];
      if (!column) {
        column = [];
        columns[columnIndex] = column;
      }
      this.appendColumnValues(columnIndex, column);
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
  public appendToColumnsObject(
    columnNames: readonly string[],
    columnsObject: Record<string, DuckDBValue[] | undefined>,
  ) {
    const columnCount = this.columnCount;
    if (columnNames.length !== columnCount) {
      throw new Error(
        `Provided number of column names (${columnNames.length}) does not match column count (${this.columnCount})`,
      );
    }
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      const columnName = columnNames[columnIndex];
      let columnValues = columnsObject[columnName];
      if (!columnValues) {
        columnValues = [];
        columnsObject[columnName] = columnValues;
      }
      this.appendColumnValues(columnIndex, columnValues);
    }
  }
  public getColumnsObject(
    columnNames: readonly string[],
  ): Record<string, DuckDBValue[]> {
    const columnsObject: Record<string, DuckDBValue[]> = {};
    this.appendToColumnsObject(columnNames, columnsObject);
    return columnsObject;
  }
  public visitColumnMajor(
    visitValue: (
      value: DuckDBValue,
      rowIndex: number,
      columnIndex: number,
      type: DuckDBType,
    ) => void,
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
      type: DuckDBType,
    ) => void,
  ) {
    const columnCount = this.columnCount;
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      const vector = this.getColumnVector(columnIndex);
      visitValue(vector.getItem(rowIndex), rowIndex, columnIndex, vector.type);
    }
  }
  public appendRowValues(rowIndex: number, values: DuckDBValue[]) {
    this.visitRowValues(rowIndex, (value) => values.push(value));
  }
  public getRowValues(rowIndex: number): DuckDBValue[] {
    const values: DuckDBValue[] = [];
    this.appendRowValues(rowIndex, values);
    return values;
  }
  public convertRowValues<T>(
    rowIndex: number,
    converter: DuckDBValueConverter<T>,
  ): (T | null)[] {
    const convertedValues: (T | null)[] = [];
    this.visitRowValues(rowIndex, (value, _, columnIndex) =>
      convertedValues.push(
        converter(value, this.getColumnVector(columnIndex).type, converter),
      ),
    );
    return convertedValues;
  }
  public visitRows(visitRow: (row: DuckDBValue[], rowIndex: number) => void) {
    const rowCount = this.rowCount;
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      visitRow(this.getRowValues(rowIndex), rowIndex);
    }
  }
  public appendToRows(rows: DuckDBValue[][]) {
    this.visitRows((row) => rows.push(row));
  }
  public getRows(): DuckDBValue[][] {
    const rows: DuckDBValue[][] = [];
    this.appendToRows(rows);
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
  public appendToRowObjects(
    columnNames: readonly string[],
    rowObjects: Record<string, DuckDBValue>[],
  ) {
    const columnCount = this.columnCount;
    if (columnNames.length !== columnCount) {
      throw new Error(
        `Provided number of column names (${columnNames.length}) does not match column count (${this.columnCount})`,
      );
    }
    const rowCount = this.rowCount;
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      let rowObject: Record<string, DuckDBValue> = {};
      this.visitRowValues(rowIndex, (value, _, columnIndex) => {
        rowObject[columnNames[columnIndex]] = value;
      });
      rowObjects.push(rowObject);
    }
  }
  public getRowObjects(columnNames: readonly string[]) {
    const rowObjects: Record<string, DuckDBValue>[] = [];
    this.appendToRowObjects(columnNames, rowObjects);
    return rowObjects;
  }
  public visitRowMajor(
    visitValue: (
      value: DuckDBValue,
      rowIndex: number,
      columnIndex: number,
      type: DuckDBType,
    ) => void,
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
          vector.type,
        );
      }
    }
  }
}
