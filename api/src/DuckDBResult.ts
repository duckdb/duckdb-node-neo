import duckdb from '@duckdb/node-bindings';
import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBLogicalType } from './DuckDBLogicalType';
import { DuckDBType } from './DuckDBType';
import { DuckDBTypeId } from './DuckDBTypeId';
import { DuckDBValueConverter } from './DuckDBValueConverter';
import { JS } from './JS';
import { JSDuckDBValueConverter } from './JSDuckDBValueConverter';
import { Json } from './Json';
import { JsonDuckDBValueConverter } from './JsonDuckDBValueConverter';
import { convertColumnsFromChunks } from './convertColumnsFromChunks';
import { convertColumnsObjectFromChunks } from './convertColumnsObjectFromChunks';
import { convertRowObjectsFromChunks } from './convertRowObjectsFromChunks';
import { convertRowsFromChunks } from './convertRowsFromChunks';
import { ResultReturnType, StatementType } from './enums';
import { getColumnsFromChunks } from './getColumnsFromChunks';
import { getColumnsObjectFromChunks } from './getColumnsObjectFromChunks';
import { getRowObjectsFromChunks } from './getRowObjectsFromChunks';
import { getRowsFromChunks } from './getRowsFromChunks';
import { DuckDBValue } from './values';

interface ClosedResultMetadata {
  returnType: ResultReturnType;
  statementType: StatementType;
  columnNames: string[];
  columnTypes: DuckDBType[];
  isStreaming: boolean;
  rowsChanged: number;
}

export class DuckDBResult {
  protected readonly result: duckdb.Result;
  private closedMetadata: ClosedResultMetadata | undefined;

  constructor(result: duckdb.Result) {
    this.result = result;
  }

  /**
   * Destroys the result, freeing its memory. On DuckDB 2.0, a streaming result
   * that has not been read to the end blocks its connection from starting
   * another statement until it is closed.
   *
   * The return type, statement type, column names and types, and rows changed
   * stay available; fetching throws. If a fetch is in flight, the result is
   * destroyed, and its connection unblocked, only once that fetch finishes.
   * Closing again does nothing.
   */
  public closeSync() {
    if (!this.closedMetadata) {
      const columnCount = this.columnCount;
      const columnNames: string[] = [];
      const columnTypes: DuckDBType[] = [];
      for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        columnNames.push(this.columnName(columnIndex));
        columnTypes.push(this.columnType(columnIndex));
      }
      this.closedMetadata = {
        returnType: this.returnType,
        statementType: this.statementType,
        columnNames,
        columnTypes,
        isStreaming: this.isStreaming,
        rowsChanged: this.rowsChanged,
      };
    }
    duckdb.destroy_result_sync(this.result);
  }

  public get returnType(): ResultReturnType {
    if (this.closedMetadata) {
      return this.closedMetadata.returnType;
    }
    return duckdb.result_return_type(this.result);
  }

  public get statementType(): StatementType {
    if (this.closedMetadata) {
      return this.closedMetadata.statementType;
    }
    return duckdb.result_statement_type(this.result);
  }

  public get columnCount(): number {
    if (this.closedMetadata) {
      return this.closedMetadata.columnNames.length;
    }
    return duckdb.column_count(this.result);
  }

  public columnName(columnIndex: number): string {
    if (this.closedMetadata) {
      return this.closedMetadata.columnNames[
        this.checkClosedColumnIndex(columnIndex)
      ];
    }
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
    if (this.closedMetadata) {
      return this.columnType(columnIndex).typeId;
    }
    return duckdb.column_type(
      this.result,
      columnIndex
    ) as number as DuckDBTypeId;
  }

  public columnLogicalType(columnIndex: number): DuckDBLogicalType {
    if (this.closedMetadata) {
      return this.columnType(columnIndex).toLogicalType();
    }
    return DuckDBLogicalType.create(
      duckdb.column_logical_type(this.result, columnIndex)
    );
  }

  public columnType(columnIndex: number): DuckDBType {
    if (this.closedMetadata) {
      return this.closedMetadata.columnTypes[
        this.checkClosedColumnIndex(columnIndex)
      ];
    }
    return this.columnLogicalType(columnIndex).asType();
  }

  public columnTypeJson(columnIndex: number): Json {
    return this.columnType(columnIndex).toJson();
  }

  public columnTypes(): DuckDBType[] {
    const columnTypes: DuckDBType[] = [];
    const columnCount = this.columnCount;
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      columnTypes.push(this.columnType(columnIndex));
    }
    return columnTypes;
  }

  public columnTypesJson(): Json {
    const columnTypesJson: Json[] = [];
    const columnCount = this.columnCount;
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      columnTypesJson.push(this.columnTypeJson(columnIndex));
    }
    return columnTypesJson;
  }

  public columnNamesAndTypesJson(): Json {
    return {
      columnNames: this.columnNames(),
      columnTypes: this.columnTypesJson(),
    };
  }

  public columnNameAndTypeObjectsJson(): Json {
    const columnNameAndTypeObjects: Json[] = [];
    const columnCount = this.columnCount;
    for (let columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      columnNameAndTypeObjects.push({
        columnName: this.columnName(columnIndex),
        columnType: this.columnTypeJson(columnIndex),
      });
    }
    return columnNameAndTypeObjects;
  }

  public get isStreaming(): boolean {
    if (this.closedMetadata) {
      return this.closedMetadata.isStreaming;
    }
    return duckdb.result_is_streaming(this.result);
  }

  public get rowsChanged(): number {
    if (this.closedMetadata) {
      return this.closedMetadata.rowsChanged;
    }
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
    return getColumnsFromChunks(chunks, this.columnCount);
  }

  public async convertColumns<T>(
    converter: DuckDBValueConverter<T>
  ): Promise<(T | null)[][]> {
    const chunks = await this.fetchAllChunks();
    return convertColumnsFromChunks(chunks, this.columnCount, converter);
  }

  public async getColumnsJS(): Promise<JS[][]> {
    return this.convertColumns(JSDuckDBValueConverter);
  }

  public async getColumnsJson(): Promise<Json[][]> {
    return this.convertColumns(JsonDuckDBValueConverter);
  }

  public async getColumnsObject(): Promise<Record<string, DuckDBValue[]>> {
    const chunks = await this.fetchAllChunks();
    return getColumnsObjectFromChunks(chunks, this.deduplicatedColumnNames());
  }

  public async convertColumnsObject<T>(
    converter: DuckDBValueConverter<T>
  ): Promise<Record<string, (T | null)[]>> {
    const chunks = await this.fetchAllChunks();
    return convertColumnsObjectFromChunks(
      chunks,
      this.deduplicatedColumnNames(),
      converter
    );
  }

  public async getColumnsObjectJS(): Promise<Record<string, JS[]>> {
    return this.convertColumnsObject(JSDuckDBValueConverter);
  }

  public async getColumnsObjectJson(): Promise<Record<string, Json[]>> {
    return this.convertColumnsObject(JsonDuckDBValueConverter);
  }

  public async getRows(): Promise<DuckDBValue[][]> {
    const chunks = await this.fetchAllChunks();
    return getRowsFromChunks(chunks);
  }

  public async convertRows<T>(
    converter: DuckDBValueConverter<T>
  ): Promise<(T | null)[][]> {
    const chunks = await this.fetchAllChunks();
    return convertRowsFromChunks(chunks, converter);
  }

  public async getRowsJS(): Promise<JS[][]> {
    return this.convertRows(JSDuckDBValueConverter);
  }

  public async getRowsJson(): Promise<Json[][]> {
    return this.convertRows(JsonDuckDBValueConverter);
  }

  public async getRowObjects(): Promise<Record<string, DuckDBValue>[]> {
    const chunks = await this.fetchAllChunks();
    return getRowObjectsFromChunks(chunks, this.deduplicatedColumnNames());
  }

  public async convertRowObjects<T>(
    converter: DuckDBValueConverter<T>
  ): Promise<Record<string, T | null>[]> {
    const chunks = await this.fetchAllChunks();
    return convertRowObjectsFromChunks(
      chunks,
      this.deduplicatedColumnNames(),
      converter
    );
  }

  public async getRowObjectsJS(): Promise<Record<string, JS>[]> {
    return this.convertRowObjects(JSDuckDBValueConverter);
  }

  public async getRowObjectsJson(): Promise<Record<string, Json>[]> {
    return this.convertRowObjects(JsonDuckDBValueConverter);
  }

  public async *[Symbol.asyncIterator](): AsyncIterableIterator<DuckDBDataChunk> {
    while (true) {
      const chunk = await this.fetchChunk();
      if (chunk && chunk.rowCount > 0) {
        yield chunk;
      } else {
        break;
      }
    }
  }

  public async *yieldRows(): AsyncIterableIterator<DuckDBValue[][]> {
    for await (const chunk of this) {
      yield getRowsFromChunks([chunk]);
    }
  }

  public async *yieldRowObjects(): AsyncIterableIterator<
    Record<string, DuckDBValue>[]
  > {
    const deduplicatedColumnNames = this.deduplicatedColumnNames();
    for await (const chunk of this) {
      yield getRowObjectsFromChunks([chunk], deduplicatedColumnNames);
    }
  }

  public async *yieldConvertedRows<T>(
    converter: DuckDBValueConverter<T>
  ): AsyncIterableIterator<(T | null)[][]> {
    for await (const chunk of this) {
      yield convertRowsFromChunks([chunk], converter);
    }
  }

  public async *yieldConvertedRowObjects<T>(
    converter: DuckDBValueConverter<T>
  ): AsyncIterableIterator<Record<string, T | null>[]> {
    const deduplicatedColumnNames = this.deduplicatedColumnNames();
    for await (const chunk of this) {
      yield convertRowObjectsFromChunks(
        [chunk],
        deduplicatedColumnNames,
        converter
      );
    }
  }

  public yieldRowsJs(): AsyncIterableIterator<JS[][]> {
    return this.yieldConvertedRows(JSDuckDBValueConverter);
  }

  public yieldRowsJson(): AsyncIterableIterator<Json[][]> {
    return this.yieldConvertedRows(JsonDuckDBValueConverter);
  }

  public yieldRowObjectJs(): AsyncIterableIterator<Record<string, JS>[]> {
    return this.yieldConvertedRowObjects(JSDuckDBValueConverter);
  }

  public yieldRowObjectJson(): AsyncIterableIterator<Record<string, Json>[]> {
    return this.yieldConvertedRowObjects(JsonDuckDBValueConverter);
  }

  /** Converts the index as the bindings do (ToUint32), then checks range. */
  private checkClosedColumnIndex(columnIndex: number): number {
    const index = columnIndex >>> 0;
    const columnCount = this.closedMetadata?.columnNames.length ?? 0;
    if (index >= columnCount) {
      throw new RangeError(
        `column index ${index} is out of range (count: ${columnCount})`
      );
    }
    return index;
  }
}
