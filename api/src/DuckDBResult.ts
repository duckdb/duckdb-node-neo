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

  public async convertColumns<T>(
    converter: DuckDBValueConverter<T>
  ): Promise<(T | null)[][]> {
    const chunks = await this.fetchAllChunks();
    return convertColumnsFromChunks(chunks, converter);
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
    const iterator = this[Symbol.asyncIterator]();
    for await (const chunk of iterator) {
      yield getRowsFromChunks([chunk]);
    }
  }

  public async *yieldRowObjects(): AsyncIterableIterator<
    Record<string, DuckDBValue>[]
  > {
    const iterator = this[Symbol.asyncIterator]();
    const deduplicatedColumnNames = this.deduplicatedColumnNames();

    for await (const chunk of iterator) {
      yield getRowObjectsFromChunks([chunk], deduplicatedColumnNames);
    }
  }

  public async *yieldConvertedRows<T>(
    converter: DuckDBValueConverter<T>,
  ): AsyncIterableIterator<Record<string, T | null>[]> {
    const iterator = this[Symbol.asyncIterator]();
    const deduplicatedColumnNames = this.deduplicatedColumnNames();

    for await (const chunk of iterator) {
      yield convertRowObjectsFromChunks(
        [chunk],
        deduplicatedColumnNames,
        converter,
      );
    }
  }

  public async *yieldRowsJs(): AsyncIterableIterator<Record<string, JS>[]> {
    return this.convertRowObjects(JSDuckDBValueConverter);
  }

  public async *yieldRowsJson(): AsyncIterableIterator<Record<string, Json>[]> {
    return this.convertRowObjects(JsonDuckDBValueConverter);
  }
}
