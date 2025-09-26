import { convertRowObjectsFromChunks } from './convertRowObjectsFromChunks';
import { convertRowsFromChunks } from './convertRowsFromChunks';
import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBLogicalType } from './DuckDBLogicalType';
import { DuckDBResult } from './DuckDBResult';
import { DuckDBType } from './DuckDBType';
import { DuckDBTypeId } from './DuckDBTypeId';
import { DuckDBValueConverter } from './DuckDBValueConverter';
import { ResultReturnType, StatementType } from './enums';
import { getRowObjectsFromChunks } from './getRowObjectsFromChunks';
import { getRowsFromChunks } from './getRowsFromChunks';
import { JS } from './JS';
import { JSDuckDBValueConverter } from './JSDuckDBValueConverter';
import { Json } from './Json';
import { JsonDuckDBValueConverter } from './JsonDuckDBValueConverter';
import { DuckDBValue } from './values';

/** Used to read the results row by row in a memory efficient manner */
export class DuckDBRowReader {
  private readonly result: DuckDBResult;
  private chunks: AsyncGenerator<DuckDBDataChunk, void, undefined>;
  private currentRowCount_: number;
  private done_: boolean;

  constructor(result: DuckDBResult) {
    this.result = result;
    this.currentRowCount_ = 0;
    this.done_ = false;
    this.chunks = this.chunksGenerator();
  }

  private async *chunksGenerator(): AsyncGenerator<
    DuckDBDataChunk,
    void,
    undefined
  > {
    while (!this.done_) {
      const chunk = await this.result.fetchChunk();
      if (chunk && chunk.rowCount > 0) {
        this.currentRowCount_ += chunk.rowCount;
        yield chunk;
      } else {
        this.done_ = true;
      }
    }
  }

  public get returnType(): ResultReturnType {
    return this.result.returnType;
  }

  public get statementType(): StatementType {
    return this.result.statementType;
  }

  public get columnCount(): number {
    return this.result.columnCount;
  }

  public columnName(columnIndex: number): string {
    return this.result.columnName(columnIndex);
  }

  public columnNames(): string[] {
    return this.result.columnNames();
  }

  public deduplicatedColumnNames(): string[] {
    return this.result.deduplicatedColumnNames();
  }

  public columnTypeId(columnIndex: number): DuckDBTypeId {
    return this.result.columnTypeId(columnIndex);
  }

  public columnLogicalType(columnIndex: number): DuckDBLogicalType {
    return this.result.columnLogicalType(columnIndex);
  }

  public columnType(columnIndex: number): DuckDBType {
    return this.result.columnType(columnIndex);
  }

  public columnTypeJson(columnIndex: number): Json {
    return this.result.columnTypeJson(columnIndex);
  }

  public columnTypes(): DuckDBType[] {
    return this.result.columnTypes();
  }

  public columnTypesJson(): Json {
    return this.result.columnTypesJson();
  }

  public columnNamesAndTypesJson(): Json {
    return this.result.columnNamesAndTypesJson();
  }

  public columnNameAndTypeObjectsJson(): Json {
    return this.result.columnNameAndTypeObjectsJson();
  }

  public get rowsChanged(): number {
    return this.result.rowsChanged;
  }

  /** Total number of rows read so far. Call `readAll` or `readUntil` to read rows. */
  public get currentRowCount() {
    return this.currentRowCount_;
  }

  /** Whether reading is done, that is, there are no more rows to read. */
  public get done() {
    return this.done_;
  }

  /**
   * Internal iterator used to return the DuckDBValue rows. The public functions on
   * this class are wrappers around this generator so that the caller can specify
   * how they want to consume the rows.
   */
  private async *getRows(): AsyncGenerator<DuckDBValue[][], void, undefined> {
    for await (const chunk of this.chunks) {
      yield getRowsFromChunks([chunk]);
    }
  }

  public async *convertRows<T>(
    converter: DuckDBValueConverter<T>
  ): AsyncGenerator<(T | null)[][], void, undefined> {
    for await (const chunk of this.chunks) {
      yield convertRowsFromChunks([chunk], converter);
    }
  }

  public getRowsJS(): AsyncGenerator<JS[][], void, undefined> {
    return this.convertRows(JSDuckDBValueConverter);
  }

  public getRowsJson(): AsyncGenerator<Json[][], void, undefined> {
    return this.convertRows(JsonDuckDBValueConverter);
  }

  public async *getRowObjects(): AsyncGenerator<
    Record<string, DuckDBValue>[],
    void,
    undefined
  > {
    for await (const chunk of this.chunks) {
      yield getRowObjectsFromChunks([chunk], this.deduplicatedColumnNames());
    }
  }

  public async *convertNextRowObjects<T>(
    converter: DuckDBValueConverter<T>
  ): AsyncGenerator<Record<string, T | null>[], void, undefined> {
    for await (const chunk of this.chunks) {
      yield convertRowObjectsFromChunks(
        [chunk],
        this.deduplicatedColumnNames(),
        converter,
      );
    }
  }

  public getRowObjectsJS(): AsyncGenerator<
    Record<string, JS>[],
    void,
    undefined
  > {
    return this.convertNextRowObjects(JSDuckDBValueConverter);
  }

  public getRowObjectsJson(): AsyncGenerator<
    Record<string, Json>[],
    void,
    undefined
  > {
    return this.convertNextRowObjects(JsonDuckDBValueConverter);
  }
}
