import duckdb from '@duckdb/node-bindings';
import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBLogicalType } from './DuckDBLogicalType';
import { DuckDBType } from './DuckDBType';
import { DuckDBTypeId } from './DuckDBTypeId';
import { ResultReturnType, StatementType } from './enums';

export class DuckDBResult {
  private readonly result: duckdb.Result;
  constructor(result: duckdb.Result) {
    this.result = result;
  }
  public dispose() {
    duckdb.destroy_result(this.result);
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
    return DuckDBLogicalType.consumeAsType(
      duckdb.column_logical_type(this.result, columnIndex)
    );
  }
  public get rowsChanged(): number {
    return duckdb.rows_changed(this.result);
  }
  public async fetchChunk(): Promise<DuckDBDataChunk> {
    return new DuckDBDataChunk(await duckdb.fetch_chunk(this.result));
  }
}
