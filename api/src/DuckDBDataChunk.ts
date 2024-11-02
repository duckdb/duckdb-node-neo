import duckdb from '@duckdb/node-bindings';
import { DuckDBVector } from './DuckDBVector';

export class DuckDBDataChunk {
  public readonly chunk: duckdb.DataChunk;
  constructor(chunk: duckdb.DataChunk) {
    this.chunk = chunk;
  }
  public static create(logical_types: duckdb.LogicalType[]): DuckDBDataChunk {
    return new DuckDBDataChunk(duckdb.create_data_chunk(logical_types));
  }
  public reset() {
    duckdb.data_chunk_reset(this.chunk);
  }
  public get columnCount(): number {
    return duckdb.data_chunk_get_column_count(this.chunk);
  }
  public getColumn(columnIndex: number): DuckDBVector<any> {
    // TODO: cache vectors?
    return DuckDBVector.create(
      duckdb.data_chunk_get_vector(this.chunk, columnIndex),
      this.rowCount
    );
  }
  public get rowCount(): number {
    return duckdb.data_chunk_get_size(this.chunk);
  }
  public set rowCount(count: number) {
    duckdb.data_chunk_set_size(this.chunk, count);
  }
}
