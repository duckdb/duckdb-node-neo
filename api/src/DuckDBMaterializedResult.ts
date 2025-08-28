import duckdb from '@databrainhq/node-bindings';
import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBResult } from './DuckDBResult';

export class DuckDBMaterializedResult extends DuckDBResult {
  constructor(result: duckdb.Result) {
    super(result);
  }
  public get rowCount(): number {
    return duckdb.row_count(this.result);
  }
  public get chunkCount(): number {
    return duckdb.result_chunk_count(this.result);
  }
  public getChunk(chunkIndex: number): DuckDBDataChunk {
    return new DuckDBDataChunk(
      duckdb.result_get_chunk(this.result, chunkIndex),
    );
  }
}
