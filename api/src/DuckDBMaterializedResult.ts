import duckdb from '@duckdb/node-bindings';
import { DuckDBDataChunk } from './DuckDBDataChunk';
import { DuckDBResult } from './DuckDBResult';
import { StatementType } from './enums';

export class DuckDBMaterializedResult extends DuckDBResult {
  private closedCounts: { rowCount: number; chunkCount: number } | undefined;
  constructor(result: duckdb.Result, preparedStatementType?: StatementType) {
    super(result, preparedStatementType);
  }
  /**
   * Same as `DuckDBResult.closeSync`; the row and chunk counts also stay
   * available. Getting a chunk throws.
   */
  public override closeSync() {
    if (!this.closedCounts) {
      this.closedCounts = {
        rowCount: this.rowCount,
        chunkCount: this.chunkCount,
      };
    }
    super.closeSync();
  }
  public get rowCount(): number {
    if (this.closedCounts) {
      return this.closedCounts.rowCount;
    }
    return duckdb.row_count(this.result);
  }
  public get chunkCount(): number {
    if (this.closedCounts) {
      return this.closedCounts.chunkCount;
    }
    return duckdb.result_chunk_count(this.result);
  }
  public getChunk(chunkIndex: number): DuckDBDataChunk {
    return new DuckDBDataChunk(duckdb.result_get_chunk(this.result, chunkIndex));
  }
}
