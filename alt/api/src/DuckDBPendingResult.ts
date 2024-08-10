import duckdb from '@duckdb-node-neo/duckdb-node-bindings';
import { DuckDBResult } from './DuckDBResult';

// Values match similar enum in C API.
export enum DuckDBPendingResultState {
  RESULT_READY = 0,
  RESULT_NOT_READY = 1,
  NO_TASKS_AVAILABLE = 3,
}

export class DuckDBPendingResult {
  private readonly pending_result: duckdb.PendingResult;
  constructor(pending_result: duckdb.PendingResult) {
    this.pending_result = pending_result;
  }
  public dispose() {
    duckdb.destroy_pending(this.pending_result);
  }
  public runTask(): DuckDBPendingResultState {
    const pending_state = duckdb.pending_execute_task(this.pending_result);
    switch (pending_state) {
      case duckdb.PendingState.RESULT_READY:
        return DuckDBPendingResultState.RESULT_READY;
      case duckdb.PendingState.RESULT_NOT_READY:
        return DuckDBPendingResultState.RESULT_NOT_READY;
      case duckdb.PendingState.ERROR:
        throw new Error(
          `Failure running pending result task: ${duckdb.pending_error(
            this.pending_result
          )}`
        );
      case duckdb.PendingState.NO_TASKS_AVAILABLE:
        return DuckDBPendingResultState.NO_TASKS_AVAILABLE;
      default:
        throw new Error(`Unexpected pending state: ${pending_state}`);
    }
  }
  public async getResult(): Promise<DuckDBResult> {
    return new DuckDBResult(await duckdb.execute_pending(this.pending_result));
  }
}
