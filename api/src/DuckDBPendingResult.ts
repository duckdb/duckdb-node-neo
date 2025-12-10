import duckdb from '@duckdb/node-bindings';
import { createResult } from './createResult';
import { DuckDBResult } from './DuckDBResult';
import { DuckDBResultReader } from './DuckDBResultReader';
import { sleep } from './sleep';

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
  public async runAllTasks(): Promise<void> {
    while (this.runTask() !== DuckDBPendingResultState.RESULT_READY) {
      await sleep(1);
    }
  }
  public async getResult(): Promise<DuckDBResult> {
    return createResult(await duckdb.execute_pending(this.pending_result));
  }
  public async read(): Promise<DuckDBResultReader> {
    return new DuckDBResultReader(await this.getResult());
  }
  public async readAll(): Promise<DuckDBResultReader> {
    const reader = new DuckDBResultReader(await this.getResult());
    await reader.readAll();
    return reader;
  }
  public async readUntil(targetRowCount: number): Promise<DuckDBResultReader> {
    const reader = new DuckDBResultReader(await this.getResult());
    await reader.readUntil(targetRowCount);
    return reader;
  }
}
