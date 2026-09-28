import duckdb from '@duckdb/node-bindings';
import { DuckDBTimestampValue } from './values/DuckDBTimestampValue';

export type WriteLogEntryFunction = (
  extraData: object | undefined,
  timestamp: DuckDBTimestampValue,
  level: string,
  logType: string,
  message: string,
) => void;

export class DuckDBLogStorage {
  readonly log_storage: duckdb.LogStorage;

  public constructor() {
    this.log_storage = duckdb.create_log_storage();
  }

  public static create({
    name,
    writeLogEntry,
    extraData,
  }: {
    name: string;
    writeLogEntry: WriteLogEntryFunction;
    extraData?: object;
  }): DuckDBLogStorage {
    const logStorage = new DuckDBLogStorage();
    logStorage.setName(name);
    logStorage.setWriteLogEntry(writeLogEntry);
    if (extraData) {
      logStorage.setExtraData(extraData);
    }
    return logStorage;
  }

  public destroySync() {
    duckdb.destroy_log_storage_sync(this.log_storage);
  }

  public setName(name: string) {
    duckdb.log_storage_set_name(this.log_storage, name);
  }

  public setWriteLogEntry(writeLogEntry: WriteLogEntryFunction) {
    duckdb.log_storage_set_write_log_entry(
      this.log_storage,
      (extraData, timestamp, level, logType, message) => {
        writeLogEntry(
          extraData,
          new DuckDBTimestampValue(timestamp.micros),
          level,
          logType,
          message,
        );
      },
    );
  }

  public setExtraData(extraData?: object) {
    duckdb.log_storage_set_extra_data(this.log_storage, extraData);
  }
}
