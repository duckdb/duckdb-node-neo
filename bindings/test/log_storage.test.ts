import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { withConnection } from './utils/withConnection';

suite('log storage', () => {
  test('registers a callback with extra data that survives handle destruction', async () => {
    await withConnection(async (connection, database) => {
      const extraData = { source: 'bindings test' };
      const entries: {
        extraData: object | undefined;
        timestamp: duckdb.Timestamp;
        level: string;
        logType: string;
        message: string;
      }[] = [];
      const storage = duckdb.create_log_storage();
      duckdb.log_storage_set_name(storage, 'node_bindings_test');
      duckdb.log_storage_set_extra_data(storage, extraData);
      duckdb.log_storage_set_write_log_entry(
        storage,
        (receivedExtraData, timestamp, level, logType, message) => {
          entries.push({
            extraData: receivedExtraData,
            timestamp,
            level,
            logType,
            message,
          });
        },
      );
      duckdb.register_log_storage(database, storage);
      duckdb.destroy_log_storage_sync(storage);
      duckdb.destroy_log_storage_sync(storage);

      const duplicateStorage = duckdb.create_log_storage();
      duckdb.log_storage_set_name(duplicateStorage, 'node_bindings_test');
      duckdb.log_storage_set_write_log_entry(duplicateStorage, () => {});
      expect(() =>
        duckdb.register_log_storage(database, duplicateStorage),
      ).toThrow('Failed to register log storage');
      duckdb.destroy_log_storage_sync(duplicateStorage);

      await duckdb.query(connection, 'SET enable_logging = true');
      await duckdb.query(
        connection,
        "SET logging_storage = 'node_bindings_test'",
      );
      await duckdb.query(connection, "SELECT write_log('bindings log entry')");

      const entry = entries.find(({ message }) =>
        message.includes('bindings log entry'),
      );
      expect(entry).toBeDefined();
      expect(entry?.extraData).toBe(extraData);
      expect(typeof entry?.timestamp.micros).toBe('bigint');
      expect(entry?.level).toBe('INFO');
      expect(entry?.logType).toBeTruthy();
    });
  });
});
