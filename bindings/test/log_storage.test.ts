import duckdb from '@duckdb/node-bindings';
import { expect, suite, test } from 'vitest';
import { execFileSync } from 'node:child_process';
import { withConnection } from './utils/withConnection';
import { withDatabase } from './utils/withDatabase';

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
        "SET logging_sink = 'node_bindings_test'",
      );
      await duckdb.query(connection, "SELECT write_log('bindings log entry')");

      await expect.poll(() => entries.some(({ message }) => message.includes('bindings log entry'))).toBe(true);
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

  test('rejects registering the same storage with another database', async () => {
    await withDatabase({}, async (firstDatabase) => {
      await withDatabase({}, async (secondDatabase) => {
        const storage = duckdb.create_log_storage();
        duckdb.log_storage_set_name(storage, 'single_registration_test');
        duckdb.log_storage_set_write_log_entry(storage, () => {});

        duckdb.register_log_storage(firstDatabase, storage);
        expect(() =>
          duckdb.register_log_storage(secondDatabase, storage),
        ).toThrow('Log storage has already been registered');
      });
    });
  });

  test('rejects registering storage with a closed database', async () => {
    await withDatabase({}, async (database) => {
      const storage = duckdb.create_log_storage();
      duckdb.log_storage_set_name(storage, 'closed_database_test');
      duckdb.log_storage_set_write_log_entry(storage, () => {});
      duckdb.close_sync(database);

      expect(() => duckdb.register_log_storage(database, storage)).toThrow(
        'Invalid database argument',
      );
    });
  });

  test('rejects setting a callback after the storage is destroyed', () => {
    const storage = duckdb.create_log_storage();
    duckdb.destroy_log_storage_sync(storage);

    expect(() =>
      duckdb.log_storage_set_write_log_entry(storage, () => {}),
    ).toThrow('Invalid log storage argument');
  });
});

// Native deadlocks must not block Vitest's own thread and timeout handling.
for (const scenario of ['worker and sync appender', 'callback sync database call', 'deferred callback exceptions']) {
  test(scenario, () => {
    const script = `
      const assert = require('node:assert/strict');
      const duckdb = require('@duckdb/node-bindings');
      const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
      (async () => {
        const db = await duckdb.open();
        const connection = await duckdb.connect(db);
        const other = await duckdb.connect(db);
        await duckdb.query(other, 'CREATE TABLE logs_target(i INTEGER)');
        let delivered = 0;
        let insideNativeCall = false;
        const storage = duckdb.create_log_storage();
        duckdb.log_storage_set_name(storage, 'deadlock_test');
        duckdb.log_storage_set_write_log_entry(storage, (_data, _time, _level, _type, message) => {
          if (message !== 'regression entry') return;
          assert.equal(insideNativeCall, false);
          delivered++;
          if (${JSON.stringify(scenario)} === 'callback sync database call') {
            const nested = duckdb.create_log_storage();
            duckdb.log_storage_set_name(nested, 'nested_' + delivered);
            duckdb.log_storage_set_write_log_entry(nested, () => {});
            duckdb.register_log_storage(db, nested);
            duckdb.destroy_log_storage_sync(nested);
          }
          if (${JSON.stringify(scenario)} === 'deferred callback exceptions') throw new Error('logger exception');
        });
        duckdb.register_log_storage(db, storage);
        duckdb.destroy_log_storage_sync(storage);
        await duckdb.query(connection, 'SET enable_logging = true');
        await duckdb.query(connection, "SET logging_sink = 'deadlock_test'");
        const query = duckdb.query(connection, "SELECT write_log('regression entry')");
        if (${JSON.stringify(scenario)} === 'worker and sync appender') {
          // Occupy JS until the worker has entered the native logging callback.
          Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
          insideNativeCall = true;
          const appender = duckdb.appender_create(other, 'main', 'logs_target');
          duckdb.appender_close_sync(appender);
          insideNativeCall = false;
        }
        await query;
        for (let i = 0; delivered < 1 && i < 100; i++) await sleep(10);
        assert.equal(delivered, 1);
        if (${JSON.stringify(scenario)} === 'deferred callback exceptions') {
          await duckdb.query(connection, "SELECT write_log('regression entry')");
          for (let i = 0; delivered < 2 && i < 100; i++) await sleep(10);
          assert.equal(delivered, 2);
        }
        duckdb.disconnect_sync(other);
        duckdb.disconnect_sync(connection);
        duckdb.close_sync(db);
      })().catch(error => { console.error(error); process.exitCode = 1; });
    `;
    expect(() => execFileSync(process.execPath, ['-e', script], {
      cwd: process.cwd(), timeout: 5000, stdio: 'pipe',
    })).not.toThrow();
  });
}
