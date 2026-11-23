import duckdb from '@duckdb/node-bindings';
import v8 from 'node:v8';
import vm from 'node:vm';
import { expect, test } from 'vitest';
import { withDatabase } from '../utils/withDatabase';

v8.setFlagsFromString('--expose-gc');
const forceGC = vm.runInNewContext('gc') as () => void;
v8.setFlagsFromString('--no-expose-gc');

function createInvalidStorage() {
  const callback = () => {};
  const callbackRef = new WeakRef(callback);
  const storage = duckdb.create_log_storage();
  duckdb.log_storage_set_name(storage, '');
  duckdb.log_storage_set_write_log_entry(storage, callback);
  return { callbackRef, storage };
}

test('releases callback state after early registration failure', async () => {
  await withDatabase({}, async (database) => {
    const { callbackRef, storage } = createInvalidStorage();

    expect(() => duckdb.register_log_storage(database, storage)).toThrow(
      'Failed to register log storage',
    );
    duckdb.destroy_log_storage_sync(storage);

    for (let attempt = 0; attempt < 10; attempt++) {
      await new Promise<void>((resolve) => setImmediate(resolve));
      forceGC();
    }
    expect(callbackRef.deref()).toBeUndefined();
  });
});

function createQueuedStorage(database: duckdb.Database, onEntry: (data: object | undefined) => void) {
  const extraData = { source: 'queued entry' };
  const callback: duckdb.WriteLogEntryFunction = (data, _timestamp, _level, _type, message) => {
    if (message === 'queued entry') onEntry(data);
  };
  const callbackRef = new WeakRef(callback);
  const extraDataRef = new WeakRef(extraData);
  const storage = duckdb.create_log_storage();
  duckdb.log_storage_set_name(storage, 'queued_refs');
  duckdb.log_storage_set_extra_data(storage, extraData);
  duckdb.log_storage_set_write_log_entry(storage, callback);
  duckdb.register_log_storage(database, storage);
  duckdb.destroy_log_storage_sync(storage);
  return { callbackRef, extraDataRef };
}

test('retains queued callback and extra data through delivery then releases them', async () => {
  let refs: ReturnType<typeof createQueuedStorage> | undefined;
  let delivered = false;
  await withDatabase({}, async (database) => {
    refs = createQueuedStorage(database, data => {
      expect(data).toEqual({ source: 'queued entry' });
      delivered = true;
    });
    const connection = await duckdb.connect(database);
    try {
      await duckdb.query(connection, 'SET enable_logging = true');
      await duckdb.query(connection, "SET logging_sink = 'queued_refs'");
      await duckdb.query(connection, "SELECT write_log('queued entry')");
      await expect.poll(() => delivered).toBe(true);
    } finally {
      duckdb.disconnect_sync(connection);
    }
  });
  for (let attempt = 0; attempt < 10; attempt++) {
    await new Promise<void>(resolve => setImmediate(resolve));
    forceGC();
  }
  expect(refs?.callbackRef.deref()).toBeUndefined();
  expect(refs?.extraDataRef.deref()).toBeUndefined();
});
