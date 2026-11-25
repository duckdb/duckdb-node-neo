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

test('retains queued callback and extra data after database closure until delivery', async () => {
  const database = await duckdb.open();
  const connection = await duckdb.connect(database);
  let databaseClosed = false;
  let delivered = false;
  const refs = createQueuedStorage(database, data => {
    expect(databaseClosed).toBe(true);
    expect(data).toEqual({ source: 'queued entry' });
    delivered = true;
  });
  try {
    await duckdb.query(connection, 'SET threads = 1');
    await duckdb.query(connection, 'SET enable_logging = true');
    await duckdb.query(connection, "SET logging_sink = 'queued_refs'");
    const prepared = await duckdb.prepare(connection, "SELECT write_log('queued entry')");
    const pending = duckdb.pending_prepared(prepared);
    try {
      // Execute on this thread without yielding, so TSFN delivery cannot run
      // until after every native owner below has been synchronously destroyed.
      let state = duckdb.pending_execute_check_state(pending);
      for (let attempt = 0; !duckdb.pending_execution_is_finished(state) && attempt < 100; attempt++) {
        state = duckdb.pending_execute_task(pending);
      }
      expect(state).toBe(duckdb.PendingState.RESULT_READY);
      expect(delivered).toBe(false);
    } finally {
      duckdb.destroy_pending_sync(pending);
      duckdb.destroy_prepare_sync(prepared);
    }
  } finally {
    duckdb.disconnect_sync(connection);
    duckdb.close_sync(database);
    databaseClosed = true;
  }
  expect(delivered).toBe(false);
  forceGC();
  expect(refs.callbackRef.deref()).toBeDefined();
  expect(refs.extraDataRef.deref()).toBeDefined();
  await expect.poll(() => delivered).toBe(true);
  for (let attempt = 0; attempt < 10; attempt++) {
    await new Promise<void>(resolve => setImmediate(resolve));
    forceGC();
  }
  expect(refs.callbackRef.deref()).toBeUndefined();
  expect(refs.extraDataRef.deref()).toBeUndefined();
});
