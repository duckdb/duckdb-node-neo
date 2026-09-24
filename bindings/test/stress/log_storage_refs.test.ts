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
