import duckdb from '@duckdb/node-bindings';
import v8 from 'node:v8';
import vm from 'node:vm';
import { expect, suite, test } from 'vitest';
import { withConnection } from '../utils/withConnection';
import { withDatabase } from '../utils/withDatabase';

// The table function counterpart of scalar_function_refs.test.ts. Table
// functions hold three user objects rather than two -- extra info, bind data and
// init data, the last of which also covers per-thread local init data -- and drop
// them across more threads, so the reference lifetime is worth exercising here
// too.
//
// Excluded from `pnpm test`; run with `pnpm test:stress`. Env teardown is covered
// by test/worker_threads.test.ts in the main suite.

v8.setFlagsFromString('--expose-gc');
const forceGC = vm.runInNewContext('gc') as () => void;
v8.setFlagsFromString('--no-expose-gc');

// Registers a table function that checks, on every callback, that the objects it
// was given earlier are still intact. A mismatch throws, which the bindings
// surface as a query error, failing the awaiting test.
//
// Nothing on the JS side retains these objects: only the references taken by the
// bindings keep them alive.
function registerCheckedFunction(
  connection: duckdb.Connection,
  name: string,
  { rowCount = 100, gcInMainFunction = false } = {},
): { callCount: () => number } {
  let calls = 0;
  const table_function = duckdb.create_table_function();
  duckdb.table_function_set_name(table_function, name);
  duckdb.table_function_set_extra_info(table_function, {
    'extra_info_token': `extra_info_${name}`,
  });
  duckdb.table_function_set_bind(table_function, (info) => {
    const extra_info = duckdb.bind_get_extra_info(info) as {
      extra_info_token: string;
    };
    if (extra_info?.extra_info_token !== `extra_info_${name}`) {
      throw new Error(`extra info corrupted during bind of ${name}`);
    }
    const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
    duckdb.bind_add_result_column(info, 'my_column', varchar_type);
    duckdb.bind_set_bind_data(info, {
      'bind_data_token': `bind_data_${name}`,
      rowCount,
    });
  });
  duckdb.table_function_set_init(table_function, (info) => {
    const bind_data = duckdb.init_get_bind_data(info) as {
      bind_data_token: string;
    };
    if (bind_data?.bind_data_token !== `bind_data_${name}`) {
      throw new Error(`bind data corrupted during init of ${name}`);
    }
    duckdb.init_set_init_data(info, {
      'init_data_token': `init_data_${name}`,
      'next_row': 0,
    });
  });
  duckdb.table_function_set_local_init(table_function, (info) => {
    duckdb.init_set_init_data(info, {
      'local_token': `local_init_${name}`,
    });
  });
  duckdb.table_function_set_function(table_function, (info, output) => {
    if (gcInMainFunction) {
      forceGC();
    }
    calls++;
    const extra_info = duckdb.function_get_extra_info(info) as {
      extra_info_token: string;
    };
    const bind_data = duckdb.function_get_bind_data(info) as {
      bind_data_token: string;
      rowCount: number;
    };
    const init_data = duckdb.function_get_init_data(info) as {
      init_data_token: string;
      next_row: number;
    };
    const local_data = duckdb.function_get_local_init_data(info) as {
      local_token: string;
    };
    if (extra_info?.extra_info_token !== `extra_info_${name}`) {
      throw new Error(`extra info corrupted in ${name}`);
    }
    if (bind_data?.bind_data_token !== `bind_data_${name}`) {
      throw new Error(`bind data corrupted in ${name}`);
    }
    if (init_data?.init_data_token !== `init_data_${name}`) {
      throw new Error(`init data corrupted in ${name}`);
    }
    if (local_data?.local_token !== `local_init_${name}`) {
      throw new Error(`local init data corrupted in ${name}`);
    }
    const chunkSize = Math.min(
      duckdb.vector_size(),
      bind_data.rowCount - init_data.next_row,
    );
    const vector = duckdb.data_chunk_get_vector(output, 0);
    for (let i = 0; i < chunkSize; i++) {
      duckdb.vector_assign_string_element(vector, i, 'ok');
    }
    duckdb.data_chunk_set_size(output, chunkSize);
    init_data.next_row += chunkSize;
  });
  duckdb.register_table_function(connection, table_function);
  duckdb.destroy_table_function_sync(table_function);
  return { callCount: () => calls };
}

suite('table function reference lifetime', () => {
  test('extra info, bind data and init data survive garbage collection', async () => {
    await withConnection(async (connection) => {
      const fn = registerCheckedFunction(connection, 'my_func', {
        gcInMainFunction: true,
      });
      for (let i = 0; i < 20; i++) {
        await duckdb.query(connection, 'select * from my_func()');
      }
      expect(fn.callCount()).toBeGreaterThanOrEqual(20);
    });
  });

  test('many registered functions under GC pressure', async () => {
    await withConnection(async (connection) => {
      for (let i = 0; i < 100; i++) {
        const name = `my_func_${i}`;
        const fn = registerCheckedFunction(connection, name, { rowCount: 10 });
        await duckdb.query(connection, `select * from ${name}()`);
        expect(fn.callCount()).toBeGreaterThan(0);
        if (i % 10 === 0) {
          forceGC();
        }
      }
      forceGC();
    });
  });

  test('concurrent queries while the JS thread is busy', async () => {
    const rounds = 10;
    await withDatabase({}, async (db) => {
      const registrar = await duckdb.connect(db);
      const connections: duckdb.Connection[] = [];
      try {
        const fn = registerCheckedFunction(registrar, 'my_func', {
          rowCount: duckdb.vector_size() * 2,
        });
        for (let i = 0; i < 8; i++) {
          connections.push(await duckdb.connect(db));
        }

        let churning = true;
        const churning_promise = (async () => {
          while (churning) {
            const garbage: object[] = [];
            for (let i = 0; i < 10000; i++) {
              garbage.push({ i });
            }
            await new Promise((resolve) => setImmediate(resolve));
          }
        })();

        try {
          // allSettled, and the finally below, keep a failure loud: rejecting
          // early would leave queries in flight and the teardown would then
          // disconnect underneath them and hang.
          for (let round = 0; round < rounds; round++) {
            const results = await Promise.allSettled(
              connections.map((connection) =>
                duckdb.query(connection, 'select * from my_func()'),
              ),
            );
            const rejected = results.filter((r) => r.status === 'rejected');
            if (rejected.length > 0) {
              throw new Error(
                `${rejected.length} of ${results.length} queries failed in round ${round}: ${rejected[0].reason}`,
              );
            }
          }
        } finally {
          churning = false;
          await churning_promise;
        }

        expect(fn.callCount()).toBeGreaterThanOrEqual(
          connections.length * rounds,
        );
      } finally {
        for (const connection of connections) {
          duckdb.disconnect_sync(connection);
        }
        duckdb.disconnect_sync(registrar);
      }
      forceGC();
    });
  });

  test('parallel scan with max threads set', async () => {
    await withConnection(async (connection) => {
      // With more than one thread, DuckDB runs local init per thread and calls
      // the main function from several threads at once. Every call still has to
      // find its own local init data intact, which the checks above assert.
      let localInits = 0;
      const table_function = duckdb.create_table_function();
      duckdb.table_function_set_name(table_function, 'my_func');
      const total = duckdb.vector_size() * 8;
      duckdb.table_function_set_bind(table_function, (info) => {
        const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
        duckdb.bind_add_result_column(info, 'my_column', varchar_type);
        duckdb.bind_set_bind_data(info, { 'remaining': total });
      });
      duckdb.table_function_set_init(table_function, (info) => {
        duckdb.init_set_max_threads(info, 4);
        duckdb.init_set_init_data(info, { 'shared_remaining': total });
      });
      duckdb.table_function_set_local_init(table_function, (info) => {
        localInits++;
        duckdb.init_set_init_data(info, { 'local_token': 'local' });
      });
      duckdb.table_function_set_function(table_function, (info, output) => {
        const init_data = duckdb.function_get_init_data(info) as {
          shared_remaining: number;
        };
        const local_data = duckdb.function_get_local_init_data(info) as {
          local_token: string;
        };
        if (local_data?.local_token !== 'local') {
          throw new Error('local init data corrupted');
        }
        // Callbacks are serialized through the JS thread, so this shared counter
        // needs no locking even though DuckDB may call from several threads.
        const chunkSize = Math.min(
          duckdb.vector_size(),
          init_data.shared_remaining,
        );
        const vector = duckdb.data_chunk_get_vector(output, 0);
        for (let i = 0; i < chunkSize; i++) {
          duckdb.vector_assign_string_element(vector, i, 'ok');
        }
        duckdb.data_chunk_set_size(output, chunkSize);
        init_data.shared_remaining -= chunkSize;
      });
      duckdb.register_table_function(connection, table_function);
      duckdb.destroy_table_function_sync(table_function);

      const result = await duckdb.query(
        connection,
        'select count(*) as n from my_func()',
      );
      const chunk = await duckdb.fetch_chunk(result);
      const vector = duckdb.data_chunk_get_vector(chunk!, 0);
      const countData = duckdb.vector_get_data(vector, 8);
      expect(new DataView(countData.buffer).getBigInt64(0, true)).toBe(
        BigInt(total),
      );
      // One local init per scanning thread: measured at 4 here, matching the
      // max threads set above, with the main function called 12 times across
      // them. How many the scheduler actually uses depends on the machine, so
      // only the row count is asserted exactly.
      expect(localInits).toBeGreaterThanOrEqual(1);
    });
  });
});
