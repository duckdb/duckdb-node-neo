import duckdb from '@duckdb/node-bindings';
import v8 from 'node:v8';
import vm from 'node:vm';
import { expect, suite, test } from 'vitest';
import { withConnection } from '../utils/withConnection';
import { withDatabase } from '../utils/withDatabase';

// These tests exercise the lifetime of the Napi::ObjectReferences held for
// user-supplied extra info and bind data (see src/napi_ref_reaper.h). They are
// slower and less deterministic than the main suite, so they are excluded from
// `pnpm test` and run with `pnpm test:stress`.
//
// To check the invariant these tests are protecting -- that a reference is only
// ever destroyed on the JS thread -- build with instrumentation enabled and run
// any suite; see the comment on duckdb_node_instrument_napi_refs in binding.gyp.

// Obtain gc() without requiring the process to be launched with --expose-gc.
v8.setFlagsFromString('--expose-gc');
const forceGC = vm.runInNewContext('gc') as () => void;
v8.setFlagsFromString('--no-expose-gc');

// Registers a scalar function that checks, on every invocation, that the objects
// it was given at registration and bind time are still intact. A mismatch throws,
// which the bindings surface as a query error, failing the awaiting test.
//
// Nothing on the JS side retains these objects: only the references taken by the
// bindings keep them alive.
function registerCheckedFunction(
  connection: duckdb.Connection,
  name: string,
  { gcInMainFunction = false }: { gcInMainFunction?: boolean } = {},
): { callCount: () => number } {
  let calls = 0;
  const scalar_function = duckdb.create_scalar_function();
  duckdb.scalar_function_set_name(scalar_function, name);
  const varchar_type = duckdb.create_logical_type(duckdb.Type.VARCHAR);
  duckdb.scalar_function_set_return_type(scalar_function, varchar_type);
  duckdb.scalar_function_set_volatile(scalar_function);
  duckdb.scalar_function_set_extra_info(scalar_function, {
    'extra_info_token': `extra_info_${name}`,
  });
  duckdb.scalar_function_set_bind(scalar_function, (info) => {
    const extra_info = duckdb.scalar_function_bind_get_extra_info(info) as {
      extra_info_token: string;
    };
    if (extra_info?.extra_info_token !== `extra_info_${name}`) {
      throw new Error(`extra info corrupted during bind of ${name}`);
    }
    duckdb.scalar_function_set_bind_data(info, {
      'bind_data_token': `bind_data_${name}`,
    });
  });
  duckdb.scalar_function_set_function(
    scalar_function,
    (info, input, output) => {
      if (gcInMainFunction) {
        forceGC();
      }
      calls++;
      const bind_data = duckdb.scalar_function_get_bind_data(info) as {
        bind_data_token: string;
      };
      const extra_info = duckdb.scalar_function_get_extra_info(info) as {
        extra_info_token: string;
      };
      if (bind_data?.bind_data_token !== `bind_data_${name}`) {
        throw new Error(`bind data corrupted in ${name}`);
      }
      if (extra_info?.extra_info_token !== `extra_info_${name}`) {
        throw new Error(`extra info corrupted in ${name}`);
      }
      const rowCount = duckdb.data_chunk_get_size(input);
      for (let i = 0; i < rowCount; i++) {
        duckdb.vector_assign_string_element(output, i, 'ok');
      }
    },
  );
  duckdb.register_scalar_function(connection, scalar_function);
  duckdb.destroy_scalar_function_sync(scalar_function);
  return { callCount: () => calls };
}

suite('scalar function reference lifetime', () => {
  test('bind data and extra info survive garbage collection', async () => {
    await withConnection(async (connection) => {
      const fn = registerCheckedFunction(connection, 'my_func', {
        gcInMainFunction: true,
      });
      for (let i = 0; i < 20; i++) {
        await duckdb.query(connection, 'select my_func() from range(100)');
      }
      expect(fn.callCount()).toBeGreaterThanOrEqual(20);
    });
  });

  test('repeated register and destroy cycles under GC pressure', async () => {
    await withConnection(async (connection) => {
      for (let i = 0; i < 100; i++) {
        const name = `my_func_${i}`;
        const fn = registerCheckedFunction(connection, name);
        await duckdb.query(connection, `select ${name}() from range(100)`);
        expect(fn.callCount()).toBeGreaterThan(0);
        if (i % 10 === 0) {
          forceGC();
        }
      }
      forceGC();
    });
  });

  test('concurrent queries while the JS thread is busy', async () => {
    await withDatabase({}, async (db) => {
      const registrar = await duckdb.connect(db);
      const connections: duckdb.Connection[] = [];
      try {
        const fn = registerCheckedFunction(registrar, 'my_func');
        for (let i = 0; i < 8; i++) {
          connections.push(await duckdb.connect(db));
        }

        // Keep the event loop busy so reference reaping has to contend with real
        // JS activity rather than an idle main thread.
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

        await Promise.all(
          connections.map((connection) =>
            duckdb.query(connection, 'select my_func() from range(5000)'),
          ),
        );

        churning = false;
        await churning_promise;

        expect(fn.callCount()).toBeGreaterThan(8);
      } finally {
        for (const connection of connections) {
          duckdb.disconnect_sync(connection);
        }
        duckdb.disconnect_sync(registrar);
      }
      forceGC();
    });
  });
});
