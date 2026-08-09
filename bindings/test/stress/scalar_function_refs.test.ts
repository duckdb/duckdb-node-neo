import duckdb from '@duckdb/node-bindings';
import { createRequire } from 'node:module';
import v8 from 'node:v8';
import vm from 'node:vm';
import { Worker } from 'node:worker_threads';
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
//
// Vitest's default pool is 'forks', so this runs on the only JS thread of a
// dedicated child process. If the pool is ever switched to 'threads', mutating
// process-global V8 flags while sibling threads run JS is unsafe, and this
// should move behind an --expose-gc launch flag instead.
//
// Note that gc() cannot collect the objects under test, because the bindings
// hold them strongly. That is the point: these tests verify the reference is
// strong rather than weak, and that reaping survives collection activity.
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
        // One chunk per query, so one call per query.
        await duckdb.query(connection, 'select my_func() from range(100)');
      }
      expect(fn.callCount()).toBe(20);
    });
  });

  // Note that only bind data is destroyed off the JS thread here. Extra info is
  // owned by the catalog entry and released at disconnect/close, both of which
  // run on the JS thread, so it always takes the reaper's inline path.
  test('many registered functions under GC pressure', async () => {
    await withConnection(async (connection) => {
      for (let i = 0; i < 100; i++) {
        const name = `my_func_${i}`;
        const fn = registerCheckedFunction(connection, name);
        await duckdb.query(connection, `select ${name}() from range(100)`);
        expect(fn.callCount()).toBe(1);
        if (i % 10 === 0) {
          forceGC();
        }
      }
      forceGC();
    });
  });

  test('concurrent queries while the JS thread is busy', async () => {
    const rounds = 25;
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

        try {
          // Each round is 8 connections x 3 chunks of 5000 rows.
          //
          // allSettled rather than all, and a finally around the churn loop, are
          // both load bearing on the failure path: Promise.all would reject on
          // the first bad query and leave the other seven in flight, and the
          // teardown below would then disconnect underneath them and hang. The
          // point of this test is to fail loudly, not to wedge the run.
          for (let round = 0; round < rounds; round++) {
            const results = await Promise.allSettled(
              connections.map((connection) =>
                duckdb.query(connection, 'select my_func() from range(5000)'),
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

        expect(fn.callCount()).toBe(connections.length * 3 * rounds);
      } finally {
        for (const connection of connections) {
          duckdb.disconnect_sync(connection);
        }
        duckdb.disconnect_sync(registrar);
      }
      forceGC();
    });
  });

  // The reaper is owned by the addon and therefore scoped to a napi_env. Under
  // worker_threads the addon is instantiated per env, so this covers both that
  // scoping and the env teardown path that runs NapiRefReaper::Shutdown -- which
  // never runs for the main env, since Node skips instance data finalizers at
  // process exit.
  test('references are reaped per env under worker_threads', async () => {
    const bindings_path = createRequire(import.meta.url).resolve(
      '@duckdb/node-bindings',
    );
    const queries_per_worker = 25;
    const chunks_per_query = 3; // range(5000) at a vector size of 2048

    const worker_source = `
      const { parentPort, workerData } = require('node:worker_threads');
      const duckdb = require(workerData.bindings_path);
      (async () => {
        const db = await duckdb.open();
        const connection = await duckdb.connect(db);
        const fn = duckdb.create_scalar_function();
        duckdb.scalar_function_set_name(fn, 'my_func');
        duckdb.scalar_function_set_return_type(
          fn,
          duckdb.create_logical_type(duckdb.Type.VARCHAR),
        );
        duckdb.scalar_function_set_volatile(fn);
        duckdb.scalar_function_set_extra_info(fn, { token: 'extra_info' });
        duckdb.scalar_function_set_bind(fn, (info) => {
          duckdb.scalar_function_set_bind_data(info, { token: 'bind_data' });
        });
        let calls = 0;
        duckdb.scalar_function_set_function(fn, (info, input, output) => {
          calls++;
          const bind_data = duckdb.scalar_function_get_bind_data(info);
          const extra_info = duckdb.scalar_function_get_extra_info(info);
          if (bind_data.token !== 'bind_data' || extra_info.token !== 'extra_info') {
            throw new Error('reference corrupted in worker');
          }
          const rowCount = duckdb.data_chunk_get_size(input);
          for (let i = 0; i < rowCount; i++) {
            duckdb.vector_assign_string_element(output, i, 'ok');
          }
        });
        duckdb.register_scalar_function(connection, fn);
        duckdb.destroy_scalar_function_sync(fn);
        for (let i = 0; i < ${queries_per_worker}; i++) {
          await duckdb.query(connection, 'select my_func() from range(5000)');
        }
        duckdb.disconnect_sync(connection);
        duckdb.close_sync(db);
        parentPort.postMessage({ calls });
      })().catch((e) => {
        parentPort.postMessage({ error: String((e && e.message) || e) });
      });
    `;

    function runWorker(): Promise<{ calls?: number; error?: string }> {
      return new Promise((resolve, reject) => {
        const worker = new Worker(worker_source, {
          eval: true,
          workerData: { bindings_path },
        });
        let message: { calls?: number; error?: string } | undefined;
        worker.on('message', (m) => {
          message = m;
        });
        worker.on('error', reject);
        worker.on('exit', (code) => {
          if (code !== 0) {
            reject(new Error(`worker exited with code ${code}`));
          } else {
            resolve(message ?? { error: 'worker sent no message' });
          }
        });
      });
    }

    // Several envs concurrently, each with its own addon instance and reaper.
    const results = await Promise.all(
      Array.from({ length: 4 }, () => runWorker()),
    );
    for (const result of results) {
      expect(result.error).toBeUndefined();
      expect(result.calls).toBe(queries_per_worker * chunks_per_query);
    }
  });
});
