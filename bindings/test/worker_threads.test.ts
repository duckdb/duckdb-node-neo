import { createRequire } from 'node:module';
import { Worker } from 'node:worker_threads';
import { expect, suite, test } from 'vitest';

// Worker threads are the only place an env is ever torn down for real: Node skips
// instance data finalizers for the main env at process exit. So this is the only
// coverage for the teardown half of the thread-safe function lifetime rules in
// src/duckdb_thread_callback.h, and for the reaper being scoped per env rather
// than per process.
//
// It caught a deadlock on its first CI run -- an unreleased thread-safe function
// blocking env teardown, from a release that could only happen once teardown had
// finished -- which reproduced on Linux but not macOS. It is deterministic and
// takes well under a second locally, so it belongs in the main suite where it
// runs on every platform, rather than in test/stress.
//
// Because the failure mode it guards against is a hang, the deadline below is
// generous: a slow runner should not fail, only a genuine hang should. When one
// happens, the reported state distinguishes "the worker never finished its work"
// from "the worker finished but its env never tore down", which is the
// distinction that matters.

const bindings_path = createRequire(import.meta.url).resolve(
  '@duckdb/node-bindings',
);

const worker_count = 4;
const queries_per_worker = 25;
const chunks_per_query = 3; // range(5000) at a vector size of 2048

const deadlineMs = 25000;
const testTimeoutMs = 40000; // Above deadlineMs, so the diagnostic below wins.

// Each worker registers a scalar function, checks on every invocation that its
// extra info and bind data are intact, then closes everything and reports how
// many times it was called.
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

interface WorkerOutcome {
  calls?: number;
  error?: string;
}

interface WorkerProgress {
  online: boolean;
  reported: boolean; // finished its work and posted a result
  exited: boolean; // its env tore down
}

function describe(progress: readonly WorkerProgress[]): string {
  const states = progress.map((p, i) => {
    if (p.exited) return `${i}:exited`;
    if (p.reported) return `${i}:WORK DONE BUT NEVER EXITED`;
    if (p.online) return `${i}:ONLINE BUT WORK UNFINISHED`;
    return `${i}:NEVER STARTED`;
  });
  return `workers did not all exit within ${deadlineMs}ms [${states.join(', ')}]`;
}

suite('worker threads', () => {
  test(
    'scalar functions work, and their envs tear down, in workers',
    async () => {
      const progress: WorkerProgress[] = Array.from(
        { length: worker_count },
        () => ({ online: false, reported: false, exited: false }),
      );
      const workers: Worker[] = [];

      const runs = progress.map((state, index) => {
        return new Promise<WorkerOutcome>((resolve, reject) => {
          const worker = new Worker(worker_source, {
            eval: true,
            workerData: { bindings_path },
          });
          workers.push(worker);
          let outcome: WorkerOutcome | undefined;
          worker.on('online', () => {
            state.online = true;
          });
          worker.on('message', (m: WorkerOutcome) => {
            outcome = m;
            state.reported = true;
          });
          worker.on('error', reject);
          // Resolving on 'exit' rather than 'message' is the point: the work
          // completing is not enough, the env has to tear down too.
          worker.on('exit', (code) => {
            state.exited = true;
            if (code !== 0) {
              reject(new Error(`worker ${index} exited with code ${code}`));
            } else {
              resolve(outcome ?? { error: 'worker sent no message' });
            }
          });
        });
      });

      let timer: NodeJS.Timeout | undefined;
      try {
        const results = await Promise.race([
          Promise.all(runs),
          new Promise<never>((_, reject) => {
            timer = setTimeout(() => reject(new Error(describe(progress))), deadlineMs);
          }),
        ]);
        for (const result of results) {
          expect(result.error).toBeUndefined();
          expect(result.calls).toBe(queries_per_worker * chunks_per_query);
        }
      } finally {
        if (timer) {
          clearTimeout(timer);
        }
        // Best effort, and deliberately not awaited: terminate() does not settle
        // for a worker wedged inside a blocking native call, and awaiting it here
        // swallowed the diagnostic above in favour of vitest's own timeout.
        for (const worker of workers) {
          void worker.terminate();
        }
      }
    },
    testTimeoutMs,
  );
});
