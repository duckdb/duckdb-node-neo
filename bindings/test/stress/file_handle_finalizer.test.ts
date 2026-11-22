import duckdb from '@duckdb/node-bindings';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import v8 from 'node:v8';
import vm from 'node:vm';
import { afterAll, beforeAll, expect, suite, test } from 'vitest';
import { withConnection } from '../utils/withConnection';

// File handles not closed with file_handle_close_sync are closed by their
// finalizer. This depends on when collection happens, so it lives here rather
// than in the main suite. gc() comes from a vm context created while
// --expose-gc is briefly on.
v8.setFlagsFromString('--expose-gc');
const forceGC = vm.runInNewContext('gc') as () => void;
v8.setFlagsFromString('--no-expose-gc');

// /dev/fd lists open descriptors on Linux and macOS. Elsewhere the check is skipped.
const countOpenFiles = fs.existsSync('/dev/fd')
  ? () => fs.readdirSync('/dev/fd').length
  : undefined;

suite('file handle finalizer', () => {
  let dir: string;

  beforeAll(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'duckdb-node-file-handle-'));
  });

  afterAll(() => {
    fs.rmSync(dir, { recursive: true, force: true });
  });

  test('handles left open are closed when collected', async () => {
    await withConnection(async (connection) => {
      const file_path = path.join(dir, 'collected.txt');
      const open_files_before = countOpenFiles?.();
      (() => {
        const context = duckdb.connection_get_client_context(connection);
        const file_system = duckdb.client_context_get_file_system(context);
        const options = duckdb.create_file_open_options();
        duckdb.file_open_options_set_flag(options, duckdb.FileFlag.WRITE, true);
        duckdb.file_open_options_set_flag(options, duckdb.FileFlag.CREATE, true);
        for (let i = 0; i < 100; i++) {
          const writer = duckdb.file_system_open_sync(file_system, file_path, options);
          duckdb.file_handle_write_sync(writer, new TextEncoder().encode(`${i}`));
        }
        // Each open handle holds a descriptor, so the count below means something.
        if (countOpenFiles && open_files_before !== undefined) {
          expect(countOpenFiles()).toBeGreaterThanOrEqual(open_files_before + 100);
        }
      })();
      forceGC();
      await new Promise((resolve) => setImmediate(resolve));
      forceGC();
      if (countOpenFiles && open_files_before !== undefined) {
        expect(countOpenFiles()).toBeLessThan(open_files_before + 10);
      }
    });
  });
});
