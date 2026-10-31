import duckdb from '@duckdb/node-bindings';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterAll, beforeAll, expect, suite, test } from 'vitest';
import { withConnection } from './utils/withConnection';
import { withDatabase } from './utils/withDatabase';

function createOptions(...flags: duckdb.FileFlag[]): duckdb.FileOpenOptions {
  const options = duckdb.create_file_open_options();
  for (const flag of flags) {
    duckdb.file_open_options_set_flag(options, flag, true);
  }
  return options;
}

function getFileSystem(connection: duckdb.Connection): duckdb.FileSystem {
  const context = duckdb.connection_get_client_context(connection);
  return duckdb.client_context_get_file_system(context);
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

suite('file system', () => {
  let dir: string;

  beforeAll(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'duckdb-node-file-system-'));
  });

  afterAll(() => {
    fs.rmSync(dir, { recursive: true, force: true });
  });

  test('write, then read back what was written', async () => {
    await withConnection(async (connection) => {
      const file_system = getFileSystem(connection);
      const file_path = path.join(dir, 'round_trip.txt');
      const data = encoder.encode('hello, file system');

      const writer = duckdb.file_system_open_sync(
        file_system,
        file_path,
        createOptions(duckdb.FileFlag.WRITE, duckdb.FileFlag.CREATE),
      );
      expect(duckdb.file_handle_write_sync(writer, data)).toBe(data.length);
      expect(duckdb.file_handle_tell(writer)).toBe(data.length);
      expect(duckdb.file_handle_size(writer)).toBe(data.length);
      duckdb.file_handle_sync_sync(writer);
      duckdb.file_handle_close_sync(writer);

      expect(fs.readFileSync(file_path, 'utf-8')).toBe('hello, file system');

      const reader = duckdb.file_system_open_sync(
        file_system,
        file_path,
        createOptions(duckdb.FileFlag.READ),
      );
      expect(duckdb.file_handle_size(reader)).toBe(data.length);
      expect(duckdb.file_handle_tell(reader)).toBe(0);

      const buffer = new Uint8Array(64);
      const bytes_read = duckdb.file_handle_read_sync(reader, buffer);
      expect(bytes_read).toBe(data.length);
      expect(decoder.decode(buffer.subarray(0, bytes_read))).toBe('hello, file system');
      expect(duckdb.file_handle_tell(reader)).toBe(data.length);

      // At the end of the file, a read returns no bytes rather than failing.
      expect(duckdb.file_handle_read_sync(reader, buffer)).toBe(0);
      duckdb.file_handle_close_sync(reader);
    });
  });

  test('seek, then read part of a file', async () => {
    await withConnection(async (connection) => {
      const file_system = getFileSystem(connection);
      const file_path = path.join(dir, 'seek.txt');
      fs.writeFileSync(file_path, '0123456789');

      const reader = duckdb.file_system_open_sync(
        file_system,
        file_path,
        createOptions(duckdb.FileFlag.READ),
      );
      duckdb.file_handle_seek(reader, 4);
      expect(duckdb.file_handle_tell(reader)).toBe(4);

      const buffer = new Uint8Array(3);
      expect(duckdb.file_handle_read_sync(reader, buffer)).toBe(3);
      expect(decoder.decode(buffer)).toBe('456');
      expect(duckdb.file_handle_tell(reader)).toBe(7);

      // Reads land in the given view only, not elsewhere in its buffer.
      const backing = new Uint8Array(8).fill(0x2e);
      duckdb.file_handle_seek(reader, 0);
      expect(duckdb.file_handle_read_sync(reader, backing.subarray(2, 5))).toBe(3);
      expect(decoder.decode(backing)).toBe('..012...');

      // An empty buffer reads nothing and leaves the position alone.
      expect(duckdb.file_handle_read_sync(reader, new Uint8Array(0))).toBe(0);
      expect(duckdb.file_handle_tell(reader)).toBe(3);
      duckdb.file_handle_close_sync(reader);
    });
  });

  test('append to an existing file', async () => {
    await withConnection(async (connection) => {
      const file_system = getFileSystem(connection);
      const file_path = path.join(dir, 'append.txt');
      fs.writeFileSync(file_path, 'abc');

      const writer = duckdb.file_system_open_sync(
        file_system,
        file_path,
        createOptions(duckdb.FileFlag.WRITE, duckdb.FileFlag.APPEND),
      );
      expect(duckdb.file_handle_write_sync(writer, encoder.encode('def'))).toBe(3);
      duckdb.file_handle_close_sync(writer);

      expect(fs.readFileSync(file_path, 'utf-8')).toBe('abcdef');
    });
  });

  test('create new fails if the file exists', async () => {
    await withConnection(async (connection) => {
      const file_system = getFileSystem(connection);
      const file_path = path.join(dir, 'create_new.txt');

      const options = createOptions(
        duckdb.FileFlag.WRITE,
        duckdb.FileFlag.CREATE,
        duckdb.FileFlag.CREATE_NEW,
      );
      const first = duckdb.file_system_open_sync(file_system, file_path, options);
      duckdb.file_handle_close_sync(first);

      expect(() => duckdb.file_system_open_sync(file_system, file_path, options)).toThrow(
        /exists/,
      );
    });
  });

  test('throw the error from DuckDB when a file cannot be opened', async () => {
    await withConnection(async (connection) => {
      const file_system = getFileSystem(connection);
      const file_path = path.join(dir, 'missing.txt');
      expect(() =>
        duckdb.file_system_open_sync(file_system, file_path, createOptions(duckdb.FileFlag.READ)),
      ).toThrow(/missing\.txt/);
    });
  });

  test('a file system can be used again after an open fails', async () => {
    await withConnection(async (connection) => {
      const file_system = getFileSystem(connection);
      const read_options = createOptions(duckdb.FileFlag.READ);
      expect(() =>
        duckdb.file_system_open_sync(file_system, path.join(dir, 'missing.txt'), read_options),
      ).toThrow();

      const file_path = path.join(dir, 'after_failure.txt');
      fs.writeFileSync(file_path, 'ok');
      const reader = duckdb.file_system_open_sync(file_system, file_path, read_options);
      expect(duckdb.file_handle_size(reader)).toBe(2);
      duckdb.file_handle_close_sync(reader);
    });
  });

  test('throw the error from DuckDB when an operation on a handle fails', async () => {
    await withConnection(async (connection) => {
      const file_system = getFileSystem(connection);
      const file_path = path.join(dir, 'read_only.txt');
      fs.writeFileSync(file_path, 'read only');

      const reader = duckdb.file_system_open_sync(
        file_system,
        file_path,
        createOptions(duckdb.FileFlag.READ),
      );
      expect(() => duckdb.file_handle_write_sync(reader, encoder.encode('nope'))).toThrow(
        /read_only\.txt/,
      );
      // The handle stays usable after a failed operation.
      const buffer = new Uint8Array(4);
      expect(duckdb.file_handle_read_sync(reader, buffer)).toBe(4);
      expect(decoder.decode(buffer)).toBe('read');
      duckdb.file_handle_close_sync(reader);
    });
  });

  test('operations on a closed handle throw', async () => {
    await withConnection(async (connection) => {
      const file_system = getFileSystem(connection);
      const file_path = path.join(dir, 'closed.txt');
      fs.writeFileSync(file_path, 'closed');

      const reader = duckdb.file_system_open_sync(
        file_system,
        file_path,
        createOptions(duckdb.FileFlag.READ),
      );
      duckdb.file_handle_close_sync(reader);

      expect(() => duckdb.file_handle_read_sync(reader, new Uint8Array(4))).toThrow(
        'File handle closed',
      );
      expect(() => duckdb.file_handle_write_sync(reader, new Uint8Array(4))).toThrow(
        'File handle closed',
      );
      expect(() => duckdb.file_handle_tell(reader)).toThrow('File handle closed');
      expect(() => duckdb.file_handle_size(reader)).toThrow('File handle closed');
      expect(() => duckdb.file_handle_seek(reader, 0)).toThrow('File handle closed');
      expect(() => duckdb.file_handle_sync_sync(reader)).toThrow('File handle closed');
      // Closing again is a no-op.
      duckdb.file_handle_close_sync(reader);
    });
  });

  test('reject invalid seek positions', async () => {
    await withConnection(async (connection) => {
      const file_system = getFileSystem(connection);
      const file_path = path.join(dir, 'seek_bounds.txt');
      fs.writeFileSync(file_path, 'seek');

      const reader = duckdb.file_system_open_sync(
        file_system,
        file_path,
        createOptions(duckdb.FileFlag.READ),
      );
      expect(() => duckdb.file_handle_seek(reader, -1)).toThrow('position out of range');
      expect(() => duckdb.file_handle_seek(reader, 1.5)).toThrow('position is not an integer');
      expect(() => duckdb.file_handle_seek(reader, Number.MAX_SAFE_INTEGER + 1)).toThrow(
        'position out of range',
      );
      expect(() => duckdb.file_handle_seek(reader, NaN)).toThrow('position is not an integer');
      expect(duckdb.file_handle_tell(reader)).toBe(0);
      duckdb.file_handle_close_sync(reader);
    });
  });

  test('reject invalid file flags', () => {
    const options = duckdb.create_file_open_options();
    expect(() => duckdb.file_open_options_set_flag(options, duckdb.FileFlag.INVALID, true)).toThrow(
      'Invalid file flag',
    );
    expect(() => duckdb.file_open_options_set_flag(options, 99 as duckdb.FileFlag, true)).toThrow(
      'Invalid file flag',
    );
    expect(() => duckdb.file_open_options_set_flag(options, -1 as duckdb.FileFlag, true)).toThrow();
  });

  test('reject clearing a file flag', () => {
    // DuckDB can only add flags. Clearing one would otherwise be ignored silently,
    // and the file opened with the flag still set.
    const options = duckdb.create_file_open_options();
    expect(() => duckdb.file_open_options_set_flag(options, duckdb.FileFlag.READ, false)).toThrow(
      'File flags cannot be cleared',
    );
  });

  test('reject a path containing a null byte', async () => {
    await withConnection(async (connection) => {
      const file_system = getFileSystem(connection);
      const file_path = path.join(dir, 'null_byte.txt');
      fs.writeFileSync(file_path, 'x');
      expect(() =>
        duckdb.file_system_open_sync(
          file_system,
          `${file_path}\0ignored`,
          createOptions(duckdb.FileFlag.READ),
        ),
      ).toThrow('File path must not contain null bytes');
    });
  });

  test('reject arguments of the wrong type', async () => {
    await withConnection(async (connection) => {
      const context = duckdb.connection_get_client_context(connection);
      const file_system = getFileSystem(connection);
      const options = createOptions(duckdb.FileFlag.READ);
      expect(() =>
        duckdb.file_system_open_sync(
          context as unknown as duckdb.FileSystem,
          path.join(dir, 'x'),
          options,
        ),
      ).toThrow('Invalid file system argument');
      expect(() =>
        duckdb.file_system_open_sync(
          file_system,
          path.join(dir, 'x'),
          file_system as unknown as duckdb.FileOpenOptions,
        ),
      ).toThrow('Invalid file open options argument');
      expect(() => duckdb.file_handle_tell(options as unknown as duckdb.FileHandle)).toThrow(
        'Invalid file handle argument',
      );
    });
  });

  test('a file handle outlives the connection it was opened through', async () => {
    await withDatabase({}, async (db) => {
      const file_path = path.join(dir, 'outlives.txt');
      fs.writeFileSync(file_path, 'still here');

      const connection = await duckdb.connect(db);
      const reader = duckdb.file_system_open_sync(
        getFileSystem(connection),
        file_path,
        createOptions(duckdb.FileFlag.READ),
      );
      duckdb.disconnect_sync(connection);

      const buffer = new Uint8Array(10);
      expect(duckdb.file_handle_read_sync(reader, buffer)).toBe(10);
      expect(decoder.decode(buffer)).toBe('still here');
      duckdb.file_handle_close_sync(reader);
    });
  });

});
