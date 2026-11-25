import duckdb from '@duckdb/node-bindings';

// Closing an already closed handle is a no-op, so fn may close it itself.
export function withFileHandle(
  file_system: duckdb.FileSystem,
  path: string,
  options: duckdb.FileOpenOptions,
  fn: (file_handle: duckdb.FileHandle) => void,
): void {
  const file_handle = duckdb.file_system_open_sync(file_system, path, options);
  try {
    fn(file_handle);
  } finally {
    duckdb.file_handle_close_sync(file_handle);
  }
}
