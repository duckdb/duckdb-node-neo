# DuckDB Node Bindings

[Node](https://nodejs.org/) bindings to the [DuckDB C API](https://duckdb.org/docs/api/c/overview).

See [@duckdb/node-api](https://www.npmjs.com/package/@duckdb/node-api) for a high-level API built on these low-level bindings.

### Supported Platforms

- Linux arm64 glibc
- Linux arm64 musl
- Linux x64 glibc
- Linux x64 musl
- Mac OS X (Darwin) arm64 (Apple Silicon)
- Mac OS X (Darwin) x64 (Intel)
- Windows (Win32) arm64
- Windows (Win32) x64

## Custom log storage

Register a V1 log storage callback with `create_log_storage`,
`log_storage_set_name`, `log_storage_set_write_log_entry`, and
`register_log_storage`. Select it with `SET logging_sink = 'your_storage_name'`
and enable logging with `SET enable_logging = true`.

Log entries are copied and queued for deferred delivery on the JavaScript event
loop, including entries produced by synchronous bindings. Database operations
can complete before their callbacks run; wait for delivery when you need to
observe an entry. Queued entries retain the callback and extra data even if the
storage handle or database is destroyed. Pending callbacks are discarded when
the Node environment shuts down. Logger exceptions are swallowed because the
logging C API has no error channel; handle errors inside your callback if needed.
