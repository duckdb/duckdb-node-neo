// Registers a scalar function and then lets the script end, so the parent test
// can check whether the process exits on its own.
//
// Thread-safe functions are created referenced and hold the event loop open, so
// unless they are unreferenced a registered scalar function keeps its process
// alive indefinitely. The database is deliberately left open in most scenarios:
// closing it destroys the extra info, which releases the thread-safe functions
// and hides the problem.
//
// Run as: node scalarFunctionExit.cjs <scenario>, with the bindings package
// resolvable from the working directory.

const duckdb = require('@duckdb/node-bindings');

const scenario = process.argv[2];

async function main() {
  const db = await duckdb.open();
  const connection = await duckdb.connect(db);

  const scalar_function = duckdb.create_scalar_function();
  duckdb.scalar_function_set_name(scalar_function, 'my_func');
  duckdb.scalar_function_set_return_type(
    scalar_function,
    duckdb.create_logical_type(duckdb.Type.VARCHAR),
  );
  duckdb.scalar_function_set_bind(scalar_function, (info) => {
    duckdb.scalar_function_set_bind_data(info, { 'token': 'bind_data' });
  });
  duckdb.scalar_function_set_function(
    scalar_function,
    (_info, input, output) => {
      const rowCount = duckdb.data_chunk_get_size(input);
      for (let i = 0; i < rowCount; i++) {
        duckdb.vector_assign_string_element(output, i, 'ok');
      }
    },
  );
  duckdb.register_scalar_function(connection, scalar_function);

  if (scenario !== 'registered_handle_kept') {
    duckdb.destroy_scalar_function_sync(scalar_function);
  }

  if (scenario !== 'registered_never_called') {
    await duckdb.query(connection, 'select my_func() from range(10)');
  }

  if (scenario === 'closed') {
    duckdb.disconnect_sync(connection);
    duckdb.close_sync(db);
  }

  // Deliberately keep the database and connection reachable from module scope in
  // the other scenarios, so nothing is collected and closed by a finalizer.
  globalThis.__kept = { db, connection };
}

main().then(
  () => process.stdout.write('done\n'),
  (e) => {
    process.stderr.write(`${(e && e.message) || e}\n`);
    process.exitCode = 1;
  },
);
