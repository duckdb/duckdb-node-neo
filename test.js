const duckdb_native = require('.');

// const config = duckdb.create_config();
// console.log(config);

console.log("DuckDB version:", duckdb_native.duckdb_library_version());


async function test() {

  const config = new duckdb_native.duckdb_config;
  duckdb_native.duckdb_create_config(config);

  /* in case someone would want to list conf options
  for (let conf_idx = 0; conf_idx < duckdb_native.duckdb_config_count(); conf_idx++) {
    const conf_name = new duckdb_native.out_string_wrapper;
    const conf_desc = new duckdb_native.out_string_wrapper;

    const status = duckdb_native.duckdb_get_config_flag(conf_idx, conf_name, conf_desc);
    if (status == duckdb_native.duckdb_state.DuckDBSuccess) {
      console.log(duckdb_native.out_get_string(conf_name), duckdb_native.out_get_string(conf_desc));
    }
  } */

  duckdb_native.duckdb_set_config(config, "threads", "1");

  const db = new duckdb_native.duckdb_database;
  const open_error = new duckdb_native.out_string_wrapper;
  const open_status = await duckdb_native.duckdb_open_ext(":memory:", db, config, open_error);

  if (open_status != duckdb_native.duckdb_state.DuckDBSuccess) {
    console.error("Failed to initialize database", duckdb_native.out_get_string(open_error));
    return;
  }

  const con = new duckdb_native.duckdb_connection;
  await duckdb_native.duckdb_connect(db, con);

  // create a statement and bind some values to it
  const prepared_statement = new duckdb_native.duckdb_prepared_statement;
  const prepare_status = await duckdb_native.duckdb_prepare(con, "FROM range(?)", prepared_statement);
  if (prepare_status != duckdb_native.duckdb_state.DuckDBSuccess) {
    console.error(duckdb_native.duckdb_prepare_error(prepared_statement));
    duckdb_native.duckdb_destroy_prepare(prepared_statement);
    return;
  }
  const state_bind = duckdb_native.duckdb_bind_int64(prepared_statement, 1, 4000);
  // TODO check state_bind

  // we want an incremental AND streaming query result
  const pending_result = new duckdb_native.duckdb_pending_result;
  await duckdb_native.duckdb_pending_prepared_streaming(prepared_statement, pending_result); // TODO can this fail?

  // pending query api, allows abandoning query processing between each call to pending_execute_task()
  const result = new duckdb_native.duckdb_result;
  var continue_execute = true;
  while (continue_execute) {
    const pending_status = await duckdb_native.duckdb_pending_execute_task(pending_result);

    switch (pending_status) {
      case duckdb_native.duckdb_pending_state.DUCKDB_PENDING_RESULT_NOT_READY:
        continue;
      case duckdb_native.duckdb_pending_state.DUCKDB_PENDING_RESULT_READY:
        await duckdb_native.duckdb_execute_pending(pending_result, result);
        continue_execute = false;
        break;
      case duckdb_native.duckdb_pending_state.DUCKDB_PENDING_ERROR:
        console.error(duckdb_native.duckdb_pending_error(pending_result)); // TODO this seems broken
        return;
    }
  }

  if (!duckdb_native.duckdb_result_is_streaming(result)) {
    // FIXME: this should also working for streaming result sets!
    return;
  }

  // now consume result set stream
  while (true) {
    const chunk = await duckdb_native.duckdb_stream_fetch_chunk(result);

    const n = duckdb_native.duckdb_data_chunk_get_size(chunk);
    if (n == 0) { // empty chunk means end of stream
      break;
    }

    // loop over columns and interpret vector bytes
    for (let col_idx = 0; col_idx < duckdb_native.duckdb_data_chunk_get_column_count(chunk); col_idx++) {
      const vector = duckdb_native.duckdb_data_chunk_get_vector(chunk, col_idx);
      const data_ptr = duckdb_native.duckdb_vector_get_data(vector);

      const type = duckdb_native.duckdb_vector_get_column_type(vector);
      const type_id = duckdb_native.duckdb_get_type_id(type);

      switch(type_id) {
        case duckdb_native.duckdb_type.DUCKDB_TYPE_BIGINT:
          const data_buf = duckdb_native.copy_buffer(data_ptr, 8 * n);
          const typed_data_arr = new BigInt64Array(data_buf.buffer);
          console.log(typed_data_arr);
          break;
          // TODO strings, structs, lists, maps, unions, ...
        default:
          console.log('Unsupported type :/');
          return;
      }
    }

    duckdb_native.duckdb_destroy_data_chunk(chunk);
  }

  // clean up again
  duckdb_native.duckdb_destroy_pending(pending_result);
  duckdb_native.duckdb_destroy_result(result);
  duckdb_native.duckdb_destroy_prepare(prepared_statement);
  duckdb_native.duckdb_disconnect(con);
  duckdb_native.duckdb_close(db);
  duckdb_native.duckdb_destroy_config(config);
}

test();
