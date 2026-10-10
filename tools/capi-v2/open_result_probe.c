/* Can a V1 connection run a query while a streaming result on it is still open?
 *
 * Through 1.5.x it can: the new statement silently aborts the unfinished one.
 * On the 2.0 branch (duckdb/duckdb#26314, #26747) the new statement is refused
 * instead, with DUCKDB_ERROR_RESOURCE_IN_USE, until the open result is drained
 * or destroyed. This is a V1 behavior change across the 2.0 boundary, with no
 * change to any V1 signature.
 *
 * Builds against either library, like chunk_probe.c:
 *
 *   cd tools/capi-v2 && cc -std=c11 -I ../../bindings/libduckdb open_result_probe.c \
 *      -L ../../bindings/libduckdb -lduckdb -Wl,-rpath,@loader_path/../../bindings/libduckdb \
 *      -o open_result_probe && ./open_result_probe
 *   cd tools/capi-v2 && cc -std=c11 -I libduckdb-v2 open_result_probe.c -L libduckdb-v2 -lduckdb \
 *      -Wl,-rpath,@loader_path/libduckdb-v2 -o open_result_probe && ./open_result_probe
 *
 * Measured 2026-10-10. Against 1.5.6:
 *
 *   second query with a stream open: ok
 *   after destroying the stream:     ok
 *
 * Against the 2.0 preview:
 *
 *   second query with a stream open: error (type 44) Resource In Use Error: connection has an open
 *     result; drain or destroy it before starting a new query
 *   after destroying the stream:     ok
 */

#include "duckdb.h"

#include <stdio.h>

int main(void) {
	duckdb_database db;
	duckdb_connection con;
	duckdb_open(NULL, &db);
	duckdb_connect(db, &con);
	duckdb_query(con, "create table t as select range i from range(100000)", NULL);

	/* Read one chunk of a streaming result, and leave the rest unread. */
	duckdb_prepared_statement prepared;
	duckdb_prepare(con, "select * from t", &prepared);
	duckdb_result streaming;
	duckdb_execute_prepared_streaming(prepared, &streaming);
	duckdb_data_chunk chunk = duckdb_fetch_chunk(streaming);
	duckdb_destroy_data_chunk(&chunk);

	duckdb_result result;
	duckdb_state state = duckdb_query(con, "select 42", &result);
	printf("second query with a stream open: %s", state == DuckDBSuccess ? "ok" : "error");
	if (state != DuckDBSuccess) {
		printf(" (type %d) %s", (int)duckdb_result_error_type(&result), duckdb_result_error(&result));
	}
	printf("\n");
	duckdb_destroy_result(&result);

	duckdb_destroy_result(&streaming);
	state = duckdb_query(con, "select 42", &result);
	printf("after destroying the stream:     %s\n", state == DuckDBSuccess ? "ok" : "error");
	duckdb_destroy_result(&result);

	duckdb_destroy_prepare(&prepared);
	duckdb_disconnect(&con);
	duckdb_close(&db);
	return 0;
}
