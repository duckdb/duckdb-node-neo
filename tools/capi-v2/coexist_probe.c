/* Does opening one database file through both V1 and V2 in a single process get
 * detected?
 *
 * This matters because the plan is to ship both surfaces from one bindings
 * package so consumers can migrate a call site at a time -- which means both
 * APIs running in one process, each opening "its" database.
 *
 *   python3 tools/capi-v2/fetch_libduckdb_v2.py
 *   cc -std=c11 -I libduckdb-v2 coexist_probe.c -L libduckdb-v2 -lduckdb \
 *      -Wl,-rpath,@loader_path/libduckdb-v2 -o coexist_probe && ./coexist_probe
 *
 * Measured against the 2.0 preview (2026-09-05):
 *
 *   v1 open:                 ok
 *   v1 open again:           ok (no conflict reported)
 *   v2 open same file:       ok (NOT detected)
 *   v2 open twice (control): refused with RESOURCE_IN_USE (code 3001)
 *
 * duckdb_v2_open rejects a file "already open under the same environment", and a
 * V1 database is not under that environment. V1 in turn only deduplicates through
 * an explicit instance cache. So the cross-API case falls between the two.
 */

#include "duckdb.h"
#include "duckdb_v2.h"

#include <stdio.h>
#include <string.h>

static duckdb_v2_str str(const char *s) {
	duckdb_v2_str v;
	v.ptr = s;
	v.len = strlen(s);
	return v;
}

int main(void) {
	const char *path = "coexist_probe.duckdb";
	remove(path);

	duckdb_database v1_db;
	if (duckdb_open(path, &v1_db) != DuckDBSuccess) {
		printf("v1 open failed\n");
		return 1;
	}
	printf("v1 open:                 ok\n");

	/* V1's own behavior, for reference: plain open does not deduplicate. */
	duckdb_database v1_db_again;
	printf("v1 open again:           %s\n",
	       duckdb_open(path, &v1_db_again) == DuckDBSuccess ? "ok (no conflict reported)" : "refused");

	/* The same file through V2, under its own environment. */
	duckdb_v2_error_info_handle err = NULL;
	duckdb_v2_environment_handle env = NULL;
	duckdb_v2_database_handle v2_db = NULL;
	duckdb_v2_create_environment(&env, &err);

	DUCKDB_V2_ERROR error = duckdb_v2_open(env, str(path), NULL, 0, &v2_db, &err);
	duckdb_v2_str text = {NULL, 0};
	if (err) {
		duckdb_v2_error_info_get_text(err, &text);
	}
	printf("v2 open same file:       %s (code %d) %.*s\n",
	       error == DUCKDB_V2_ERROR_NONE ? "ok (NOT detected)" : "refused", (int)error,
	       (int)text.len, text.ptr ? text.ptr : "");
	if (err) {
		duckdb_v2_error_info_destroy(&err);
	}

	/* Control: V2 does detect the same file twice within one environment. */
	if (error == DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_database_handle v2_db_again = NULL;
		DUCKDB_V2_ERROR control = duckdb_v2_open(env, str(path), NULL, 0, &v2_db_again, &err);
		printf("v2 open twice (control): %s (code %d)\n",
		       control == DUCKDB_V2_ERROR_NONE ? "ok" : "refused with RESOURCE_IN_USE", (int)control);
		if (err) {
			duckdb_v2_error_info_destroy(&err);
		}
		if (control == DUCKDB_V2_ERROR_NONE) {
			duckdb_v2_close(&v2_db_again);
		}
	}

	duckdb_v2_close(&v2_db);
	duckdb_v2_destroy_environment(&env);
	duckdb_close(&v1_db);
	duckdb_close(&v1_db_again);
	remove(path);
	return 0;
}
