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
 * Measured against the 2.0 preview (2026-10-10):
 *
 *   v1 open:                           ok
 *   v1 open again:                     ok (no conflict reported)
 *   v2 attach same file:               ok (NOT detected) (code 0)
 *   v2 attach again, same instance:    refused (code 3001)
 *   v2 attach again, second instance:  refused (code 3001)
 *   v2 attach again, second env:       ok (NOT detected) (code 0)
 *
 * duckdb_v2_instance_attach rejects a path "already attached on this or any
 * other instance of the environment". A V1 database is under no environment,
 * and neither is an instance of a second environment. V1 in turn only
 * deduplicates through an explicit instance cache. So the cross-API case falls
 * between the two, and so does a second environment.
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

/* Attaches path to instance, prints the outcome, and returns the code. */
static DUCKDB_V2_ERROR attach(const char *label, duckdb_v2_instance_handle instance, const char *path) {
	duckdb_v2_error_info_handle err = NULL;
	duckdb_v2_str view = str(path);
	DUCKDB_V2_ERROR error = duckdb_v2_instance_attach(instance, &view, NULL, NULL, true, &err);
	duckdb_v2_str text = {NULL, 0};
	if (err) {
		duckdb_v2_error_info_get_text(err, &text);
	}
	printf("%-35s%s (code %d) %.*s\n", label, error == DUCKDB_V2_ERROR_NONE ? "ok (NOT detected)" : "refused",
	       (int)error, (int)text.len, text.ptr ? text.ptr : "");
	if (err) {
		duckdb_v2_error_info_destroy(&err);
	}
	return error;
}

int main(void) {
	const char *path = "coexist_probe.duckdb";
	remove(path);

	duckdb_database v1_db;
	if (duckdb_open(path, &v1_db) != DuckDBSuccess) {
		printf("v1 open failed\n");
		return 1;
	}
	printf("%-35sok\n", "v1 open:");

	/* V1's own behavior, for reference: plain open does not deduplicate. */
	duckdb_database v1_db_again;
	printf("%-35s%s\n", "v1 open again:",
	       duckdb_open(path, &v1_db_again) == DuckDBSuccess ? "ok (no conflict reported)" : "refused");

	/* The same file through V2, under its own environment. */
	duckdb_v2_environment_handle env = NULL;
	duckdb_v2_instance_handle instance = NULL;
	duckdb_v2_instance_handle second_instance = NULL;
	duckdb_v2_environment_create(&env, NULL);
	duckdb_v2_instance_create(env, &instance, NULL);
	duckdb_v2_instance_create(env, &second_instance, NULL);

	if (attach("v2 attach same file:", instance, path) == DUCKDB_V2_ERROR_NONE) {
		/* Controls: V2 detects the same file within one environment, whichever
		 * instance of it asks. */
		attach("v2 attach again, same instance:", instance, path);
		attach("v2 attach again, second instance:", second_instance, path);

		/* But not across environments. */
		duckdb_v2_environment_handle second_env = NULL;
		duckdb_v2_instance_handle other = NULL;
		duckdb_v2_environment_create(&second_env, NULL);
		duckdb_v2_instance_create(second_env, &other, NULL);
		attach("v2 attach again, second env:", other, path);
		duckdb_v2_instance_destroy(&other);
		duckdb_v2_environment_destroy(&second_env);
	}

	duckdb_v2_instance_destroy(&second_instance);
	duckdb_v2_instance_destroy(&instance);
	duckdb_v2_environment_destroy(&env);
	duckdb_close(&v1_db);
	duckdb_close(&v1_db_again);
	remove(path);
	return 0;
}
