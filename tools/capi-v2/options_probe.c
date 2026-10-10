/* Can V2 enumerate configuration options without a database?
 *
 * V1 can: duckdb_config_count / duckdb_get_config_flag need nothing open, which is
 * what configurationOptionDescriptions.ts relies on. V2 enumerates through an
 * instance, but an instance starts with no database attached and is not started
 * until the first attach or connection -- so the question is whether enumeration
 * works on one that has not started.
 *
 *   python3 tools/capi-v2/fetch_libduckdb_v2.py
 *   cc -std=c11 -I libduckdb-v2 options_probe.c -L libduckdb-v2 -lduckdb \
 *      -Wl,-rpath,@loader_path/libduckdb-v2 -o options_probe && ./options_probe
 *
 * Measured against the 2.0 preview (2026-10-10):
 *
 *   before startup: count=192 code=0, first option: access_mode -- Access mode of
 *     the database (AUTOMATIC, READ_ONLY or READ_WRITE).
 */

#include "duckdb_v2.h"

#include <stdio.h>
#include <string.h>

int main(void) {
	duckdb_v2_environment_handle env = NULL;
	duckdb_v2_instance_handle instance = NULL;
	duckdb_v2_error_info_handle err = NULL;
	duckdb_v2_environment_create(&env, NULL);
	duckdb_v2_instance_create(env, &instance, NULL);

	idx_t count = 0;
	DUCKDB_V2_ERROR error = duckdb_v2_instance_get_option_count(instance, &count, &err);
	printf("before startup: count=%llu code=%d", (unsigned long long)count, (int)error);

	duckdb_v2_option_handle option = NULL;
	if (error == DUCKDB_V2_ERROR_NONE && count > 0 &&
	    duckdb_v2_instance_get_option_by_index(instance, 0, &option, &err) == DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_identifier_t name = {NULL, 0};
		duckdb_v2_str description = {NULL, 0};
		duckdb_v2_option_get_name(option, &name, &err);
		duckdb_v2_option_get_description(option, &description, &err);
		printf(", first option: %.*s -- %.*s", (int)name.len, name.ptr, (int)description.len, description.ptr);
		duckdb_v2_option_destroy(&option);
	}
	printf("\n");
	if (err) {
		duckdb_v2_error_info_destroy(&err);
	}

	duckdb_v2_instance_destroy(&instance);
	duckdb_v2_environment_destroy(&env);
	return 0;
}
