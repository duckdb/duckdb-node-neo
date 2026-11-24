/* Can V2 enumerate configuration options without a database?
 *
 * V1 can: duckdb_config_count / duckdb_get_config_flag need nothing open, which is
 * what configurationOptionDescriptions.ts relies on. V2 enumerates through a
 * config handle borrowed from an instance, connection or context, so the
 * question is whether an instance with no database attached is enough.
 *
 * When this was first asked, an instance was not started until the first attach
 * or connection, and options hung off the instance itself. Since
 * duckdb/duckdb#26820 an instance starts as soon as it is created (startup-only
 * options go to duckdb_v2_instance_create_with_options), and options are read
 * through duckdb_v2_config_*.
 *
 *   python3 tools/capi-v2/fetch_libduckdb_v2.py
 *   cc -std=c11 -I libduckdb-v2 options_probe.c -L libduckdb-v2 -lduckdb \
 *      -Wl,-rpath,@loader_path/libduckdb-v2 -o options_probe && ./options_probe
 *
 * Measured against the 2.0 preview (2026-10-10), through the instance:
 *
 *   before startup: count=192 code=0, first option: access_mode -- Access mode of
 *     the database (AUTOMATIC, READ_ONLY or READ_WRITE).
 *
 * Against 2.0.1 (2026-11-21), through the instance's config:
 *
 *   nothing attached: count=208 code=0, first option: access_mode -- Access mode
 *     of the database (AUTOMATIC, READ_ONLY or READ_WRITE).
 */

/* Every V2 function is marked unstable in 2.0, and the header declares none of
 * them unless the consumer opts in. */
#define DUCKDB_V2_API_ALLOW_UNSTABLE 1
#include "duckdb_v2.h"

#include <stdio.h>
#include <string.h>

int main(void) {
	duckdb_v2_environment_handle env = NULL;
	duckdb_v2_instance_handle instance = NULL;
	duckdb_v2_config_handle config = NULL;
	duckdb_v2_error_info_handle err = NULL;
	duckdb_v2_environment_create(&env, NULL);
	duckdb_v2_instance_create(env, &instance, NULL);
	duckdb_v2_instance_get_config(instance, &config, NULL);

	idx_t count = 0;
	DUCKDB_V2_ERROR error = duckdb_v2_config_get_option_count(config, &count, &err);
	printf("nothing attached: count=%llu code=%d", (unsigned long long)count, (int)error);

	duckdb_v2_option_handle option = NULL;
	if (error == DUCKDB_V2_ERROR_NONE && count > 0 &&
	    duckdb_v2_config_get_option_by_index(config, 0, &option, &err) == DUCKDB_V2_ERROR_NONE) {
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
