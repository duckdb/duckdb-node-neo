/* A minimal end-to-end exercise of the DuckDB C API V2, used to confirm that the
 * 2.0 preview library works and to show the shape of the calling convention Node
 * Neo's bindings will have to adopt.
 *
 *   python3 tools/capi-v2/fetch_libduckdb_v2.py
 *   cc -std=c11 -I libduckdb-v2 smoke_v2.c -L libduckdb-v2 -lduckdb \
 *      -Wl,-rpath,@loader_path/libduckdb-v2 -o smoke_v2 && ./smoke_v2
 *
 * Note what V1 does in one call and V2 spreads across several: opening needs an
 * environment, querying goes through a statement iterator, and a result is always
 * a stream. Every call returns a status and writes its product to an out-param.
 */

#include "duckdb_v2.h"

#include <stdio.h>
#include <string.h>

static duckdb_v2_str str(const char *s) {
	duckdb_v2_str v;
	v.ptr = s;
	v.len = s ? strlen(s) : 0;
	return v;
}

/* Every V2 call reports through a return code plus an optional error handle that
 * carries the message. The handle is owned by the caller even on the paths that
 * abandon the operation. */
#define CHECK(expr)                                                                                                    \
	do {                                                                                                               \
		DUCKDB_V2_ERROR error = (expr);                                                                                \
		if (error != DUCKDB_V2_ERROR_NONE) {                                                                           \
			duckdb_v2_str text = {NULL, 0};                                                                            \
			if (err) {                                                                                                 \
				duckdb_v2_error_info_get_text(err, &text);                                                             \
			}                                                                                                          \
			printf("FAIL %s -> %d: %.*s\n", #expr, (int)error, (int)text.len, text.ptr ? text.ptr : "");                \
			if (err) {                                                                                                 \
				duckdb_v2_error_info_destroy(&err);                                                                    \
			}                                                                                                          \
			return 1;                                                                                                  \
		}                                                                                                              \
	} while (0)

int main(void) {
	duckdb_v2_error_info_handle err = NULL;
	duckdb_v2_environment_handle env = NULL;
	duckdb_v2_database_handle db = NULL;
	duckdb_v2_connection_handle conn = NULL;
	duckdb_v2_statement_iterator_handle iterator = NULL;
	duckdb_v2_sql_statement_handle statement = NULL;
	duckdb_v2_result_handle result = NULL;
	duckdb_v2_schema_handle schema = NULL;

	CHECK(duckdb_v2_create_environment(&env, &err));
	CHECK(duckdb_v2_open(env, str(":memory:"), NULL, 0, &db, &err));
	CHECK(duckdb_v2_connect(db, &conn, &err));

	/* SQL text is parsed into a statement iterator, replacing both duckdb_query
	 * and duckdb_extract_statements. */
	CHECK(duckdb_v2_parse_sql(conn, "select 42 as answer, 'hi' as greeting, [1,2,3] as lst", &iterator, &err));
	CHECK(duckdb_v2_statement_iterator_next(iterator, &statement, &err));
	CHECK(duckdb_v2_statement_execute(conn, statement, NULL, NULL, 0, &result, &err));

	/* Column names and types arrive together as a schema rather than as
	 * per-column duckdb_column_name / duckdb_column_type calls. */
	CHECK(duckdb_v2_result_get_schema(result, &schema, &err));
	idx_t column_count = 0;
	CHECK(duckdb_v2_schema_get_count(schema, &column_count, &err));
	printf("columns: %llu\n", (unsigned long long)column_count);
	for (idx_t i = 0; i < column_count; i++) {
		duckdb_v2_identifier_t name = {NULL, 0};
		duckdb_v2_logical_type_handle type = NULL;
		/* Both are borrowed from the schema, and neither may be destroyed. */
		CHECK(duckdb_v2_schema_get_field(schema, i, &name, &type, &err));
		char text[256];
		idx_t length = 0;
		CHECK(duckdb_v2_logical_type_to_text(type, text, sizeof(text), &length, &err));
		printf("  [%llu] %.*s : %s\n", (unsigned long long)i, (int)name.len, name.ptr, text);
	}

	/* Results are always streamed. WAITING means the query has produced nothing
	 * yet, which is where an async binding would yield instead of blocking in
	 * result_wait. This replaces the pending-result interface. */
	idx_t rows = 0;
	while (1) {
		duckdb_v2_data_chunk_handle chunk = NULL;
		DUCKDB_V2_RESULT_STEP_STATUS status;
		CHECK(duckdb_v2_result_step(result, &chunk, &status, &err));
		if (status == DUCKDB_V2_RESULT_STEP_STATUS_WAITING) {
			CHECK(duckdb_v2_result_wait(result, &err));
			continue;
		}
		if (status != DUCKDB_V2_RESULT_STEP_STATUS_CHUNK) {
			break;
		}
		idx_t size = 0;
		CHECK(duckdb_v2_data_chunk_get_size(chunk, &size, &err));
		rows += size;

		duckdb_v2_vector_handle vector = NULL;
		CHECK(duckdb_v2_data_chunk_get_vector(chunk, 0, &vector, &err));
		/* One view carries data, validity, selection and count together, so a
		 * reader no longer has to special-case flat versus dictionary vectors. */
		duckdb_v2_vector_view view;
		CHECK(duckdb_v2_vector_get_view(vector, &view, &err));
		printf("chunk rows=%llu answer[0]=%d\n", (unsigned long long)size, ((const int32_t *)view.data)[0]);

		duckdb_v2_data_chunk_destroy(&chunk);
	}
	printf("total rows: %llu\n", (unsigned long long)rows);

	duckdb_v2_schema_destroy(&schema);
	duckdb_v2_result_destroy(&result);
	duckdb_v2_sql_statement_destroy(&statement);
	duckdb_v2_statement_iterator_destroy(&iterator);
	duckdb_v2_disconnect(&conn);
	duckdb_v2_close(&db);
	duckdb_v2_destroy_environment(&env);

	printf("OK\n");
	return 0;
}
