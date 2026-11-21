/* Does the V2 data chunk API accept a VARIANT column, where V1 does not?
 *
 * chunk_probe.c establishes that duckdb_create_data_chunk returns null for a
 * VARIANT logical type, on the library we ship and on the 2.0 preview alike.
 * This asks the same of V2 -- of duckdb_v2_data_chunk_create, and of
 * duckdb_v2_column_data_collection_create, which is the one
 * that matters, since the collection is what bulk append fills when there is
 * no appender.
 *
 * The VARIANT logical type is borrowed from a result schema rather than
 * constructed. When this was written, V2 built types only through a context,
 * which is handed out only inside callbacks; since duckdb/duckdb#26820 a
 * factory from a connection or instance builds them too, but the borrowed type
 * keeps this probe about chunks rather than type construction.
 *
 *   python3 tools/capi-v2/fetch_libduckdb_v2.py
 *   cd tools/capi-v2 && cc -std=c11 -I libduckdb-v2 chunk_probe_v2.c \
 *      -L libduckdb-v2 -lduckdb -Wl,-rpath,@loader_path/libduckdb-v2 \
 *      -o chunk_probe_v2 && ./chunk_probe_v2
 *
 * Measured against the 2.0 preview (2026-09-12), and unchanged against 2.0.1
 * (2026-11-21, through a factory from the connection):
 *
 *   borrowed column 1 type: VARIANT
 *
 *   [INTEGER]            -> code=0 chunk=non-NULL columns=1
 *   [VARIANT]            -> code=0 chunk=non-NULL columns=1
 *   [INTEGER, VARIANT]   -> code=0 chunk=non-NULL columns=2
 *
 *   column data collection:
 *   [INTEGER, VARIANT]   -> code=0 collection=non-NULL
 *
 * So V2 never had the V1 refusal. Note also that V2 reports failure through
 * a return code plus an error handle rather than a sentinel, so a refusal here
 * could not go unnoticed the way V1's null does.
 */
#include <stdio.h>
#include <string.h>
/* Every V2 function is marked unstable in 2.0, and the header declares none of
 * them unless the consumer opts in. */
#define DUCKDB_V2_API_ALLOW_UNSTABLE 1
#include <duckdb_v2.h>

static duckdb_v2_str str(const char *s) {
  duckdb_v2_str v; v.ptr = s; v.len = s ? strlen(s) : 0; return v;
}

#define CHECK(expr)                                                            \
  do {                                                                         \
    DUCKDB_V2_ERROR e = (expr);                                                \
    if (e != DUCKDB_V2_ERROR_NONE) {                                           \
      duckdb_v2_str t = {NULL, 0};                                             \
      if (err) duckdb_v2_error_info_get_text(err, &t);                         \
      printf("SETUP FAIL %s -> %d: %.*s\n", #expr, (int)e, (int)t.len,         \
             t.ptr ? t.ptr : "");                                              \
      return 1;                                                                \
    }                                                                          \
  } while (0)

static void try_chunk(duckdb_v2_factory_handle factory, const char *label,
                      duckdb_v2_logical_type_handle *types, idx_t n) {
  duckdb_v2_data_chunk_handle chunk = NULL;
  duckdb_v2_error_info_handle err = NULL;
  DUCKDB_V2_ERROR e = duckdb_v2_data_chunk_create(factory, types, n, &chunk, &err);
  printf("%-22s -> code=%d", label, (int)e);
  if (e == DUCKDB_V2_ERROR_NONE) {
    idx_t cols = 0;
    duckdb_v2_error_info_handle e2 = NULL;
    duckdb_v2_data_chunk_get_vector_count(chunk, &cols, &e2);
    printf(" chunk=%s columns=%llu", chunk ? "non-NULL" : "NULL",
           (unsigned long long)cols);
    duckdb_v2_data_chunk_destroy(&chunk);
  } else {
    duckdb_v2_str t = {NULL, 0};
    if (err) duckdb_v2_error_info_get_text(err, &t);
    printf(" msg=\"%.*s\"", (int)t.len, t.ptr ? t.ptr : "");
    if (err) duckdb_v2_error_info_destroy(&err);
  }
  printf("\n");
}

int main(void) {
  duckdb_v2_error_info_handle err = NULL;
  duckdb_v2_environment_handle env = NULL;
  duckdb_v2_instance_handle instance = NULL;
  duckdb_v2_connection_handle conn = NULL;
  duckdb_v2_statement_iterator_handle it = NULL;
  duckdb_v2_sql_statement_handle st = NULL;
  duckdb_v2_result_handle res = NULL;
  duckdb_v2_schema_handle schema = NULL;
  duckdb_v2_factory_handle factory = NULL;

  duckdb_v2_str memory = str(":memory:");
  CHECK(duckdb_v2_environment_create(&env, &err));
  CHECK(duckdb_v2_instance_create(env, &instance, &err));
  CHECK(duckdb_v2_instance_attach(instance, NULL, &memory, NULL, true, &err));
  CHECK(duckdb_v2_connection_create(instance, &conn, &err));
  CHECK(duckdb_v2_connection_get_factory(conn, &factory, &err));
  CHECK(duckdb_v2_parse_sql(conn, "select 1 as i, 42::variant as v", true, &it, &err));
  CHECK(duckdb_v2_statement_iterator_next(it, &st, &err));
  CHECK(duckdb_v2_statement_execute(conn, st, NULL, NULL, 0, &res, &err));
  CHECK(duckdb_v2_result_get_schema(res, &schema, &err));

  duckdb_v2_identifier_t name = {NULL, 0};
  duckdb_v2_logical_type_handle t_int = NULL, t_var = NULL;
  CHECK(duckdb_v2_schema_get_field(schema, 0, &name, &t_int, &err));
  CHECK(duckdb_v2_schema_get_field(schema, 1, &name, &t_var, &err));

  char text[128]; idx_t len = 0;
  CHECK(duckdb_v2_logical_type_to_text(t_var, text, sizeof(text), &len, &err));
  printf("borrowed column 1 type: %s\n\n", text);

  duckdb_v2_logical_type_handle ok[1]  = { t_int };
  duckdb_v2_logical_type_handle bad[1] = { t_var };
  duckdb_v2_logical_type_handle mix[2] = { t_int, t_var };

  try_chunk(factory, "[INTEGER]",          ok,  1);
  try_chunk(factory, "[VARIANT]",          bad, 1);
  try_chunk(factory, "[INTEGER, VARIANT]", mix, 2);

  /* The collection is what bulk append actually fills in V2. */
  printf("\ncolumn data collection:\n");
  duckdb_v2_column_data_collection_handle coll = NULL;
  duckdb_v2_error_info_handle cerr = NULL;
  DUCKDB_V2_ERROR ce = duckdb_v2_column_data_collection_create(
      factory, mix, 2, &coll, &cerr);
  printf("%-22s -> code=%d", "[INTEGER, VARIANT]", (int)ce);
  if (ce == DUCKDB_V2_ERROR_NONE) {
    printf(" collection=%s", coll ? "non-NULL" : "NULL");
    duckdb_v2_column_data_collection_destroy(&coll);
  } else {
    duckdb_v2_str t = {NULL, 0};
    if (cerr) duckdb_v2_error_info_get_text(cerr, &t);
    printf(" msg=\"%.*s\"", (int)t.len, t.ptr ? t.ptr : "");
    if (cerr) duckdb_v2_error_info_destroy(&cerr);
  }
  printf("\n");
  return 0;
}
