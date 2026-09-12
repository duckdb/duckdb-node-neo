/* Does duckdb_create_data_chunk return null for a column type it refuses, or a
 * real chunk reporting zero columns?
 *
 * This matters because the two are indistinguishable from JS: reading a null
 * handle through duckdb_data_chunk_get_column_count returns 0, so a refusal
 * looks like a chunk that merely came back empty. If the handle is null, a
 * binding that wraps it hands JS something that is a hazard on every call, and
 * the check belongs in the binding rather than in the API layer.
 *
 * Built against the V1 library we ship today, not the 2.0 preview, because the
 * behaviour is V1's and the bindings' -- but the answer carries into V2, where
 * a column data collection is created from a fixed set of column types the
 * same way.
 *
 *   cd tools/capi-v2 && cc -std=c11 -I ../../bindings/libduckdb chunk_probe.c \
 *      -L ../../bindings/libduckdb -lduckdb \
 *      -Wl,-rpath,@loader_path/../../bindings/libduckdb -o chunk_probe && ./chunk_probe
 *
 * Measured against DuckDB 1.5.5, and unchanged against the 2.0 preview
 * (2026-09-12):
 *
 *   VARIANT logical type: created, type id=41
 *
 *   [INTEGER]            handle=non-NULL columns=1 size=0
 *   [VARIANT]            handle=NULL
 *   [INTEGER, VARIANT]   handle=NULL
 *   [] (zero types)      handle=non-NULL columns=0 size=0
 *
 * So: null, all-or-nothing, and a zero-column chunk is legitimate -- zero
 * columns is not the signal, the null is.
 */
#include <stdio.h>
#include <duckdb.h>

static void probe(const char *label, duckdb_logical_type *types, idx_t n) {
  duckdb_data_chunk chunk = duckdb_create_data_chunk(types, n);
  printf("%-20s handle=%s", label, chunk == NULL ? "NULL" : "non-NULL");
  if (chunk != NULL) {
    printf(" columns=%llu size=%llu",
      (unsigned long long)duckdb_data_chunk_get_column_count(chunk),
      (unsigned long long)duckdb_data_chunk_get_size(chunk));
    duckdb_destroy_data_chunk(&chunk);
  }
  printf("\n");
}

int main(void) {
  duckdb_logical_type i32 = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type var = duckdb_create_logical_type(DUCKDB_TYPE_VARIANT);
  printf("VARIANT logical type: %s, type id=%d\n\n",
         var == NULL ? "NULL" : "created", (int)duckdb_get_type_id(var));

  duckdb_logical_type ok[1]  = { i32 };
  duckdb_logical_type bad[1] = { var };
  duckdb_logical_type mix[2] = { i32, var };

  probe("[INTEGER]",          ok,  1);
  probe("[VARIANT]",          bad, 1);
  probe("[INTEGER, VARIANT]", mix, 2);
  probe("[] (zero types)",    ok,  0);

  duckdb_destroy_logical_type(&i32);
  duckdb_destroy_logical_type(&var);
  return 0;
}
