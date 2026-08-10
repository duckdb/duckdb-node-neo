import duckdb, { Value } from '@duckdb/node-bindings';
import { DuckDBVector } from './DuckDBVector';
import { DuckDBValue } from './values';

/**
 * Converts a `duckdb.Value` into a `DuckDBValue`.
 *
 * Results normally arrive as vectors rather than values, so this is for the
 * places that hand back a value instead: table function parameters, and
 * `duckdb_get_table_names`.
 *
 * It works by referencing the value into a vector of its own and reading the one
 * element back, which reuses the vector reading path rather than duplicating it.
 * That matters for more than tidiness: the value accessors cannot reach inside an
 * ARRAY or a UNION at all, because `duckdb_get_list_child` and
 * `duckdb_get_struct_child` guard on the exact logical type id and return null
 * for both. Going through a vector covers every type, and leaves one
 * implementation of how each type is represented rather than two that can drift.
 *
 * The cost is roughly an extra microsecond per scalar value, which is fine here
 * and would not be in a per-row path. Per-row data already arrives as vectors and
 * never comes through here.
 */
export function readValue(value: Value): DuckDBValue {
  // Capacity of one: the vector exists only to hold this single value. It is
  // destroyed when collected, and getItem materializes what it returns, so
  // nothing handed back keeps a reference to the vector's memory.
  const vector = duckdb.create_vector(duckdb.get_value_type(value), 1);
  duckdb.vector_reference_value(vector, value);
  return DuckDBVector.create(vector, 1).getItem(0);
}
