import duckdb from '@duckdb/node-bindings';

export function withLogicalType(logical_type: duckdb.LogicalType, fn: (logical_type: duckdb.LogicalType) => void) {
  try {
    fn(logical_type);
  } finally {
    duckdb.destroy_logical_type(logical_type);
  }
}
