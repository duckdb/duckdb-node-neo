import duckdb from '@duckdb/node-bindings';

export function withLogicalType(logical_type: duckdb.LogicalType, fn: (logical_type: duckdb.LogicalType) => void) {
  fn(logical_type);
}
