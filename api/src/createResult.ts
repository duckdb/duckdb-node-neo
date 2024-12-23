import duckdb from '@duckdb/node-bindings';
import { DuckDBMaterializedResult } from './DuckDBMaterializedResult';
import { DuckDBResult } from './DuckDBResult';

export function createResult(result: duckdb.Result) {
  if (duckdb.result_is_streaming(result)) {
    return new DuckDBResult(result);
  } else {
    return new DuckDBMaterializedResult(result);
  }
}
