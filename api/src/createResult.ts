import duckdb from '@duckdb/node-bindings';
import { DuckDBMaterializedResult } from './DuckDBMaterializedResult';
import { DuckDBResult } from './DuckDBResult';
import { StatementType } from './enums';

export function createResult(
  result: duckdb.Result,
  preparedStatementType?: StatementType
) {
  if (duckdb.result_is_streaming(result)) {
    return new DuckDBResult(result, preparedStatementType);
  } else {
    return new DuckDBMaterializedResult(result, preparedStatementType);
  }
}
