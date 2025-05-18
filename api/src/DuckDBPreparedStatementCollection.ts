import { DuckDBPreparedStatement } from './DuckDBPreparedStatement';

export interface DuckDBPreparedStatementCollection {
  add(prepared: DuckDBPreparedStatement): void;
}
