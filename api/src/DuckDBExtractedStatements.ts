import duckdb from '@duckdb/node-bindings';
import { DuckDBPreparedStatement } from './DuckDBPreparedStatement';

export class DuckDBExtractedStatements {
  private readonly connection: duckdb.Connection;
  private readonly extracted_statements: duckdb.ExtractedStatements;
  private readonly statement_count: number;
  constructor(
    connection: duckdb.Connection,
    extracted_statements: duckdb.ExtractedStatements,
    statement_count: number
  ) {
    this.connection = connection;
    this.extracted_statements = extracted_statements;
    this.statement_count = statement_count;
  }
  public dispose() {
    duckdb.destroy_extracted(this.extracted_statements);
  }
  public get count(): number {
    return this.statement_count;
  }
  public async prepare(index: number): Promise<DuckDBPreparedStatement> {
    return new DuckDBPreparedStatement(
      await duckdb.prepare_extracted_statement(
        this.connection,
        this.extracted_statements,
        index
      )
    );
  }
}
