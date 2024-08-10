import duckdb from '@duckdb-node-neo/duckdb-node-bindings';
import { DuckDBAppender } from './DuckDBAppender';
import { DuckDBExtractedStatements } from './DuckDBExtractedStatements';
import { DuckDBInstance } from './DuckDBInstance';
import { DuckDBPreparedStatement } from './DuckDBPreparedStatement';
import { DuckDBResult } from './DuckDBResult';

export class DuckDBConnection {
  private readonly connection: duckdb.Connection;
  constructor(connection: duckdb.Connection) {
    this.connection = connection;
  }
  public static async create(
    instance: DuckDBInstance
  ): Promise<DuckDBConnection> {
    return instance.connect();
  }
  public dispose(): Promise<void> {
    return duckdb.disconnect(this.connection);
  }
  public interrupt() {
    duckdb.interrupt(this.connection);
  }
  public get progress(): duckdb.QueryProgress {
    return duckdb.query_progress(this.connection);
  }
  public async run(sql: string): Promise<DuckDBResult> {
    return new DuckDBResult(await duckdb.query(this.connection, sql));
  }
  public async prepare(sql: string): Promise<DuckDBPreparedStatement> {
    return new DuckDBPreparedStatement(
      await duckdb.prepare(this.connection, sql)
    );
  }
  public async extractStatements(
    sql: string
  ): Promise<DuckDBExtractedStatements> {
    const { extracted_statements, statement_count } =
      await duckdb.extract_statements(this.connection, sql);
    if (statement_count === 0) {
      try {
        throw new Error(
          `Failed to extract statements: ${duckdb.extract_statements_error(
            extracted_statements
          )}`
        );
      } finally {
        duckdb.destroy_extracted(extracted_statements);
      }
    }
    return new DuckDBExtractedStatements(
      this.connection,
      extracted_statements,
      statement_count
    );
  }
  public async createAppender(
    schema: string,
    table: string
  ): Promise<DuckDBAppender> {
    return new DuckDBAppender(
      duckdb.appender_create(this.connection, schema, table)
    );
  }
}
