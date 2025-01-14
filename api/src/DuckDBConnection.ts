import duckdb from '@duckdb/node-bindings';
import { DuckDBAppender } from './DuckDBAppender';
import { DuckDBExtractedStatements } from './DuckDBExtractedStatements';
import { DuckDBInstance } from './DuckDBInstance';
import { DuckDBMaterializedResult } from './DuckDBMaterializedResult';
import { DuckDBPendingResult } from './DuckDBPendingResult';
import { DuckDBPreparedStatement } from './DuckDBPreparedStatement';
import { DuckDBResult } from './DuckDBResult';
import { DuckDBResultReader } from './DuckDBResultReader';
import { DuckDBType } from './DuckDBType';
import { DuckDBValue } from './values';

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
  /** Same as disconnect. */
  public close() {
    return this.disconnect();
  }
  public disconnect() {
    return duckdb.disconnect(this.connection);
  }
  public interrupt() {
    duckdb.interrupt(this.connection);
  }
  public get progress(): duckdb.QueryProgress {
    return duckdb.query_progress(this.connection);
  }
  public async run(
    sql: string,
    values?: DuckDBValue[] | Record<string, DuckDBValue>,
    types?: DuckDBType[] | Record<string, DuckDBType>
  ): Promise<DuckDBMaterializedResult> {
    if (values) {
      const prepared = await this.prepare(sql);
      prepared.bind(values, types);
      return prepared.run();
    } else {
      return new DuckDBMaterializedResult(
        await duckdb.query(this.connection, sql)
      );
    }
  }
  public async runAndRead(
    sql: string,
    values?: DuckDBValue[] | Record<string, DuckDBValue>,
    types?: DuckDBType[] | Record<string, DuckDBType>
  ): Promise<DuckDBResultReader> {
    return new DuckDBResultReader(await this.run(sql, values, types));
  }
  public async runAndReadAll(
    sql: string,
    values?: DuckDBValue[] | Record<string, DuckDBValue>,
    types?: DuckDBType[] | Record<string, DuckDBType>
  ): Promise<DuckDBResultReader> {
    const reader = new DuckDBResultReader(await this.run(sql, values, types));
    await reader.readAll();
    return reader;
  }
  public async runAndReadUntil(
    sql: string,
    targetRowCount: number,
    values?: DuckDBValue[] | Record<string, DuckDBValue>,
    types?: DuckDBType[] | Record<string, DuckDBType>
  ): Promise<DuckDBResultReader> {
    const reader = new DuckDBResultReader(await this.run(sql, values, types));
    await reader.readUntil(targetRowCount);
    return reader;
  }
  public async stream(
    sql: string,
    values?: DuckDBValue[] | Record<string, DuckDBValue>,
    types?: DuckDBType[] | Record<string, DuckDBType>
  ): Promise<DuckDBResult> {
    const prepared = await this.prepare(sql);
    if (values) {
      prepared.bind(values, types);
    }
    return prepared.stream();
  }
  public async streamAndRead(
    sql: string,
    values?: DuckDBValue[] | Record<string, DuckDBValue>,
    types?: DuckDBType[] | Record<string, DuckDBType>
  ): Promise<DuckDBResultReader> {
    return new DuckDBResultReader(await this.stream(sql, values, types));
  }
  public async streamAndReadAll(
    sql: string,
    values?: DuckDBValue[] | Record<string, DuckDBValue>,
    types?: DuckDBType[] | Record<string, DuckDBType>
  ): Promise<DuckDBResultReader> {
    const reader = new DuckDBResultReader(
      await this.stream(sql, values, types)
    );
    await reader.readAll();
    return reader;
  }
  public async streamAndReadUntil(
    sql: string,
    targetRowCount: number,
    values?: DuckDBValue[] | Record<string, DuckDBValue>,
    types?: DuckDBType[] | Record<string, DuckDBType>
  ): Promise<DuckDBResultReader> {
    const reader = new DuckDBResultReader(
      await this.stream(sql, values, types)
    );
    await reader.readUntil(targetRowCount);
    return reader;
  }
  public async start(
    sql: string,
    values?: DuckDBValue[] | Record<string, DuckDBValue>,
    types?: DuckDBType[] | Record<string, DuckDBType>
  ): Promise<DuckDBPendingResult> {
    const prepared = await this.prepare(sql);
    if (values) {
      prepared.bind(values, types);
    }
    return prepared.start();
  }
  public async startStream(
    sql: string,
    values?: DuckDBValue[] | Record<string, DuckDBValue>,
    types?: DuckDBType[] | Record<string, DuckDBType>
  ): Promise<DuckDBPendingResult> {
    const prepared = await this.prepare(sql);
    if (values) {
      prepared.bind(values, types);
    }
    return prepared.startStream();
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
      throw new Error(
        `Failed to extract statements: ${duckdb.extract_statements_error(
          extracted_statements
        )}`
      );
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
