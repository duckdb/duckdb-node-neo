import duckdb from '@duckdb/node-bindings';
import { DuckDBAppender } from './DuckDBAppender';
import { DuckDBExtractedStatements } from './DuckDBExtractedStatements';
import { DuckDBInstance } from './DuckDBInstance';
import { DuckDBMaterializedResult } from './DuckDBMaterializedResult';
import { DuckDBPendingResult } from './DuckDBPendingResult';
import { DuckDBPreparedStatement } from './DuckDBPreparedStatement';
import { DuckDBPreparedStatementWeakRefCollection } from './DuckDBPreparedStatementWeakRefCollection';
import { DuckDBResult } from './DuckDBResult';
import { DuckDBResultReader } from './DuckDBResultReader';
import { DuckDBType } from './DuckDBType';
import { DuckDBValue } from './values';

export class DuckDBConnection {
  private readonly connection: duckdb.Connection;
  private readonly preparedStatements: DuckDBPreparedStatementWeakRefCollection;
  constructor(connection: duckdb.Connection) {
    this.connection = connection;
    this.preparedStatements = new DuckDBPreparedStatementWeakRefCollection();
  }
  public static async create(
    instance?: DuckDBInstance
  ): Promise<DuckDBConnection> {
    if (instance) {
      return instance.connect();
    }
    return (await DuckDBInstance.fromCache()).connect();
  }
  /** Same as disconnectSync. */
  public closeSync() {
    return this.disconnectSync();
  }
  public disconnectSync() {
    this.preparedStatements.destroySync();
    return duckdb.disconnect_sync(this.connection);
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
    types?: DuckDBType[] | Record<string, DuckDBType | undefined>
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
    types?: DuckDBType[] | Record<string, DuckDBType | undefined>
  ): Promise<DuckDBResultReader> {
    return new DuckDBResultReader(await this.run(sql, values, types));
  }
  public async runAndReadAll(
    sql: string,
    values?: DuckDBValue[] | Record<string, DuckDBValue>,
    types?: DuckDBType[] | Record<string, DuckDBType | undefined>
  ): Promise<DuckDBResultReader> {
    const reader = new DuckDBResultReader(await this.run(sql, values, types));
    await reader.readAll();
    return reader;
  }
  public async runAndReadUntil(
    sql: string,
    targetRowCount: number,
    values?: DuckDBValue[] | Record<string, DuckDBValue>,
    types?: DuckDBType[] | Record<string, DuckDBType | undefined>
  ): Promise<DuckDBResultReader> {
    const reader = new DuckDBResultReader(await this.run(sql, values, types));
    await reader.readUntil(targetRowCount);
    return reader;
  }
  public async stream(
    sql: string,
    values?: DuckDBValue[] | Record<string, DuckDBValue>,
    types?: DuckDBType[] | Record<string, DuckDBType | undefined>
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
    types?: DuckDBType[] | Record<string, DuckDBType | undefined>
  ): Promise<DuckDBResultReader> {
    return new DuckDBResultReader(await this.stream(sql, values, types));
  }
  public async streamAndReadAll(
    sql: string,
    values?: DuckDBValue[] | Record<string, DuckDBValue>,
    types?: DuckDBType[] | Record<string, DuckDBType | undefined>
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
    types?: DuckDBType[] | Record<string, DuckDBType | undefined>
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
    types?: DuckDBType[] | Record<string, DuckDBType | undefined>
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
    types?: DuckDBType[] | Record<string, DuckDBType | undefined>
  ): Promise<DuckDBPendingResult> {
    const prepared = await this.prepare(sql);
    if (values) {
      prepared.bind(values, types);
    }
    return prepared.startStream();
  }
  public async prepare(sql: string): Promise<DuckDBPreparedStatement> {
    const prepared = new DuckDBPreparedStatement(
      await duckdb.prepare(this.connection, sql)
    );
    this.preparedStatements.add(prepared);
    return prepared;
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
      statement_count,
      this.preparedStatements
    );
  }
  public async createAppender(
    table: string,
    schema?: string | null,
    catalog?: string | null
  ): Promise<DuckDBAppender> {
    return new DuckDBAppender(
      duckdb.appender_create_ext(
        this.connection,
        catalog ?? null,
        schema ?? null,
        table
      )
    );
  }
}
