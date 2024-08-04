import duckdb from '@jraymakers/duckdb-node-bindings';

export class DuckDBAppender {
  private readonly appender: duckdb.Appender;
  constructor(appender: duckdb.Appender) {
    this.appender = appender;
  }
  public async dispose() {
    duckdb.appender_destroy(this.appender);
  }
  public async flush() {
    duckdb.appender_flush(this.appender);
  }
  // TODO
}
