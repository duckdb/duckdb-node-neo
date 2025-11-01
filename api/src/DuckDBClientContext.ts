import duckdb from '@duckdb/node-bindings';

export class DuckDBClientContext {
  private readonly client_context: duckdb.ClientContext;

  constructor(client_context: duckdb.ClientContext) {
    this.client_context = client_context;
  }

  public get connectionId(): number {
    return duckdb.client_context_get_connection_id(this.client_context);
  }
}
