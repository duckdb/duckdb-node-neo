import duckdb from '@duckdb/node-bindings';
import { DuckDBCatalog } from './DuckDBCatalog';

export class DuckDBClientContext {
  private readonly client_context: duckdb.ClientContext;

  constructor(client_context: duckdb.ClientContext) {
    this.client_context = client_context;
  }

  public getCatalog(name: string): DuckDBCatalog | null {
    const catalog = duckdb.client_context_get_catalog(this.client_context, name);
    return catalog ? new DuckDBCatalog(catalog, this.client_context) : null;
  }

  public get connectionId(): number {
    return duckdb.client_context_get_connection_id(this.client_context);
  }
}
