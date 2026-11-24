import duckdb from '@duckdb/node-bindings';
import { DuckDBCatalog } from './DuckDBCatalog';

export class DuckDBClientContext {
  private readonly client_context: duckdb.ClientContext;

  constructor(client_context: duckdb.ClientContext) {
    this.client_context = client_context;
  }

  /**
   * Retrieves a catalog by name within an active transaction, such as during
   * a registered function callback. Returns null if no transaction is active
   * or the catalog does not exist. The connection must remain open.
   *
   * Use the returned catalog's getEntry only within the transaction or callback
   * in which it was acquired. Its typeName is copied and remains readable
   * after that scope ends.
   */
  public getCatalog(name: string): DuckDBCatalog | null {
    const catalog = duckdb.client_context_get_catalog(this.client_context, name);
    return catalog ? new DuckDBCatalog(catalog, this.client_context) : null;
  }

  public get connectionId(): number {
    return duckdb.client_context_get_connection_id(this.client_context);
  }
}
