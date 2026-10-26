import duckdb from '@duckdb/node-bindings';
import { DuckDBCatalogEntry } from './DuckDBCatalogEntry';
import { CatalogEntryType } from './enums';

export class DuckDBCatalog {
  private readonly catalog: duckdb.Catalog;
  private readonly client_context: duckdb.ClientContext;

  /**
   * Backend type name copied when this catalog is acquired. Remains readable
   * after COMMIT, ROLLBACK, DETACH, or disconnect.
   */
  public readonly typeName: string;

  constructor(catalog: duckdb.Catalog, client_context: duckdb.ClientContext) {
    this.catalog = catalog;
    this.client_context = client_context;
    this.typeName = duckdb.catalog_get_type_name(catalog);
  }

  /**
   * Looks up an entry and copies its name and type into a metadata snapshot.
   *
   * Call only within the transaction in which this catalog was acquired,
   * while its database remains attached and its connection remains open.
   * Retaining this wrapper does not extend those lifetimes. Calls outside
   * this scope are unsupported; this method does not check for expired handles.
   *
   * For a catalog acquired from a scalar or table function callback's client
   * context, perform lookups synchronously within that same callback. Store
   * returned entry snapshots in bind data for use in later callbacks.
   *
   * Returns null if the schema or entry does not exist. Returned snapshots
   * remain readable after the lookup scope ends and do not track later changes.
   */
  public getEntry(
    type: CatalogEntryType,
    schemaName: string,
    entryName: string
  ): DuckDBCatalogEntry | null {
    const entry = duckdb.catalog_get_entry(
      this.catalog, this.client_context, type, schemaName, entryName
    );
    return entry ? new DuckDBCatalogEntry(
      duckdb.catalog_entry_get_name(entry),
      duckdb.catalog_entry_get_type(entry)
    ) : null;
  }
}
