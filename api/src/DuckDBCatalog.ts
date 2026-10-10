import duckdb from '@duckdb/node-bindings';
import { DuckDBCatalogEntry } from './DuckDBCatalogEntry';
import { CatalogEntryType } from './enums';

export class DuckDBCatalog {
  private readonly catalog: duckdb.Catalog;
  private readonly client_context: duckdb.ClientContext;

  constructor(catalog: duckdb.Catalog, client_context: duckdb.ClientContext) {
    this.catalog = catalog;
    this.client_context = client_context;
  }

  public get typeName(): string {
    return duckdb.catalog_get_type_name(this.catalog);
  }

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
