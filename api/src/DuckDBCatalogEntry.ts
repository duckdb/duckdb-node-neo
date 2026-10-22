import type { CatalogEntryType } from './enums';

/**
 * Entry metadata copied at lookup time. Remains readable after the lookup
 * scope ends and does not track later catalog changes.
 */
export class DuckDBCatalogEntry {
  public readonly name: string;
  public readonly type: CatalogEntryType;

  constructor(name: string, type: CatalogEntryType) {
    this.name = name;
    this.type = type;
  }
}
