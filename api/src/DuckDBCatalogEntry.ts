import type { CatalogEntryType } from './enums';

export class DuckDBCatalogEntry {
  public readonly name: string;
  public readonly type: CatalogEntryType;

  constructor(name: string, type: CatalogEntryType) {
    this.name = name;
    this.type = type;
  }
}
