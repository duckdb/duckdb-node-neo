import { BaseDuckDBType } from './BaseDuckDBType';
import type { DuckDBType } from './DuckDBType';
import { DuckDBLogicalType } from '../DuckDBLogicalType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { Json } from '../Json';
import { quotedIdentifier } from '../sql';

export class DuckDBStructType extends BaseDuckDBType<DuckDBTypeId.STRUCT> {
  public readonly entryNames: readonly string[];
  public readonly entryTypes: readonly DuckDBType[];
  public readonly entryIndexes: Readonly<Record<string, number>>;
  public constructor(
    entryNames: readonly string[],
    entryTypes: readonly DuckDBType[],
    alias?: string
  ) {
    super(DuckDBTypeId.STRUCT, alias);
    if (entryNames.length !== entryTypes.length) {
      throw new Error(`Could not create DuckDBStructType: \
        entryNames length (${entryNames.length}) does not match entryTypes length (${entryTypes.length})`);
    }
    this.entryNames = entryNames;
    this.entryTypes = entryTypes;
    const entryIndexes: Record<string, number> = {};
    for (let i = 0; i < entryNames.length; i++) {
      entryIndexes[entryNames[i]] = i;
    }
    this.entryIndexes = entryIndexes;
  }
  public get entryCount() {
    return this.entryNames.length;
  }
  /**
   * Whether this is an unnamed struct, i.e. a tuple, such as the type of
   * `(1, 'a')` or `row(1, 'a')`. DuckDB reports these as STRUCTs whose entry
   * names are all empty. A STRUCT with no entries is not a tuple.
   *
   * Values of a tuple type are `DuckDBTupleValue`s, whose items are
   * positional, rather than `DuckDBStructValue`s.
   */
  public get isTuple(): boolean {
    return (
      this.entryNames.length > 0 && this.entryNames.every((name) => name === '')
    );
  }
  public indexForEntry(entryName: string): number {
    return this.entryIndexes[entryName];
  }
  public typeForEntry(entryName: string): DuckDBType {
    return this.entryTypes[this.entryIndexes[entryName]];
  }
  public toString(): string {
    if (this.entryNames.length === 0) {
      return 'STRUCT';
    }
    if (this.isTuple) {
      return `TUPLE(${this.entryTypes.join(', ')})`;
    }
    const parts: string[] = [];
    for (let i = 0; i < this.entryNames.length; i++) {
      parts.push(
        `${quotedIdentifier(this.entryNames[i])} ${this.entryTypes[i]}`
      );
    }
    return `STRUCT(${parts.join(', ')})`;
  }
  public override toLogicalType(): DuckDBLogicalType {
    const logicalType = DuckDBLogicalType.createStruct(
      this.entryNames,
      this.entryTypes.map((t) => t.toLogicalType())
    );
    if (this.alias) {
      logicalType.alias = this.alias;
    }
    return logicalType;
  }
  public override toJson(): Json {
    return {
      typeId: this.typeId,
      entryNames: [...this.entryNames],
      entryTypes: this.entryTypes.map(t => t.toJson()),
      ...(this.alias ? { alias: this.alias } : {}),
    };
  }
}
export function STRUCT(
  entries: Record<string, DuckDBType>,
  alias?: string
): DuckDBStructType {
  const entryNames = Object.keys(entries);
  const entryTypes = Object.values(entries);
  return new DuckDBStructType(entryNames, entryTypes, alias);
}
/** Creates a tuple type: an unnamed STRUCT with the given entry types. */
export function TUPLE(
  entryTypes: readonly DuckDBType[],
  alias?: string
): DuckDBStructType {
  return new DuckDBStructType(
    entryTypes.map(() => ''),
    entryTypes,
    alias
  );
}
