import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBLogicalType } from '../DuckDBLogicalType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { Json } from '../Json';
import { quotedString } from '../sql';

export class DuckDBEnumType extends BaseDuckDBType<DuckDBTypeId.ENUM> {
  public readonly values: readonly string[];
  public readonly valueIndexes: Readonly<Record<string, number>>;
  public readonly internalTypeId: DuckDBTypeId;
  public constructor(
    values: readonly string[],
    internalTypeId: DuckDBTypeId,
    alias?: string
  ) {
    super(DuckDBTypeId.ENUM, alias);
    this.values = values;
    // Null prototype: with a plain object literal, a lookup of an inherited key
    // such as 'toString' or 'constructor' returns a function instead of
    // undefined, so neither this map nor indexForValue could tell a member from
    // an Object.prototype property.
    const valueIndexes: Record<string, number> = Object.create(null);
    for (let i = 0; i < values.length; i++) {
      valueIndexes[values[i]] = i;
    }
    this.valueIndexes = valueIndexes;
    this.internalTypeId = internalTypeId;
  }
  /**
   * The index of the given value in this enum.
   *
   * Throws if the value is not a member: storing an unknown value would
   * otherwise write index 0, silently substituting the enum's first member.
   */
  public indexForValue(value: string): number {
    const index = this.valueIndexes[value];
    if (index === undefined) {
      throw new Error(
        `${quotedString(value)} is not a member of ${this.toStringForError()}`
      );
    }
    return index;
  }
  /**
   * Like toString, but bounded: an enum can have millions of members, and all of
   * them in an error message would bury the part that matters.
   */
  private toStringForError(): string {
    if (this.alias) {
      return this.alias;
    }
    const shown = 8;
    if (this.values.length <= shown) {
      return this.toString();
    }
    const listed = this.values.slice(0, shown).map(quotedString).join(', ');
    return `ENUM(${listed}, and ${this.values.length - shown} more)`;
  }
  public toString(): string {
    return `ENUM(${this.values.map(quotedString).join(', ')})`;
  }
  public override toLogicalType(): DuckDBLogicalType {
    const logicalType = DuckDBLogicalType.createEnum(this.values);
    if (this.alias) {
      logicalType.alias = this.alias;
    }
    return logicalType;
  }
  public override toJson(): Json {
    return {
      typeId: this.typeId,
      values: [...this.values],
      internalTypeId: this.internalTypeId,
      ...(this.alias ? { alias: this.alias } : {}),
    };
  }
}
export function ENUM8(
  values: readonly string[],
  alias?: string
): DuckDBEnumType {
  return new DuckDBEnumType(values, DuckDBTypeId.UTINYINT, alias);
}
export function ENUM16(
  values: readonly string[],
  alias?: string
): DuckDBEnumType {
  return new DuckDBEnumType(values, DuckDBTypeId.USMALLINT, alias);
}
export function ENUM32(
  values: readonly string[],
  alias?: string
): DuckDBEnumType {
  return new DuckDBEnumType(values, DuckDBTypeId.UINTEGER, alias);
}
export function ENUM(
  values: readonly string[],
  alias?: string
): DuckDBEnumType {
  if (values.length < 256) {
    return ENUM8(values, alias);
  } else if (values.length < 65536) {
    return ENUM16(values, alias);
  } else if (values.length < 4294967296) {
    return ENUM32(values, alias);
  } else {
    throw new Error(
      `ENUM types cannot have more than 4294967295 values; received ${values.length}`
    );
  }
}
