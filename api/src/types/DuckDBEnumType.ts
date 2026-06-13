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
    const valueIndexes: Record<string, number> = {};
    for (let i = 0; i < values.length; i++) {
      valueIndexes[values[i]] = i;
    }
    this.valueIndexes = valueIndexes;
    this.internalTypeId = internalTypeId;
  }
  public indexForValue(value: string): number {
    return this.valueIndexes[value];
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
