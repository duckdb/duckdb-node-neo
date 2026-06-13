import { BaseDuckDBType } from './BaseDuckDBType';
import type { DuckDBType } from './DuckDBType';
import { DuckDBLogicalType } from '../DuckDBLogicalType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { Json } from '../Json';

export class DuckDBMapType extends BaseDuckDBType<DuckDBTypeId.MAP> {
  public readonly keyType: DuckDBType;
  public readonly valueType: DuckDBType;
  public constructor(
    keyType: DuckDBType,
    valueType: DuckDBType,
    alias?: string
  ) {
    super(DuckDBTypeId.MAP, alias);
    this.keyType = keyType;
    this.valueType = valueType;
  }
  public toString(): string {
    return `MAP(${this.keyType}, ${this.valueType})`;
  }
  public override toLogicalType(): DuckDBLogicalType {
    const logicalType = DuckDBLogicalType.createMap(
      this.keyType.toLogicalType(),
      this.valueType.toLogicalType()
    );
    if (this.alias) {
      logicalType.alias = this.alias;
    }
    return logicalType;
  }
  public override toJson(): Json {
    return {
      typeId: this.typeId,
      keyType: this.keyType.toJson(),
      valueType: this.valueType.toJson(),
      ...(this.alias ? { alias: this.alias } : {}),
    };
  }
}
export function MAP(
  keyType: DuckDBType,
  valueType: DuckDBType,
  alias?: string
): DuckDBMapType {
  return new DuckDBMapType(keyType, valueType, alias);
}
