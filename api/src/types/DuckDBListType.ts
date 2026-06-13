import { BaseDuckDBType } from './BaseDuckDBType';
import type { DuckDBType } from './DuckDBType';
import { DuckDBLogicalType } from '../DuckDBLogicalType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { Json } from '../Json';

export class DuckDBListType extends BaseDuckDBType<DuckDBTypeId.LIST> {
  public readonly valueType: DuckDBType;
  public constructor(valueType: DuckDBType, alias?: string) {
    super(DuckDBTypeId.LIST, alias);
    this.valueType = valueType;
  }
  public toString(): string {
    return `${this.valueType}[]`;
  }
  public override toLogicalType(): DuckDBLogicalType {
    const logicalType = DuckDBLogicalType.createList(
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
      valueType: this.valueType.toJson(),
      ...(this.alias ? { alias: this.alias } : {}),
    };
  }
}
export function LIST(valueType: DuckDBType, alias?: string): DuckDBListType {
  return new DuckDBListType(valueType, alias);
}
