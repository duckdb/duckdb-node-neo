import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBLogicalType } from '../DuckDBLogicalType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { Json } from '../Json';

export class DuckDBDecimalType extends BaseDuckDBType<DuckDBTypeId.DECIMAL> {
  public readonly width: number;
  public readonly scale: number;
  public constructor(width: number, scale: number, alias?: string) {
    super(DuckDBTypeId.DECIMAL, alias);
    this.width = width;
    this.scale = scale;
  }
  public toString(): string {
    return `DECIMAL(${this.width},${this.scale})`;
  }
  public override toLogicalType(): DuckDBLogicalType {
    const logicalType = DuckDBLogicalType.createDecimal(this.width, this.scale);
    if (this.alias) {
      logicalType.alias = this.alias;
    }
    return logicalType;
  }
  public override toJson(): Json {
    return {
      typeId: this.typeId,
      width: this.width,
      scale: this.scale,
      ...(this.alias ? { alias: this.alias } : {}),
    };
  }
  public static readonly default = new DuckDBDecimalType(18, 3);
}
export function DECIMAL(
  width?: number,
  scale?: number,
  alias?: string
): DuckDBDecimalType {
  if (width === undefined) {
    return DuckDBDecimalType.default;
  }
  if (scale === undefined) {
    return new DuckDBDecimalType(width, 0);
  }
  return new DuckDBDecimalType(width, scale, alias);
}
