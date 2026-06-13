import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBDoubleType extends BaseDuckDBType<DuckDBTypeId.DOUBLE> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.DOUBLE, alias);
  }
  public static readonly instance = new DuckDBDoubleType();
  public static create(alias?: string): DuckDBDoubleType {
    return alias ? new DuckDBDoubleType(alias) : DuckDBDoubleType.instance;
  }
  public static readonly Max = Number.MAX_VALUE;
  public static readonly Min = -Number.MAX_VALUE;
  public get max() {
    return DuckDBDoubleType.Max;
  }
  public get min() {
    return DuckDBDoubleType.Min;
  }
}
export const DOUBLE = DuckDBDoubleType.instance;
