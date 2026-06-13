import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBIntegerType extends BaseDuckDBType<DuckDBTypeId.INTEGER> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.INTEGER, alias);
  }
  public static readonly instance = new DuckDBIntegerType();
  public static create(alias?: string): DuckDBIntegerType {
    return alias ? new DuckDBIntegerType(alias) : DuckDBIntegerType.instance;
  }
  public static readonly Max = 2 ** 31 - 1;
  public static readonly Min = -(2 ** 31);
  public get max() {
    return DuckDBIntegerType.Max;
  }
  public get min() {
    return DuckDBIntegerType.Min;
  }
}
export const INTEGER = DuckDBIntegerType.instance;
