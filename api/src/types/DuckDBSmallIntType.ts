import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBSmallIntType extends BaseDuckDBType<DuckDBTypeId.SMALLINT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.SMALLINT, alias);
  }
  public static readonly instance = new DuckDBSmallIntType();
  public static create(alias?: string): DuckDBSmallIntType {
    return alias ? new DuckDBSmallIntType(alias) : DuckDBSmallIntType.instance;
  }
  public static readonly Max = 2 ** 15 - 1;
  public static readonly Min = -(2 ** 15);
  public get max() {
    return DuckDBSmallIntType.Max;
  }
  public get min() {
    return DuckDBSmallIntType.Min;
  }
}
export const SMALLINT = DuckDBSmallIntType.instance;
