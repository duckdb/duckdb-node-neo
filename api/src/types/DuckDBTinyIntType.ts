import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBTinyIntType extends BaseDuckDBType<DuckDBTypeId.TINYINT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.TINYINT, alias);
  }
  public static readonly instance = new DuckDBTinyIntType();
  public static create(alias?: string): DuckDBTinyIntType {
    return alias ? new DuckDBTinyIntType(alias) : DuckDBTinyIntType.instance;
  }
  public static readonly Max = 2 ** 7 - 1;
  public static readonly Min = -(2 ** 7);
  public get max() {
    return DuckDBTinyIntType.Max;
  }
  public get min() {
    return DuckDBTinyIntType.Min;
  }
}
export const TINYINT = DuckDBTinyIntType.instance;
