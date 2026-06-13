import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBUSmallIntType extends BaseDuckDBType<DuckDBTypeId.USMALLINT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.USMALLINT, alias);
  }
  public static readonly instance = new DuckDBUSmallIntType();
  public static create(alias?: string): DuckDBUSmallIntType {
    return alias ? new DuckDBUSmallIntType(alias) : DuckDBUSmallIntType.instance;
  }
  public static readonly Max = 2 ** 16 - 1;
  public static readonly Min = 0;
  public get max() {
    return DuckDBUSmallIntType.Max;
  }
  public get min() {
    return DuckDBUSmallIntType.Min;
  }
}
export const USMALLINT = DuckDBUSmallIntType.instance;
