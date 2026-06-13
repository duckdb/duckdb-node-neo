import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBUIntegerType extends BaseDuckDBType<DuckDBTypeId.UINTEGER> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.UINTEGER, alias);
  }
  public static readonly instance = new DuckDBUIntegerType();
  public static create(alias?: string): DuckDBUIntegerType {
    return alias ? new DuckDBUIntegerType(alias) : DuckDBUIntegerType.instance;
  }
  public static readonly Max = 2 ** 32 - 1;
  public static readonly Min = 0;
  public get max() {
    return DuckDBUIntegerType.Max;
  }
  public get min() {
    return DuckDBUIntegerType.Min;
  }
}
export const UINTEGER = DuckDBUIntegerType.instance;
