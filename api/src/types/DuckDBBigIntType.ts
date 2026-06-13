import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBBigIntType extends BaseDuckDBType<DuckDBTypeId.BIGINT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.BIGINT, alias);
  }
  public static readonly instance = new DuckDBBigIntType();
  public static create(alias?: string): DuckDBBigIntType {
    return alias ? new DuckDBBigIntType(alias) : DuckDBBigIntType.instance;
  }
  public static readonly Max: bigint = 2n ** 63n - 1n;
  public static readonly Min: bigint = -(2n ** 63n);
  public get max() {
    return DuckDBBigIntType.Max;
  }
  public get min() {
    return DuckDBBigIntType.Min;
  }
}
export const BIGINT = DuckDBBigIntType.instance;
