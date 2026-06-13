import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBUBigIntType extends BaseDuckDBType<DuckDBTypeId.UBIGINT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.UBIGINT, alias);
  }
  public static readonly instance = new DuckDBUBigIntType();
  public static create(alias?: string): DuckDBUBigIntType {
    return alias ? new DuckDBUBigIntType(alias) : DuckDBUBigIntType.instance;
  }
  public static readonly Max: bigint = 2n ** 64n - 1n;
  public static readonly Min: bigint = 0n;
  public get max() {
    return DuckDBUBigIntType.Max;
  }
  public get min() {
    return DuckDBUBigIntType.Min;
  }
}
export const UBIGINT = DuckDBUBigIntType.instance;
