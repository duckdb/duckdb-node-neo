import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBUHugeIntType extends BaseDuckDBType<DuckDBTypeId.UHUGEINT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.UHUGEINT, alias);
  }
  public static readonly instance = new DuckDBUHugeIntType();
  public static create(alias?: string): DuckDBUHugeIntType {
    return alias ? new DuckDBUHugeIntType(alias) : DuckDBUHugeIntType.instance;
  }
  public static readonly Max: bigint = 2n ** 128n - 1n;
  public static readonly Min: bigint = 0n;
  public get max() {
    return DuckDBUHugeIntType.Max;
  }
  public get min() {
    return DuckDBUHugeIntType.Min;
  }
}
export const UHUGEINT = DuckDBUHugeIntType.instance;
