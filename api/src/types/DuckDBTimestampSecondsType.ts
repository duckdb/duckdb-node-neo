import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { DuckDBTimestampSecondsValue } from '../values';

export class DuckDBTimestampSecondsType extends BaseDuckDBType<DuckDBTypeId.TIMESTAMP_S> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.TIMESTAMP_S, alias);
  }
  public static readonly instance = new DuckDBTimestampSecondsType();
  public static create(alias?: string): DuckDBTimestampSecondsType {
    return alias ? new DuckDBTimestampSecondsType(alias) : DuckDBTimestampSecondsType.instance;
  }
  public get epoch() {
    return DuckDBTimestampSecondsValue.Epoch;
  }
  public get max() {
    return DuckDBTimestampSecondsValue.Max;
  }
  public get min() {
    return DuckDBTimestampSecondsValue.Min;
  }
  public get posInf() {
    return DuckDBTimestampSecondsValue.PosInf;
  }
  public get negInf() {
    return DuckDBTimestampSecondsValue.NegInf;
  }
}
export const TIMESTAMP_S = DuckDBTimestampSecondsType.instance;
