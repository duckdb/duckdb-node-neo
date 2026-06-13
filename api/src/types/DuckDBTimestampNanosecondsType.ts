import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { DuckDBTimestampNanosecondsValue } from '../values';

export class DuckDBTimestampNanosecondsType extends BaseDuckDBType<DuckDBTypeId.TIMESTAMP_NS> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.TIMESTAMP_NS, alias);
  }
  public static readonly instance = new DuckDBTimestampNanosecondsType();
  public static create(alias?: string): DuckDBTimestampNanosecondsType {
    return alias ? new DuckDBTimestampNanosecondsType(alias) : DuckDBTimestampNanosecondsType.instance;
  }
  public get epoch() {
    return DuckDBTimestampNanosecondsValue.Epoch;
  }
  public get max() {
    return DuckDBTimestampNanosecondsValue.Max;
  }
  public get min() {
    return DuckDBTimestampNanosecondsValue.Min;
  }
  public get posInf() {
    return DuckDBTimestampNanosecondsValue.PosInf;
  }
  public get negInf() {
    return DuckDBTimestampNanosecondsValue.NegInf;
  }
}
export const TIMESTAMP_NS = DuckDBTimestampNanosecondsType.instance;
