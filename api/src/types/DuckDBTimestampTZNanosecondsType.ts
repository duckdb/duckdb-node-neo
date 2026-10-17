import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { DuckDBTimestampTZNanosecondsValue } from '../values';

export class DuckDBTimestampTZNanosecondsType extends BaseDuckDBType<DuckDBTypeId.TIMESTAMP_TZ_NS> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.TIMESTAMP_TZ_NS, alias);
  }
  public toString(): string {
    return 'TIMESTAMPTZ_NS';
  }
  public static readonly instance = new DuckDBTimestampTZNanosecondsType();
  public static create(alias?: string): DuckDBTimestampTZNanosecondsType {
    return alias ? new DuckDBTimestampTZNanosecondsType(alias) : DuckDBTimestampTZNanosecondsType.instance;
  }
  public get epoch() {
    return DuckDBTimestampTZNanosecondsValue.Epoch;
  }
  public get max() {
    return DuckDBTimestampTZNanosecondsValue.Max;
  }
  public get min() {
    return DuckDBTimestampTZNanosecondsValue.Min;
  }
  public get posInf() {
    return DuckDBTimestampTZNanosecondsValue.PosInf;
  }
  public get negInf() {
    return DuckDBTimestampTZNanosecondsValue.NegInf;
  }
}
export const TIMESTAMPTZ_NS = DuckDBTimestampTZNanosecondsType.instance;
