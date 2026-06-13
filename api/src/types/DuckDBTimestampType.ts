import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { DuckDBTimestampValue } from '../values';

export class DuckDBTimestampType extends BaseDuckDBType<DuckDBTypeId.TIMESTAMP> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.TIMESTAMP, alias);
  }
  public static readonly instance = new DuckDBTimestampType();
  public static create(alias?: string): DuckDBTimestampType {
    return alias ? new DuckDBTimestampType(alias) : DuckDBTimestampType.instance;
  }
  public get epoch() {
    return DuckDBTimestampValue.Epoch;
  }
  public get max() {
    return DuckDBTimestampValue.Max;
  }
  public get min() {
    return DuckDBTimestampValue.Min;
  }
  public get posInf() {
    return DuckDBTimestampValue.PosInf;
  }
  public get negInf() {
    return DuckDBTimestampValue.NegInf;
  }
}
export const TIMESTAMP = DuckDBTimestampType.instance;

export type DuckDBTimestampMicrosecondsType = DuckDBTimestampType;
export const DuckDBTimestampMicrosecondsType = DuckDBTimestampType;
