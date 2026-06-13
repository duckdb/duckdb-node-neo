import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { DuckDBTimeNSValue } from '../values';

export class DuckDBTimeNSType extends BaseDuckDBType<DuckDBTypeId.TIME_NS> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.TIME_NS, alias);
  }
  public static readonly instance = new DuckDBTimeNSType();
  public static create(alias?: string): DuckDBTimeNSType {
    return alias ? new DuckDBTimeNSType(alias) : DuckDBTimeNSType.instance;
  }
  public get max() {
    return DuckDBTimeNSValue.Max;
  }
  public get min() {
    return DuckDBTimeNSValue.Min;
  }
}
export const TIME_NS = DuckDBTimeNSType.instance;
