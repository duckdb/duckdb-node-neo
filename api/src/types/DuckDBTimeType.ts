import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { DuckDBTimeValue } from '../values';

export class DuckDBTimeType extends BaseDuckDBType<DuckDBTypeId.TIME> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.TIME, alias);
  }
  public static readonly instance = new DuckDBTimeType();
  public static create(alias?: string): DuckDBTimeType {
    return alias ? new DuckDBTimeType(alias) : DuckDBTimeType.instance;
  }
  public get max() {
    return DuckDBTimeValue.Max;
  }
  public get min() {
    return DuckDBTimeValue.Min;
  }
}
export const TIME = DuckDBTimeType.instance;
