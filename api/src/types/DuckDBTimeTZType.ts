import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { DuckDBTimeTZValue } from '../values';

export class DuckDBTimeTZType extends BaseDuckDBType<DuckDBTypeId.TIME_TZ> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.TIME_TZ, alias);
  }
  public toString(): string {
    return 'TIME WITH TIME ZONE';
  }
  public static readonly instance = new DuckDBTimeTZType();
  public static create(alias?: string): DuckDBTimeTZType {
    return alias ? new DuckDBTimeTZType(alias) : DuckDBTimeTZType.instance;
  }
  public get max() {
    return DuckDBTimeTZValue.Max;
  }
  public get min() {
    return DuckDBTimeTZValue.Min;
  }
}
export const TIMETZ = DuckDBTimeTZType.instance;
