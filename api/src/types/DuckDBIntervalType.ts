import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBIntervalType extends BaseDuckDBType<DuckDBTypeId.INTERVAL> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.INTERVAL, alias);
  }
  public static readonly instance = new DuckDBIntervalType();
  public static create(alias?: string): DuckDBIntervalType {
    return alias ? new DuckDBIntervalType(alias) : DuckDBIntervalType.instance;
  }
}
export const INTERVAL = DuckDBIntervalType.instance;
