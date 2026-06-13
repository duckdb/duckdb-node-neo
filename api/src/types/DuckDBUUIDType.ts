import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { DuckDBUUIDValue } from '../values';

export class DuckDBUUIDType extends BaseDuckDBType<DuckDBTypeId.UUID> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.UUID, alias);
  }
  public static readonly instance = new DuckDBUUIDType();
  public static create(alias?: string): DuckDBUUIDType {
    return alias ? new DuckDBUUIDType(alias) : DuckDBUUIDType.instance;
  }
  public get max() {
    return DuckDBUUIDValue.Max;
  }
  public get min() {
    return DuckDBUUIDValue.Min;
  }
}
export const UUID = DuckDBUUIDType.instance;
