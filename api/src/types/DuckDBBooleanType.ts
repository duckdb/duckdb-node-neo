import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBBooleanType extends BaseDuckDBType<DuckDBTypeId.BOOLEAN> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.BOOLEAN, alias);
  }
  public static readonly instance = new DuckDBBooleanType();
  public static create(alias?: string): DuckDBBooleanType {
    return alias ? new DuckDBBooleanType(alias) : DuckDBBooleanType.instance;
  }
}
export const BOOLEAN = DuckDBBooleanType.instance;
