import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBAnyType extends BaseDuckDBType<DuckDBTypeId.ANY> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.ANY, alias);
  }
  public static readonly instance = new DuckDBAnyType();
  public static create(alias?: string): DuckDBAnyType {
    return alias ? new DuckDBAnyType(alias) : DuckDBAnyType.instance;
  }
}
export const ANY = DuckDBAnyType.instance;
