import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBBlobType extends BaseDuckDBType<DuckDBTypeId.BLOB> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.BLOB, alias);
  }
  public static readonly instance = new DuckDBBlobType();
  public static create(alias?: string): DuckDBBlobType {
    return alias ? new DuckDBBlobType(alias) : DuckDBBlobType.instance;
  }
}
export const BLOB = DuckDBBlobType.instance;
