import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBSQLNullType extends BaseDuckDBType<DuckDBTypeId.SQLNULL> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.SQLNULL, alias);
  }
  public static readonly instance = new DuckDBSQLNullType();
  public static create(alias?: string): DuckDBSQLNullType {
    return alias ? new DuckDBSQLNullType(alias) : DuckDBSQLNullType.instance;
  }
}
export const SQLNULL = DuckDBSQLNullType.instance;
