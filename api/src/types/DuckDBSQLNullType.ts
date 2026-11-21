import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBSQLNullType extends BaseDuckDBType<DuckDBTypeId.SQLNULL> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.SQLNULL, alias);
  }
  /** DuckDB's name for the type, quoted because NULL is a keyword. */
  public toString(): string {
    return '"NULL"';
  }
  public static readonly instance = new DuckDBSQLNullType();
  public static create(alias?: string): DuckDBSQLNullType {
    return alias ? new DuckDBSQLNullType(alias) : DuckDBSQLNullType.instance;
  }
}
export const SQLNULL = DuckDBSQLNullType.instance;
