import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBIntegerLiteralType extends BaseDuckDBType<DuckDBTypeId.INTEGER_LITERAL> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.INTEGER_LITERAL, alias);
  }
  public static readonly instance = new DuckDBIntegerLiteralType();
  public static create(alias?: string): DuckDBIntegerLiteralType {
    return alias ? new DuckDBIntegerLiteralType(alias) : DuckDBIntegerLiteralType.instance;
  }
}
export const INTEGER_LITERAL = DuckDBIntegerLiteralType.instance;
