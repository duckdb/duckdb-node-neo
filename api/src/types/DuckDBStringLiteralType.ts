import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBStringLiteralType extends BaseDuckDBType<DuckDBTypeId.STRING_LITERAL> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.STRING_LITERAL, alias);
  }
  public static readonly instance = new DuckDBStringLiteralType();
  public static create(alias?: string): DuckDBStringLiteralType {
    return alias ? new DuckDBStringLiteralType(alias) : DuckDBStringLiteralType.instance;
  }
}
export const STRING_LITERAL = DuckDBStringLiteralType.instance;
