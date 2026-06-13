import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBVarCharType extends BaseDuckDBType<DuckDBTypeId.VARCHAR> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.VARCHAR, alias);
  }
  public static readonly instance = new DuckDBVarCharType();
  public static create(alias?: string): DuckDBVarCharType {
    return alias ? new DuckDBVarCharType(alias) : DuckDBVarCharType.instance;
  }
}
export const VARCHAR = DuckDBVarCharType.instance;
