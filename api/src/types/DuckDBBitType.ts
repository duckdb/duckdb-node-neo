import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBBitType extends BaseDuckDBType<DuckDBTypeId.BIT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.BIT, alias);
  }
  public static readonly instance = new DuckDBBitType();
  public static create(alias?: string): DuckDBBitType {
    return alias ? new DuckDBBitType(alias) : DuckDBBitType.instance;
  }
}
export const BIT = DuckDBBitType.instance;
