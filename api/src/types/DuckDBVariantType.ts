import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';

export class DuckDBVariantType extends BaseDuckDBType<DuckDBTypeId.VARIANT> {
  public constructor(alias?: string) {
    super(DuckDBTypeId.VARIANT, alias);
  }
  public static readonly instance = new DuckDBVariantType();
  public static create(alias?: string): DuckDBVariantType {
    return alias ? new DuckDBVariantType(alias) : DuckDBVariantType.instance;
  }
}
export const VARIANT = DuckDBVariantType.instance;
