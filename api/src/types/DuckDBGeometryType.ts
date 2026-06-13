import { BaseDuckDBType } from './BaseDuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { Json } from '../Json';
import { quotedString } from '../sql';

export class DuckDBGeometryType extends BaseDuckDBType<DuckDBTypeId.GEOMETRY> {
  /**
   * Coordinate Reference System (e.g. a WKT2 string) attached to the type.
   *
   * Read-only: setting `crs` on a `DuckDBGeometryType` is not propagated by
   * `toLogicalType()`. To produce a GEOMETRY logical type carrying a CRS,
   * construct it via SQL (e.g. `'POINT(1 2)'::GEOMETRY('<crs>')`).
   */
  public readonly crs?: string;
  public constructor(alias?: string, crs?: string) {
    super(DuckDBTypeId.GEOMETRY, alias);
    this.crs = crs;
  }
  public override toString(): string {
    return this.crs ? `GEOMETRY(${quotedString(this.crs)})` : 'GEOMETRY';
  }
  public override toJson(): Json {
    return {
      typeId: this.typeId,
      ...(this.crs ? { crs: this.crs } : {}),
      ...(this.alias ? { alias: this.alias } : {}),
    };
  }
  public static readonly instance = new DuckDBGeometryType();
  public static create(alias?: string, crs?: string): DuckDBGeometryType {
    return alias || crs
      ? new DuckDBGeometryType(alias, crs)
      : DuckDBGeometryType.instance;
  }
}
export const GEOMETRY = DuckDBGeometryType.instance;
