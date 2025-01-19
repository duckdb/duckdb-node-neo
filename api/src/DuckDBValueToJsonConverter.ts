import { DuckDBType } from './DuckDBType';
import { DuckDBTypeId } from './DuckDBTypeId';
import { DuckDBValueConverter } from './DuckDBValueConverter';
import {
  DuckDBArrayValue,
  DuckDBIntervalValue,
  DuckDBListValue,
  DuckDBMapValue,
  DuckDBStructValue,
  DuckDBUnionValue,
  DuckDBValue,
} from './values';

export type Json =
  | null
  | boolean
  | number
  | string
  | Json[]
  | { [key: string]: Json };

export class DuckDBValueToJsonConverter implements DuckDBValueConverter<Json> {
  public static readonly default = new DuckDBValueToJsonConverter();

  public convertValue(value: DuckDBValue, type: DuckDBType): Json {
    if (value == null) {
      return null;
    }
    switch (type.typeId) {
      case DuckDBTypeId.BOOLEAN:
        return Boolean(value);
      case DuckDBTypeId.TINYINT:
      case DuckDBTypeId.SMALLINT:
      case DuckDBTypeId.INTEGER:
      case DuckDBTypeId.UTINYINT:
      case DuckDBTypeId.USMALLINT:
      case DuckDBTypeId.UINTEGER:
        return Number(value);
      case DuckDBTypeId.FLOAT:
      case DuckDBTypeId.DOUBLE:
        if (Number.isFinite(value)) {
          return Number(value);
        }
        return String(value);
      case DuckDBTypeId.BIGINT:
      case DuckDBTypeId.UBIGINT:
      case DuckDBTypeId.HUGEINT:
      case DuckDBTypeId.UHUGEINT:
        return String(value);
      case DuckDBTypeId.DATE:
      case DuckDBTypeId.TIME:
      case DuckDBTypeId.TIMESTAMP:
      case DuckDBTypeId.TIMESTAMP_S:
      case DuckDBTypeId.TIMESTAMP_MS:
      case DuckDBTypeId.TIMESTAMP_NS:
      case DuckDBTypeId.TIME_TZ:
      case DuckDBTypeId.TIMESTAMP_TZ:
        return String(value);
      case DuckDBTypeId.INTERVAL:
        if (value instanceof DuckDBIntervalValue) {
          return {
            months: value.months,
            days: value.days,
            micros: String(value.micros),
          };
        }
        return null;
      case DuckDBTypeId.VARCHAR:
      case DuckDBTypeId.BLOB:
      case DuckDBTypeId.BIT:
        return String(value);
      case DuckDBTypeId.DECIMAL:
      case DuckDBTypeId.VARINT:
        return String(value);
      case DuckDBTypeId.ENUM:
        return String(value);
      case DuckDBTypeId.LIST:
        if (value instanceof DuckDBListValue) {
          return value.items.map((v) => this.convertValue(v, type.valueType));
        }
        return null;
      case DuckDBTypeId.STRUCT:
        if (value instanceof DuckDBStructValue) {
          const result: { [key: string]: Json } = {};
          for (const key in value.entries) {
            result[key] = this.convertValue(
              value.entries[key],
              type.typeForEntry(key)
            );
          }
          return result;
        }
        return null;
      case DuckDBTypeId.MAP:
        if (value instanceof DuckDBMapValue) {
          return value.entries.map((entry) => ({
            key: this.convertValue(entry.key, type.keyType),
            value: this.convertValue(entry.value, type.valueType),
          }));
        }
        return null;
      case DuckDBTypeId.ARRAY:
        if (value instanceof DuckDBArrayValue) {
          return value.items.map((v) => this.convertValue(v, type.valueType));
        }
        return null;
      case DuckDBTypeId.UNION:
        if (value instanceof DuckDBUnionValue) {
          return {
            tag: value.tag,
            value: this.convertValue(
              value.value,
              type.memberTypeForTag(value.tag)
            ),
          };
        }
        return null;
      case DuckDBTypeId.UUID:
        return String(value);
    }
    return null;
  }
}
