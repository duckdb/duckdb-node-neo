import { createDuckDBValueConverter } from './createDuckDBValueConverter';
import { DuckDBTypeId } from './DuckDBTypeId';
import { DuckDBValueConverter } from './DuckDBValueConverter';
import {
  arrayFromArrayValue,
  arrayFromListValue,
  booleanFromValue,
  jsonNumberFromValue,
  jsonObjectFromIntervalValue,
  nullConverter,
  numberFromValue,
  objectArrayFromMapValue,
  objectFromStructValue,
  objectFromUnionValue,
  stringFromValue,
  unsupportedConverter,
} from './DuckDBValueConverters';
import { Json } from './Json';

const JsonConvertersByTypeId: Record<
  DuckDBTypeId,
  DuckDBValueConverter<Json>
> = {
  [DuckDBTypeId.INVALID]: unsupportedConverter,
  [DuckDBTypeId.BOOLEAN]: booleanFromValue,
  [DuckDBTypeId.TINYINT]: numberFromValue,
  [DuckDBTypeId.SMALLINT]: numberFromValue,
  [DuckDBTypeId.INTEGER]: numberFromValue,
  [DuckDBTypeId.BIGINT]: numberFromValue,
  [DuckDBTypeId.UTINYINT]: numberFromValue,
  [DuckDBTypeId.USMALLINT]: numberFromValue,
  [DuckDBTypeId.UINTEGER]: numberFromValue,
  [DuckDBTypeId.UBIGINT]: numberFromValue,
  [DuckDBTypeId.FLOAT]: jsonNumberFromValue,
  [DuckDBTypeId.DOUBLE]: jsonNumberFromValue,
  [DuckDBTypeId.TIMESTAMP]: stringFromValue,
  [DuckDBTypeId.DATE]: stringFromValue,
  [DuckDBTypeId.TIME]: stringFromValue,
  [DuckDBTypeId.INTERVAL]: jsonObjectFromIntervalValue,
  [DuckDBTypeId.HUGEINT]: numberFromValue,
  [DuckDBTypeId.UHUGEINT]: numberFromValue,
  [DuckDBTypeId.VARCHAR]: stringFromValue,
  [DuckDBTypeId.BLOB]: stringFromValue,
  [DuckDBTypeId.DECIMAL]: stringFromValue,
  [DuckDBTypeId.TIMESTAMP_S]: stringFromValue,
  [DuckDBTypeId.TIMESTAMP_MS]: stringFromValue,
  [DuckDBTypeId.TIMESTAMP_NS]: stringFromValue,
  [DuckDBTypeId.ENUM]: stringFromValue,
  [DuckDBTypeId.LIST]: arrayFromListValue,
  [DuckDBTypeId.STRUCT]: objectFromStructValue,
  [DuckDBTypeId.MAP]: objectArrayFromMapValue,
  [DuckDBTypeId.ARRAY]: arrayFromArrayValue,
  [DuckDBTypeId.UUID]: stringFromValue,
  [DuckDBTypeId.UNION]: objectFromUnionValue,
  [DuckDBTypeId.BIT]: stringFromValue,
  [DuckDBTypeId.TIME_TZ]: stringFromValue,
  [DuckDBTypeId.TIMESTAMP_TZ]: stringFromValue,
  [DuckDBTypeId.ANY]: unsupportedConverter,
  [DuckDBTypeId.VARINT]: stringFromValue,
  [DuckDBTypeId.SQLNULL]: nullConverter,
};

export const JsonDuckDBValueConverter = createDuckDBValueConverter(
  JsonConvertersByTypeId,
);
