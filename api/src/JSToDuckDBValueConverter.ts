import { DuckDBType } from './DuckDBType';
import { DuckDBTypeId } from './DuckDBTypeId';
import { JS } from './JS';
import {
  JSInputTypeFor,
  JSInputTypeForTypeId,
} from './JSInputTypeForTypeId';
import {
  ToDuckDBValueConverterFor,
  createToDuckDBValueConverter,
} from './ToDuckDBValueConverter';
import {
  arrayValueFromArray,
  bigintFromNumeric,
  bitValueFromBytes,
  blobValueFromBytes,
  dateValueFromDate,
  decimalValueFromNumeric,
  geometryValueFromBytes,
  intervalValueFromObject,
  listValueFromArray,
  mapValueFromEntries,
  nullFromNull,
  structValueFromObject,
  timeNSValueFromNanos,
  timeTZValueFromObject,
  timeValueFromMicros,
  timestampMillisValueFromDate,
  timestampNanosValueFromDate,
  timestampSecondsValueFromDate,
  timestampTZValueFromDate,
  timestampValueFromDate,
  unionValueFromObject,
  unwritableConverter,
  uuidValueFromString,
  valueFromBoolean,
  valueFromNumber,
  valueFromString,
  variantValueFromJS,
} from './ToDuckDBValueConverters';
import { DuckDBValue } from './values';

const JSInputConvertersByTypeId: {
  [Id in DuckDBTypeId]: ToDuckDBValueConverterFor<
    JSInputTypeForTypeId[Id],
    JS
  >;
} = {
  [DuckDBTypeId.INVALID]: unwritableConverter,
  [DuckDBTypeId.BOOLEAN]: valueFromBoolean,
  [DuckDBTypeId.TINYINT]: valueFromNumber,
  [DuckDBTypeId.SMALLINT]: valueFromNumber,
  [DuckDBTypeId.INTEGER]: valueFromNumber,
  [DuckDBTypeId.BIGINT]: bigintFromNumeric,
  [DuckDBTypeId.UTINYINT]: valueFromNumber,
  [DuckDBTypeId.USMALLINT]: valueFromNumber,
  [DuckDBTypeId.UINTEGER]: valueFromNumber,
  [DuckDBTypeId.UBIGINT]: bigintFromNumeric,
  [DuckDBTypeId.FLOAT]: valueFromNumber,
  [DuckDBTypeId.DOUBLE]: valueFromNumber,
  [DuckDBTypeId.TIMESTAMP]: timestampValueFromDate,
  [DuckDBTypeId.DATE]: dateValueFromDate,
  [DuckDBTypeId.TIME]: timeValueFromMicros,
  [DuckDBTypeId.INTERVAL]: intervalValueFromObject,
  [DuckDBTypeId.HUGEINT]: bigintFromNumeric,
  [DuckDBTypeId.UHUGEINT]: bigintFromNumeric,
  [DuckDBTypeId.VARCHAR]: valueFromString,
  [DuckDBTypeId.BLOB]: blobValueFromBytes,
  [DuckDBTypeId.DECIMAL]: decimalValueFromNumeric,
  [DuckDBTypeId.TIMESTAMP_S]: timestampSecondsValueFromDate,
  [DuckDBTypeId.TIMESTAMP_MS]: timestampMillisValueFromDate,
  [DuckDBTypeId.TIMESTAMP_NS]: timestampNanosValueFromDate,
  [DuckDBTypeId.ENUM]: valueFromString,
  [DuckDBTypeId.LIST]: listValueFromArray,
  [DuckDBTypeId.STRUCT]: structValueFromObject,
  [DuckDBTypeId.MAP]: mapValueFromEntries,
  [DuckDBTypeId.ARRAY]: arrayValueFromArray,
  [DuckDBTypeId.UUID]: uuidValueFromString,
  [DuckDBTypeId.UNION]: unionValueFromObject,
  [DuckDBTypeId.BIT]: bitValueFromBytes,
  [DuckDBTypeId.TIME_TZ]: timeTZValueFromObject,
  [DuckDBTypeId.TIMESTAMP_TZ]: timestampTZValueFromDate,
  [DuckDBTypeId.ANY]: unwritableConverter,
  [DuckDBTypeId.BIGNUM]: bigintFromNumeric,
  [DuckDBTypeId.SQLNULL]: nullFromNull,
  [DuckDBTypeId.STRING_LITERAL]: unwritableConverter,
  [DuckDBTypeId.INTEGER_LITERAL]: unwritableConverter,
  [DuckDBTypeId.TIME_NS]: timeNSValueFromNanos,
  [DuckDBTypeId.GEOMETRY]: geometryValueFromBytes,
  [DuckDBTypeId.VARIANT]: variantValueFromJS,
};

/**
 * Converts a JS value into the `DuckDBValue` representation for a DuckDB type:
 * the inverse of `JSDuckDBValueConverter`.
 *
 * Pass this to `DuckDBDataChunk.setRowsConverted` and friends to fill a chunk
 * from JS values. `jsToDuckDBValue` below is the single-value form, and checks
 * its input against a statically known type.
 */
export const JSToDuckDBValueConverter = createToDuckDBValueConverter<
  JS,
  JSInputTypeForTypeId
>(JSInputConvertersByTypeId);

/**
 * Converts a JS value into the `DuckDBValue` representation for `type`.
 *
 * The inverse of `JSDuckDBValueConverter`. Accepts what
 * `JSInputTypeForTypeId` declares for the type's id, plus `null` for any type.
 * Generic over the type, so a statically known one checks its input exactly:
 * `jsToDuckDBValue(new Date(), TIMESTAMP)` is accepted and
 * `jsToDuckDBValue(new Date(), INTEGER)` is not. Passing a `DuckDBType` that
 * is only known as the union widens the input to the union of every entry,
 * which is as precise as the type information allows.
 *
 * Pure TypeScript: it constructs no `duckdb_value` and needs no connection, so
 * it composes with every write entry point — `DuckDBAppender.appendValue`,
 * `DuckDBPreparedStatement.bindValue`, and `DuckDBDataChunk.setRows` all take
 * a `DuckDBValue`.
 */
export function jsToDuckDBValue<T extends DuckDBType>(
  value: JSInputTypeFor<T> | null,
  type: T
): DuckDBValue {
  // The converter chain is typed against JS, which the input table exceeds for
  // VARIANT alone (a DuckDBVariantValue is not a JS value). Narrowed back at
  // the same boundary the factory documents.
  return JSToDuckDBValueConverter(
    value as JS,
    type,
    JSToDuckDBValueConverter
  );
}
