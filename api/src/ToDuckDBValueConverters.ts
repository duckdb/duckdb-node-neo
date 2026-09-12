import {
  DuckDBArrayType,
  DuckDBDecimalType,
  DuckDBListType,
  DuckDBMapType,
  DuckDBStructType,
  DuckDBType,
  DuckDBUnionType,
} from './DuckDBType';
import { ToDuckDBValueConverter } from './ToDuckDBValueConverter';
import {
  DuckDBBitValue,
  DuckDBBlobValue,
  DuckDBDateValue,
  DuckDBDecimalValue,
  DuckDBGeometryValue,
  DuckDBIntervalValue,
  DuckDBTimeNSValue,
  DuckDBTimeTZValue,
  DuckDBTimeValue,
  DuckDBTimestampMillisecondsValue,
  DuckDBTimestampNanosecondsValue,
  DuckDBTimestampSecondsValue,
  DuckDBTimestampTZValue,
  DuckDBTimestampValue,
  DuckDBUUIDValue,
  DuckDBValue,
  DuckDBVariantValue,
  arrayValue,
  listValue,
  mapValue,
  structValue,
  unionValue,
  variantValue,
} from './values';

const MILLIS_PER_DAY = 24 * 60 * 60 * 1000;

/**
 * The epoch milliseconds of `value`, refusing an Invalid Date.
 *
 * Without this, DATE turns NaN into a `DuckDBDateValue(NaN)` and hands it back,
 * while the timestamps reach `BigInt(NaN)` and throw "The number NaN cannot be
 * converted to a BigInt" — neither of which names the real problem.
 */
function millisFromDate(value: Date): number {
  const millis = value.getTime();
  if (Number.isNaN(millis)) {
    throw new Error(`Cannot convert an Invalid Date`);
  }
  return millis;
}

export function unwritableConverter(_: unknown, type: DuckDBType): never {
  throw new Error(`Cannot write values of type: ${type}`);
}

export function nullFromNull(): null {
  return null;
}

export function valueFromBoolean(value: boolean): boolean {
  return Boolean(value);
}

export function valueFromNumber(value: number): number {
  return value;
}

export function bigintFromNumeric(value: bigint | number): bigint {
  return typeof value === 'bigint' ? value : BigInt(value);
}

export function valueFromString(value: string): string {
  return value;
}

export function blobValueFromBytes(value: Uint8Array): DuckDBBlobValue {
  return new DuckDBBlobValue(value);
}

export function bitValueFromBytes(value: Uint8Array): DuckDBBitValue {
  return new DuckDBBitValue(value);
}

export function geometryValueFromBytes(value: Uint8Array): DuckDBGeometryValue {
  return new DuckDBGeometryValue(value);
}

export function dateValueFromDate(value: Date): DuckDBDateValue {
  // A DATE has no time component; the reader produces midnight UTC, so this is
  // exact for a round trip and truncates toward the earlier day otherwise.
  return new DuckDBDateValue(Math.floor(millisFromDate(value) / MILLIS_PER_DAY));
}

export function timeValueFromMicros(value: bigint): DuckDBTimeValue {
  return new DuckDBTimeValue(value);
}

export function timeNSValueFromNanos(value: bigint): DuckDBTimeNSValue {
  return new DuckDBTimeNSValue(value);
}

export function timeTZValueFromObject(value: {
  micros: bigint;
  offset: number;
}): DuckDBTimeTZValue {
  return DuckDBTimeTZValue.fromMicrosAndOffset(value.micros, value.offset);
}

export function timestampValueFromDate(value: Date): DuckDBTimestampValue {
  return new DuckDBTimestampValue(BigInt(millisFromDate(value)) * 1000n);
}

export function timestampTZValueFromDate(value: Date): DuckDBTimestampTZValue {
  return new DuckDBTimestampTZValue(BigInt(millisFromDate(value)) * 1000n);
}

export function timestampSecondsValueFromDate(
  value: Date
): DuckDBTimestampSecondsValue {
  return new DuckDBTimestampSecondsValue(
    BigInt(Math.floor(millisFromDate(value) / 1000))
  );
}

export function timestampMillisValueFromDate(
  value: Date
): DuckDBTimestampMillisecondsValue {
  return new DuckDBTimestampMillisecondsValue(BigInt(millisFromDate(value)));
}

export function timestampNanosValueFromDate(
  value: Date
): DuckDBTimestampNanosecondsValue {
  return new DuckDBTimestampNanosecondsValue(
    BigInt(millisFromDate(value)) * 1000000n
  );
}

export function intervalValueFromObject(value: {
  months: number;
  days: number;
  micros: bigint;
}): DuckDBIntervalValue {
  return new DuckDBIntervalValue(value.months, value.days, value.micros);
}

export function decimalValueFromNumeric(
  value: number | bigint,
  type: DuckDBType
): DuckDBDecimalValue {
  if (!(type instanceof DuckDBDecimalType)) {
    throw new Error(`Expected DuckDBDecimalType`);
  }
  return typeof value === 'bigint'
    ? new DuckDBDecimalValue(value, type.width, type.scale)
    : DuckDBDecimalValue.fromDouble(value, type.width, type.scale);
}

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function uuidValueFromString(value: string): DuckDBUUIDValue {
  if (!UUID_PATTERN.test(value)) {
    throw new Error(`Not a UUID string: ${value}`);
  }
  return DuckDBUUIDValue.fromUint128(BigInt(`0x${value.replace(/-/g, '')}`));
}

export function listValueFromArray<T>(
  value: readonly T[],
  type: DuckDBType,
  converter: ToDuckDBValueConverter<T>
): DuckDBValue {
  if (!(type instanceof DuckDBListType)) {
    throw new Error(`Expected DuckDBListType`);
  }
  return listValue(
    value.map((item) => converter(item, type.valueType, converter))
  );
}

export function arrayValueFromArray<T>(
  value: readonly T[],
  type: DuckDBType,
  converter: ToDuckDBValueConverter<T>
): DuckDBValue {
  if (!(type instanceof DuckDBArrayType)) {
    throw new Error(`Expected DuckDBArrayType`);
  }
  if (value.length !== type.length) {
    throw new Error(
      `Expected ${type.length} items for ${type}, got ${value.length}`
    );
  }
  return arrayValue(
    value.map((item) => converter(item, type.valueType, converter))
  );
}

export function structValueFromObject<T>(
  value: Readonly<Record<string, T>>,
  type: DuckDBType,
  converter: ToDuckDBValueConverter<T>
): DuckDBValue {
  if (!(type instanceof DuckDBStructType)) {
    throw new Error(`Expected DuckDBStructType`);
  }
  const entries: Record<string, DuckDBValue> = {};
  for (const key of type.entryNames) {
    if (!(key in value)) {
      throw new Error(`Missing entry for struct key: ${key}`);
    }
    entries[key] = converter(value[key], type.typeForEntry(key), converter);
  }
  return structValue(entries);
}

export function mapValueFromEntries<T>(
  value: readonly { key: T; value: T }[],
  type: DuckDBType,
  converter: ToDuckDBValueConverter<T>
): DuckDBValue {
  if (!(type instanceof DuckDBMapType)) {
    throw new Error(`Expected DuckDBMapType`);
  }
  return mapValue(
    value.map((entry) => ({
      key: converter(entry.key, type.keyType, converter),
      value: converter(entry.value, type.valueType, converter),
    }))
  );
}

export function unionValueFromObject<T>(
  value: { tag: string; value: T },
  type: DuckDBType,
  converter: ToDuckDBValueConverter<T>
): DuckDBValue {
  if (!(type instanceof DuckDBUnionType)) {
    throw new Error(`Expected DuckDBUnionType`);
  }
  // memberTypeForTag indexes by tag and returns undefined for one it does not
  // have, which would reach the dispatch as an undefined type. Unlike the read
  // path, where the tag comes from a decoded value, here it is caller input.
  if (!(value.tag in type.tagMemberIndexes)) {
    throw new Error(
      `Tag ${JSON.stringify(value.tag)} is not a member of ${type}`
    );
  }
  return unionValue(
    value.tag,
    converter(value.value, type.memberTypeForTag(value.tag), converter)
  );
}

/**
 * A VARIANT carries the type of the value inside it, so writing one means
 * saying what that type is. A `DuckDBVariantValue` already does — pass it
 * through. A bare scalar does not, but is unambiguous enough for
 * `typeForValue` to infer downstream, so wrap it.
 */
export function variantValueFromJS(
  value: DuckDBVariantValue | boolean | number | bigint | string
): DuckDBVariantValue {
  return value instanceof DuckDBVariantValue ? value : variantValue(value);
}
