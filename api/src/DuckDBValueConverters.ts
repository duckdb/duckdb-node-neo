import {
  DuckDBArrayType,
  DuckDBListType,
  DuckDBMapType,
  DuckDBStructType,
  DuckDBType,
  DuckDBUnionType,
} from './DuckDBType';
import { DuckDBTypeId } from './DuckDBTypeId';
import { DuckDBValueConverter } from './DuckDBValueConverter';
import { typeForValue } from './typeForValue';
import {
  DuckDBArrayValue,
  DuckDBBitValue,
  DuckDBBlobValue,
  DuckDBDateValue,
  DuckDBDecimalValue,
  DuckDBGeometryValue,
  DuckDBIntervalValue,
  DuckDBListValue,
  DuckDBMapValue,
  DuckDBStructValue,
  DuckDBTimeTZValue,
  DuckDBTimeValue,
  DuckDBTimestampMillisecondsValue,
  DuckDBTimestampNanosecondsValue,
  DuckDBTimestampSecondsValue,
  DuckDBTimestampTZValue,
  DuckDBTimestampValue,
  DuckDBUnionValue,
  DuckDBValue,
  DuckDBVariantValue,
} from './values';

const MIN_DATE_DAYS = -100000000;
const MAX_DATE_DAYS = 100000000;
//                      -8640000000000
const MIN_DATE_MILLIS = -8640000000000000;
const MAX_DATE_MILLIS = 8640000000000000;
const MILLS_PER_DAY = 24 * 60 * 60 * 1000;

export function unsupportedConverter(_: DuckDBValue, type: DuckDBType): null {
  throw new Error(`Unsupported type: ${type}`);
}

export function nullConverter(_: DuckDBValue): null {
  return null;
}

export function booleanFromValue(value: DuckDBValue): boolean {
  return Boolean(value);
}

export function numberFromValue(value: DuckDBValue): number {
  return Number(value);
}

export function jsonNumberFromValue(value: DuckDBValue): number | string {
  if (Number.isFinite(value)) {
    return Number(value);
  }
  return String(value);
}

export function bigintFromBigIntValue(
  value: DuckDBValue,
  type: DuckDBType
): bigint {
  if (typeof value === 'bigint') {
    return value;
  }
  throw new Error(`Expected bigint value for type ${type}`);
}

export function stringFromValue(value: DuckDBValue): string {
  return String(value);
}

export function bytesFromBlobValue(value: DuckDBValue): Uint8Array {
  if (value instanceof DuckDBBlobValue) {
    return value.bytes;
  }
  throw new Error(`Expected DuckDBBlobValue`);
}

export function bytesFromBitValue(value: DuckDBValue): Uint8Array {
  if (value instanceof DuckDBBitValue) {
    return value.data;
  }
  throw new Error(`Expected DuckDBBitValue`);
}

export function bytesFromGeometryValue(value: DuckDBValue): Uint8Array {
  if (value instanceof DuckDBGeometryValue) {
    return value.bytes;
  }
  throw new Error(`Expected DuckDBGeometryValue`);
}

export function dateFromDateValue(value: DuckDBValue): Date {
  if (value instanceof DuckDBDateValue) {
    if (MIN_DATE_DAYS <= value.days && value.days <= MAX_DATE_DAYS) {
      return new Date(value.days * MILLS_PER_DAY);
    }
    throw new Error(`DATE value out of range for JS Date: ${value.days} days`);
  }
  throw new Error(`Expected DuckDBDateValue`);
}

export function bigintFromTimeValue(value: DuckDBValue): bigint {
  if (value instanceof DuckDBTimeValue) {
    return value.micros;
  }
  throw new Error(`Expected DuckDBTimeValue`);
}

export function dateFromTimestampValue(value: DuckDBValue): Date {
  if (value instanceof DuckDBTimestampValue) {
    const millis = value.micros / 1000n;
    if (MIN_DATE_MILLIS <= millis && millis <= MAX_DATE_MILLIS) {
      return new Date(Number(millis));
    }
    throw new Error(
      `TIMESTAMP value out of range for JS Date: ${value.micros} micros`
    );
  }
  throw new Error(`Expected DuckDBTimestampValue`);
}

export function dateFromTimestampSecondsValue(value: DuckDBValue): Date {
  if (value instanceof DuckDBTimestampSecondsValue) {
    const millis = value.seconds * 1000n;
    if (MIN_DATE_MILLIS <= millis && millis <= MAX_DATE_MILLIS) {
      return new Date(Number(millis));
    }
    throw new Error(
      `TIMESTAMP_S value out of range for JS Date: ${value.seconds} seconds`
    );
  }
  throw new Error(`Expected DuckDBTimestampSecondsValue`);
}

export function dateFromTimestampMillisecondsValue(value: DuckDBValue): Date {
  if (value instanceof DuckDBTimestampMillisecondsValue) {
    const millis = value.millis;
    if (MIN_DATE_MILLIS <= millis && millis <= MAX_DATE_MILLIS) {
      return new Date(Number(millis));
    }
    throw new Error(
      `TIMESTAMP_MS value out of range for JS Date: ${value.millis} millis`
    );
  }
  throw new Error(`Expected DuckDBTimestampMillisecondsValue`);
}

export function dateFromTimestampNanosecondsValue(value: DuckDBValue): Date {
  if (value instanceof DuckDBTimestampNanosecondsValue) {
    const millis = value.nanos / 1000000n;
    if (MIN_DATE_MILLIS <= millis && millis <= MAX_DATE_MILLIS) {
      return new Date(Number(millis));
    }
    throw new Error(
      `TIMESTAMP_NS value out of range for JS Date: ${value.nanos} nanos`
    );
  }
  throw new Error(`Expected DuckDBTimestampNanosecondsValue`);
}

export function objectFromTimeTZValue(value: DuckDBValue): {
  micros: bigint;
  offset: number;
} {
  if (value instanceof DuckDBTimeTZValue) {
    return {
      micros: value.micros,
      offset: value.offset,
    };
  }
  throw new Error(`Expected DuckDBTimeTZValue`);
}

export function dateFromTimestampTZValue(value: DuckDBValue): Date {
  if (value instanceof DuckDBTimestampTZValue) {
    const millis = value.micros / 1000n;
    if (MIN_DATE_MILLIS <= millis && millis <= MAX_DATE_MILLIS) {
      return new Date(Number(millis));
    }
    throw new Error(
      `TIMESTAMPTZ value out of range for JS Date: ${value.micros} micros`
    );
  }
  throw new Error(`Expected DuckDBTimestampTZValue`);
}

export function objectFromIntervalValue(value: DuckDBValue): {
  months: number;
  days: number;
  micros: bigint;
} {
  if (value instanceof DuckDBIntervalValue) {
    return {
      months: value.months,
      days: value.days,
      micros: value.micros,
    };
  }
  throw new Error(`Expected DuckDBIntervalValue`);
}

export function jsonObjectFromIntervalValue(value: DuckDBValue): {
  months: number;
  days: number;
  micros: string;
} {
  if (value instanceof DuckDBIntervalValue) {
    return {
      months: value.months,
      days: value.days,
      micros: String(value.micros),
    };
  }
  throw new Error(`Expected DuckDBIntervalValue`);
}

export function doubleFromDecimalValue(value: DuckDBValue): number {
  if (value instanceof DuckDBDecimalValue) {
    return value.toDouble();
  }
  throw new Error(`Expected DuckDBDecimalValue`);
}

export function arrayFromListValue<T>(
  value: DuckDBValue,
  type: DuckDBType,
  converter: DuckDBValueConverter<T>
): (T | null)[] {
  if (value instanceof DuckDBListValue && type instanceof DuckDBListType) {
    return value.items.map((v) => converter(v, type.valueType, converter));
  }
  throw new Error(`Expected DuckDBListValue and DuckDBListType`);
}

export function objectFromStructValue<T>(
  value: DuckDBValue,
  type: DuckDBType,
  converter: DuckDBValueConverter<T>
): { [key: string]: T | null } {
  if (value instanceof DuckDBStructValue && type instanceof DuckDBStructType) {
    const result: { [key: string]: T | null } = {};
    for (const key in value.entries) {
      result[key] = converter(
        value.entries[key],
        type.typeForEntry(key),
        converter
      );
    }
    return result;
  }
  throw new Error(`Expected DuckDBStructValue and DuckDBStructType`);
}

export function objectArrayFromMapValue<T>(
  value: DuckDBValue,
  type: DuckDBType,
  converter: DuckDBValueConverter<T>
): { key: T | null; value: T | null }[] {
  if (value instanceof DuckDBMapValue && type instanceof DuckDBMapType) {
    return value.entries.map((entry) => ({
      key: converter(entry.key, type.keyType, converter),
      value: converter(entry.value, type.valueType, converter),
    }));
  }
  throw new Error(`Expected DuckDBMapValue and DuckDBMapType`);
}

export function arrayFromArrayValue<T>(
  value: DuckDBValue,
  type: DuckDBType,
  converter: DuckDBValueConverter<T>
): (T | null)[] {
  if (value instanceof DuckDBArrayValue && type instanceof DuckDBArrayType) {
    return value.items.map((v) => converter(v, type.valueType, converter));
  }
  throw new Error(`Expected DuckDBArrayValue and DuckDBArrayType`);
}

export function objectFromUnionValue<T>(
  value: DuckDBValue,
  type: DuckDBType,
  converter: DuckDBValueConverter<T>
): { tag: string; value: T | null } {
  if (value instanceof DuckDBUnionValue && type instanceof DuckDBUnionType) {
    return {
      tag: value.tag,
      value: converter(
        value.value,
        type.memberTypeForTag(value.tag),
        converter
      ),
    };
  }
  throw new Error(`Expected DuckDBUnionValue and DuckDBUnionType`);
}

/**
 * Converter for VARIANT values: unwraps the `DuckDBVariantValue` wrapper
 * and recursively walks the inner value, re-dispatching each scalar leaf
 * through the supplied converter with a freshly-derived `DuckDBType`.
 *
 * A walker is needed (rather than just dispatching through the converter)
 * because VARIANT containers are *heterogeneous* — items in a list or
 * fields in a struct can have different types — while
 * `arrayFromListValue` / `objectFromStructValue` / `objectArrayFromMapValue`
 * assume a homogeneous element type pulled from the enclosing
 * `DuckDBListType` / `DuckDBStructType` / `DuckDBMapType`. The walker
 * emits the same output shape those converters do, but recovers each
 * child's type via `typeForValue`.
 */
export function fromVariantValue<T>(
  value: DuckDBValue,
  _type: DuckDBType,
  converter: DuckDBValueConverter<T>
): T | null {
  if (!(value instanceof DuckDBVariantValue)) {
    throw new Error(`Expected DuckDBVariantValue`);
  }
  return convertVariantInner(value.value, converter);
}

function convertVariantInner<T>(
  value: DuckDBValue,
  converter: DuckDBValueConverter<T>
): T | null {
  if (value === null) {
    return null;
  }
  if (value instanceof DuckDBListValue || value instanceof DuckDBArrayValue) {
    return value.items.map((item) =>
      convertVariantInner(item, converter)
    ) as unknown as T;
  }
  if (value instanceof DuckDBStructValue) {
    const result: { [key: string]: T | null } = {};
    for (const key in value.entries) {
      result[key] = convertVariantInner(value.entries[key], converter);
    }
    return result as unknown as T;
  }
  if (value instanceof DuckDBMapValue) {
    return value.entries.map((entry) => ({
      key: convertVariantInner(entry.key, converter),
      value: convertVariantInner(entry.value, converter),
    })) as unknown as T;
  }
  if (value instanceof DuckDBUnionValue) {
    return {
      tag: value.tag,
      value: convertVariantInner(value.value, converter),
    } as unknown as T;
  }
  if (value instanceof DuckDBVariantValue) {
    // Variant-of-variant: unwrap and continue.
    return convertVariantInner(value.value, converter);
  }
  // Scalar (or non-recursable rich value like DECIMAL, BLOB, DATE, etc.):
  // dispatch through the regular converter with its own recovered type.
  const inferredType = typeForValue(value);
  // `typeForValue` infers BIGINT for any JS integer number outside INT32
  // range (e.g. a UINT32 VARIANT payload of 4_294_967_295). The BIGINT
  // converters expect a `bigint`, not a `number`, and would otherwise
  // throw. Coerce to bigint so the dispatch lands on a compatible reader.
  if (
    typeof value === 'number' &&
    inferredType.typeId === DuckDBTypeId.BIGINT
  ) {
    return converter(BigInt(value), inferredType, converter);
  }
  return converter(value, inferredType, converter);
}
