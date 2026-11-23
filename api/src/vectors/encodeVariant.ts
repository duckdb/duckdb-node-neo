import { DuckDBType, SQLNULL } from '../DuckDBType';
import { DuckDBTypeId } from '../DuckDBTypeId';
import { typeForValue } from '../typeForValue';
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
  DuckDBTimeNSValue,
  DuckDBTimeTZValue,
  DuckDBTimeValue,
  DuckDBTimestampMillisecondsValue,
  DuckDBTimestampNanosecondsValue,
  DuckDBTimestampSecondsValue,
  DuckDBTimestampTZNanosecondsValue,
  DuckDBTimestampTZValue,
  DuckDBTimestampValue,
  DuckDBTupleValue,
  DuckDBUUIDValue,
  DuckDBUnionValue,
  DuckDBValue,
  DuckDBVariantValue,
} from '../values';
import {
  getBytesFromBigNum,
  littleEndian,
  setInt128,
  setInt16,
  setInt32,
  setInt64,
  setUInt128,
  setUInt16,
  setUInt32,
  setUInt64,
  setUInt8,
} from './dataAccessors';
import { VariantLogicalType } from './VariantLogicalType';

/**
 * Encodes one VARIANT row into VARIANT's physical layout, the reverse of
 * `DuckDBVariantVector.getItem`. Returns the row as a `DuckDBStructValue`
 * shaped like `STRUCT(keys, children, values, data)`, or `null` for a
 * SQL-NULL row.
 *
 * `type` says what `value` is. When it is VARIANT, the value's own type is
 * used: the `type` of a `DuckDBVariantValue` if it has one, otherwise
 * `typeForValue`. The encoding follows DuckDB's cast to VARIANT:
 *
 * - STRUCT becomes an OBJECT; LIST, ARRAY and tuples become an ARRAY.
 * - MAP becomes an ARRAY of `{key, value}` OBJECTs.
 * - UNION becomes its selected member; ENUM becomes VARCHAR.
 * - A NULL nested inside a value becomes VARIANT_NULL. A NULL at the root
 *   (including one inside a VARIANT or UNION wrapper) makes the row NULL.
 *
 * Throws if `value` does not match its type, or if a number is out of
 * range for it.
 */
export function encodeVariant(
  value: DuckDBValue,
  type: DuckDBType
): DuckDBStructValue | null {
  const root = unwrap(value, type);
  if (root.value === null) {
    return null;
  }
  const encoder = new VariantRowEncoder();
  encoder.writeValue(root.value, root.type);
  return encoder.toStructValue();
}

/**
 * Strips VARIANT and UNION wrappers, which have no node of their own, and
 * returns the value and type underneath. Any NULL comes back with type
 * SQLNULL.
 */
function unwrap(
  value: DuckDBValue,
  type: DuckDBType
): { value: DuckDBValue; type: DuckDBType } {
  for (;;) {
    if (value === null || type.typeId === DuckDBTypeId.SQLNULL) {
      return { value: null, type: SQLNULL };
    }
    if (type.typeId === DuckDBTypeId.VARIANT) {
      if (value instanceof DuckDBVariantValue) {
        type = value.type ?? typeForValue(value.value);
        value = value.value;
      } else {
        type = typeForValue(value);
      }
    } else if (type.typeId === DuckDBTypeId.UNION) {
      if (!(value instanceof DuckDBUnionValue)) {
        throw new Error(`input is not a DuckDBUnionValue`);
      }
      const memberType = type.memberTypeForTag(value.tag);
      if (memberType === undefined) {
        throw new Error(`${type} has no member '${value.tag}'`);
      }
      type = memberType;
      value = value.value;
    } else {
      return { value, type };
    }
  }
}

interface VariantChild {
  /** Index into the row's keys; `null` for an ARRAY child. */
  keysIndex: number | null;
  /** Index into the row's values. */
  valuesIndex: number;
}

interface VariantNode {
  typeId: VariantLogicalType;
  /** Where the node's payload starts in the row's data. */
  byteOffset: number;
}

/**
 * Builds one row. Each node appends an entry to `values` and its payload
 * to `data`, so the root is at values index 0. An OBJECT or ARRAY reserves
 * a contiguous run of `children` before writing its children, so a child's
 * own children land after that run.
 */
class VariantRowEncoder {
  private readonly keys: string[] = [];
  private readonly keyIndexes = new Map<string, number>();
  private readonly children: VariantChild[] = [];
  private readonly values: VariantNode[] = [];
  private readonly data = new VariantDataWriter();

  public toStructValue(): DuckDBStructValue {
    return new DuckDBStructValue({
      keys: new DuckDBListValue(this.keys),
      children: new DuckDBListValue(
        this.children.map(
          (child) =>
            new DuckDBStructValue({
              keys_index: child.keysIndex,
              values_index: child.valuesIndex,
            })
        )
      ),
      values: new DuckDBListValue(
        this.values.map(
          (node) =>
            new DuckDBStructValue({
              type_id: node.typeId,
              byte_offset: node.byteOffset,
            })
        )
      ),
      data: new DuckDBBlobValue(this.data.toBytes()),
    });
  }

  /** Writes one node and its descendants; returns the node's values index. */
  public writeValue(value: DuckDBValue, type: DuckDBType): number {
    ({ value, type } = unwrap(value, type));
    if (value === null) {
      return this.addNode(VariantLogicalType.VARIANT_NULL);
    }
    const data = this.data;
    switch (type.typeId) {
      case DuckDBTypeId.BOOLEAN:
        if (typeof value !== 'boolean') {
          throw new Error(`input is not a boolean`);
        }
        return this.addNode(
          value ? VariantLogicalType.BOOL_TRUE : VariantLogicalType.BOOL_FALSE
        );
      case DuckDBTypeId.TINYINT: {
        const index = this.addNode(VariantLogicalType.INT8);
        data.writeInt8(asNumber(value));
        return index;
      }
      case DuckDBTypeId.SMALLINT: {
        const index = this.addNode(VariantLogicalType.INT16);
        data.write(2, setInt16, asNumber(value));
        return index;
      }
      case DuckDBTypeId.INTEGER: {
        const index = this.addNode(VariantLogicalType.INT32);
        data.write(4, setInt32, asNumber(value));
        return index;
      }
      case DuckDBTypeId.BIGINT: {
        const index = this.addNode(VariantLogicalType.INT64);
        data.write(8, setInt64, asBigInt64(value));
        return index;
      }
      case DuckDBTypeId.HUGEINT: {
        const index = this.addNode(VariantLogicalType.INT128);
        data.write(16, setInt128, asBigInt(value));
        return index;
      }
      case DuckDBTypeId.UTINYINT: {
        const index = this.addNode(VariantLogicalType.UINT8);
        data.write(1, setUInt8, asNumber(value));
        return index;
      }
      case DuckDBTypeId.USMALLINT: {
        const index = this.addNode(VariantLogicalType.UINT16);
        data.write(2, setUInt16, asNumber(value));
        return index;
      }
      case DuckDBTypeId.UINTEGER: {
        const index = this.addNode(VariantLogicalType.UINT32);
        data.write(4, setUInt32, asNumber(value));
        return index;
      }
      case DuckDBTypeId.UBIGINT: {
        const index = this.addNode(VariantLogicalType.UINT64);
        data.write(8, setUInt64, asBigInt64(value));
        return index;
      }
      case DuckDBTypeId.UHUGEINT: {
        const index = this.addNode(VariantLogicalType.UINT128);
        data.write(16, setUInt128, asBigInt(value));
        return index;
      }
      case DuckDBTypeId.FLOAT: {
        const index = this.addNode(VariantLogicalType.FLOAT);
        data.write(4, setFloat32, asNumber(value));
        return index;
      }
      case DuckDBTypeId.DOUBLE: {
        const index = this.addNode(VariantLogicalType.DOUBLE);
        data.write(8, setFloat64, asNumber(value));
        return index;
      }
      case DuckDBTypeId.DECIMAL: {
        // The value's own width and scale, not the type's: `value` is scaled
        // by them, and a VARIANT DECIMAL stores them alongside it.
        const decimal = asInstance(value, DuckDBDecimalValue);
        const { width, scale } = decimal;
        const index = this.addNode(VariantLogicalType.DECIMAL);
        data.writeVarint(width);
        data.writeVarint(scale);
        if (width <= 4) {
          data.write(2, setInt16, Number(decimal.value));
        } else if (width <= 9) {
          data.write(4, setInt32, Number(decimal.value));
        } else if (width <= 18) {
          data.write(8, setInt64, decimal.value);
        } else if (width <= 38) {
          data.write(16, setInt128, decimal.value);
        } else {
          throw new Error(`DECIMAL width too large: ${width}`);
        }
        return index;
      }
      case DuckDBTypeId.VARCHAR: {
        if (typeof value !== 'string') {
          throw new Error(`input is not a string`);
        }
        const index = this.addNode(VariantLogicalType.VARCHAR);
        data.writeBytes(textEncoder.encode(value));
        return index;
      }
      case DuckDBTypeId.ENUM: {
        if (typeof value !== 'string') {
          throw new Error(`input is not a string`);
        }
        type.indexForValue(value); // throws if not a member
        const index = this.addNode(VariantLogicalType.VARCHAR);
        data.writeBytes(textEncoder.encode(value));
        return index;
      }
      case DuckDBTypeId.BLOB: {
        const blob = asInstance(value, DuckDBBlobValue);
        const index = this.addNode(VariantLogicalType.BLOB);
        data.writeBytes(blob.bytes);
        return index;
      }
      case DuckDBTypeId.BIT: {
        const bit = asInstance(value, DuckDBBitValue);
        const index = this.addNode(VariantLogicalType.BITSTRING);
        data.writeBytes(bit.data);
        return index;
      }
      case DuckDBTypeId.BIGNUM: {
        const index = this.addNode(VariantLogicalType.BIGNUM);
        data.writeBytes(getBytesFromBigNum(asBigInt(value)));
        return index;
      }
      case DuckDBTypeId.GEOMETRY: {
        const geometry = asInstance(value, DuckDBGeometryValue);
        const index = this.addNode(VariantLogicalType.GEOMETRY);
        data.writeBytes(geometry.bytes);
        return index;
      }
      case DuckDBTypeId.UUID: {
        const uuid = asInstance(value, DuckDBUUIDValue);
        const index = this.addNode(VariantLogicalType.UUID);
        data.write(16, setInt128, uuid.hugeint);
        return index;
      }
      case DuckDBTypeId.DATE: {
        const date = asInstance(value, DuckDBDateValue);
        const index = this.addNode(VariantLogicalType.DATE);
        data.write(4, setInt32, date.days);
        return index;
      }
      case DuckDBTypeId.TIME: {
        const time = asInstance(value, DuckDBTimeValue);
        const index = this.addNode(VariantLogicalType.TIME_MICROS);
        data.write(8, setInt64, time.micros);
        return index;
      }
      case DuckDBTypeId.TIME_NS: {
        const time = asInstance(value, DuckDBTimeNSValue);
        const index = this.addNode(VariantLogicalType.TIME_NANOS);
        data.write(8, setInt64, time.nanos);
        return index;
      }
      case DuckDBTypeId.TIME_TZ: {
        const time = asInstance(value, DuckDBTimeTZValue);
        const index = this.addNode(VariantLogicalType.TIME_MICROS_TZ);
        data.write(8, setUInt64, time.bits);
        return index;
      }
      case DuckDBTypeId.TIMESTAMP_S: {
        const ts = asInstance(value, DuckDBTimestampSecondsValue);
        const index = this.addNode(VariantLogicalType.TIMESTAMP_SEC);
        data.write(8, setInt64, ts.seconds);
        return index;
      }
      case DuckDBTypeId.TIMESTAMP_MS: {
        const ts = asInstance(value, DuckDBTimestampMillisecondsValue);
        const index = this.addNode(VariantLogicalType.TIMESTAMP_MILIS);
        data.write(8, setInt64, ts.millis);
        return index;
      }
      case DuckDBTypeId.TIMESTAMP: {
        const ts = asInstance(value, DuckDBTimestampValue);
        const index = this.addNode(VariantLogicalType.TIMESTAMP_MICROS);
        data.write(8, setInt64, ts.micros);
        return index;
      }
      case DuckDBTypeId.TIMESTAMP_NS: {
        const ts = asInstance(value, DuckDBTimestampNanosecondsValue);
        const index = this.addNode(VariantLogicalType.TIMESTAMP_NANOS);
        data.write(8, setInt64, ts.nanos);
        return index;
      }
      case DuckDBTypeId.TIMESTAMP_TZ: {
        const ts = asInstance(value, DuckDBTimestampTZValue);
        const index = this.addNode(VariantLogicalType.TIMESTAMP_MICROS_TZ);
        data.write(8, setInt64, ts.micros);
        return index;
      }
      case DuckDBTypeId.TIMESTAMP_TZ_NS: {
        const ts = asInstance(value, DuckDBTimestampTZNanosecondsValue);
        const index = this.addNode(VariantLogicalType.TIMESTAMP_NANOS_TZ);
        data.write(8, setInt64, ts.nanos);
        return index;
      }
      case DuckDBTypeId.INTERVAL: {
        const interval = asInstance(value, DuckDBIntervalValue);
        const index = this.addNode(VariantLogicalType.INTERVAL);
        data.write(4, setInt32, interval.months);
        data.write(4, setInt32, interval.days);
        data.write(8, setInt64, interval.micros);
        return index;
      }
      case DuckDBTypeId.LIST: {
        const { items } = asInstance(value, DuckDBListValue);
        const itemType = type.valueType;
        return this.addArray(items.length, (i) =>
          this.writeValue(items[i], itemType)
        );
      }
      case DuckDBTypeId.ARRAY: {
        const { items } = asInstance(value, DuckDBArrayValue);
        if (items.length !== type.length) {
          throw new Error(
            `Expected ${type.length} items for ${type}, got ${items.length}`
          );
        }
        const itemType = type.valueType;
        return this.addArray(items.length, (i) =>
          this.writeValue(items[i], itemType)
        );
      }
      case DuckDBTypeId.MAP: {
        const { entries } = asInstance(value, DuckDBMapValue);
        const { keyType, valueType } = type;
        return this.addArray(entries.length, (i) =>
          this.addObject(MAP_ENTRY_KEYS, (k) =>
            k === 0
              ? this.writeValue(entries[i].key, keyType)
              : this.writeValue(entries[i].value, valueType)
          )
        );
      }
      case DuckDBTypeId.STRUCT: {
        const { entryNames, entryTypes } = type;
        if (type.isTuple) {
          const { items } = asInstance(value, DuckDBTupleValue);
          if (items.length !== entryTypes.length) {
            throw new Error(
              `Expected ${entryTypes.length} items for ${type}, got ${items.length}`
            );
          }
          return this.addArray(items.length, (i) =>
            this.writeValue(items[i], entryTypes[i])
          );
        }
        const { entries } = asInstance(value, DuckDBStructValue);
        for (const name of entryNames) {
          if (!Object.hasOwn(entries, name)) {
            throw new Error(`STRUCT entry '${name}' is missing from input`);
          }
        }
        return this.addObject(entryNames, (k) =>
          this.writeValue(entries[entryNames[k]], entryTypes[k])
        );
      }
      default:
        throw new Error(`Cannot write a value of type ${type} to a VARIANT`);
    }
  }

  private addNode(typeId: VariantLogicalType): number {
    this.values.push({ typeId, byteOffset: this.data.length });
    return this.values.length - 1;
  }

  private addArray(count: number, writeChild: (i: number) => number): number {
    return this.addContainer(VariantLogicalType.ARRAY, count, null, writeChild);
  }

  private addObject(
    keys: readonly string[],
    writeChild: (i: number) => number
  ): number {
    return this.addContainer(
      VariantLogicalType.OBJECT,
      keys.length,
      keys,
      writeChild
    );
  }

  /**
   * Writes an OBJECT or ARRAY node: its payload is `<varint count>`, then
   * `<varint children index>` if `count > 0`.
   */
  private addContainer(
    typeId: VariantLogicalType,
    count: number,
    keys: readonly string[] | null,
    writeChild: (i: number) => number
  ): number {
    const index = this.addNode(typeId);
    this.data.writeVarint(count);
    if (count === 0) {
      return index;
    }
    const childrenIndex = this.children.length;
    this.data.writeVarint(childrenIndex);
    for (let i = 0; i < count; i++) {
      this.children.push({
        keysIndex: keys ? this.keyIndex(keys[i]) : null,
        valuesIndex: 0,
      });
    }
    for (let i = 0; i < count; i++) {
      this.children[childrenIndex + i].valuesIndex = writeChild(i);
    }
    return index;
  }

  private keyIndex(key: string): number {
    let index = this.keyIndexes.get(key);
    if (index === undefined) {
      index = this.keys.length;
      this.keys.push(key);
      this.keyIndexes.set(key, index);
    }
    return index;
  }
}

const MAP_ENTRY_KEYS = ['key', 'value'];

const textEncoder = new TextEncoder();

/** A growable byte buffer for one row's `data` blob. */
class VariantDataWriter {
  private bytes = new Uint8Array(64);
  private view = new DataView(this.bytes.buffer);
  private _length = 0;

  public get length(): number {
    return this._length;
  }

  public toBytes(): Uint8Array {
    return this.bytes.slice(0, this._length);
  }

  /** Writes `value` with a setter from `dataAccessors`, which range-checks it. */
  public write<T>(
    size: number,
    setter: (dataView: DataView, offset: number, value: T) => void,
    value: T
  ) {
    // Reserve first: it may replace `this.view`.
    const offset = this.reserve(size);
    setter(this.view, offset, value);
  }

  public writeInt8(value: number) {
    if (!Number.isInteger(value)) {
      throw new Error(`number is not an integer`);
    }
    if (value < -128 || value > 127) {
      throw new Error(`number out of int8 range`);
    }
    const offset = this.reserve(1);
    this.view.setInt8(offset, value);
  }

  /** Writes an unsigned LEB128 varint, the inverse of `varintDecode`. */
  public writeVarint(value: number) {
    do {
      // Arithmetic, not bitwise, so values up to 2**32-1 stay unsigned.
      let byte = value % 128;
      value = Math.floor(value / 128);
      if (value > 0) {
        byte |= 0x80;
      }
      const offset = this.reserve(1);
      this.bytes[offset] = byte;
    } while (value > 0);
  }

  /** Writes a length-prefixed payload: `<varint length><bytes>`. */
  public writeBytes(bytes: Uint8Array) {
    this.writeVarint(bytes.byteLength);
    const offset = this.reserve(bytes.byteLength);
    this.bytes.set(bytes, offset);
  }

  private reserve(size: number): number {
    const offset = this._length;
    const required = offset + size;
    if (required > this.bytes.byteLength) {
      const grown = new Uint8Array(
        Math.max(required, this.bytes.byteLength * 2)
      );
      grown.set(this.bytes.subarray(0, offset));
      this.bytes = grown;
      this.view = new DataView(grown.buffer);
    }
    this._length = required;
    return offset;
  }
}

function setFloat32(dataView: DataView, offset: number, value: number) {
  dataView.setFloat32(offset, value, littleEndian);
}

function setFloat64(dataView: DataView, offset: number, value: number) {
  dataView.setFloat64(offset, value, littleEndian);
}

function asNumber(value: DuckDBValue): number {
  if (typeof value !== 'number') {
    throw new Error(`input is not a number`);
  }
  return value;
}

function asBigInt(value: DuckDBValue): bigint {
  if (typeof value !== 'bigint') {
    throw new Error(`input is not a bigint`);
  }
  return value;
}

/** BIGINT and UBIGINT also take an integral number, as `createValue` does. */
function asBigInt64(value: DuckDBValue): bigint {
  if (typeof value === 'number') {
    if (!Number.isInteger(value)) {
      throw new Error(`number is not an integer`);
    }
    return BigInt(value);
  }
  return asBigInt(value);
}

/** Takes the class itself, so a private constructor (as UUID has) is fine. */
function asInstance<T>(
  value: DuckDBValue,
  cls: { prototype: T; name: string }
): T {
  if (!(value instanceof (cls as unknown as abstract new () => T))) {
    throw new Error(`input is not a ${cls.name}`);
  }
  return value;
}
