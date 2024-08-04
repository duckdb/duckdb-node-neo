import os from 'os';
import duckdb from '@duckdb/node-bindings';
import { DuckDBLogicalType } from './DuckDBLogicalType';
import {
  DuckDBArrayType,
  DuckDBBigIntType,
  DuckDBBitType,
  DuckDBBlobType,
  DuckDBBooleanType,
  DuckDBDateType,
  DuckDBDecimalType,
  DuckDBDoubleType,
  DuckDBEnumType,
  DuckDBFloatType,
  DuckDBHugeIntType,
  DuckDBIntegerType,
  DuckDBIntervalType,
  DuckDBListType,
  DuckDBMapType,
  DuckDBSmallIntType,
  DuckDBStructEntryType,
  DuckDBStructType,
  DuckDBTimeTZType,
  DuckDBTimeType,
  DuckDBTimestampMillisecondsType,
  DuckDBTimestampNanosecondsType,
  DuckDBTimestampSecondsType,
  DuckDBTimestampType,
  DuckDBTinyIntType,
  DuckDBType,
  DuckDBUBigIntType,
  DuckDBUHugeIntType,
  DuckDBUIntegerType,
  DuckDBUSmallIntType,
  DuckDBUTinyIntType,
  DuckDBUUIDType,
  DuckDBUnionType,
  DuckDBVarCharType,
} from './DuckDBType';
import { DuckDBTypeId } from './DuckDBTypeId';

const littleEndian = os.endianness() === 'LE';

// function getInt8(dataView: DataView, offset: number): number {
//   return dataView.getInt8(offset);
// }

function getUInt8(dataView: DataView, offset: number): number {
  return dataView.getUint8(offset);
}

function getInt16(dataView: DataView, offset: number): number {
  return dataView.getInt16(offset, littleEndian);
}

function getUInt16(dataView: DataView, offset: number): number {
  return dataView.getUint16(offset, littleEndian);
}

function getInt32(dataView: DataView, offset: number): number {
  return dataView.getInt32(offset, littleEndian);
}

function getUInt32(dataView: DataView, offset: number): number {
  return dataView.getUint32(offset, littleEndian);
}

function getInt64(dataView: DataView, offset: number): bigint {
  return dataView.getBigInt64(offset, littleEndian);
}

function getUInt64(dataView: DataView, offset: number): bigint {
  return dataView.getBigUint64(offset, littleEndian);
}

// function getFloat32(dataView: DataView, offset: number): number {
//   return dataView.getFloat32(offset, littleEndian);
// }

// function getFloat64(dataView: DataView, offset: number): number {
//   return dataView.getFloat64(offset, littleEndian);
// }

function getInt128(dataView: DataView, offset: number): bigint {
  const lower = getUInt64(dataView, offset);
  const upper = getInt64(dataView, offset + 8);
  return (upper << BigInt(64)) + lower;
}

function getUInt128(dataView: DataView, offset: number): bigint {
  const lower = getUInt64(dataView, offset);
  const upper = getUInt64(dataView, offset + 8);
  return BigInt.asUintN(64, upper) << BigInt(64) | BigInt.asUintN(64, lower);
}

function getStringBytes(dataView: DataView, offset: number): Uint8Array {
  const lengthInBytes = dataView.getUint32(offset, true);
  if (lengthInBytes <= 12) {
    return new Uint8Array(dataView.buffer, dataView.byteOffset + offset + 4, lengthInBytes);
  } else {
    return duckdb.get_data_from_pointer(dataView.buffer, dataView.byteOffset + offset + 8, lengthInBytes);
  }
}

const textDecoder = new TextDecoder();

function getString(dataView: DataView, offset: number): string | null {
  const stringBytes = getStringBytes(dataView, offset);
  if (!stringBytes) {
    // should we throw here instead?
    return null;
  }
  return textDecoder.decode(stringBytes);
}

function getBuffer(dataView: DataView, offset: number): Buffer | null {
  const stringBytes = getStringBytes(dataView, offset);
  if (!stringBytes) {
    // should we throw here instead?
    return null;
  }
  return Buffer.from(stringBytes);
}

function getBoolean1(dataView: DataView, offset: number): boolean {
  return getUInt8(dataView, offset) !== 0
}

function getBoolean2(dataView: DataView, offset: number): boolean {
  return getUInt16(dataView, offset) !== 0
}

function getBoolean4(dataView: DataView, offset: number): boolean {
  return getUInt32(dataView, offset) !== 0
}

function getBoolean8(dataView: DataView, offset: number): boolean {
  return getUInt64(dataView, offset) !== BigInt(0)
}

function makeGetBoolean(): (dataView: DataView, offset: number) => boolean {
  switch (duckdb.sizeof_bool) {
    case 1:
      return getBoolean1;
    case 2:
      return getBoolean2;
    case 4:
      return getBoolean4;
    case 8:
      return getBoolean8;
    default:
      throw new Error(`Unsupported boolean size: ${duckdb.sizeof_bool}`);
  }
}

const getBoolean = makeGetBoolean();

export class DuckDBSmallDecimal {
  public readonly scaledValue: number;
  public readonly type: DuckDBDecimalType;

  public constructor(scaledValue: number, type: DuckDBDecimalType) {
    this.scaledValue = scaledValue;
    this.type = type;
  }
}

export class DuckDBLargeDecimal {
  public readonly scaledValue: bigint;
  public readonly type: DuckDBDecimalType;

  public constructor(scaledValue: bigint, type: DuckDBDecimalType) {
    this.scaledValue = scaledValue;
    this.type = type;
  }
}

function getDecimal2(dataView: DataView, offset: number, type: DuckDBDecimalType): DuckDBSmallDecimal {
  const scaledValue = getInt16(dataView, offset);
  return new DuckDBSmallDecimal(scaledValue, type);
}

function getDecimal4(dataView: DataView, offset: number, type: DuckDBDecimalType): DuckDBSmallDecimal {
  const scaledValue = getInt32(dataView, offset);
  return new DuckDBSmallDecimal(scaledValue, type);
}

function getDecimal8(dataView: DataView, offset: number, type: DuckDBDecimalType): DuckDBLargeDecimal {
  const scaledValue = getInt64(dataView, offset);
  return new DuckDBLargeDecimal(scaledValue, type);
}

function getDecimal16(dataView: DataView, offset: number, type: DuckDBDecimalType): DuckDBLargeDecimal {
  const scaledValue = getInt128(dataView, offset);
  return new DuckDBLargeDecimal(scaledValue, type);
}

function vectorData(vector: duckdb.Vector, byteCount: number): Uint8Array {
  return duckdb.vector_get_data(vector, byteCount);
}

// This version of DuckDBValidity is almost 10x slower.
// class DuckDBValidity {
//   private readonly validity_pointer: ddb.uint64_pointer;
//   private readonly offset: number;
//   private constructor(validity_pointer: ddb.uint64_pointer, offset: number = 0) {
//     this.validity_pointer = validity_pointer;
//     this.offset = offset;
//   }
//   public static fromVector(vector: ddb.duckdb_vector, itemCount: number, offset: number = 0): DuckDBValidity {
//     const validity_pointer = ddb.duckdb_vector_get_validity(vector);
//     return new DuckDBValidity(validity_pointer, offset);
//   }
//   public itemValid(itemIndex: number): boolean {
//     return ddb.duckdb_validity_row_is_valid(this.validity_pointer, this.offset + itemIndex);
//   }
//   public slice(offset: number): DuckDBValidity {
//     return new DuckDBValidity(this.validity_pointer, this.offset + offset);
//   }
// }

class DuckDBValidity {
  private readonly data: BigUint64Array | null;
  private readonly offset: number;
  private constructor(data: BigUint64Array | null, offset: number) {
    this.data = data;
    this.offset = offset;
  }
  public static fromVector(vector: duckdb.Vector, itemCount: number): DuckDBValidity {
    const bigintCount = Math.ceil(itemCount / 64);
    const bytes = duckdb.vector_get_validity(vector, bigintCount * 8);
    const bigints = new BigUint64Array(bytes.buffer, bytes.byteOffset, bigintCount);
    return new DuckDBValidity(bigints, 0);
  }
  public itemValid(itemIndex: number): boolean {
    if (!this.data) {
      return true;
    }
    const bit = this.offset + itemIndex;
    return (this.data[Math.floor(bit / 64)] & (BigInt(1) << BigInt(bit % 64))) !== BigInt(0);
  }
  public slice(offset: number): DuckDBValidity {
    return new DuckDBValidity(this.data, this.offset + offset);
  }
}

export abstract class DuckDBVector<T> {
  public static standardSize(): number {
    return duckdb.vector_size();
  }
  public static create(vector: duckdb.Vector, itemCount: number, knownType?: DuckDBType): DuckDBVector<any> {
    const vectorType = knownType ? knownType : DuckDBLogicalType.consumeAsType(duckdb.vector_get_column_type(vector));
    switch (vectorType.typeId) {
      case DuckDBTypeId.BOOLEAN:
        return DuckDBBooleanVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.TINYINT:
        return DuckDBTinyIntVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.SMALLINT:
        return DuckDBSmallIntVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.INTEGER:
        return DuckDBIntegerVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.BIGINT:
        return DuckDBBigIntVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.UTINYINT:
        return DuckDBUTinyIntVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.USMALLINT:
        return DuckDBUSmallIntVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.UINTEGER:
        return DuckDBUIntegerVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.UBIGINT:
        return DuckDBUBigIntVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.FLOAT:
        return DuckDBFloatVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.DOUBLE:
        return DuckDBDoubleVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.TIMESTAMP:
        return DuckDBTimestampVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.DATE:
        return DuckDBDateVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.TIME:
        return DuckDBTimeVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.INTERVAL:
        return DuckDBIntervalVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.HUGEINT:
        return DuckDBHugeIntVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.UHUGEINT:
        return DuckDBUHugeIntVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.VARCHAR:
        return DuckDBVarCharVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.BLOB:
        return DuckDBBlobVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.DECIMAL:
        if (vectorType instanceof DuckDBDecimalType) {
          const { width } = vectorType;
          if (width <= 0) {
            throw new Error(`DECIMAL width not positive: ${width}`);
          } else if (width <= 4) {
            return DuckDBDecimal2Vector.fromRawVector(vectorType, vector, itemCount);
          } else if (width <= 9) {
            return DuckDBDecimal4Vector.fromRawVector(vectorType, vector, itemCount);
          } else if (width <= 18) {
            return DuckDBDecimal8Vector.fromRawVector(vectorType, vector, itemCount);
          } else if (width <= 38) {
            return DuckDBDecimal16Vector.fromRawVector(vectorType, vector, itemCount);
          } else {
            throw new Error(`DECIMAL width too large: ${width}`);
          }
        }
        throw new Error('DuckDBType has DECIMAL type id but is not an instance of DuckDBDecimalType');
      case DuckDBTypeId.TIMESTAMP_S:
        return DuckDBTimestampSecondsVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.TIMESTAMP_MS:
        return DuckDBTimestampMillisecondsVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.TIMESTAMP_NS:
        return DuckDBTimestampNanosecondsVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.ENUM:
      if (vectorType instanceof DuckDBEnumType) {
        const { internalTypeId } = vectorType;
        switch (internalTypeId) {
          case DuckDBTypeId.UTINYINT:
            return DuckDBEnum1Vector.fromRawVector(vectorType, vector, itemCount);
          case DuckDBTypeId.USMALLINT:
            return DuckDBEnum2Vector.fromRawVector(vectorType, vector, itemCount);
          case DuckDBTypeId.UINTEGER:
            return DuckDBEnum4Vector.fromRawVector(vectorType, vector, itemCount);
          default:
            throw new Error(`unsupported ENUM internal type: ${internalTypeId}`);
        }
      }
      throw new Error('DuckDBType has ENUM type id but is not an instance of DuckDBEnumType');
      case DuckDBTypeId.LIST:
        if (vectorType instanceof DuckDBListType) {
          return DuckDBListVector.fromRawVector(vectorType, vector, itemCount);
        }
        throw new Error('DuckDBType has LIST type id but is not an instance of DuckDBListType');
      case DuckDBTypeId.STRUCT:
        if (vectorType instanceof DuckDBStructType) {
          return DuckDBStructVector.fromRawVector(vectorType, vector, itemCount);
        }
        throw new Error('DuckDBType has STRUCT type id but is not an instance of DuckDBStructType');
      case DuckDBTypeId.MAP:
        if (vectorType instanceof DuckDBMapType) {
          return DuckDBMapVector.fromRawVector(vectorType, vector, itemCount);
        }
        throw new Error('DuckDBType has MAP type id but is not an instance of DuckDBMapType');
      case DuckDBTypeId.ARRAY:
        if (vectorType instanceof DuckDBArrayType) {
          return DuckDBArrayVector.fromRawVector(vectorType, vector, itemCount);
        }
        throw new Error('DuckDBType has ARRAY type id but is not an instance of DuckDBArrayType');
      case DuckDBTypeId.UUID:
        return DuckDBUUIDVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.UNION:
        if (vectorType instanceof DuckDBUnionType) {
          return DuckDBUnionVector.fromRawVector(vectorType, vector, itemCount);
        }
        throw new Error('DuckDBType has UNION type id but is not an instance of DuckDBUnionType');
      case DuckDBTypeId.BIT:
        return DuckDBBitVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.TIME_TZ:
        return DuckDBTimeTZVector.fromRawVector(vector, itemCount);
      case DuckDBTypeId.TIMESTAMP_TZ:
        return DuckDBTimestampVector.fromRawVector(vector, itemCount);
      default:
        throw new Error(`Invalid type id: ${vectorType.typeId}`);
    }
  }
  public abstract get type(): DuckDBType;
  public abstract get itemCount(): number;
  public abstract getItem(itemIndex: number): T | null;
  public abstract slice(offset: number, length: number): DuckDBVector<T>;
}

export class DuckDBBooleanVector extends DuckDBVector<boolean> {
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly _itemCount: number;
  constructor(dataView: DataView, validity: DuckDBValidity, itemCount: number) {
    super();
    this.dataView = dataView;
    this.validity = validity;
    this._itemCount = itemCount;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBBooleanVector {
    const data = vectorData(vector, itemCount * duckdb.sizeof_bool);
    const dataView = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBBooleanVector(dataView, validity, itemCount);
  }
  public override get type(): DuckDBBooleanType {
    return DuckDBBooleanType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): boolean | null {
    return this.validity.itemValid(itemIndex) ? getBoolean(this.dataView, itemIndex * duckdb.sizeof_bool) : null;
  }
  public override slice(offset: number, length: number): DuckDBBooleanVector {
    return new DuckDBBooleanVector(
      new DataView(this.dataView.buffer, this.dataView.byteOffset + offset * duckdb.sizeof_bool, length * duckdb.sizeof_bool),
      this.validity.slice(offset),
      length,
    );
  }
}

export class DuckDBTinyIntVector extends DuckDBVector<number> {
  private readonly items: Int8Array;
  private readonly validity: DuckDBValidity;
  constructor(items: Int8Array, validity: DuckDBValidity) {
    super();
    this.items = items
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBTinyIntVector {
    const data = vectorData(vector, itemCount * Int8Array.BYTES_PER_ELEMENT);
    const items = new Int8Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBTinyIntVector(items, validity);
  }
  public override get type(): DuckDBTinyIntType {
    return DuckDBTinyIntType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): number | null {
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override slice(offset: number, length: number): DuckDBTinyIntVector {
    return new DuckDBTinyIntVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBSmallIntVector extends DuckDBVector<number> {
  private readonly items: Int16Array;
  private readonly validity: DuckDBValidity;
  constructor(items: Int16Array, validity: DuckDBValidity) {
    super();
    this.items = items;
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBSmallIntVector {
    const data = vectorData(vector, itemCount * Int16Array.BYTES_PER_ELEMENT);
    const items = new Int16Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBSmallIntVector(items, validity);
  }
  public override get type(): DuckDBSmallIntType {
    return DuckDBSmallIntType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): number | null {
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override slice(offset: number, length: number): DuckDBSmallIntVector {
    return new DuckDBSmallIntVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBIntegerVector extends DuckDBVector<number> {
  private readonly items: Int32Array;
  private readonly validity: DuckDBValidity;
  constructor(items: Int32Array, validity: DuckDBValidity) {
    super();
    this.items = items;
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBIntegerVector {
    const data = vectorData(vector, itemCount * Int32Array.BYTES_PER_ELEMENT);
    const items = new Int32Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBIntegerVector(items, validity);
  }
  public override get type(): DuckDBIntegerType {
    return DuckDBIntegerType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): number | null {
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override slice(offset: number, length: number): DuckDBIntegerVector {
    return new DuckDBIntegerVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBBigIntVector extends DuckDBVector<bigint> {
  private readonly items: BigInt64Array;
  private readonly validity: DuckDBValidity;
  constructor(items: BigInt64Array, validity: DuckDBValidity) {
    super();
    this.items = items;
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBBigIntVector {
    const data = vectorData(vector, itemCount * BigInt64Array.BYTES_PER_ELEMENT);
    const items = new BigInt64Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBBigIntVector(items, validity);
  }
  public override get type(): DuckDBBigIntType {
    return DuckDBBigIntType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): bigint | null {
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override slice(offset: number, length: number): DuckDBBigIntVector {
    return new DuckDBBigIntVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBUTinyIntVector extends DuckDBVector<number> {
  private readonly items: Uint8Array;
  private readonly validity: DuckDBValidity;
  constructor(items: Uint8Array, validity: DuckDBValidity) {
    super();
    this.items = items
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBUTinyIntVector {
    const data = vectorData(vector, itemCount * Uint8Array.BYTES_PER_ELEMENT);
    const items = new Uint8Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBUTinyIntVector(items, validity);
  }
  public override get type(): DuckDBUTinyIntType {
    return DuckDBUTinyIntType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): number | null {
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override slice(offset: number, length: number): DuckDBUTinyIntVector {
    return new DuckDBUTinyIntVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBUSmallIntVector extends DuckDBVector<number> {
  private readonly items: Uint16Array;
  private readonly validity: DuckDBValidity;
  constructor(items: Uint16Array, validity: DuckDBValidity) {
    super();
    this.items = items;
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBUSmallIntVector {
    const data = vectorData(vector, itemCount * Uint16Array.BYTES_PER_ELEMENT);
    const items = new Uint16Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBUSmallIntVector(items, validity);
  }
  public override get type(): DuckDBUSmallIntType {
    return DuckDBUSmallIntType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): number | null {
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override slice(offset: number, length: number): DuckDBUSmallIntVector {
    return new DuckDBUSmallIntVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBUIntegerVector extends DuckDBVector<number> {
  private readonly items: Uint32Array;
  private readonly validity: DuckDBValidity;
  constructor(items: Uint32Array, validity: DuckDBValidity) {
    super();
    this.items = items;
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBUIntegerVector {
    const data = vectorData(vector, itemCount * Uint32Array.BYTES_PER_ELEMENT);
    const items = new Uint32Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBUIntegerVector(items, validity);
  }
  public override get type(): DuckDBUIntegerType {
    return DuckDBUIntegerType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): number | null {
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override slice(offset: number, length: number): DuckDBUIntegerVector {
    return new DuckDBUIntegerVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBUBigIntVector extends DuckDBVector<bigint> {
  private readonly items: BigUint64Array;
  private readonly validity: DuckDBValidity;
  constructor(items: BigUint64Array, validity: DuckDBValidity) {
    super();
    this.items = items;
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBUBigIntVector {
    const data = vectorData(vector, itemCount * BigUint64Array.BYTES_PER_ELEMENT);
    const items = new BigUint64Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBUBigIntVector(items, validity);
  }
  public override get type(): DuckDBUBigIntType {
    return DuckDBUBigIntType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): bigint | null {
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override slice(offset: number, length: number): DuckDBUBigIntVector {
    return new DuckDBUBigIntVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBFloatVector extends DuckDBVector<number> {
  private readonly items: Float32Array;
  private readonly validity: DuckDBValidity;
  constructor(items: Float32Array, validity: DuckDBValidity) {
    super();
    this.items = items;
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBFloatVector {
    const data = vectorData(vector, itemCount * Float32Array.BYTES_PER_ELEMENT);
    const items = new Float32Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBFloatVector(items, validity);
  }
  public override get type(): DuckDBFloatType {
    return DuckDBFloatType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): number | null {
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override slice(offset: number, length: number): DuckDBFloatVector {
    return new DuckDBFloatVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBDoubleVector extends DuckDBVector<number> {
  private readonly items: Float64Array;
  private readonly validity: DuckDBValidity;
  constructor(items: Float64Array, validity: DuckDBValidity) {
    super();
    this.items = items;
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBDoubleVector {
    const data = vectorData(vector, itemCount * Float64Array.BYTES_PER_ELEMENT);
    const items = new Float64Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBDoubleVector(items, validity);
  }
  public override get type(): DuckDBDoubleType {
    return DuckDBDoubleType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): number | null {
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override slice(offset: number, length: number): DuckDBDoubleVector {
    return new DuckDBDoubleVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBTimestampVector extends DuckDBVector<bigint> {
  private readonly items: BigInt64Array;
  private readonly validity: DuckDBValidity;
  constructor(items: BigInt64Array, validity: DuckDBValidity) {
    super();
    this.items = items;
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBTimestampVector {
    const data = vectorData(vector, itemCount * BigInt64Array.BYTES_PER_ELEMENT);
    const items = new BigInt64Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBTimestampVector(items, validity);
  }
  public override get type(): DuckDBTimestampType {
    return DuckDBTimestampType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): bigint | null { // microseconds
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override slice(offset: number, length: number): DuckDBTimestampVector {
    return new DuckDBTimestampVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBDateVector extends DuckDBVector<number> {
  private readonly items: Int32Array;
  private readonly validity: DuckDBValidity;
  constructor(items: Int32Array, validity: DuckDBValidity) {
    super();
    this.items = items;
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBDateVector {
    const data = vectorData(vector, itemCount * Int32Array.BYTES_PER_ELEMENT);
    const items = new Int32Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBDateVector(items, validity);
  }
  public override get type(): DuckDBDateType {
    return DuckDBDateType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): number | null { // days
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override slice(offset: number, length: number): DuckDBDateVector {
    return new DuckDBDateVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBTimeVector extends DuckDBVector<bigint> {
  private readonly items: BigInt64Array;
  private readonly validity: DuckDBValidity;
  constructor(items: BigInt64Array, validity: DuckDBValidity) {
    super();
    this.items = items;
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBTimeVector {
    const data = vectorData(vector, itemCount * BigInt64Array.BYTES_PER_ELEMENT);
    const items = new BigInt64Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBTimeVector(items, validity);
  }
  public override get type(): DuckDBTimeType {
    return DuckDBTimeType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): bigint | null { // microseconds
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override slice(offset: number, length: number): DuckDBTimeVector {
    return new DuckDBTimeVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBInterval {
  public readonly months: number;
  public readonly days: number;
  public readonly micros: bigint;

  public constructor(months: number, days: number, micros: bigint) {
    this.months = months;
    this.days = days;
    this.micros = micros;
  }
}

export class DuckDBIntervalVector extends DuckDBVector<DuckDBInterval> {
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly _itemCount: number;
  constructor(dataView: DataView, validity: DuckDBValidity, itemCount: number) {
    super();
    this.dataView = dataView;
    this.validity = validity;
    this._itemCount = itemCount;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBIntervalVector {
    const data = vectorData(vector, itemCount * 16);
    const dataView = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBIntervalVector(dataView, validity, itemCount);
  }
  public override get type(): DuckDBHugeIntType {
    return DuckDBIntervalType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): DuckDBInterval | null {
    if (!this.validity.itemValid(itemIndex)) {
      return null;
    }
    const itemStart = itemIndex * 16;
    const months = getInt32(this.dataView, itemStart);
    const days = getInt32(this.dataView, itemStart + 4);
    const micros = getInt64(this.dataView, itemStart + 8);
    return new DuckDBInterval(months, days, micros);
  }
  public override slice(offset: number, length: number): DuckDBIntervalVector {
    return new DuckDBIntervalVector(
      new DataView(this.dataView.buffer, this.dataView.byteOffset + offset * 16, length * 16),
      this.validity.slice(offset),
      length,
    );
  }
}

export class DuckDBHugeIntVector extends DuckDBVector<bigint> {
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly _itemCount: number;
  constructor(dataView: DataView, validity: DuckDBValidity, itemCount: number) {
    super();
    this.dataView = dataView;
    this.validity = validity;
    this._itemCount = itemCount;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBHugeIntVector {
    const data = vectorData(vector, itemCount * 16);
    const dataView = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBHugeIntVector(dataView, validity, itemCount);
  }
  public override get type(): DuckDBHugeIntType {
    return DuckDBHugeIntType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): bigint | null {
    return this.validity.itemValid(itemIndex) ? getInt128(this.dataView, itemIndex * 16) : null;
  }
  public override slice(offset: number, length: number): DuckDBHugeIntVector {
    return new DuckDBHugeIntVector(
      new DataView(this.dataView.buffer, this.dataView.byteOffset + offset * 16, length * 16),
      this.validity.slice(offset),
      length,
    );
  }
}

export class DuckDBUHugeIntVector extends DuckDBVector<bigint> {
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly _itemCount: number;
  constructor(dataView: DataView, validity: DuckDBValidity, itemCount: number) {
    super();
    this.dataView = dataView;
    this.validity = validity;
    this._itemCount = itemCount;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBUHugeIntVector {
    const data = vectorData(vector, itemCount * 16);
    const dataView = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBUHugeIntVector(dataView, validity, itemCount);
  }
  public override get type(): DuckDBUHugeIntType {
    return DuckDBUHugeIntType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): bigint | null {
    return this.validity.itemValid(itemIndex) ? getUInt128(this.dataView, itemIndex * 16) : null;
  }
  public override slice(offset: number, length: number): DuckDBUHugeIntVector {
    return new DuckDBUHugeIntVector(
      new DataView(this.dataView.buffer, this.dataView.byteOffset + offset * 16, length * 16),
      this.validity.slice(offset),
      length,
    );
  }
}

export class DuckDBVarCharVector extends DuckDBVector<string> {
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly _itemCount: number;
  constructor(dataView: DataView, validity: DuckDBValidity, itemCount: number) {
    super();
    this.dataView = dataView;
    this.validity = validity;
    this._itemCount = itemCount;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBVarCharVector {
    const data = vectorData(vector, itemCount * 16);
    const dataView = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBVarCharVector(dataView, validity, itemCount);
  }
  public override get type(): DuckDBVarCharType {
    return DuckDBVarCharType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): string | null {
    return this.validity.itemValid(itemIndex) ? getString(this.dataView, itemIndex * 16) : null;
  }
  public override slice(offset: number, length: number): DuckDBVarCharVector {
    return new DuckDBVarCharVector(
      new DataView(this.dataView.buffer, this.dataView.byteOffset + offset * 16, length * 16),
      this.validity.slice(offset),
      length,
    );
  }
}

export class DuckDBBlobVector extends DuckDBVector<Uint8Array> {
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly _itemCount: number;
  constructor(dataView: DataView, validity: DuckDBValidity, itemCount: number) {
    super();
    this.dataView = dataView;
    this.validity = validity;
    this._itemCount = itemCount;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBBlobVector {
    const data = vectorData(vector, itemCount * 16);
    const dataView = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBBlobVector(dataView, validity, itemCount);
  }
  public override get type(): DuckDBBlobType {
    return DuckDBBlobType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): Uint8Array | null {
    return this.validity.itemValid(itemIndex) ? getBuffer(this.dataView, itemIndex * 16) : null;
  }
  public override slice(offset: number, length: number): DuckDBBlobVector {
    return new DuckDBBlobVector(
      new DataView(this.dataView.buffer, this.dataView.byteOffset + offset * 16, length * 16),
      this.validity.slice(offset),
      length,
    );
  }
}

export class DuckDBDecimal2Vector extends DuckDBVector<DuckDBSmallDecimal> {
  private readonly decimalType: DuckDBDecimalType;
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly _itemCount: number;
  constructor(decimalType: DuckDBDecimalType, dataView: DataView, validity: DuckDBValidity, itemCount: number) {
    super();
    this.decimalType = decimalType;
    this.dataView = dataView;
    this.validity = validity;
    this._itemCount = itemCount;
  }
  static fromRawVector(decimalType: DuckDBDecimalType, vector: duckdb.Vector, itemCount: number): DuckDBDecimal2Vector {
    const data = vectorData(vector, itemCount * 2);
    const dataView = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBDecimal2Vector(decimalType, dataView, validity, itemCount);
  }
  public override get type(): DuckDBDecimalType {
    return this.decimalType;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): DuckDBSmallDecimal | null {
    return this.validity.itemValid(itemIndex) ? getDecimal2(this.dataView, itemIndex * 2, this.decimalType) : null;
  }
  public getScaledValue(itemIndex: number): number | null {
    return this.validity.itemValid(itemIndex) ? getInt16(this.dataView, itemIndex * 2) : null;
  }
  public override slice(offset: number, length: number): DuckDBDecimal2Vector {
    return new DuckDBDecimal2Vector(
      this.decimalType,
      new DataView(this.dataView.buffer, this.dataView.byteOffset + offset * 2, length * 2),
      this.validity.slice(offset),
      length,
    );
  }
}

export class DuckDBDecimal4Vector extends DuckDBVector<DuckDBSmallDecimal> {
  private readonly decimalType: DuckDBDecimalType;
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly _itemCount: number;
  constructor(decimalType: DuckDBDecimalType, dataView: DataView, validity: DuckDBValidity, itemCount: number) {
    super();
    this.decimalType = decimalType;
    this.dataView = dataView;
    this.validity = validity;
    this._itemCount = itemCount;
  }
  static fromRawVector(decimalType: DuckDBDecimalType, vector: duckdb.Vector, itemCount: number): DuckDBDecimal4Vector {
    const data = vectorData(vector, itemCount * 4);
    const dataView = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBDecimal4Vector(decimalType, dataView, validity, itemCount);
  }
  public override get type(): DuckDBDecimalType {
    return this.decimalType;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): DuckDBSmallDecimal | null {
    return this.validity.itemValid(itemIndex) ? getDecimal4(this.dataView, itemIndex * 4, this.decimalType) : null;
  }
  public getScaledValue(itemIndex: number): number | null {
    return this.validity.itemValid(itemIndex) ? getInt32(this.dataView, itemIndex * 4) : null;
  }
  public override slice(offset: number, length: number): DuckDBDecimal4Vector {
    return new DuckDBDecimal4Vector(
      this.decimalType,
      new DataView(this.dataView.buffer, this.dataView.byteOffset + offset * 4, length * 4),
      this.validity.slice(offset),
      length,
    );
  }
}

export class DuckDBDecimal8Vector extends DuckDBVector<DuckDBLargeDecimal> {
  private readonly decimalType: DuckDBDecimalType;
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly _itemCount: number;
  constructor(decimalType: DuckDBDecimalType, dataView: DataView, validity: DuckDBValidity, itemCount: number) {
    super();
    this.decimalType = decimalType;
    this.dataView = dataView;
    this.validity = validity;
    this._itemCount = itemCount;
  }
  static fromRawVector(decimalType: DuckDBDecimalType, vector: duckdb.Vector, itemCount: number): DuckDBDecimal8Vector {
    const data = vectorData(vector, itemCount * 8);
    const dataView = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBDecimal8Vector(decimalType, dataView, validity, itemCount);
  }
  public override get type(): DuckDBDecimalType {
    return this.decimalType;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): DuckDBLargeDecimal | null {
    return this.validity.itemValid(itemIndex) ? getDecimal8(this.dataView, itemIndex * 8, this.decimalType) : null;
  }
  public getScaledValue(itemIndex: number): bigint | null {
    return this.validity.itemValid(itemIndex) ? getInt64(this.dataView, itemIndex * 8) : null;
  }
  public override slice(offset: number, length: number): DuckDBDecimal8Vector {
    return new DuckDBDecimal8Vector(
      this.decimalType,
      new DataView(this.dataView.buffer, this.dataView.byteOffset + offset * 8, length * 8),
      this.validity.slice(offset),
      length,
    );
  }
}

export class DuckDBDecimal16Vector extends DuckDBVector<DuckDBLargeDecimal> {
  private readonly decimalType: DuckDBDecimalType;
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly _itemCount: number;
  constructor(decimalType: DuckDBDecimalType, dataView: DataView, validity: DuckDBValidity, itemCount: number) {
    super();
    this.decimalType = decimalType;
    this.dataView = dataView;
    this.validity = validity;
    this._itemCount = itemCount;
  }
  static fromRawVector(decimalType: DuckDBDecimalType, vector: duckdb.Vector, itemCount: number): DuckDBDecimal16Vector {
    const data = vectorData(vector, itemCount * 16);
    const dataView = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBDecimal16Vector(decimalType, dataView, validity, itemCount);
  }
  public override get type(): DuckDBDecimalType {
    return this.decimalType;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): DuckDBLargeDecimal | null {
    return this.validity.itemValid(itemIndex) ? getDecimal16(this.dataView, itemIndex * 16, this.decimalType) : null;
  }
  public getScaledValue(itemIndex: number): bigint | null {
    return this.validity.itemValid(itemIndex) ? getInt128(this.dataView, itemIndex * 16) : null;
  }
  public override slice(offset: number, length: number): DuckDBDecimal16Vector {
    return new DuckDBDecimal16Vector(
      this.decimalType,
      new DataView(this.dataView.buffer, this.dataView.byteOffset + offset * 16, length * 16),
      this.validity.slice(offset),
      length,
    );
  }
}

export class DuckDBTimestampSecondsVector extends DuckDBVector<bigint> {
  private readonly items: BigInt64Array;
  private readonly validity: DuckDBValidity;
  constructor(items: BigInt64Array, validity: DuckDBValidity) {
    super();
    this.items = items;
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBTimestampSecondsVector {
    const data = vectorData(vector, itemCount * BigInt64Array.BYTES_PER_ELEMENT);
    const items = new BigInt64Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBTimestampSecondsVector(items, validity);
  }
  public override get type(): DuckDBTimestampSecondsType {
    return DuckDBTimestampSecondsType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): bigint | null { // seconds
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override slice(offset: number, length: number): DuckDBTimestampSecondsVector {
    return new DuckDBTimestampSecondsVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBTimestampMillisecondsVector extends DuckDBVector<bigint> {
  private readonly items: BigInt64Array;
  private readonly validity: DuckDBValidity;
  constructor(items: BigInt64Array, validity: DuckDBValidity) {
    super();
    this.items = items;
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBTimestampMillisecondsVector {
    const data = vectorData(vector, itemCount * BigInt64Array.BYTES_PER_ELEMENT);
    const items = new BigInt64Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBTimestampMillisecondsVector(items, validity);
  }
  public override get type(): DuckDBTimestampMillisecondsType {
    return DuckDBTimestampMillisecondsType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): bigint | null { // milliseconds
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override slice(offset: number, length: number): DuckDBTimestampMillisecondsVector {
    return new DuckDBTimestampMillisecondsVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBTimestampNanosecondsVector extends DuckDBVector<bigint> {
  private readonly items: BigInt64Array;
  private readonly validity: DuckDBValidity;
  constructor(items: BigInt64Array, validity: DuckDBValidity) {
    super();
    this.items = items;
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBTimestampNanosecondsVector {
    const data = vectorData(vector, itemCount * BigInt64Array.BYTES_PER_ELEMENT);
    const items = new BigInt64Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBTimestampNanosecondsVector(items, validity);
  }
  public override get type(): DuckDBTimestampNanosecondsType {
    return DuckDBTimestampNanosecondsType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): bigint | null { // nanoseconds
    return this.validity.itemValid(itemIndex) ? this.items[itemIndex] : null;
  }
  public override slice(offset: number, length: number): DuckDBTimestampNanosecondsVector {
    return new DuckDBTimestampNanosecondsVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBEnum1Vector extends DuckDBVector<string> {
  private readonly enumType: DuckDBEnumType;
  private readonly items: Uint8Array;
  private readonly validity: DuckDBValidity;
  constructor(enumType: DuckDBEnumType, items: Uint8Array, validity: DuckDBValidity) {
    super();
    this.enumType = enumType;
    this.items = items
    this.validity = validity;
  }
  static fromRawVector(enumType: DuckDBEnumType, vector: duckdb.Vector, itemCount: number): DuckDBEnum1Vector {
    const data = vectorData(vector, itemCount);
    const items = new Uint8Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBEnum1Vector(enumType, items, validity);
  }
  public override get type(): DuckDBEnumType {
    return this.enumType;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): string | null {
    return this.validity.itemValid(itemIndex) ? this.enumType.values[this.items[itemIndex]] : null;
  }
  public override slice(offset: number, length: number): DuckDBEnum1Vector {
    return new DuckDBEnum1Vector(this.enumType, this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBEnum2Vector extends DuckDBVector<string> {
  private readonly enumType: DuckDBEnumType;
  private readonly items: Uint16Array;
  private readonly validity: DuckDBValidity;
  constructor(enumType: DuckDBEnumType, items: Uint16Array, validity: DuckDBValidity) {
    super();
    this.enumType = enumType;
    this.items = items
    this.validity = validity;
  }
  static fromRawVector(enumType: DuckDBEnumType, vector: duckdb.Vector, itemCount: number): DuckDBEnum2Vector {
    const data = vectorData(vector, itemCount * 2);
    const items = new Uint16Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBEnum2Vector(enumType, items, validity);
  }
  public override get type(): DuckDBEnumType {
    return this.enumType;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): string | null {
    return this.validity.itemValid(itemIndex) ? this.enumType.values[this.items[itemIndex]] : null;
  }
  public override slice(offset: number, length: number): DuckDBEnum2Vector {
    return new DuckDBEnum2Vector(this.enumType, this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBEnum4Vector extends DuckDBVector<string> {
  private readonly enumType: DuckDBEnumType;
  private readonly items: Uint32Array;
  private readonly validity: DuckDBValidity;
  constructor(enumType: DuckDBEnumType, items: Uint32Array, validity: DuckDBValidity) {
    super();
    this.enumType = enumType;
    this.items = items
    this.validity = validity;
  }
  static fromRawVector(enumType: DuckDBEnumType, vector: duckdb.Vector, itemCount: number): DuckDBEnum4Vector {
    const data = vectorData(vector, itemCount * 4);
    const items = new Uint32Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBEnum4Vector(enumType, items, validity);
  }
  public override get type(): DuckDBEnumType {
    return this.enumType;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): string | null {
    return this.validity.itemValid(itemIndex) ? this.enumType.values[this.items[itemIndex]] : null;
  }
  public override slice(offset: number, length: number): DuckDBEnum4Vector {
    return new DuckDBEnum4Vector(this.enumType, this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}

export class DuckDBListVector<TValue = any> extends DuckDBVector<DuckDBVector<TValue>> {
  private readonly listType: DuckDBListType;
  private readonly entryData: BigUint64Array;
  private readonly validity: DuckDBValidity;
  private readonly childData: DuckDBVector<TValue>;
  private readonly _itemCount: number;
  constructor(
    listType: DuckDBListType,
    entryData: BigUint64Array,
    validity: DuckDBValidity,
    childData: DuckDBVector<TValue>,
    itemCount: number,
  ) {
    super();
    this.listType = listType;
    this.entryData = entryData;
    this.validity = validity;
    this.childData = childData;
    this._itemCount = itemCount;
  }
  static fromRawVector(listType: DuckDBListType, vector: duckdb.Vector, itemCount: number): DuckDBListVector {
    const data = vectorData(vector, itemCount * BigUint64Array.BYTES_PER_ELEMENT * 2);
    const entryData = new BigUint64Array(data.buffer, data.byteOffset, itemCount * 2);

    const validity = DuckDBValidity.fromVector(vector, itemCount);

    const child_vector = duckdb.list_vector_get_child(vector);
    const child_vector_size = duckdb.list_vector_get_size(vector);
    const childData = DuckDBVector.create(child_vector, child_vector_size, listType.valueType);

    return new DuckDBListVector(listType, entryData, validity, childData, itemCount);
  }
  public override get type(): DuckDBListType {
    return this.listType;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): DuckDBVector<TValue> | null {
    if (!this.validity.itemValid(itemIndex)) {
      return null;
    }
    const entryDataStartIndex = itemIndex * 2;
    const offset = Number(this.entryData[entryDataStartIndex]);
    const length = Number(this.entryData[entryDataStartIndex + 1]);
    return this.childData.slice(offset, length);
  }
  public override slice(offset: number, length: number): DuckDBListVector<TValue> {
    const entryDataStartIndex = offset * 2;
    return new DuckDBListVector<TValue>(
      this.listType,
      this.entryData.slice(entryDataStartIndex, entryDataStartIndex + length * 2),
      this.validity.slice(offset),
      this.childData,
      length,
    );
  }
}

export interface DuckDBStructEntry {
  readonly name: string;
  readonly value: any;
}

export class DuckDBStructVector extends DuckDBVector<readonly DuckDBStructEntry[]> {
  private readonly structType: DuckDBStructType;
  private readonly _itemCount: number;
  private readonly entryVectors: readonly DuckDBVector<any>[];
  private readonly validity: DuckDBValidity;
  constructor(structType: DuckDBStructType, itemCount: number, entryVectors: readonly DuckDBVector<any>[], validity: DuckDBValidity) {
    super();
    this.structType = structType;
    this._itemCount = itemCount;
    this.entryVectors = entryVectors;
    this.validity = validity;
  }
  static fromRawVector(structType: DuckDBStructType, vector: duckdb.Vector, itemCount: number): DuckDBStructVector {
    const entryCount = structType.entries.length;
    const entryVectors: DuckDBVector<any>[] = [];
    for (let i = 0; i < entryCount; i++) {
      const entry = structType.entries[i];
      const child_vector = duckdb.struct_vector_get_child(vector, i);
      entryVectors.push(DuckDBVector.create(child_vector, itemCount, entry.valueType));
    }
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBStructVector(structType, itemCount, entryVectors, validity);
  }
  public override get type(): DuckDBStructType {
    return this.structType;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): readonly DuckDBStructEntry[] | null {
    if (!this.validity.itemValid(itemIndex)) {
      return null;
    }
    const entries: DuckDBStructEntry[] = [];
    const entryCount = this.structType.entries.length;
    for (let i = 0; i < entryCount; i++) {
      const entry = this.structType.entries[i];
      const entryVector = this.entryVectors[i];
      entries.push({ name: entry.name, value: entryVector.getItem(itemIndex) });
    }
    return entries;
  }
  public getItemValue(itemIndex: number, entryIndex: number): any | null {
    if (!this.validity.itemValid(itemIndex)) {
      return null;
    }
    const entryVector = this.entryVectors[entryIndex];
    return entryVector.getItem(itemIndex);
  }
  public override slice(offset: number, length: number): DuckDBStructVector {
    return new DuckDBStructVector(
      this.structType,
      length,
      this.entryVectors.map(entryVector => entryVector.slice(offset, length)),
      this.validity.slice(offset),
    );
  }
}

export interface DuckDBMapEntry {
  readonly key: any;
  readonly value: any;
}

// MAP = LIST(STRUCT(key KEY_TYPE, value VALUE_TYPE))
export class DuckDBMapVector extends DuckDBVector<readonly DuckDBMapEntry[]> {
  private readonly mapType: DuckDBMapType;
  private readonly listVector: DuckDBListVector;
  constructor(mapType: DuckDBMapType, listVector: DuckDBListVector) {
    super();
    this.mapType = mapType;
    this.listVector = listVector;
  }
  static fromRawVector(mapType: DuckDBMapType, vector: duckdb.Vector, itemCount: number): DuckDBMapVector {
    const listVectorType = new DuckDBListType(new DuckDBStructType([
      { name: 'key', valueType: mapType.keyType },
      { name: 'value', valueType: mapType.valueType }
    ]));
    return new DuckDBMapVector(mapType, DuckDBListVector.fromRawVector(listVectorType, vector, itemCount));
  }
  public override get type(): DuckDBType {
    return this.mapType;
  }
  public override get itemCount(): number {
    return this.listVector.itemCount;
  }
  public override getItem(itemIndex: number): readonly DuckDBMapEntry[] | null {
    const itemVector = this.listVector.getItem(itemIndex);
    if (!itemVector) {
      return null;
    }
    if (!(itemVector instanceof DuckDBStructVector)) {
      throw new Error('item in map list vector is not a struct');
    }
    const entries: DuckDBMapEntry[] = [];
    const itemEntryCount = itemVector.itemCount;
    for (let i = 0; i < itemEntryCount; i++) {
      const entry = itemVector.getItem(i);
      if (!entry) {
        throw new Error('null entry in map struct');
      }
      const keyEntry = entry[0];
      const valueEntry = entry[1];
      entries.push({ key: keyEntry.value, value: valueEntry.value });
    }
    return entries;
  }
  public override slice(offset: number, length: number): DuckDBMapVector {
    return new DuckDBMapVector(
      this.mapType,
      this.listVector.slice(offset, length),
    );
  }
}

export class DuckDBArrayVector<TValue = any> extends DuckDBVector<DuckDBVector<TValue>> {
  private readonly arrayType: DuckDBArrayType;
  private readonly validity: DuckDBValidity;
  private readonly childData: DuckDBVector<TValue>;
  private readonly _itemCount: number;
  constructor(
    arrayType: DuckDBArrayType,
    validity: DuckDBValidity,
    childData: DuckDBVector<TValue>,
    itemCount: number,
  ) {
    super();
    this.arrayType = arrayType;
    this.validity = validity;
    this.childData = childData;
    this._itemCount = itemCount;
  }
  static fromRawVector(arrayType: DuckDBArrayType, vector: duckdb.Vector, itemCount: number): DuckDBArrayVector {
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    const child_vector = duckdb.array_vector_get_child(vector);
    const childItemsPerArray = DuckDBArrayVector.itemSize(arrayType) * arrayType.length;
    const childData = DuckDBVector.create(child_vector, itemCount * childItemsPerArray, arrayType.valueType);
    return new DuckDBArrayVector(arrayType, validity, childData, itemCount);
  }
  private static itemSize(arrayType: DuckDBArrayType): number {
    if (arrayType.valueType instanceof DuckDBArrayType) {
      return DuckDBArrayVector.itemSize(arrayType.valueType);
    } else {
      return 1;
    }
  }
  public override get type(): DuckDBArrayType {
    return this.arrayType;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): DuckDBVector<TValue> | null {
    if (!this.validity.itemValid(itemIndex)) {
      return null;
    }
    return this.childData.slice(itemIndex * this.arrayType.length, this.arrayType.length);
  }
  public override slice(offset: number, length: number): DuckDBArrayVector<TValue> {
    return new DuckDBArrayVector<TValue>(
      this.arrayType,
      this.validity.slice(offset),
      this.childData.slice(offset * this.arrayType.length, length * this.arrayType.length),
      length,
    );
  }
}

export class DuckDBUUIDVector extends DuckDBVector<bigint> {
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly _itemCount: number;
  constructor(dataView: DataView, validity: DuckDBValidity, itemCount: number) {
    super();
    this.dataView = dataView;
    this.validity = validity;
    this._itemCount = itemCount;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBUUIDVector {
    const data = vectorData(vector, itemCount * 16);
    const dataView = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBUUIDVector(dataView, validity, itemCount);
  }
  public override get type(): DuckDBUUIDType {
    return DuckDBUUIDType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): bigint | null {
    return this.validity.itemValid(itemIndex) ? getInt128(this.dataView, itemIndex * 16) : null;
  }
  public override slice(offset: number, length: number): DuckDBUUIDVector {
    return new DuckDBUUIDVector(
      new DataView(this.dataView.buffer, this.dataView.byteOffset + offset * 16, length * 16),
      this.validity.slice(offset),
      length,
    );
  }
}

export interface DuckDBUnionAlternative {
  readonly tag: string;
  readonly value: any;
}

// UNION = STRUCT with first entry named "tag"
export class DuckDBUnionVector extends DuckDBVector<DuckDBUnionAlternative> {
  private readonly unionType: DuckDBUnionType;
  private readonly structVector: DuckDBStructVector;
  constructor(unionType: DuckDBUnionType, structVector: DuckDBStructVector) {
    super();
    this.unionType = unionType;
    this.structVector = structVector;
  }
  static fromRawVector(unionType: DuckDBUnionType, vector: duckdb.Vector, itemCount: number): DuckDBUnionVector {
    const structEntryTypes: DuckDBStructEntryType[] = [{ name: 'tag', valueType: DuckDBUTinyIntType.instance }];
    for (const alternative of unionType.alternatives) {
      structEntryTypes.push({ name: alternative.tag, valueType: alternative.valueType });
    }
    const structVectorType = new DuckDBStructType(structEntryTypes);
    return new DuckDBUnionVector(unionType, DuckDBStructVector.fromRawVector(structVectorType, vector, itemCount));
  }
  public override get type(): DuckDBUnionType {
    return this.unionType;
  }
  public override get itemCount(): number {
    return this.structVector.itemCount;
  }
  public override getItem(itemIndex: number): DuckDBUnionAlternative | null {
    const tagValue = this.structVector.getItemValue(itemIndex, 0);
    if (tagValue == null) {
      return null;
    }
    const alternativeIndex = Number(tagValue);
    const tag = this.unionType.alternatives[alternativeIndex].tag;
    const entryIndex = alternativeIndex + 1;
    const value = this.structVector.getItemValue(itemIndex, entryIndex);
    return { tag, value };
  }
  public override slice(offset: number, length: number): DuckDBUnionVector {
    return new DuckDBUnionVector(
      this.unionType,
      this.structVector.slice(offset, length),
    );
  }
}

export class DuckDBBitValue {
  private readonly data: Uint8Array;

  constructor(data: Uint8Array) {
    this.data = data;
  }

  public static fromString(str: string): DuckDBBitValue {
    if (!/^[01]*$/.test(str)) {
      throw new Error(`input string must only contain '0's and '1's`);
    }

    const byteCount = Math.ceil(str.length / 8) + 1;
    const paddingBitCount = (8 - (str.length % 8)) % 8;

    const data = new Uint8Array(byteCount);
    let byteIndex = 0;

    // first byte contains count of padding bits
    data[byteIndex++] = paddingBitCount;

    let byte = 0;
    let bitIndex = 0;

    // padding consists of 1s in MSB of second byte
    while (bitIndex < paddingBitCount) {
      byte <<= 1;
      byte |= 1;
      bitIndex++;
    }

    let charIndex = 0;

    while (byteIndex < byteCount) {
      while (bitIndex < 8) {
        byte <<= 1;
        if (str[charIndex++] === '1') {
          byte |= 1;
        }
        bitIndex++;
      }
      data[byteIndex++] = byte;
      byte = 0;
      bitIndex = 0;
    }

    return new DuckDBBitValue(data);
  }

  private padding(): number {
    return this.data[0];
  }

  public length(): number {
    return (this.data.length - 1) * 8 - this.padding();
  }

  public getBool(index: number): boolean {
    const dataIndex = Math.floor(index / 8) + 1;
    return (this.data[dataIndex] & (1 << (index % 8))) !== 0;
  }

  public getBit(index: number): 0 | 1 {
    return this.getBool(index) ? 1 : 0;
  }

  public toString(): string {
    const chars = Array.from<string>({ length: this.length() });
    for (let i = 0; i < this.length(); i++) {
      chars[i] = this.getBool(i) ? '1' : '0';
    }
    return chars.join('');
  }
}

export class DuckDBBitVector extends DuckDBVector<DuckDBBitValue> {
  private readonly dataView: DataView;
  private readonly validity: DuckDBValidity;
  private readonly _itemCount: number;
  constructor(dataView: DataView, validity: DuckDBValidity, itemCount: number) {
    super();
    this.dataView = dataView;
    this.validity = validity;
    this._itemCount = itemCount;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBBitVector {
    const data = vectorData(vector, itemCount * 16);
    const dataView = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBBitVector(dataView, validity, itemCount);
  }
  public override get type(): DuckDBBitType {
    return DuckDBBitType.instance;
  }
  public override get itemCount(): number {
    return this._itemCount;
  }
  public override getItem(itemIndex: number): DuckDBBitValue | null {
    if (!this.validity.itemValid(itemIndex)) {
      return null;
    }
    const bytes = getStringBytes(this.dataView, itemIndex * 16);
    return bytes ? new DuckDBBitValue(bytes) : null;
  }
  public override slice(offset: number, length: number): DuckDBBitVector {
    return new DuckDBBitVector(
      new DataView(this.dataView.buffer, this.dataView.byteOffset + offset * 16, length * 16),
      this.validity.slice(offset),
      length,
    );
  }
}

export class DuckDBTimeTZValue {
  /** Ranges from 0 to 86400000000 (= 24 * 60 * 60 * 1000 * 1000) */
  public readonly microseconds: number;

  /** In seconds, ranges from -57599 to 57599 (= 16 * 60 * 60 - 1) */
  public readonly offset: number;

  public constructor(microseconds: number, offset: number) {
    this.microseconds = microseconds;
    this.offset = offset;
  }

  public static TIME_BITS = 40;
	public static OFFSET_BITS = 24;
	public static MAX_OFFSET = 16 * 60 * 60 - 1; // ±15:59:59 = 57599 seconds

  public static fromBits(bits: bigint): DuckDBTimeTZValue {
    const microseconds = Number(BigInt.asUintN(DuckDBTimeTZValue.TIME_BITS, bits >> BigInt(DuckDBTimeTZValue.OFFSET_BITS)));
    const offset = DuckDBTimeTZValue.MAX_OFFSET - Number(BigInt.asUintN(DuckDBTimeTZValue.OFFSET_BITS, bits));
    return new DuckDBTimeTZValue(microseconds, offset);
  }
}

export class DuckDBTimeTZVector extends DuckDBVector<DuckDBTimeTZValue> {
  private readonly items: BigUint64Array;
  private readonly validity: DuckDBValidity;
  constructor(items: BigUint64Array, validity: DuckDBValidity) {
    super();
    this.items = items;
    this.validity = validity;
  }
  static fromRawVector(vector: duckdb.Vector, itemCount: number): DuckDBTimeTZVector {
    const data = vectorData(vector, itemCount * BigUint64Array.BYTES_PER_ELEMENT);
    const items = new BigUint64Array(data.buffer, data.byteOffset, itemCount);
    const validity = DuckDBValidity.fromVector(vector, itemCount);
    return new DuckDBTimeTZVector(items, validity);
  }
  public override get type(): DuckDBTimeTZType {
    return DuckDBTimeTZType.instance;
  }
  public override get itemCount(): number {
    return this.items.length;
  }
  public override getItem(itemIndex: number): DuckDBTimeTZValue | null {
    return this.validity.itemValid(itemIndex) ? DuckDBTimeTZValue.fromBits(this.items[itemIndex]) : null;
  }
  public override slice(offset: number, length: number): DuckDBTimeTZVector {
    return new DuckDBTimeTZVector(this.items.slice(offset, offset + length), this.validity.slice(offset));
  }
}
