import duckdb from '@duckdb/node-bindings';
import os from 'os';
import { ExpectedLogicalType } from './ExpectedLogicalType';
import { isValid } from './isValid';

const littleEndian = os.endianness() === 'LE';

function getInt8(dataView: DataView, offset: number): number {
  return dataView.getInt8(offset);
}

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

function getFloat32(dataView: DataView, offset: number): number {
  return dataView.getFloat32(offset, littleEndian);
}

function getFloat64(dataView: DataView, offset: number): number {
  return dataView.getFloat64(offset, littleEndian);
}

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

/**
 * Gets the bytes either in or referenced by a `duckdb_string_t`
 * that is at `ofset` of the given `DataView`.
 */
function getStringBytes(dv: DataView, offset: number): Uint8Array {
  const lengthInBytes = getUInt32(dv, offset);
  if (lengthInBytes <= 12) {
    return new Uint8Array(dv.buffer, dv.byteOffset + offset + 4, lengthInBytes);
  } else {
    return duckdb.get_data_from_pointer(dv.buffer as ArrayBuffer, dv.byteOffset + offset + 8, lengthInBytes);
  }
}

const decoder = new TextDecoder();

/**
 * Gets the UTF-8 string either in or referenced by a `duckdb_string_t`
 * that is at `offset` of the given `DataView`.
 */
function getString(dv: DataView, offset: number): string {
  return decoder.decode(getStringBytes(dv, offset));
}

function getBuffer(dv: DataView, offset: number): Buffer {
  return Buffer.from(getStringBytes(dv, offset));
}

export function getValue(logicalType: ExpectedLogicalType, validity: BigUint64Array | null, dv: DataView, index: number): any {
  if (!isValid(validity, index)) {
    return null;
  }
  switch (logicalType.typeId) {
    case duckdb.Type.BOOLEAN:
      return getUInt8(dv, index) !== 0;

    case duckdb.Type.TINYINT:
      return getInt8(dv, index);
    case duckdb.Type.SMALLINT:
      return getInt16(dv, index * 2);
    case duckdb.Type.INTEGER:
      return getInt32(dv, index * 4);
    case duckdb.Type.BIGINT:
      return getInt64(dv, index * 8);
    
    case duckdb.Type.UTINYINT:
      return getUInt8(dv, index);
    case duckdb.Type.USMALLINT:
      return getUInt16(dv, index * 2);
    case duckdb.Type.UINTEGER:
      return getUInt32(dv, index * 4);
    case duckdb.Type.UBIGINT:
      return getUInt64(dv, index * 8);
    
    case duckdb.Type.FLOAT:
      return getFloat32(dv, index * 4);
    case duckdb.Type.DOUBLE:
      return getFloat64(dv, index * 8);
    
    case duckdb.Type.TIMESTAMP:
      return getInt64(dv, index * 8);
    case duckdb.Type.DATE:
      return getInt32(dv, index * 4);
    case duckdb.Type.TIME:
      return getInt64(dv, index * 8);
    case duckdb.Type.INTERVAL:
      return {
        months: getInt32(dv, index * 16 + 0),
        days: getInt32(dv, index * 16 + 4),
        micros: getInt64(dv, index * 16 + 8),
      };
    
    case duckdb.Type.HUGEINT:
      return getInt128(dv, index * 16);
    case duckdb.Type.UHUGEINT:
      return getUInt128(dv, index * 16);

    case duckdb.Type.VARCHAR:
      return getString(dv, index * 16);
    case duckdb.Type.BLOB:
      return getBuffer(dv, index * 16);

    case duckdb.Type.DECIMAL:
      switch (logicalType.internalType) {
        case duckdb.Type.SMALLINT:
          return getInt16(dv, index * 2);
        case duckdb.Type.INTEGER:
          return getInt32(dv, index * 4);
        case duckdb.Type.BIGINT:
          return getInt64(dv, index * 8);
        case duckdb.Type.HUGEINT:
          return getInt128(dv, index * 16);
        default:
          throw new Error(`unsupported DECIMAL internal type: ${duckdb.Type[logicalType.typeId]}`);
      }

    case duckdb.Type.TIMESTAMP_S:
      return getInt64(dv, index * 8);
    case duckdb.Type.TIMESTAMP_MS:
      return getInt64(dv, index * 8);
    case duckdb.Type.TIMESTAMP_NS:
      return getInt64(dv, index * 8);
    
    case duckdb.Type.ENUM:
      switch (logicalType.internalType) {
        case duckdb.Type.UTINYINT:
          return getUInt8(dv, index);
        case duckdb.Type.USMALLINT:
          return getUInt16(dv, index * 2);
        case duckdb.Type.UINTEGER:
          return getUInt32(dv, index * 4);
        default:
          throw new Error(`unsupported ENUM internal type: ${duckdb.Type[logicalType.typeId]}`);
      }
    
    // LIST
    // STRUCT
    // MAP
    // ARRAY

    case duckdb.Type.UUID:
      return getInt128(dv, index * 16);
    
    // UNION
    
    case duckdb.Type.BIT:
      return getBuffer(dv, index * 16);

    case duckdb.Type.TIME_TZ:
      return getInt64(dv, index * 8);
    case duckdb.Type.TIMESTAMP_TZ:
      return getInt64(dv, index * 8);

    case duckdb.Type.BIGNUM:
      return getBuffer(dv, index * 16);
    
    case duckdb.Type.SQLNULL:
      return null;
    
    default:
      throw new Error(`getValue not implemented for type: ${duckdb.Type[logicalType.typeId]}`);
  }
}

export function getListEntry(validity: BigUint64Array | null, dv: DataView, index: number): [bigint, bigint] | null {
  if (!isValid(validity, index)) {
    return null;
  }
  const offset = getUInt64(dv, index * 16);
  const length = getUInt64(dv, index * 16 + 8);
  return [offset, length];
}
