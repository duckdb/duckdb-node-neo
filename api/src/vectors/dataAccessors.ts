import duckdb from '@duckdb/node-bindings';
import os from 'os';
import { DuckDBDecimalType } from '../DuckDBType';
import { DuckDBDecimalValue } from '../values';

export const littleEndian = os.endianness() === 'LE';

// function getInt8(dataView: DataView, offset: number): number {
//   return dataView.getInt8(offset);
// }

export function getUInt8(dataView: DataView, offset: number): number {
  return dataView.getUint8(offset);
}

export function getInt16(dataView: DataView, offset: number): number {
  return dataView.getInt16(offset, littleEndian);
}

export function getUInt16(dataView: DataView, offset: number): number {
  return dataView.getUint16(offset, littleEndian);
}

export function getInt32(dataView: DataView, offset: number): number {
  return dataView.getInt32(offset, littleEndian);
}

export function getUInt32(dataView: DataView, offset: number): number {
  return dataView.getUint32(offset, littleEndian);
}

export function getInt64(dataView: DataView, offset: number): bigint {
  return dataView.getBigInt64(offset, littleEndian);
}

export function getUInt64(dataView: DataView, offset: number): bigint {
  return dataView.getBigUint64(offset, littleEndian);
}

// function getFloat32(dataView: DataView, offset: number): number {
//   return dataView.getFloat32(offset, littleEndian);
// }

// function getFloat64(dataView: DataView, offset: number): number {
//   return dataView.getFloat64(offset, littleEndian);
// }

export function getInt128(dataView: DataView, offset: number): bigint {
  const lower = getUInt64(dataView, offset);
  const upper = getInt64(dataView, offset + 8);
  return (upper << BigInt(64)) + lower;
}

export function getUInt128(dataView: DataView, offset: number): bigint {
  const lower = getUInt64(dataView, offset);
  const upper = getUInt64(dataView, offset + 8);
  return (BigInt.asUintN(64, upper) << BigInt(64)) | BigInt.asUintN(64, lower);
}

export function getStringBytes(dataView: DataView, offset: number): Uint8Array {
  const lengthInBytes = dataView.getUint32(offset, true);
  if (lengthInBytes <= 12) {
    return new Uint8Array(
      dataView.buffer,
      dataView.byteOffset + offset + 4,
      lengthInBytes
    );
  } else {
    return duckdb.get_data_from_pointer(
      dataView.buffer as ArrayBuffer,
      dataView.byteOffset + offset + 8,
      lengthInBytes
    );
  }
}

export const textDecoder = new TextDecoder();

export function getString(dataView: DataView, offset: number): string {
  const stringBytes = getStringBytes(dataView, offset);
  return textDecoder.decode(stringBytes);
}

export function getBuffer(dataView: DataView, offset: number): Buffer {
  const stringBytes = getStringBytes(dataView, offset);
  return Buffer.from(stringBytes);
}

export function getBigNumFromBytes(bytes: Uint8Array): bigint {
  const firstByte = bytes[0];
  const positive = (firstByte & 0x80) > 0;
  const uint64Mask = positive ? 0n : 0xffffffffffffffffn;
  const uint8Mask = positive ? 0 : 0xff;
  const dv = new DataView( // bytes is big endian
    bytes.buffer,
    bytes.byteOffset + 3,
    bytes.byteLength - 3
  );
  const lastUint64Offset = dv.byteLength - 8;
  let offset = 0;
  let result = 0n;
  while (offset <= lastUint64Offset) {
    result = (result << 64n) | (dv.getBigUint64(offset) ^ uint64Mask);
    offset += 8;
  }
  while (offset < dv.byteLength) {
    result = (result << 8n) | BigInt(dv.getUint8(offset) ^ uint8Mask);
    offset += 1;
  }
  return positive ? result : -result;
}

export function getBytesFromBigNum(bignum: bigint): Uint8Array {
  const numberBytes: number[] = []; // little endian
  const negative = bignum < 0;
  if (bignum === 0n) {
    numberBytes.push(0);
  } else {
    let vi = bignum < 0 ? -bignum : bignum;
    while (vi !== 0n) {
      numberBytes.push(Number(BigInt.asUintN(8, vi)));
      vi >>= 8n;
    }
  }
  const bigNumBytes = new Uint8Array(3 + numberBytes.length); // big endian
  let header = 0x800000 | numberBytes.length;
  if (negative) {
    header = ~header;
  }
  bigNumBytes[0] = 0xff & (header >> 16);
  bigNumBytes[1] = 0xff & (header >> 8);
  bigNumBytes[2] = 0xff & header;
  for (let i = 0; i < numberBytes.length; i++) {
    const byte = numberBytes[numberBytes.length - 1 - i];
    bigNumBytes[3 + i] = negative ? ~byte : byte;
  }
  return bigNumBytes;
}

export function getBoolean1(dataView: DataView, offset: number): boolean {
  return getUInt8(dataView, offset) !== 0;
}

export function getBoolean2(dataView: DataView, offset: number): boolean {
  return getUInt16(dataView, offset) !== 0;
}

export function getBoolean4(dataView: DataView, offset: number): boolean {
  return getUInt32(dataView, offset) !== 0;
}

export function getBoolean8(dataView: DataView, offset: number): boolean {
  return getUInt64(dataView, offset) !== BigInt(0);
}

export function makeGetBoolean(): (dataView: DataView, offset: number) => boolean {
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

export const getBoolean = makeGetBoolean();

// function setInt8(dataView: DataView, offset: number, value: number) {
//   dataView.setInt8(offset, value);
// }

export function setUInt8(dataView: DataView, offset: number, value: number) {
  dataView.setUint8(offset, value);
}

export function setInt16(dataView: DataView, offset: number, value: number) {
  dataView.setInt16(offset, value, littleEndian);
}

export function setUInt16(dataView: DataView, offset: number, value: number) {
  dataView.setUint16(offset, value, littleEndian);
}

export function setInt32(dataView: DataView, offset: number, value: number) {
  dataView.setInt32(offset, value, littleEndian);
}

export function setUInt32(dataView: DataView, offset: number, value: number) {
  dataView.setUint32(offset, value, littleEndian);
}

export function setInt64(dataView: DataView, offset: number, value: bigint) {
  dataView.setBigInt64(offset, value, littleEndian);
}

export function setUInt64(dataView: DataView, offset: number, value: bigint) {
  dataView.setBigUint64(offset, value, littleEndian);
}

export function setInt128(dataView: DataView, offset: number, value: bigint) {
  const lower = BigInt.asUintN(64, value);
  const upper = BigInt.asIntN(64, value >> BigInt(64));
  dataView.setBigUint64(offset, lower, littleEndian);
  dataView.setBigInt64(offset + 8, upper, littleEndian);
}

export function setUInt128(dataView: DataView, offset: number, value: bigint) {
  const lower = BigInt.asUintN(64, value);
  const upper = BigInt.asUintN(64, value >> BigInt(64));
  dataView.setBigUint64(offset, lower, littleEndian);
  dataView.setBigUint64(offset + 8, upper, littleEndian);
}

export function setBoolean1(dataView: DataView, offset: number, value: boolean) {
  setUInt8(dataView, offset, value ? 1 : 0);
}

export function setBoolean2(dataView: DataView, offset: number, value: boolean) {
  setUInt16(dataView, offset, value ? 1 : 0);
}

export function setBoolean4(dataView: DataView, offset: number, value: boolean) {
  setUInt32(dataView, offset, value ? 1 : 0);
}

export function setBoolean8(dataView: DataView, offset: number, value: boolean) {
  setUInt64(dataView, offset, value ? BigInt(1) : BigInt(0));
}

export function makeSetBoolean(): (
  dataView: DataView,
  offset: number,
  value: boolean
) => void {
  switch (duckdb.sizeof_bool) {
    case 1:
      return setBoolean1;
    case 2:
      return setBoolean2;
    case 4:
      return setBoolean4;
    case 8:
      return setBoolean8;
    default:
      throw new Error(`Unsupported boolean size: ${duckdb.sizeof_bool}`);
  }
}

export const setBoolean = makeSetBoolean();

export function getDecimal16(
  dataView: DataView,
  offset: number,
  type: DuckDBDecimalType
): DuckDBDecimalValue {
  const value = getInt16(dataView, offset);
  return new DuckDBDecimalValue(BigInt(value), type.width, type.scale);
}

export function getDecimal32(
  dataView: DataView,
  offset: number,
  type: DuckDBDecimalType
): DuckDBDecimalValue {
  const value = getInt32(dataView, offset);
  return new DuckDBDecimalValue(BigInt(value), type.width, type.scale);
}

export function getDecimal64(
  dataView: DataView,
  offset: number,
  type: DuckDBDecimalType
): DuckDBDecimalValue {
  const value = getInt64(dataView, offset);
  return new DuckDBDecimalValue(value, type.width, type.scale);
}

export function getDecimal128(
  dataView: DataView,
  offset: number,
  type: DuckDBDecimalType
): DuckDBDecimalValue {
  const value = getInt128(dataView, offset);
  return new DuckDBDecimalValue(value, type.width, type.scale);
}

export function vectorData(vector: duckdb.Vector, byteCount: number): Uint8Array {
  return duckdb.vector_get_data(vector, byteCount);
}
