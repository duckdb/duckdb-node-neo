import { bytesFromString } from '../conversion/bytesFromString';
import { stringFromBlob } from '../conversion/stringFromBlob';

export class DuckDBGeometryValue {
  /** In WKB. */
  public readonly bytes: Uint8Array;

  /** Takes raw WKB. */
  public constructor(bytes: Uint8Array) {
    this.bytes = bytes;
  }

  /** Returns WKB in string form matching DuckDB's BLOB-to-VARCHAR. */
  public toString(): string {
    return stringFromBlob(this.bytes);
  }

  /** Takes WKB in string form matching DuckDB's BLOB-to-VARCHAR. */
  public static fromString(str: string): DuckDBGeometryValue {
    return new DuckDBGeometryValue(Buffer.from(bytesFromString(str)));
  }
}

/** Takes WKB, either raw or in string form matching DuckDB's BLOB-to-VARCHAR. */
export function geometryValue(input: Uint8Array | string): DuckDBGeometryValue {
  if (typeof input === 'string') {
    return DuckDBGeometryValue.fromString(input);
  }
  return new DuckDBGeometryValue(input);
}
