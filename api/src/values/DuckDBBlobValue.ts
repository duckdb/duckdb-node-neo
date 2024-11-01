import { stringFromBlob } from '../conversion/stringFromBlob';

const textEncoder = new TextEncoder();

export class DuckDBBlobValue {
  public readonly bytes: Uint8Array;

  public constructor(bytes: Uint8Array) {
    this.bytes = bytes;
  }

  /** Matches BLOB-to-VARCHAR conversion behavior of DuckDB. */
  public toString(): string {
    return stringFromBlob(this.bytes);
  }

  public static fromString(str: string): DuckDBBlobValue {
    return new DuckDBBlobValue(Buffer.from(textEncoder.encode(str)));
  }
}

export function blobValue(bytes: Uint8Array): DuckDBBlobValue {
  return new DuckDBBlobValue(bytes);
}
