import duckdb from '@duckdb/node-bindings';

// Flat numeric vectors already read their data through native-endian typed arrays
// (e.g. `new Int32Array(buffer)`), so the library effectively requires a little-endian
// platform. On such platforms a byte view of the validity bitmap can be bit-tested with
// fast Number ops instead of per-cell BigInt math.
const littleEndian = new Uint8Array(new Uint16Array([1]).buffer)[0] === 1;

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

export class DuckDBValidity {
  private data: BigUint64Array | null;
  private dataBytes: Uint8Array | null;
  private readonly offset: number;
  private readonly itemCount: number;
  private constructor(
    data: BigUint64Array | null,
    offset: number,
    itemCount: number
  ) {
    this.data = data;
    this.dataBytes = null;
    this.offset = offset;
    this.itemCount = itemCount;
  }
  public static fromVector(
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBValidity {
    const uint64Count = Math.ceil(itemCount / 64);
    const bytes = duckdb.vector_get_validity(vector, uint64Count * 8);
    if (!bytes) {
      return new DuckDBValidity(null, 0, itemCount);
    }
    const bigints = new BigUint64Array(
      bytes.buffer,
      bytes.byteOffset,
      uint64Count
    );
    return new DuckDBValidity(bigints, 0, itemCount);
  }
  /**
   * True when no validity buffer is present, meaning every item is valid. Lets bulk
   * readers skip the per-item validity check entirely on the common all-valid path.
   */
  public allValid(): boolean {
    return this.data === null;
  }
  /**
   * Validity bitmap as a byte view (little-endian platforms only), or null when all
   * items are valid. Used by the compiled row builder to inline the bit test with
   * Number ops. Paired with {@link bitOffset}.
   */
  public bytesLE(): Uint8Array | null {
    if (!this.data || !littleEndian) {
      return null;
    }
    let bytes = this.dataBytes;
    if (!bytes) {
      bytes = new Uint8Array(
        this.data.buffer,
        this.data.byteOffset,
        this.data.byteLength
      );
      this.dataBytes = bytes;
    }
    return bytes;
  }
  public bitOffset(): number {
    return this.offset;
  }
  public itemValid(itemIndex: number): boolean {
    const data = this.data;
    if (!data) {
      return true;
    }
    const bit = this.offset + itemIndex;
    if (littleEndian) {
      // Byte-granular Number bit test; avoids per-cell BigInt math.
      let bytes = this.dataBytes;
      if (!bytes) {
        bytes = new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
        this.dataBytes = bytes;
      }
      return (bytes[bit >> 3] & (1 << (bit & 7))) !== 0;
    }
    return (data[Math.floor(bit / 64)] & (BigInt(1) << BigInt(bit % 64))) !== 0n;
  }
  public setItemValid(itemIndex: number, valid: boolean) {
    if (!this.data && !valid) {
      // An item is being set to invalid and we don't have a data buffer, so create it. If an item is being set to valid
      // and we don't have a data buffer, then there's nothing to do; a null data buffer indicates all items are valid.
      const uint64Count = Math.ceil(this.itemCount / 64);
      const buffer = new ArrayBuffer(uint64Count * 8);
      this.data = new BigUint64Array(buffer, 0, uint64Count);
      // Initialize to all items to valid.
      for (let i = 0; i < this.data.length; i++) {
        this.data[i] = 0xffffffffffffffffn;
      }
    }
    if (this.data) {
      const bit = this.offset + itemIndex;
      const uint64Index = Math.floor(bit / 64);
      const uint64WithBitSet = BigInt(1) << BigInt(bit % 64);
      if (valid) {
        if ((this.data[uint64Index] & uint64WithBitSet) === 0n) {
          this.data[uint64Index] |= uint64WithBitSet;
        }
      } else {
        if ((this.data[uint64Index] & uint64WithBitSet) !== 0n) {
          this.data[uint64Index] &= ~uint64WithBitSet;
        }
      }
    }
  }
  public flush(vector: duckdb.Vector) {
    if (this.data) {
      duckdb.vector_ensure_validity_writable(vector);
      duckdb.copy_data_to_vector_validity(
        vector,
        0,
        this.data.buffer as ArrayBuffer,
        this.data.byteOffset,
        this.data.byteLength
      );
    }
  }
  public slice(offset: number, itemCount: number): DuckDBValidity {
    return new DuckDBValidity(this.data, this.offset + offset, itemCount);
  }
}
