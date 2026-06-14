import duckdb from '@duckdb/node-bindings';

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
  private readonly offset: number;
  private readonly itemCount: number;
  private constructor(
    data: BigUint64Array | null,
    offset: number,
    itemCount: number
  ) {
    this.data = data;
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
  public itemValid(itemIndex: number): boolean {
    if (!this.data) {
      return true;
    }
    const bit = this.offset + itemIndex;
    return (
      (this.data[Math.floor(bit / 64)] & (BigInt(1) << BigInt(bit % 64))) !==
      BigInt(0)
    );
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
