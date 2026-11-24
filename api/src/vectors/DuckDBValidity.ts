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

/**
 * A validity bit mask, shared by a vector and its slices so that a NULL set
 * through either is visible to both. `data` is null while every row is valid.
 */
interface ValidityMask {
  data: BigUint64Array | null;
  /** Row count of the vector the mask covers, slices included. */
  readonly itemCount: number;
}

export class DuckDBValidity {
  private readonly mask: ValidityMask;
  private readonly offset: number;
  private constructor(mask: ValidityMask, offset: number) {
    this.mask = mask;
    this.offset = offset;
  }
  public static fromVector(
    vector: duckdb.Vector,
    itemCount: number
  ): DuckDBValidity {
    const uint64Count = Math.ceil(itemCount / 64);
    const bytes = duckdb.vector_get_validity(vector, uint64Count * 8);
    const data = bytes
      ? new BigUint64Array(bytes.buffer, bytes.byteOffset, uint64Count)
      : null;
    return new DuckDBValidity({ data, itemCount }, 0);
  }
  public itemValid(itemIndex: number): boolean {
    const { data } = this.mask;
    if (!data) {
      return true;
    }
    const bit = this.offset + itemIndex;
    return (
      (data[Math.floor(bit / 64)] & (BigInt(1) << BigInt(bit % 64))) !==
      BigInt(0)
    );
  }
  public setItemValid(itemIndex: number, valid: boolean) {
    if (!this.mask.data && !valid) {
      // An item is being set to invalid and we don't have a data buffer, so create it, sized for the whole mask rather
      // than this slice of it. If an item is being set to valid and we don't have a data buffer, then there's nothing to
      // do; a null data buffer indicates all items are valid.
      const uint64Count = Math.ceil(this.mask.itemCount / 64);
      const buffer = new ArrayBuffer(uint64Count * 8);
      this.mask.data = new BigUint64Array(buffer, 0, uint64Count);
      // Initialize to all items to valid.
      this.mask.data.fill(0xffffffffffffffffn);
    }
    const { data } = this.mask;
    if (data) {
      const bit = this.offset + itemIndex;
      const uint64Index = Math.floor(bit / 64);
      const uint64WithBitSet = BigInt(1) << BigInt(bit % 64);
      if (valid) {
        if ((data[uint64Index] & uint64WithBitSet) === 0n) {
          data[uint64Index] |= uint64WithBitSet;
        }
      } else {
        if ((data[uint64Index] & uint64WithBitSet) !== 0n) {
          data[uint64Index] &= ~uint64WithBitSet;
        }
      }
    }
  }
  /**
   * Writes the whole mask, not just this slice's rows: rows are bits, so a
   * slice's rows need not start or end on a byte boundary.
   */
  public flush(vector: duckdb.Vector) {
    const { data } = this.mask;
    if (data) {
      duckdb.vector_ensure_validity_writable(vector);
      duckdb.copy_data_to_vector_validity(
        vector,
        0,
        data.buffer as ArrayBuffer,
        data.byteOffset,
        data.byteLength
      );
    }
  }
  public slice(offset: number): DuckDBValidity {
    return new DuckDBValidity(this.mask, this.offset + offset);
  }
}
