export class DuckDBUUIDValue {
  public readonly hugeint: bigint;

  private constructor(hugeint: bigint) {
    this.hugeint = hugeint;
  }

    /** Return the UUID as an unsigned 128-bit integer in a JS BigInt. */
  public toUint128(): bigint {
    // UUID values are stored with their MSB flipped so their numeric ordering matches their string ordering.
    return (this.hugeint ^ 0x80000000000000000000000000000000n) & 0xffffffffffffffffffffffffffffffffn;
  }

  public toString(): string {
    // Prepend with a (hex) 1 before converting to a hex string.
    // This ensures the trailing 32 characters are the hex digits we want, left padded with zeros as needed.
    const hex = (this.toUint128() | 0x100000000000000000000000000000000n).toString(16);
    return `${hex.substring(1, 9)}-${hex.substring(9, 13)}-${hex.substring(13, 17)}-${hex.substring(17, 21)}-${hex.substring(21, 33)}`;
  }

  /** Create a DuckDBUUIDValue from an unsigned 128-bit integer in a JS BigInt. */
  public static fromUint128(uint128: bigint): DuckDBUUIDValue {
    return new DuckDBUUIDValue((uint128 ^ 0x80000000000000000000000000000000n) & 0xffffffffffffffffffffffffffffffffn);
  }

  /**
   * Create a DuckDBUUIDValue from a HUGEINT as stored by DuckDB.
   *
   * UUID values are stored with their MSB flipped so their numeric ordering matches their string ordering.
   */
  public static fromStoredHugeInt(hugeint: bigint): DuckDBUUIDValue {
    return new DuckDBUUIDValue(hugeint);
  }

  public static readonly Max = new DuckDBUUIDValue(2n ** 127n - 1n); //  7fffffffffffffffffffffffffffffff
  public static readonly Min = new DuckDBUUIDValue(-(2n ** 127n)); //  80000000000000000000000000000000
}

/** Create a DuckDBUUIDValue from an unsigned 128-bit integer in a JS BigInt. */
export function uuidValue(uint128: bigint): DuckDBUUIDValue {
  return DuckDBUUIDValue.fromUint128(uint128);
}
