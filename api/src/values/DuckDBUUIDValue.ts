export class DuckDBUUIDValue {
  public readonly hugeint: bigint;

  public constructor(hugeint: bigint) {
    this.hugeint = hugeint;
  }

  public toString(): string {
    // Truncate to 32 hex digits, then prepend with a hex 1, before converting to a hex string.
    // This ensures the trailing 32 characters are the hex digits we want, left padded with zeros as needed.
    const hex = ((this.hugeint & 0xffffffffffffffffffffffffffffffffn) | 0x100000000000000000000000000000000n).toString(16);
    return `${hex.substring(1, 9)}-${hex.substring(9, 13)}-${hex.substring(13, 17)}-${hex.substring(17, 21)}-${hex.substring(21, 33)}`;
  }

  public static readonly Max = new DuckDBUUIDValue(2n ** 127n - 1n);
  public static readonly Min = new DuckDBUUIDValue(-(2n ** 127n));
}

export function uuidValue(hugeint: bigint): DuckDBUUIDValue {
  return new DuckDBUUIDValue(hugeint);
}
