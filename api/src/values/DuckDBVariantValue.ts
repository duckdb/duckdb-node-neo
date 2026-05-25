import { displayStringForDuckDBValue } from '../conversion/displayStringForDuckDBValue';
// Type-only import avoids a runtime cycle with ../DuckDBType, which itself
// pulls in the values barrel.
import type { DuckDBType } from '../DuckDBType';
import { DuckDBValue } from './DuckDBValue';

/**
 * Wraps a single decoded VARIANT row value. The wrapper distinguishes a
 * SQL-NULL row (returned as bare `null`) from an embedded `VARIANT_NULL`
 * tag (returned as `new DuckDBVariantValue(null)`).
 *
 * The optional `type` records the exact DuckDB type of the inner value
 * (recovered from the on-disk variant tag at decode time). The append /
 * bind paths use it so heterogeneous content round-trips faithfully —
 * `typeForValue` is a best-effort fallback when `type` is not provided.
 */
export class DuckDBVariantValue {
  public readonly value: DuckDBValue;
  public readonly type?: DuckDBType;
  public constructor(value: DuckDBValue, type?: DuckDBType) {
    this.value = value;
    this.type = type;
  }
  public toString(): string {
    return displayStringForDuckDBValue(this.value);
  }
}

export function variantValue(
  value: DuckDBValue,
  type?: DuckDBType,
): DuckDBVariantValue {
  return new DuckDBVariantValue(value, type);
}
