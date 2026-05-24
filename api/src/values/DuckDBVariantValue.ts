import { displayStringForDuckDBValue } from '../conversion/displayStringForDuckDBValue';
import { DuckDBValue } from './DuckDBValue';

/**
 * Wraps a single decoded VARIANT row value. The wrapper distinguishes a
 * SQL-NULL row (returned as bare `null`) from an embedded `VARIANT_NULL`
 * tag (returned as `new DuckDBVariantValue(null)`).
 */
export class DuckDBVariantValue {
  public readonly value: DuckDBValue;
  public constructor(value: DuckDBValue) {
    this.value = value;
  }
  public toString(): string {
    return displayStringForDuckDBValue(this.value);
  }
}

export function variantValue(value: DuckDBValue): DuckDBVariantValue {
  return new DuckDBVariantValue(value);
}
