import { Decimal } from '@duckdb/node-bindings';
import { stringFromDecimal } from '../conversion/stringFromDecimal';

export class DuckDBDecimalValue<T extends number | bigint = number | bigint> implements Decimal {
  public readonly width: number;
  public readonly scale: number;
  public readonly scaledValue: T;

  public constructor(width: number, scale: number, scaledValue: T) {
    this.width = width;
    this.scale = scale;
    this.scaledValue = scaledValue;
  }

  get value(): bigint {
    return BigInt(this.scaledValue);
  }

  public toString(): string {
    return stringFromDecimal(this.value, this.scale);
  }
}

export function decimalValue<T extends number | bigint = number | bigint>(
  width: number,
  scale: number,
  scaledValue: T
): DuckDBDecimalValue<T> {
  return new DuckDBDecimalValue(width, scale, scaledValue);
}

export function decimalNumber(
  width: number,
  scale: number,
  scaledValue: number
): DuckDBDecimalValue<number> {
  return new DuckDBDecimalValue(width, scale, scaledValue);
}

export function decimalBigint(
  width: number,
  scale: number,
  scaledValue: bigint
): DuckDBDecimalValue<bigint> {
  return new DuckDBDecimalValue(width, scale, scaledValue);
}
