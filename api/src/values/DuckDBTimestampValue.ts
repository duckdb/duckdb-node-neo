import duckdb, { Timestamp, TimestampParts } from '@databrainhq/node-bindings';
import { getDuckDBTimestampStringFromMicroseconds } from '../conversion/dateTimeStringConversion';
import { DuckDBTimestampMillisecondsValue } from './DuckDBTimestampMillisecondsValue';

export type { TimestampParts };

export class DuckDBTimestampValue implements Timestamp {
  public readonly micros: bigint;

  public constructor(micros: bigint) {
    this.micros = micros;
  }

  public get isFinite(): boolean {
    return duckdb.is_finite_timestamp(this);
  }

  public toString(): string {
    return getDuckDBTimestampStringFromMicroseconds(this.micros);
  }

  public toParts(): TimestampParts {
    return duckdb.from_timestamp(this);
  }

  public static fromParts(parts: TimestampParts): DuckDBTimestampValue {
    return new DuckDBTimestampValue(duckdb.to_timestamp(parts).micros);
  }

  public static readonly Epoch = new DuckDBTimestampValue(0n);
  public static readonly Max = new DuckDBTimestampValue(2n ** 63n - 2n);
  public static readonly Min = new DuckDBTimestampValue(
    DuckDBTimestampMillisecondsValue.Min.millis * 1000n,
  );
  public static readonly PosInf = new DuckDBTimestampValue(2n ** 63n - 1n);
  public static readonly NegInf = new DuckDBTimestampValue(-(2n ** 63n - 1n));
}

export type DuckDBTimestampMicrosecondsValue = DuckDBTimestampValue;
export const DuckDBTimestampMicrosecondsValue = DuckDBTimestampValue;

export function timestampValue(
  microsOrParts: bigint | TimestampParts,
): DuckDBTimestampValue {
  if (typeof microsOrParts === 'bigint') {
    return new DuckDBTimestampValue(microsOrParts);
  }
  return DuckDBTimestampValue.fromParts(microsOrParts);
}
