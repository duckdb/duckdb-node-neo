import duckdb, { TimestampMilliseconds } from '@duckdb/node-bindings';
import { getDuckDBTimestampStringFromMilliseconds } from '../conversion/dateTimeStringConversion';
import { DuckDBTimestampSecondsValue } from './DuckDBTimestampSecondsValue';

export class DuckDBTimestampMillisecondsValue implements TimestampMilliseconds {
  public readonly millis: bigint;

  public constructor(millis: bigint) {
    this.millis = millis;
  }

  public get isFinite(): boolean {
    return duckdb.is_finite_timestamp_ms(this);
  }

  public toString(): string {
    return getDuckDBTimestampStringFromMilliseconds(this.millis);
  }

  public static readonly Epoch = new DuckDBTimestampMillisecondsValue(0n);
  public static readonly Max = new DuckDBTimestampMillisecondsValue((2n ** 63n - 2n) / 1000n);
  public static readonly Min = new DuckDBTimestampMillisecondsValue(DuckDBTimestampSecondsValue.Min.seconds * 1000n);
  public static readonly PosInf = new DuckDBTimestampMillisecondsValue(2n ** 63n - 1n);
  public static readonly NegInf = new DuckDBTimestampMillisecondsValue(-(2n ** 63n - 1n));
}

export function timestampMillisValue(millis: bigint): DuckDBTimestampMillisecondsValue {
  return new DuckDBTimestampMillisecondsValue(millis);
}
