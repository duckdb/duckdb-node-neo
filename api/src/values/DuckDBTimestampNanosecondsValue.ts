import duckdb, { TimestampNanoseconds } from '@duckdb/node-bindings';
import { getDuckDBTimestampStringFromNanoseconds } from '../conversion/dateTimeStringConversion';

export class DuckDBTimestampNanosecondsValue implements TimestampNanoseconds {
  public readonly nanos: bigint;

  public constructor(nanos: bigint) {
    this.nanos = nanos;
  }

  public get isFinite(): boolean {
    return duckdb.is_finite_timestamp_ns(this);
  }

  public toString(): string {
    return getDuckDBTimestampStringFromNanoseconds(this.nanos);
  }

  public static readonly Epoch = new DuckDBTimestampNanosecondsValue(0n);
  public static readonly Max = new DuckDBTimestampNanosecondsValue(2n ** 63n - 2n);
  public static readonly Min = new DuckDBTimestampNanosecondsValue(-9223286400000000000n);
  public static readonly PosInf = new DuckDBTimestampNanosecondsValue(2n ** 63n - 1n);
  public static readonly NegInf = new DuckDBTimestampNanosecondsValue(-(2n ** 63n - 1n));
}

export function timestampNanosValue(nanos: bigint): DuckDBTimestampNanosecondsValue {
  return new DuckDBTimestampNanosecondsValue(nanos);
}
