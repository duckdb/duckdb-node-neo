import duckdb, { TimestampNanoseconds } from '@duckdb/node-bindings';
import { getDuckDBTimestampStringFromNanoseconds } from '../conversion/dateTimeStringConversion';
import { DuckDBTimestampNanosecondsValue } from './DuckDBTimestampNanosecondsValue';
import { DuckDBTimestampTZValue } from './DuckDBTimestampTZValue';

export class DuckDBTimestampTZNanosecondsValue implements TimestampNanoseconds {
  public readonly nanos: bigint;

  public constructor(nanos: bigint) {
    this.nanos = nanos;
  }

  public get isFinite(): boolean {
    return duckdb.is_finite_timestamp_ns(this);
  }

  /** Formats in the time zone set by `DuckDBTimestampTZValue.timezoneOffsetInMinutes`. */
  public toString(): string {
    return getDuckDBTimestampStringFromNanoseconds(
      this.nanos,
      DuckDBTimestampTZValue.timezoneOffsetInMinutes
    );
  }

  public static readonly Epoch = new DuckDBTimestampTZNanosecondsValue(0n);
  public static readonly Max = new DuckDBTimestampTZNanosecondsValue(
    DuckDBTimestampNanosecondsValue.Max.nanos
  );
  public static readonly Min = new DuckDBTimestampTZNanosecondsValue(
    DuckDBTimestampNanosecondsValue.Min.nanos
  );
  public static readonly PosInf = new DuckDBTimestampTZNanosecondsValue(
    DuckDBTimestampNanosecondsValue.PosInf.nanos
  );
  public static readonly NegInf = new DuckDBTimestampTZNanosecondsValue(
    DuckDBTimestampNanosecondsValue.NegInf.nanos
  );
}

export function timestampTZNanosValue(
  nanos: bigint
): DuckDBTimestampTZNanosecondsValue {
  return new DuckDBTimestampTZNanosecondsValue(nanos);
}
