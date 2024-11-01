import { getDuckDBTimestampStringFromSeconds } from '../conversion/dateTimeStringConversion';

export class DuckDBTimestampSecondsValue {
  public readonly seconds: bigint;

  public constructor(seconds: bigint) {
    this.seconds = seconds;
  }

  public toString(): string {
    return getDuckDBTimestampStringFromSeconds(this.seconds);
  }

  public static readonly Epoch = new DuckDBTimestampSecondsValue(0n);
  public static readonly Max = new DuckDBTimestampSecondsValue( 9223372036854n);
  public static readonly Min = new DuckDBTimestampSecondsValue(-9223372022400n); // from test_all_types() select epoch(timestamp_s)::bigint;
}

export function timestampSecondsValue(seconds: bigint): DuckDBTimestampSecondsValue {
  return new DuckDBTimestampSecondsValue(seconds);
}
