import { getDuckDBTimestampStringFromNanoseconds } from '../conversion/dateTimeStringConversion';

export class DuckDBTimestampNanosecondsValue {
  public readonly nanoseconds: bigint;

  public constructor(nanoseconds: bigint) {
    this.nanoseconds = nanoseconds;
  }

  public toString(): string {
    return getDuckDBTimestampStringFromNanoseconds(this.nanoseconds);
  }

  public static readonly Epoch = new DuckDBTimestampNanosecondsValue(0n);
  public static readonly Max = new DuckDBTimestampNanosecondsValue(2n ** 63n - 2n);
  public static readonly Min = new DuckDBTimestampNanosecondsValue(-9223286400000000000n);
}

export function timestampNanosValue(nanoseconds: bigint): DuckDBTimestampNanosecondsValue {
  return new DuckDBTimestampNanosecondsValue(nanoseconds);
}
