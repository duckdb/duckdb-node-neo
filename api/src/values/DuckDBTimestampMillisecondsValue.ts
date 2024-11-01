import { getDuckDBTimestampStringFromMilliseconds } from '../conversion/dateTimeStringConversion';
import { DuckDBTimestampSecondsValue } from './DuckDBTimestampSecondsValue';

export class DuckDBTimestampMillisecondsValue {
  public readonly milliseconds: bigint;

  public constructor(milliseconds: bigint) {
    this.milliseconds = milliseconds;
  }

  public toString(): string {
    return getDuckDBTimestampStringFromMilliseconds(this.milliseconds);
  }

  public static readonly Epoch = new DuckDBTimestampMillisecondsValue(0n);
  public static readonly Max = new DuckDBTimestampMillisecondsValue((2n ** 63n - 2n) / 1000n);
  public static readonly Min = new DuckDBTimestampMillisecondsValue(DuckDBTimestampSecondsValue.Min.seconds * 1000n);
}

export function timestampMillisValue(milliseconds: bigint): DuckDBTimestampMillisecondsValue {
  return new DuckDBTimestampMillisecondsValue(milliseconds);
}
