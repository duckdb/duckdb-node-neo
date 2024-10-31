import { Timestamp } from '@duckdb/node-bindings';
import { DuckDBTimestampMillisecondsValue } from './DuckDBTimestampMillisecondsValue';
import { getDuckDBTimestampStringFromMicroseconds } from '../conversion/dateTimeStringConversion';

export class DuckDBTimestampValue implements Timestamp {
  public readonly micros: bigint;

  public constructor(micros: bigint) {
    this.micros = micros;
  }

  public toString(): string {
    return getDuckDBTimestampStringFromMicroseconds(this.micros);
  }
  
  public static readonly Epoch = new DuckDBTimestampValue(0n);
  public static readonly Max = new DuckDBTimestampValue(2n ** 63n - 2n);
  public static readonly Min = new DuckDBTimestampValue(DuckDBTimestampMillisecondsValue.Min.milliseconds * 1000n);
  public static readonly PosInf = new DuckDBTimestampValue(2n ** 63n - 1n);
  public static readonly NegInf = new DuckDBTimestampValue(-(2n ** 63n - 1n));
}

export type DuckDBTimestampMicrosecondsValue = DuckDBTimestampValue;
export const DuckDBTimestampMicrosecondsValue = DuckDBTimestampValue;

export function timestampValue(micros: bigint): DuckDBTimestampValue {
  return new DuckDBTimestampValue(micros);
}
