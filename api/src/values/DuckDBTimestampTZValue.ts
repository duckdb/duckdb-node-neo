import { getDuckDBTimestampStringFromMicroseconds } from '../conversion/dateTimeStringConversion';
import { DuckDBTimestampValue } from './DuckDBTimestampValue';

export class DuckDBTimestampTZValue {
  public readonly micros: bigint;

  public constructor(micros: bigint) {
    this.micros = micros;
  }

  public toString(): string {
    // TODO: adjust micros for local timezone offset, and pass in timezone string
    return getDuckDBTimestampStringFromMicroseconds(this.micros);
  }

  public static readonly Epoch = new DuckDBTimestampTZValue(0n);
  public static readonly Max = new DuckDBTimestampTZValue(DuckDBTimestampValue.Max.micros);
  public static readonly Min = new DuckDBTimestampTZValue(DuckDBTimestampValue.Min.micros);
  public static readonly PosInf = new DuckDBTimestampTZValue(DuckDBTimestampValue.PosInf.micros);
  public static readonly NegInf = new DuckDBTimestampTZValue(DuckDBTimestampValue.NegInf.micros);
}

export function timestampTZValue(micros: bigint): DuckDBTimestampTZValue {
  return new DuckDBTimestampTZValue(micros);
}
