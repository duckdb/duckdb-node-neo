import duckdb, { Timestamp, TimestampParts } from '@duckdb/node-bindings';
import { getDuckDBTimestampStringFromMicroseconds } from '../conversion/dateTimeStringConversion';
import { DuckDBTimestampValue } from './DuckDBTimestampValue';

export class DuckDBTimestampTZValue implements Timestamp {
  public static timezoneOffsetInMinutes: number =
    -new Date().getTimezoneOffset();

  public readonly micros: bigint;

  public constructor(micros: bigint) {
    this.micros = micros;
  }

  public get isFinite(): boolean {
    return duckdb.is_finite_timestamp(this);
  }

  public toString(): string {
    return getDuckDBTimestampStringFromMicroseconds(
      this.micros,
      DuckDBTimestampTZValue.timezoneOffsetInMinutes
    );
  }

  public toParts(): TimestampParts {
    return duckdb.from_timestamp(this);
  }

  public static fromParts(parts: TimestampParts): DuckDBTimestampTZValue {
    return new DuckDBTimestampTZValue(duckdb.to_timestamp(parts).micros);
  }

  public static readonly Epoch = new DuckDBTimestampTZValue(0n);
  public static readonly Max = new DuckDBTimestampTZValue(
    DuckDBTimestampValue.Max.micros
  );
  public static readonly Min = new DuckDBTimestampTZValue(
    DuckDBTimestampValue.Min.micros
  );
  public static readonly PosInf = new DuckDBTimestampTZValue(
    DuckDBTimestampValue.PosInf.micros
  );
  public static readonly NegInf = new DuckDBTimestampTZValue(
    DuckDBTimestampValue.NegInf.micros
  );
}

export function timestampTZValue(microsOrParts: bigint | TimestampParts): DuckDBTimestampTZValue {
  if (typeof microsOrParts === 'bigint') {
    return new DuckDBTimestampTZValue(microsOrParts);
  }
  return DuckDBTimestampTZValue.fromParts(microsOrParts);
}
