import duckdb, { Time, TimeParts } from '@databrainhq/node-bindings';
import { getDuckDBTimeStringFromMicrosecondsInDay } from '../conversion/dateTimeStringConversion';

export type { TimeParts };

export class DuckDBTimeValue implements Time {
  public readonly micros: bigint;

  public constructor(micros: bigint) {
    this.micros = micros;
  }

  public toString(): string {
    return getDuckDBTimeStringFromMicrosecondsInDay(this.micros);
  }

  public toParts(): TimeParts {
    return duckdb.from_time(this);
  }

  public static fromParts(parts: TimeParts): DuckDBTimeValue {
    return new DuckDBTimeValue(duckdb.to_time(parts).micros);
  }

  public static readonly Max = new DuckDBTimeValue(
    24n * 60n * 60n * 1000n * 1000n,
  );
  public static readonly Min = new DuckDBTimeValue(0n);
}

export function timeValue(microsOrParts: bigint | TimeParts): DuckDBTimeValue {
  if (typeof microsOrParts === 'bigint') {
    return new DuckDBTimeValue(microsOrParts);
  }
  return DuckDBTimeValue.fromParts(microsOrParts);
}
