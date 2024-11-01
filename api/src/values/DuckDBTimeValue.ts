import { Time } from '@duckdb/node-bindings';
import { getDuckDBTimeStringFromMicrosecondsInDay } from '../conversion/dateTimeStringConversion';

export class DuckDBTimeValue implements Time {
  public readonly micros: bigint;

  public constructor(micros: bigint) {
    this.micros = micros;
  }

  public toString(): string {
    return getDuckDBTimeStringFromMicrosecondsInDay(this.micros);
  }

  public static readonly Max = new DuckDBTimeValue(24n * 60n * 60n * 1000n * 1000n);
  public static readonly Min = new DuckDBTimeValue(0n);
}

export function timeValue(micros: bigint): DuckDBTimeValue {
  return new DuckDBTimeValue(micros);
}
