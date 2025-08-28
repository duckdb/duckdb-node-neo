import { Interval } from '@databrainhq/node-bindings';
import { getDuckDBIntervalString } from '../conversion/dateTimeStringConversion';

export class DuckDBIntervalValue implements Interval {
  public readonly months: number;
  public readonly days: number;
  public readonly micros: bigint;

  public constructor(months: number, days: number, micros: bigint) {
    this.months = months;
    this.days = days;
    this.micros = micros;
  }

  public toString(): string {
    return getDuckDBIntervalString(this.months, this.days, this.micros);
  }
}

export function intervalValue(
  months: number,
  days: number,
  micros: bigint,
): DuckDBIntervalValue {
  return new DuckDBIntervalValue(months, days, micros);
}
