import { Date_ } from '@duckdb/node-bindings';
import { getDuckDBDateStringFromDays } from '../conversion/dateTimeStringConversion';

export class DuckDBDateValue implements Date_ {
  public readonly days: number;

  public constructor(days: number) {
    this.days = days;
  }

  public toString(): string {
    return getDuckDBDateStringFromDays(this.days);
  }

  public static readonly Epoch = new DuckDBDateValue(0);

  public static readonly Max = new DuckDBDateValue(2 ** 31 - 2);
  public static readonly Min = new DuckDBDateValue(-(2 ** 31 - 2));

  public static readonly PosInf = new DuckDBDateValue(2 ** 31 - 1);
  public static readonly NegInf = new DuckDBDateValue(-(2 ** 31 - 1));
}

export function dateValue(days: number): DuckDBDateValue {
  return new DuckDBDateValue(days);
}
