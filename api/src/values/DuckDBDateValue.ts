import duckdb, { Date_, DateParts } from '@duckdb/node-bindings';
import { getDuckDBDateStringFromDays } from '../conversion/dateTimeStringConversion';

export type { DateParts };

export class DuckDBDateValue implements Date_ {
  public readonly days: number;

  public constructor(days: number) {
    this.days = days;
  }

  public get isFinite(): boolean {
    return duckdb.is_finite_date(this);
  }

  public toString(): string {
    return getDuckDBDateStringFromDays(this.days);
  }

  public toParts(): DateParts {
    return duckdb.from_date(this);
  }

  public static fromParts(parts: DateParts): DuckDBDateValue {
    return new DuckDBDateValue(duckdb.to_date(parts).days);
  }

  public static readonly Epoch = new DuckDBDateValue(0);

  public static readonly Max = new DuckDBDateValue(2 ** 31 - 2);
  public static readonly Min = new DuckDBDateValue(-(2 ** 31 - 2));

  public static readonly PosInf = new DuckDBDateValue(2 ** 31 - 1);
  public static readonly NegInf = new DuckDBDateValue(-(2 ** 31 - 1));
}

export function dateValue(daysOrParts: number | DateParts): DuckDBDateValue {
  if (typeof daysOrParts === 'number') {
    return new DuckDBDateValue(daysOrParts);
  }
  return DuckDBDateValue.fromParts(daysOrParts);
}
