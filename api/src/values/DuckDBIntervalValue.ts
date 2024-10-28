import { Interval } from '@duckdb/node-bindings';
import { DuckDBIntervalType } from '../DuckDBType';

export class DuckDBIntervalValue implements Interval {
  public readonly months: number;
  public readonly days: number;
  public readonly micros: bigint;

  public constructor(months: number, days: number, micros: bigint) {
    this.months = months;
    this.days = days;
    this.micros = micros;
  }

  public get type(): DuckDBIntervalType {
    return DuckDBIntervalType.instance;
  }
}
