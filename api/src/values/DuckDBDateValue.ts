import { Date_ } from '@duckdb/node-bindings';
import { DuckDBDateType } from '../DuckDBType';

export class DuckDBDateValue implements Date_ {
  public readonly days: number;

  public constructor(days: number) {
    this.days = days;
  }

  public get type(): DuckDBDateType {
    return DuckDBDateType.instance;
  }
}
