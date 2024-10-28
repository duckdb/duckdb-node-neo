import { Time } from '@duckdb/node-bindings';
import { DuckDBTimeType } from '../DuckDBType';

export class DuckDBTimeValue implements Time {
  public readonly micros: bigint;

  public constructor(micros: bigint) {
    this.micros = micros;
  }

  public get type(): DuckDBTimeType {
    return DuckDBTimeType.instance;
  }
}
