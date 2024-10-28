import { Timestamp } from '@duckdb/node-bindings';
import { DuckDBTimestampType } from '../DuckDBType';

export class DuckDBTimestampValue implements Timestamp {
  public readonly micros: bigint;

  public constructor(micros: bigint) {
    this.micros = micros;
  }

  public get type(): DuckDBTimestampType {
    return DuckDBTimestampType.instance;
  }
}

export type DuckDBTimestampMicrosecondsValue = DuckDBTimestampValue;
export const DuckDBTimestampMicrosecondsValue = DuckDBTimestampValue;
