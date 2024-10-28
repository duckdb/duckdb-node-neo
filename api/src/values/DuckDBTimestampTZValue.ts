import { DuckDBTimestampTZType } from '../DuckDBType';

export class DuckDBTimestampTZValue {
  public readonly micros: bigint;

  public constructor(micros: bigint) {
    this.micros = micros;
  }

  public get type(): DuckDBTimestampTZType {
    return DuckDBTimestampTZType.instance;
  }
}
