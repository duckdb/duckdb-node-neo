import { DuckDBTimestampSecondsType } from '../DuckDBType';

export class DuckDBTimestampSecondsValue {
  public readonly seconds: bigint;

  public constructor(seconds: bigint) {
    this.seconds = seconds;
  }

  public get type(): DuckDBTimestampSecondsType {
    return DuckDBTimestampSecondsType.instance;
  }
}
