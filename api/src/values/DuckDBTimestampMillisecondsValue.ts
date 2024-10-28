import { DuckDBTimestampMillisecondsType } from '../DuckDBType';

export class DuckDBTimestampMillisecondsValue {
  public readonly milliseconds: bigint;

  public constructor(milliseconds: bigint) {
    this.milliseconds = milliseconds;
  }

  public get type(): DuckDBTimestampMillisecondsType {
    return DuckDBTimestampMillisecondsType.instance;
  }
}
