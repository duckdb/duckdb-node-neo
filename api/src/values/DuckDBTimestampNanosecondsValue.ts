import { DuckDBTimestampNanosecondsType } from '../DuckDBType';

export class DuckDBTimestampNanosecondsValue {
  public readonly nanoseconds: bigint;

  public constructor(nanoseconds: bigint) {
    this.nanoseconds = nanoseconds;
  }

  public get type(): DuckDBTimestampNanosecondsType {
    return DuckDBTimestampNanosecondsType.instance;
  }
}
