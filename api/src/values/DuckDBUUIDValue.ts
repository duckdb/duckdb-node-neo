import { DuckDBUUIDType } from '../DuckDBType';

export class DuckDBUUIDValue {
  public readonly hugeint: bigint;

  public constructor(hugeint: bigint) {
    this.hugeint = hugeint;
  }

  public get type(): DuckDBUUIDType {
    return DuckDBUUIDType.instance;
  }
}
