import { DuckDBBlobType } from '../DuckDBType';

export class DuckDBBlobValue {
  public readonly bytes: Uint8Array;

  public constructor(bytes: Uint8Array) {
    this.bytes = bytes;
  }

  public get type(): DuckDBBlobType {
    return DuckDBBlobType.instance;
  }
}
